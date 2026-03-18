#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline.py — Update raw sources + rebuild processed dataset.

Two modes:
- LOCAL (recommended): download CORDIS zips, then rebuild data/processed/subsidy_base.{csv,parquet}
- STREAMLIT CLOUD: never downloads big sources (avoids "Oh no"). Only checks that processed parquet exists.

Author: Charlotte Crocicchia (rewritten & hardened)
"""

from __future__ import annotations

import io
import json
import os
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import requests

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROC_DIR = BASE_DIR / "data" / "processed"

STATE_PATH = PROC_DIR / "_state.json"
LOCK_PATH = PROC_DIR / "_build.lock"

OUT_CSV = PROC_DIR / "subsidy_base.csv"
OUT_PARQUET = PROC_DIR / "subsidy_base.parquet"

HEADERS = {"User-Agent": "SubsidyRadar/1.0 (+mission)"}

# CORDIS (official bulk zips)
CORDIS_URLS = {
    "horizon_europe": "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip",
    "h2020": "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip",
}

# ============================
# Environment detection
# ============================
def is_streamlit_cloud() -> bool:
    """
    Streamlit Community Cloud containers typically expose these env vars.
    We keep detection conservative.
    """
    # common: running Streamlit headless in cloud
    if os.getenv("STREAMLIT_SERVER_HEADLESS") == "true":
        return True
    # some deployments set STREAMLIT_RUNTIME or similar
    if os.getenv("STREAMLIT_RUNTIME"):
        return True
    if os.getenv("STREAMLIT_SHARING_MODE"):
        return True
    if os.getenv("IS_STREAMLIT_CLOUD") == "1":
        return True
    # Strong fallback for Streamlit Community Cloud mounts.
    base = str(BASE_DIR).replace("\\", "/").lower()
    if base.startswith("/mount/src/") or ("/mount/src/" in base):
        return True
    # fallback: user can force via env
    if os.getenv("SUBSIDY_RADAR_CLOUD") == "1":
        return True
    return False


# ============================
# Lock + state
# ============================
def _read_state() -> Dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _write_state(d: Dict) -> None:
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(d, indent=2), encoding="utf-8")


def _acquire_lock(timeout_sec: int = 600) -> None:
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    while LOCK_PATH.exists():
        if time.time() - t0 > timeout_sec:
            raise RuntimeError("Build lock timeout (another process may be stuck).")
        time.sleep(0.25)
    LOCK_PATH.write_text(str(time.time()), encoding="utf-8")


def _release_lock() -> None:
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


# ============================
# Stamps (cheap “did it change?”)
# ============================
def _http_stamp(url: str) -> str:
    """
    Returns a string stamp from HEAD (ETag / Last-Modified / Content-Length).
    If HEAD fails, falls back to GET headers-only.
    """
    try:
        r = requests.head(url, timeout=30, headers=HEADERS, allow_redirects=True)
        if r.status_code >= 400:
            raise RuntimeError(f"HEAD {r.status_code}")
        etag = r.headers.get("ETag", "")
        lm = r.headers.get("Last-Modified", "")
        cl = r.headers.get("Content-Length", "")
        return f"etag={etag}|lm={lm}|cl={cl}"
    except Exception:
        try:
            r = requests.get(url, timeout=30, headers=HEADERS, stream=True, allow_redirects=True)
            r.raise_for_status()
            etag = r.headers.get("ETag", "")
            lm = r.headers.get("Last-Modified", "")
            cl = r.headers.get("Content-Length", "")
            return f"etag={etag}|lm={lm}|cl={cl}"
        except Exception:
            return ""

# ============================
# Download helpers
# ============================
def _download_and_extract_zip(url: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=600, headers=HEADERS)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(out_dir)


def _download_stream(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=600, headers=HEADERS, allow_redirects=True) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def _parquet_columns(path: Path) -> list:
    if not path.exists():
        return []
    try:
        import duckdb

        con = duckdb.connect(database=":memory:")
        p = path.as_posix().replace("'", "''")
        df = con.execute(f"SELECT * FROM read_parquet('{p}') LIMIT 0").fetchdf()
        return [str(c) for c in df.columns]
    except Exception:
        try:
            import pyarrow.parquet as pq

            return [str(c) for c in pq.ParquetFile(path).schema.names]
        except Exception:
            return []


# ============================
# Public API
# ============================
@dataclass
class UpdateResult:
    rebuilt: bool
    reason: str


def ensure_data_updated(force: bool = False, verbose: bool = False) -> UpdateResult:
    """
    LOCAL:
      checks remote stamps; downloads raw + rebuilds processed dataset if needed.
    STREAMLIT CLOUD:
      never downloads big sources; only checks parquet exists (and rebuilds from local raw if present).

    The "Refresh" button in app can call this incrementally (force=False by default).
    Set force=True only for a full rebuild.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROC_DIR.mkdir(parents=True, exist_ok=True)

    cloud = is_streamlit_cloud()

    # Cloud mode: no downloads
    if cloud:
        if OUT_PARQUET.exists():
            return UpdateResult(rebuilt=False, reason="cloud_mode:parquet_present")
        return UpdateResult(
            rebuilt=False,
            reason="cloud_mode:missing_parquet (generate locally then commit/push data/processed/subsidy_base.parquet)",
        )

    state = _read_state()

    # Compute stamps
    cordis_stamps = {k: _http_stamp(url) for k, url in CORDIS_URLS.items()}
    need_core = force or (not OUT_PARQUET.exists()) or (not OUT_CSV.exists())
    reasons = []
    if force:
        reasons.append("forced")
    if not OUT_PARQUET.exists():
        reasons.append("missing_processed_parquet")
    if not OUT_CSV.exists():
        reasons.append("missing_processed_csv")
    if OUT_PARQUET.exists():
        cols = set(_parquet_columns(OUT_PARQUET))
        required = {"pic", "value_chain_stage", "project_status"}
        if not required.issubset(cols):
            need_core = True
            reasons.append("missing_schema_cols")

    # Compare stored stamps
    prev = state.get("stamps", {})
    if not need_core:
        for k, s in cordis_stamps.items():
            if s and prev.get(f"cordis_{k}") != s:
                need_core = True
                reasons.append(f"cordis_changed:{k}")
    connectors_manifest = BASE_DIR / "data" / "external" / "connectors_manifest.csv"
    need_connectors = connectors_manifest.exists()

    if not need_core and not need_connectors:
        return UpdateResult(rebuilt=False, reason="up_to_date")

    _acquire_lock()
    try:
        ran_connectors = False
        connectors_updated = False
        # Optional external connectors (API/MCP), incremental via data/external/connectors_manifest.csv
        if need_connectors:
            try:
                from incremental_connectors import run_incremental_connectors

                state, connector_results = run_incremental_connectors(BASE_DIR, state=state, force=force, verbose=verbose)
                ran_connectors = len(connector_results) > 0
                connectors_updated = any((r.ok and r.ran) for r in connector_results)
                if connectors_updated:
                    reasons.append("external_connectors_updated")
            except Exception as e:
                if verbose:
                    print(f"[pipeline][WARN] connectors step failed: {e}")

        if need_core:
            # Download raw
            for name, url in CORDIS_URLS.items():
                if verbose:
                    print(f"[pipeline] Download CORDIS {name}")
                _download_and_extract_zip(url, RAW_DIR / "cordis" / name)

            # Build processed
            if verbose:
                print("[pipeline] Build processed dataset")
            from process_build import build_processed_dataset

            build_processed_dataset(raw_dir=RAW_DIR, out_csv=OUT_CSV)

            # Update core data state only when rebuild happened
            state["stamps"] = {
                **{f"cordis_{k}": v for k, v in cordis_stamps.items()},
            }
            state["last_build_ts"] = time.time()

        if ran_connectors and not need_core:
            state["last_connectors_ts"] = time.time()

        _write_state(state)

        rebuilt_any = bool(need_core or connectors_updated)
        if not rebuilt_any and need_connectors and not need_core:
            return UpdateResult(rebuilt=False, reason="connectors_checked_no_update")
        return UpdateResult(rebuilt=rebuilt_any, reason=";".join(reasons) if reasons else ("rebuild" if need_core else "connectors_updated"))
    finally:
        _release_lock()


def main() -> None:
    # Incremental by default. Set SUBSIDY_FORCE_REBUILD=1 for a full forced refresh.
    force = os.getenv("SUBSIDY_FORCE_REBUILD", "0") == "1"
    res = ensure_data_updated(force=force, verbose=True)
    print(f"[OK] rebuilt={res.rebuilt} reason={res.reason} out={OUT_PARQUET}")


if __name__ == "__main__":
    main()
