#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline.py — Update raw sources + rebuild processed dataset.

Two modes:
- LOCAL (recommended): download CORDIS zips + ADEME csv, then rebuild data/processed/subsidy_base.{csv,parquet}
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
from typing import Dict, Optional, Tuple

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

# ADEME dataset on data.gouv.fr (dataset id stable)
ADEME_DATASET_API = "https://www.data.gouv.fr/api/1/datasets/640afdff7a07961cdc232d19/"


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


def _pick_best_csv_resource(resources: list) -> Optional[dict]:
    csvs = []
    for r in resources:
        fmt = str(r.get("format", "")).lower()
        mime = str(r.get("mime", "")).lower()
        title = str(r.get("title", "")).lower()
        if fmt == "csv" or "csv" in mime or title.endswith(".csv") or " csv" in title:
            csvs.append(r)
    if not csvs:
        return None

    # prefer latest non-doc
    def key(r: dict) -> Tuple[int, str]:
        title = str(r.get("title", "")).lower()
        is_doc = int(("swagger" in title) or ("documentation" in title) or ("api" in title))
        last = str(r.get("last_modified") or r.get("created_at") or "")
        return (is_doc, last)

    csvs.sort(key=key)
    return csvs[-1]


def _ademe_url_and_stamp() -> Tuple[Optional[str], str]:
    try:
        js = requests.get(ADEME_DATASET_API, timeout=60, headers=HEADERS).json()
        r = _pick_best_csv_resource(js.get("resources", []))
        if not r:
            return None, ""
        url = r.get("url") or (r.get("latest") or {}).get("url")
        stamp = str(r.get("last_modified") or r.get("created_at") or "")
        return url, stamp
    except Exception:
        return None, ""


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

    The "Refresh" button in app should call this with force=True on LOCAL machines.
    On Cloud, do NOT call it to download; use build_events.py only.
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
    ademe_url, ademe_stamp = _ademe_url_and_stamp()

    need = force or (not OUT_PARQUET.exists()) or (not OUT_CSV.exists())
    reasons = []
    if force:
        reasons.append("forced")
    if not OUT_PARQUET.exists():
        reasons.append("missing_processed_parquet")
    if not OUT_CSV.exists():
        reasons.append("missing_processed_csv")

    # Compare stored stamps
    prev = state.get("stamps", {})
    if not need:
        for k, s in cordis_stamps.items():
            if s and prev.get(f"cordis_{k}") != s:
                need = True
                reasons.append(f"cordis_changed:{k}")
        if ademe_stamp and prev.get("ademe_stamp") != ademe_stamp:
            need = True
            reasons.append("ademe_changed")

    if not need:
        return UpdateResult(rebuilt=False, reason="up_to_date")

    _acquire_lock()
    try:
        # Download raw
        for name, url in CORDIS_URLS.items():
            if verbose:
                print(f"[pipeline] Download CORDIS {name}")
            _download_and_extract_zip(url, RAW_DIR / "cordis" / name)

        # ADEME optional
        if ademe_url:
            if verbose:
                print("[pipeline] Download ADEME CSV")
            _download_stream(ademe_url, RAW_DIR / "france" / "ademe" / "ademe_aides_full.csv")

        # Build processed
        if verbose:
            print("[pipeline] Build processed dataset")
        from process_build import build_processed_dataset

        build_processed_dataset(raw_dir=RAW_DIR, out_csv=OUT_CSV)

        # Update state
        state["stamps"] = {
            **{f"cordis_{k}": v for k, v in cordis_stamps.items()},
            "ademe_stamp": ademe_stamp,
        }
        state["last_build_ts"] = time.time()
        _write_state(state)

        return UpdateResult(rebuilt=True, reason=";".join(reasons) if reasons else "rebuild")
    finally:
        _release_lock()


def main() -> None:
    # On your Mac: use force=True to actually update and rebuild everything.
    res = ensure_data_updated(force=True, verbose=True)
    print(f"[OK] rebuilt={res.rebuilt} reason={res.reason} out={OUT_PARQUET}")


if __name__ == "__main__":
    main()