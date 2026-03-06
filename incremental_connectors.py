#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
incremental_connectors.py — Optional incremental ingestion for external APIs/MCP.

This module is intentionally generic and safe-by-default:
- no manifest => no-op
- per-connector isolation (errors do not stop all connectors)
- incremental behavior based on remote stamp + local state
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

DEFAULT_TIMEOUT = 90
DEFAULT_HEADERS = {"User-Agent": "SubsidyRadar/1.0 (+external-connectors)"}
ENV_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass
class ConnectorResult:
    connector_id: str
    ran: bool
    ok: bool
    reason: str
    output_file: str
    stamp: str


def _to_bool(x: Any) -> bool:
    return str(x).strip().lower() in {"1", "true", "yes", "y", "oui"}


def _is_placeholder_url(url: str) -> bool:
    u = str(url or "").strip().lower()
    return ("example." in u) or ("localhost" in u)


def _extract_env_refs(*texts: Any) -> List[str]:
    refs: List[str] = []
    for t in texts:
        s = str(t or "")
        refs.extend(ENV_REF_RE.findall(s))
    return sorted(set([x for x in refs if str(x).strip()]))


def _required_env_from_row(row: pd.Series) -> List[str]:
    raw_required = str(row.get("required_env", "")).strip()
    listed = [x.strip() for x in raw_required.split(",") if x.strip()]
    inferred = _extract_env_refs(
        row.get("url", ""),
        row.get("headers_json", ""),
        row.get("params_json", ""),
        row.get("mcp_command", ""),
    )
    return sorted(set(listed + inferred))


def _json_or_empty(s: Any) -> Dict[str, Any]:
    txt = str(s or "").strip()
    if not txt:
        return {}
    try:
        js = json.loads(txt)
        return js if isinstance(js, dict) else {}
    except Exception:
        return {}


def _expand_env(v: Any) -> Any:
    """
    Expands $VAR / ${VAR} placeholders in strings recursively.
    """
    if isinstance(v, str):
        return os.path.expandvars(v)
    if isinstance(v, dict):
        return {k: _expand_env(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_expand_env(x) for x in v]
    return v


def _http_stamp(url: str, headers: Dict[str, str]) -> str:
    try:
        r = requests.head(url, timeout=30, headers=headers, allow_redirects=True)
        if r.status_code >= 400:
            raise RuntimeError(f"HEAD {r.status_code}")
        etag = r.headers.get("ETag", "")
        lm = r.headers.get("Last-Modified", "")
        cl = r.headers.get("Content-Length", "")
        return f"etag={etag}|lm={lm}|cl={cl}"
    except Exception:
        return ""


def _write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(payload)
    tmp.replace(path)


def _run_api_connector(row: pd.Series, prev: Dict[str, Any], force: bool) -> ConnectorResult:
    cid = str(row.get("connector_id", "")).strip() or "unnamed"
    kind = str(row.get("kind", "api_json")).strip().lower()
    url = _expand_env(str(row.get("url", "")).strip())
    output_file = _expand_env(str(row.get("output_file", "")).strip() or f"external/{cid}.json")
    headers = DEFAULT_HEADERS.copy()
    headers.update(_expand_env(_json_or_empty(row.get("headers_json", ""))))
    params = _expand_env(_json_or_empty(row.get("params_json", "")))
    method = _expand_env(str(row.get("method", "GET")).strip().upper())

    if not url:
        return ConnectorResult(cid, ran=False, ok=False, reason="missing_url", output_file=output_file, stamp="")

    remote_stamp = _http_stamp(url, headers=headers)
    prev_stamp = str(prev.get("stamp", ""))
    need = bool(force) or (not remote_stamp) or (remote_stamp != prev_stamp)
    if not need:
        return ConnectorResult(cid, ran=False, ok=True, reason="up_to_date", output_file=output_file, stamp=prev_stamp)

    try:
        if method == "POST":
            r = requests.post(url, headers=headers, json=params, timeout=DEFAULT_TIMEOUT)
        else:
            r = requests.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()

        payload = r.content
        if kind == "api_json":
            # normalize json output for deterministic diffs/stamps
            js = r.json()
            payload = json.dumps(js, ensure_ascii=False, indent=2).encode("utf-8")

        out_path = Path(output_file)
        _write_bytes(out_path, payload)
        stamp = remote_stamp or ("sha256=" + hashlib.sha256(payload).hexdigest())
        return ConnectorResult(cid, ran=True, ok=True, reason="updated", output_file=output_file, stamp=stamp)
    except Exception as e:
        return ConnectorResult(cid, ran=True, ok=False, reason=f"api_error:{e}", output_file=output_file, stamp=prev_stamp)


def _run_mcp_connector(row: pd.Series, prev: Dict[str, Any], force: bool) -> ConnectorResult:
    cid = str(row.get("connector_id", "")).strip() or "unnamed_mcp"
    cmd = _expand_env(str(row.get("mcp_command", "")).strip())
    output_file = _expand_env(str(row.get("output_file", "")).strip() or f"external/{cid}.json")
    interval_h = float(row.get("interval_hours", 12) or 12)

    if not cmd:
        return ConnectorResult(cid, ran=False, ok=False, reason="missing_mcp_command", output_file=output_file, stamp="")

    last_ts = float(prev.get("last_run_ts", 0.0) or 0.0)
    elapsed_h = (time.time() - last_ts) / 3600.0 if last_ts > 0 else 999999.0
    if (not force) and elapsed_h < interval_h:
        return ConnectorResult(cid, ran=False, ok=True, reason="interval_not_elapsed", output_file=output_file, stamp=str(prev.get("stamp", "")))

    try:
        proc = subprocess.run(
            shlex.split(cmd),
            capture_output=True,
            text=True,
            check=True,
            timeout=DEFAULT_TIMEOUT,
        )
        txt = (proc.stdout or "").strip()
        payload = txt.encode("utf-8")
        out_path = Path(output_file)
        _write_bytes(out_path, payload)
        stamp = "sha256=" + hashlib.sha256(payload).hexdigest()
        return ConnectorResult(cid, ran=True, ok=True, reason="updated", output_file=output_file, stamp=stamp)
    except Exception as e:
        return ConnectorResult(cid, ran=True, ok=False, reason=f"mcp_error:{e}", output_file=output_file, stamp=str(prev.get("stamp", "")))


def run_incremental_connectors(base_dir: Path, state: Dict[str, Any], force: bool = False, verbose: bool = False) -> Tuple[Dict[str, Any], List[ConnectorResult]]:
    """
    Manifest format: data/external/connectors_manifest.csv
    Required columns:
    - connector_id
    - enabled (true/false)
    - kind: api_json | api_csv | mcp
    Optional:
    - url, method, headers_json, params_json
    - mcp_command, interval_hours
    - output_file (absolute or relative to base_dir)
    """
    manifest = base_dir / "data" / "external" / "connectors_manifest.csv"
    if not manifest.exists():
        return state, []

    try:
        df = pd.read_csv(manifest, dtype=str).fillna("")
    except Exception:
        return state, []
    if df.empty:
        return state, []

    st = dict(state or {})
    ext_state = st.get("external_connectors", {}) or {}
    results: List[ConnectorResult] = []

    for _, row in df.iterrows():
        cid = str(row.get("connector_id", "")).strip() or f"row_{_}"
        prev = ext_state.get(cid, {}) if isinstance(ext_state, dict) else {}
        kind = str(row.get("kind", "api_json")).strip().lower()
        url = str(row.get("url", "")).strip()

        enabled_explicit = _to_bool(row.get("enabled", "false"))
        enabled_if_env = _to_bool(row.get("enabled_if_env", "true"))
        required_env = _required_env_from_row(row)
        missing_env = [v for v in required_env if not str(os.getenv(v, "")).strip()]
        auto_enabled = enabled_if_env and len(missing_env) == 0
        effective_enabled = bool(enabled_explicit or auto_enabled)

        if kind != "mcp" and _is_placeholder_url(url):
            effective_enabled = False
            skip_reason = "placeholder_url"
        elif not effective_enabled:
            skip_reason = ("missing_env:" + ",".join(missing_env)) if missing_env else "disabled"
        else:
            skip_reason = ""

        if not effective_enabled:
            res = ConnectorResult(
                connector_id=cid,
                ran=False,
                ok=True,
                reason=skip_reason,
                output_file=str(row.get("output_file", "")).strip(),
                stamp=str(prev.get("stamp", "")),
            )
            results.append(res)
            ext_state[cid] = {
                "stamp": str(prev.get("stamp", "")),
                "last_run_ts": float(prev.get("last_run_ts", 0.0) or 0.0),
                "last_reason": res.reason,
                "last_ok": bool(res.ok),
                "output_file": res.output_file,
                "required_env": ",".join(required_env),
                "missing_env": ",".join(missing_env),
                "enabled_explicit": bool(enabled_explicit),
                "enabled_if_env": bool(enabled_if_env),
            }
            if verbose:
                print(f"[connector] {cid}: ran=False ok=True reason={res.reason}")
            continue

        # Resolve relative output path against repo root.
        out_file = str(row.get("output_file", "")).strip()
        if out_file and not Path(out_file).is_absolute():
            row = row.copy()
            row["output_file"] = str((base_dir / out_file).resolve())

        if kind == "mcp":
            res = _run_mcp_connector(row, prev=prev, force=force)
        else:
            res = _run_api_connector(row, prev=prev, force=force)
        results.append(res)

        ext_state[cid] = {
            "stamp": res.stamp,
            "last_run_ts": time.time(),
            "last_reason": res.reason,
            "last_ok": bool(res.ok),
            "output_file": res.output_file,
            "required_env": ",".join(required_env),
            "missing_env": ",".join(missing_env),
            "enabled_explicit": bool(enabled_explicit),
            "enabled_if_env": bool(enabled_if_env),
        }
        if verbose:
            print(f"[connector] {cid}: ran={res.ran} ok={res.ok} reason={res.reason}")

    st["external_connectors"] = ext_state
    return st, results
