#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_events.py — Build data/external/events.csv from:
- EC Newsroom RSS feeds
- EUR-Lex / Cellar SPARQL endpoint

Robust version for Streamlit Cloud:
- timeouts + user-agent
- per-source error isolation
- atomic write
"""

from __future__ import annotations

import csv
import errno
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import feedparser
import requests
from SPARQLWrapper import SPARQLWrapper, JSON


# =========================
# Paths
# =========================
BASE_DIR = Path(__file__).resolve().parent
EVENTS_PATH = BASE_DIR / "data" / "external" / "events.csv"
EVENTS_META_PATH = BASE_DIR / "data" / "external" / "events_meta.json"
EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)

# =========================
# Network config (cloud-proof)
# =========================
DEFAULT_TIMEOUT = 25  # seconds
HEADERS = {
    "User-Agent": "SubsidyIntelligenceRadar/1.0 (Streamlit; contact: internal)",
    "Accept": "application/rss+xml,application/xml,text/xml,*/*",
}


# =========================
# RSS sources
# =========================
RSS_FEEDS: List[Tuple[str, str]] = [
    ("EC Newsroom — Horizon2020 Energy", "https://ec.europa.eu/newsroom/horizon2020/feed?lang=en&orderby=item_date&topic_id=615"),
    ("EC Newsroom — Horizon2020 Environment", "https://ec.europa.eu/newsroom/horizon2020/feed?lang=en&orderby=item_date&topic_id=613"),
]

# =========================
# EUR-Lex / Cellar SPARQL
# =========================
CELLAR_SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"

TAG_RULES: List[Tuple[str, str]] = [
    (r"\bhydrogen\b|\bh2\b|\belectroly", "H2"),
    (r"\bccus\b|\bccs\b|\bcarbon capture\b|\bco2\b|\bstorage\b", "CCUS"),
    (r"\bbatter(y|ies)\b|\blithium\b|\bcell\b", "BAT"),
    (r"\bsmr\b|\bnuclear\b|\beuratom\b", "NUC"),
    (r"\bsolar\b|\bpv\b|\bcsp\b", "SOL"),
    (r"\bwind\b|\boffshore\b|\bonshore\b", "WND"),
    (r"\bbioenergy\b|\bbiofuel\b|\bsaf\b|\bbiomass\b", "BIO"),
    (r"\bai\b|\bartificial intelligence\b|\bmachine learning\b|\bdigital\b", "AI"),
    (r"\bmaterial(s)?\b|\badvanced materials\b", "MAT"),
    (r"\be-mobility\b|\bev\b|\belectric vehicle\b", "EMOB"),
    (r"\bregulation\b|\bdirective\b|\bact\b|\blegislation\b|\bcompliance\b", "REG"),
    (r"\bgeopolit\b|\bsecurity of supply\b|\benergy security\b|\bwar\b", "GEO"),
    (r"\binflation\b|\binterest rate\b|\bfinanc(e|ial)\b|\bbudget\b", "FIN"),
    (r"\bsupply chain\b|\bindustry\b|\bmanufactur", "IND"),
]


def infer_tag(text: str) -> str:
    t = (text or "").lower()
    for pattern, tag in TAG_RULES:
        if re.search(pattern, t, flags=re.IGNORECASE):
            return tag
    return "REG"


def theme_from_tag(tag: str) -> str:
    mapping = {
        "H2": "Hydrogen (H2)",
        "CCUS": "CCUS",
        "BAT": "Batteries & Storage",
        "NUC": "Nuclear & SMR",
        "SOL": "Solar (PV/CSP)",
        "WND": "Wind",
        "BIO": "Bioenergy & SAF",
        "AI": "AI & Digital",
        "MAT": "Advanced materials",
        "EMOB": "E-mobility",
        "REG": "Regulation",
        "GEO": "Geopolitics",
        "FIN": "Climate & Finance",
        "IND": "Industry & Supply chain",
    }
    return mapping.get(tag, "Regulation")


@dataclass
class Event:
    date: datetime
    theme: str
    tag: str
    title: str
    source: str
    url: str
    impact_direction: str
    notes: str

    @property
    def date_str(self) -> str:
        return self.date.strftime("%Y-%m-%d")


def _safe_dt_from_feed_entry(entry: dict) -> Optional[datetime]:
    # feedparser: struct_time
    for key in ("published_parsed", "updated_parsed"):
        if entry.get(key):
            stt = entry[key]
            return datetime(*stt[:6], tzinfo=timezone.utc).astimezone(timezone.utc).replace(tzinfo=None)

    # fallback: raw string
    for key in ("published", "updated"):
        if entry.get(key):
            s = str(entry[key]).strip()
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                pass
    return None


def _feedparser_parse(url: str) -> feedparser.FeedParserDict:
    """
    feedparser can fetch itself, but to control timeout/headers we fetch with requests first.
    """
    r = requests.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return feedparser.parse(r.content)


def fetch_rss_events(limit_per_feed: int = 80) -> List[Event]:
    out: List[Event] = []
    for source_name, url in RSS_FEEDS:
        try:
            d = _feedparser_parse(url)
        except Exception as e:
            print(f"[WARN] RSS failed: {source_name} — {e}")
            continue

        for entry in (d.entries or [])[:limit_per_feed]:
            dt = _safe_dt_from_feed_entry(entry)
            if not dt:
                continue
            title = (entry.get("title") or "").strip() or "(no title)"
            link = (entry.get("link") or "").strip()
            summary = (entry.get("summary") or "").strip()
            blob = f"{title}\n{summary}"
            tag = infer_tag(blob)
            theme = theme_from_tag(tag)
            notes = (summary[:800] + ("…" if len(summary) > 800 else "")).strip()

            out.append(Event(
                date=dt,
                theme=theme,
                tag=tag,
                title=title,
                source=source_name,
                url=link,
                impact_direction="+",
                notes=notes,
            ))
    return out


def fetch_eurlex_sparql_events(keywords: List[str], days_back: int = 540, limit: int = 250) -> List[Event]:
    kw_filters = " || ".join([f'CONTAINS(LCASE(STR(?title)), "{k.lower()}")' for k in keywords])
    if not kw_filters:
        kw_filters = "true"

    since_dt = (datetime.now(timezone.utc).date() - timedelta(days=int(days_back))).strftime("%Y-%m-%d")

    query = f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX dct: <http://purl.org/dc/terms/>
SELECT DISTINCT ?work ?title ?date ?celex WHERE {{
  ?work a cdm:work .
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  OPTIONAL {{ ?work cdm:resource_legal_id_celex ?celex . }}
  OPTIONAL {{ ?work dct:title ?title . FILTER (lang(?title) = "en") }}

  FILTER(BOUND(?date) && ?date >= "{since_dt}"^^<http://www.w3.org/2001/XMLSchema#date>)
  FILTER(BOUND(?title))
  FILTER({kw_filters})
}}
ORDER BY DESC(?date)
LIMIT {int(limit)}
"""

    sparql = SPARQLWrapper(CELLAR_SPARQL_ENDPOINT)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.setTimeout(DEFAULT_TIMEOUT)  # important on cloud

    try:
        res = sparql.query().convert()
    except Exception as e:
        print(f"[WARN] SPARQL failed — {e}")
        return []

    events: List[Event] = []
    for b in res.get("results", {}).get("bindings", []):
        title = b.get("title", {}).get("value", "").strip()
        date_s = b.get("date", {}).get("value", "").strip()
        celex = b.get("celex", {}).get("value", "").strip()
        if not title or not date_s:
            continue
        try:
            dt = datetime.fromisoformat(date_s)
        except Exception:
            continue

        blob = f"{title} {celex}"
        tag = infer_tag(blob)
        theme = theme_from_tag(tag)
        notes = f"CELEX: {celex}" if celex else ""

        events.append(Event(
            date=dt,
            theme=theme,
            tag=tag,
            title=title,
            source="EUR-Lex (Cellar SPARQL)",
            url=(f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}" if celex else ""),
            impact_direction="+",
            notes=notes,
        ))
    return events


def load_existing_events(path: Path) -> List[Event]:
    if not path.exists():
        return []
    out: List[Event] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ds = str(row.get("date", "")).strip()
                if not ds:
                    continue
                try:
                    dt = datetime.fromisoformat(ds)
                except Exception:
                    continue
                out.append(
                    Event(
                        date=dt,
                        theme=str(row.get("theme", "")).strip(),
                        tag=str(row.get("tag", "")).strip(),
                        title=str(row.get("title", "")).strip(),
                        source=str(row.get("source", "")).strip(),
                        url=str(row.get("url", "")).strip(),
                        impact_direction=str(row.get("impact_direction", "")).strip(),
                        notes=str(row.get("notes", "")).strip(),
                    )
                )
    except Exception:
        return []
    return out


def _event_score(e: Event) -> int:
    score = 0
    if str(e.url or "").strip():
        score += 3
    if str(e.notes or "").strip():
        score += 1
    if str(e.source or "").strip():
        score += 1
    return score


def dedupe(events: List[Event]) -> List[Event]:
    best: dict = {}
    for e in events:
        key = (e.date_str, str(e.tag).strip().upper(), str(e.title).strip().lower())
        prev = best.get(key)
        if prev is None or _event_score(e) > _event_score(prev):
            best[key] = e
    return sorted(best.values(), key=lambda x: (x.date, x.source, x.title))


def atomic_write_events_csv(events: List[Event], path: Path) -> None:
    rows = dedupe(events)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "theme", "tag", "title", "source", "url", "impact_direction", "notes"])
            for e in rows:
                w.writerow([e.date_str, e.theme, e.tag, e.title, e.source, e.url, e.impact_direction, e.notes])
        try:
            tmp.replace(path)
        except OSError as e:
            if e.errno == errno.EXDEV:
                # Defensive fallback if runtime still reports cross-device rename.
                shutil.move(str(tmp), str(path))
            else:
                raise
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def load_events_meta(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_events_meta(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def main() -> None:
    force_refresh = str(os.getenv("SUBSIDY_EVENTS_FORCE", "0")).strip() == "1"
    try:
        min_refresh_h = max(0.0, float(str(os.getenv("SUBSIDY_EVENTS_MIN_REFRESH_HOURS", "24")).strip()))
    except Exception:
        min_refresh_h = 24.0
    try:
        days_back = max(1, int(str(os.getenv("SUBSIDY_EVENTS_DAYS_BACK", "540")).strip()))
    except Exception:
        days_back = 540

    meta = load_events_meta(EVENTS_META_PATH)
    last_build_ts = float(meta.get("last_build_ts", 0.0) or 0.0)
    now_ts = datetime.now(timezone.utc).timestamp()
    age_h = ((now_ts - last_build_ts) / 3600.0) if last_build_ts > 0 else 999999.0
    if (not force_refresh) and EVENTS_PATH.exists() and (age_h < min_refresh_h):
        print(f"[SKIP] events refresh skipped (age={age_h:.1f}h < min_refresh={min_refresh_h:.1f}h).")
        print(f"[OK] Keeping existing events file: {EVENTS_PATH}")
        return

    existing_events = load_existing_events(EVENTS_PATH)
    rss_events = fetch_rss_events(limit_per_feed=80)

    keywords = [
        "hydrogen", "battery", "batteries", "carbon", "ccs", "ccus",
        "net-zero", "industry act", "renewable", "electricity", "ai act",
        "gas", "security of supply",
    ]
    eurlex_events = fetch_eurlex_sparql_events(keywords=keywords, days_back=days_back, limit=250)

    all_events = existing_events + rss_events + eurlex_events
    deduped = dedupe(all_events)
    atomic_write_events_csv(deduped, EVENTS_PATH)

    write_events_meta(
        EVENTS_META_PATH,
        {
            "last_build_ts": now_ts,
            "last_build_utc": datetime.now(timezone.utc).isoformat(),
            "min_refresh_hours": min_refresh_h,
            "days_back": days_back,
            "existing_events": len(existing_events),
            "rss_events": len(rss_events),
            "sparql_events": len(eurlex_events),
            "total_deduped": len(deduped),
            "mode": "append_only",
        },
    )

    print(f"[OK] Existing events kept: {len(existing_events)}")
    print(f"[OK] RSS events: {len(rss_events)}")
    print(f"[OK] SPARQL events: {len(eurlex_events)}")
    print(f"[OK] Total (deduped): {len(deduped)}")
    print(f"[OK] Wrote: {EVENTS_PATH}")
    print(f"[OK] Wrote meta: {EVENTS_META_PATH}")


if __name__ == "__main__":
    main()
