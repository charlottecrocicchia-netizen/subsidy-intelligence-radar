#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 16:46:05 2026

@author: charlottecrocicchia
"""

from __future__ import annotations

import re
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Dict, Optional, Tuple

import requests
import feedparser
from SPARQLWrapper import SPARQLWrapper, JSON


# =========================
# Paths (mêmes que ton app)
# =========================
BASE_DIR = Path(__file__).resolve().parent   # <-- Script/
EVENTS_PATH = BASE_DIR / "data" / "external" / "events.csv"
EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)


# =========================
# Config: RSS sources
# =========================
# Note: certains flux "newsroom" ont un endpoint /feed?... (visible sur les pages "Rss Feed for ...").
# Exemple vu pour Horizon2020 Energy topic: page topic/615 -> feed?lang=en&orderby=item_date&topic_id=615 :contentReference[oaicite:3]{index=3}
RSS_FEEDS: List[Tuple[str, str]] = [
    # (source_name, feed_url)
    ("EC Newsroom — Horizon2020 Energy", "https://ec.europa.eu/newsroom/horizon2020/feed?lang=en&orderby=item_date&topic_id=615"),
    ("EC Newsroom — Horizon2020 Environment", "https://ec.europa.eu/newsroom/horizon2020/feed?lang=en&orderby=item_date&topic_id=613"),
    # Ajoute d’autres “Newsroom” si tu veux (DG CLIMA, DG ENER, etc. selon pages qui exposent un RSS)
]


# =========================
# Config: EUR-Lex / Cellar SPARQL
# =========================
CELLAR_SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"  # :contentReference[oaicite:4]{index=4}

# Mots-clés -> tags
TAG_RULES: List[Tuple[str, str]] = [
    (r"\bhydrogen\b|\bh2\b|\belectroly", "H2"),
    (r"\bccus\b|\bccs\b|\bcarbon capture\b|\bco2\b|\bstorage\b", "CCUS"),
    (r"\bbatter(y|ies)\b|\bstorage\b|\blithium\b|\bcell\b", "BAT"),
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
    return "REG"  # fallback “transversal”


def theme_from_tag(tag: str) -> str:
    # Tu peux ajuster ces libellés (l’app matche par theme ou par tag)
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
    impact_direction: str  # "+", "-", "0"
    notes: str

    @property
    def date_str(self) -> str:
        return self.date.strftime("%Y-%m-%d")


def _safe_dt_from_struct(entry: dict) -> Optional[datetime]:
    # feedparser uses struct_time sometimes
    for key in ["published_parsed", "updated_parsed"]:
        if key in entry and entry[key]:
            stt = entry[key]
            return datetime(*stt[:6], tzinfo=timezone.utc).astimezone(timezone.utc).replace(tzinfo=None)
    # fallback: try raw strings
    for key in ["published", "updated"]:
        if key in entry and entry[key]:
            try:
                return datetime.fromisoformat(entry[key].replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                pass
    return None


def fetch_rss_events(limit_per_feed: int = 50) -> List[Event]:
    out: List[Event] = []
    for source_name, url in RSS_FEEDS:
        d = feedparser.parse(url)
        for e in d.entries[:limit_per_feed]:
            dt = _safe_dt_from_struct(e)
            if not dt:
                continue
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            summary = (e.get("summary") or "").strip()
            blob = f"{title}\n{summary}"
            tag = infer_tag(blob)
            theme = theme_from_tag(tag)
            notes = (summary[:800] + ("…" if len(summary) > 800 else "")).strip()
            if link:
                notes = f"{notes}\nLink: {link}".strip()
            out.append(Event(
                date=dt,
                theme=theme,
                tag=tag,
                title=title or "(no title)",
                source=source_name,
                impact_direction="+",
                notes=notes
            ))
    return out


def fetch_eurlex_sparql_events(
    keywords: List[str],
    days_back: int = 365,
    limit: int = 200
) -> List[Event]:
    """
    Pull recent legal acts metadata from Cellar SPARQL endpoint.
    We keep it lightweight: title + date + celex where possible.
    """
    # Simple keyword filter (title/label contains)
    # Note: SPARQL over Cellar is powerful; this is intentionally conservative & robust.
    kw_filters = " || ".join([f'CONTAINS(LCASE(STR(?title)), "{k.lower()}")' for k in keywords])
    if not kw_filters:
        kw_filters = "true"

    # Date lower bound
    since = (datetime.utcnow().date()).toordinal() - days_back
    since_dt = datetime.fromordinal(since).strftime("%Y-%m-%d")

    query = f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX dct: <http://purl.org/dc/terms/>
SELECT DISTINCT ?work ?title ?date ?celex WHERE {{
  ?work a cdm:work .
  OPTIONAL {{ ?work cdm:work_has_resource-type ?rt . }}
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
    res = sparql.query().convert()

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
            impact_direction="+",
            notes=notes
        ))
    return events


def dedupe(events: List[Event]) -> List[Event]:
    seen = set()
    out = []
    for e in sorted(events, key=lambda x: (x.date, x.source, x.title)):
        key = (e.date_str, e.tag, e.title.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def write_events_csv(events: List[Event], path: Path) -> None:
    rows = dedupe(events)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "theme", "tag", "title", "source", "impact_direction", "notes"])
        for e in rows:
            w.writerow([e.date_str, e.theme, e.tag, e.title, e.source, e.impact_direction, e.notes])


def main():
    # 1) RSS events
    rss_events = fetch_rss_events(limit_per_feed=80)

    # 2) EUR-Lex / Cellar SPARQL events (keywords = tech + policy)
    keywords = [
        "hydrogen", "battery", "batteries", "carbon", "ccs", "ccus",
        "net-zero", "industry act", "renewable", "electricity", "ai act",
        "gas", "security of supply"
    ]
    eurlex_events = fetch_eurlex_sparql_events(keywords=keywords, days_back=540, limit=250)

    all_events = rss_events + eurlex_events
    write_events_csv(all_events, EVENTS_PATH)

    print(f"[OK] Wrote {len(dedupe(all_events))} events to: {EVENTS_PATH}")


if __name__ == "__main__":
    main()