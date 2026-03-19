#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
process_build.py — Build data/processed/subsidy_base.csv (+ .parquet)

- Loads CORDIS (Horizon Europe + Horizon 2020) from data/raw/cordis/<program>/{project,organization}.csv
- Normalizes schema + types
- Writes outputs atomically to avoid partial files during refresh on Streamlit Cloud

Author: Charlotte Crocicchia (rewritten & hardened)
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent


# ============================================================
# Theme inference — V2 VECTORIZED (fast pandas str.contains)
# ============================================================
# Each keyword has a weight:
#   2 = highly specific (a single match is enough)
#   1 = moderately specific (needs 2+ matches or another weight-2 keyword)
# A theme is only assigned if total score >= THEME_MIN_SCORE (default 2).

THEME_MIN_SCORE = 2

THEME_KEYWORDS_WEIGHTED: Dict[str, List[tuple]] = {
    # =====================================================================
    # With \b word-boundary matching, single domain words are SAFE.
    # "hydrogen" won't match in "dehydrogenation" etc.
    # The old problem was find() on "ion", "ev", "process" — NOT on real
    # domain words. So we restore them as weight-2 (sufficient alone).
    # =====================================================================

    "Hydrogen (H2)": [
        ("hydrogen", 2), ("electrolyser", 2), ("electrolyzer", 2),
        ("electrolysis", 2),
        ("fuel cell", 2), ("fuel cells", 2), ("green hydrogen", 2),
        ("proton exchange membrane", 2), ("power-to-gas", 2), ("power to gas", 2),
        ("hydrogen storage", 2), ("hydrogen transport", 2), ("hydrogen pipeline", 2),
        ("hydrogen refuelling", 2), ("hydrogen refueling", 2),
        ("H2 production", 2), ("H2 infrastructure", 2),
        ("PEM", 1), ("SOFC", 1), ("SOEC", 1),
    ],

    "Solar (PV/CSP)": [
        ("solar", 2), ("photovoltaic", 2), ("photovoltaics", 2),
        ("perovskite", 1),
        ("solar cell", 2), ("solar panel", 2), ("solar energy", 2),
        ("solar power", 2), ("solar farm", 2), ("concentrated solar", 2),
        ("solar thermal", 2), ("solar module", 2),
        ("agrivoltaic", 2), ("agri-voltaic", 2),
        ("PV system", 1), ("PV module", 1), ("PV plant", 1),
        ("CSP", 1),
    ],

    "Wind": [
        ("wind turbine", 2), ("wind energy", 2), ("wind power", 2),
        ("wind farm", 2), ("offshore wind", 2), ("onshore wind", 2),
        ("floating wind", 2), ("wind blade", 2), ("wind rotor", 2),
        ("wind tower", 1), ("wake effect", 1),
    ],

    "Bioenergy & SAF": [
        ("bioenergy", 2), ("biofuel", 2), ("biofuels", 2), ("biogas", 2),
        ("biomass", 2), ("biomethane", 2), ("bioethanol", 2), ("biodiesel", 2),
        ("biorefinery", 2), ("bio-refinery", 2), ("microalgae", 2),
        ("lignocellulosic", 2), ("anaerobic digestion", 2),
        ("sustainable aviation fuel", 2), ("e-fuel", 2), ("efuel", 2),
        ("waste-to-energy", 1), ("pyrolysis", 1), ("gasification", 1),
    ],

    "CCUS": [
        ("CCUS", 2), ("CCS", 2),
        ("carbon capture", 2), ("CO2 capture", 2), ("CO2 storage", 2),
        ("CO2 transport", 2), ("CO2 utilisation", 2), ("CO2 utilization", 2),
        ("direct air capture", 2), ("carbon sequestration", 2),
        ("carbon mineralization", 2), ("geological storage", 1),
    ],

    "Nuclear & SMR": [
        ("nuclear", 2), ("fission", 2), ("fusion", 2), ("tokamak", 2),
        ("small modular reactor", 2), ("SMR", 1),
        ("nuclear energy", 2), ("nuclear power", 2), ("nuclear reactor", 2),
        ("nuclear fuel", 2), ("nuclear waste", 2),
        ("molten salt reactor", 2), ("fast breeder", 2),
        ("nuclear decommission", 2), ("nuclear safety", 2),
        ("fusion plasma", 2), ("tritium", 1), ("ITER", 1),
        ("Euratom", 2),
    ],

    "Batteries & Storage": [
        ("battery", 2), ("batteries", 2), ("lithium", 2),
        ("supercapacitor", 2), ("gigafactory", 2),
        ("energy storage", 2), ("grid storage", 2), ("stationary storage", 2),
        ("solid-state battery", 2), ("solid state battery", 2),
        ("sodium-ion battery", 2), ("redox flow battery", 2),
        ("battery cell", 2), ("battery pack", 2), ("battery management", 2),
        ("battery recycling", 2), ("battery storage", 2),
        ("electrode material", 1), ("electrolyte material", 1),
        ("cathode material", 1), ("anode material", 1),
        ("second life battery", 2), ("battery aging", 2),
    ],

    "AI & Digital": [
        ("artificial intelligence", 2), ("machine learning", 2),
        ("deep learning", 2), ("digital twin", 2), ("digital twins", 2),
        ("natural language processing", 2), ("computer vision", 1),
        ("neural network", 1), ("reinforcement learning", 1),
        ("big data", 1), ("data-driven", 1),
        ("IoT", 1), ("internet of things", 1),
        ("cloud computing", 1), ("edge computing", 1),
        ("cybersecurity", 1), ("quantum computing", 1),
        ("robotic", 1), ("robotics", 1),
    ],

    "Advanced materials": [
        ("graphene", 2), ("nanomaterial", 2), ("nanomaterials", 2),
        ("nanocomposite", 2), ("metamaterial", 2), ("carbon nanotube", 2),
        ("advanced material", 2), ("advanced materials", 2),
        ("composite material", 2), ("functional coating", 2),
        ("critical raw material", 2), ("critical raw materials", 2),
        ("surface engineering", 1), ("ceramic material", 1),
        ("polymer composite", 1), ("membrane technology", 1),
        ("separator material", 1), ("rare earth", 1),
        ("raw materials", 1), ("nanotech", 1),
        ("coating", 1), ("composite", 1),
    ],

    "E-mobility": [
        ("electric vehicle", 2), ("electric vehicles", 2),
        ("electric car", 2), ("electric bus", 2), ("electric truck", 2),
        ("electric drivetrain", 2), ("e-mobility", 2), ("emobility", 2),
        ("EV charging", 2), ("charging station", 2), ("charging stations", 2),
        ("charging infrastructure", 2), ("vehicle-to-grid", 2), ("V2G", 2),
        ("smart charging", 2), ("fast charging", 1),
        ("battery electric vehicle", 2), ("plug-in hybrid", 1),
        ("electric mobility", 2), ("powertrain", 1),
    ],

    "Climate & Environment": [
        ("climate", 2), ("biodiversity", 2), ("ecosystem", 2),
        ("pollution", 2), ("circular economy", 2),
        ("greenhouse gas", 2), ("GHG emission", 2), ("carbon footprint", 2),
        ("climate adaptation", 2), ("climate mitigation", 2),
        ("climate resilience", 2), ("climate change", 2),
        ("environmental monitoring", 1), ("air quality", 1),
        ("water treatment", 1), ("water purification", 2),
        ("waste management", 1), ("waste stream", 2), ("urban waste", 2),
        ("nature-based solution", 2), ("ocean health", 1),
        ("recycling", 1), ("life cycle assessment", 1),
        ("environment", 1), ("adaptation", 1),
    ],

    "Industry & Manufacturing": [
        ("manufacturing", 2), ("factory", 1), ("industrial", 1),
        ("advanced manufacturing", 2), ("smart factory", 2),
        ("industry 4.0", 2), ("industrie 4.0", 2),
        ("industrial decarbonisation", 2), ("industrial decarbonization", 2),
        ("process intensification", 2), ("industrial automation", 2),
        ("predictive maintenance", 2), ("additive manufacturing", 2),
        ("3D printing", 1), ("pilot line", 1),
        ("semiconductor", 2), ("System-on-Chip", 2),
        ("integrated circuit", 2), ("chip fabrication", 2),
        ("photonic circuit", 2), ("automation", 1),
    ],

    "Transport & Aviation": [
        ("aviation", 2), ("aircraft", 2), ("aeronautic", 2),
        ("aeronautics", 2), ("railway", 1), ("rail", 1),
        ("maritime", 1), ("shipping", 1), ("logistics", 1),
        ("sustainable aviation", 2), ("aircraft design", 2),
        ("aircraft demonstrator", 2), ("passenger aircraft", 2),
        ("rail transport", 2), ("maritime transport", 2),
        ("freight logistics", 1), ("low-emission transport", 2),
        ("urban mobility", 1), ("autonomous vehicle", 1),
        ("unmanned aerial", 1), ("air traffic", 1),
        ("clean aviation", 2), ("rolling stock", 2),
        ("mobility", 1), ("transport", 1),
    ],

    "Health & Biotech": [
        ("health", 2), ("medical", 2), ("clinical", 2),
        ("vaccine", 2), ("biotech", 2), ("diagnostic", 1),
        ("drug discovery", 2), ("pharmaceutical", 1),
        ("bioprocessing", 2), ("biomanufacturing", 2),
        ("medical device", 2), ("genomic", 1), ("genomics", 1),
        ("proteomics", 1), ("therapeutic", 1),
        ("digital health", 2), ("tissue engineering", 2),
        ("clinical trial", 1), ("personalised medicine", 2),
        ("personalized medicine", 2), ("drug delivery", 2),
        ("disease", 1), ("patient", 1), ("cancer", 2),
        ("biomarker", 2), ("infection", 1), ("pathogen", 1),
    ],

    "Space": [
        ("satellite", 2), ("space", 2), ("orbit", 1), ("orbital", 1),
        ("earth observation", 2), ("space launch", 2),
        ("space propulsion", 2), ("space robotics", 2),
        ("planetary exploration", 2), ("Copernicus", 1),
        ("space debris", 2), ("low earth orbit", 2),
        ("Galileo", 1), ("launcher", 1),
    ],

    "Agriculture & Food": [
        ("agriculture", 2), ("farming", 2), ("crop", 2), ("food", 2),
        ("soil", 1), ("aquaculture", 2), ("agri-biotech", 2),
        ("precision agriculture", 2), ("agroecology", 2),
        ("food processing", 1), ("food safety", 1),
        ("alternative protein", 2), ("vertical farming", 2),
        ("food system", 1), ("sustainable farming", 2),
        ("livestock", 1), ("fisheries", 1), ("irrigation", 1),
    ],

    "Security & Resilience": [
        ("security", 1), ("cyber", 1), ("defence", 1), ("defense", 1),
        ("critical infrastructure protection", 2), ("cyber resilience", 2),
        ("energy security", 1), ("disaster risk reduction", 2),
        ("civil security", 2), ("border security", 2),
        ("counter-terrorism", 2), ("CBRN", 2),
        ("surveillance", 1), ("emergency response", 1),
        ("resilience", 1), ("crisis", 1),
    ],
}

# Exclusion patterns: if a pattern matches, the theme is NOT assigned
THEME_EXCLUSIONS: Dict[str, List[str]] = {
    "Batteries & Storage": [
        r"\bsemiconductor\b", r"\bchip fabricat", r"\b2nm\b",
        r"\bsystem-on-chip\b", r"\bpassenger aircraft\b",
    ],
    "Nuclear & SMR": [
        r"\bparticle accelerat", r"\bice core\b", r"\bice sheet\b",
    ],
    "Wind": [
        r"\btidal\b", r"\bgeothermal\b",
    ],
    "E-mobility": [
        r"\bcitizen\b", r"\brenaissance\b",
    ],
    "CCUS": [
        r"\bforest\b", r"\becosystem restoration\b",
    ],
}

# --- Pre-build ONE combined regex per theme (fast) ---
_THEME_W2_RE: Dict[str, re.Pattern] = {}   # weight-2 keywords combined
_THEME_W1_RE: Dict[str, re.Pattern] = {}   # weight-1 keywords combined
_THEME_EXCL_RE: Dict[str, re.Pattern] = {}

def _build_combined_pattern(keywords: List[str]) -> Optional[re.Pattern]:
    if not keywords:
        return None
    alts = "|".join(r"\b" + re.escape(k) + r"\b" for k in keywords)
    return re.compile(alts, re.IGNORECASE)

for _th, _kws in THEME_KEYWORDS_WEIGHTED.items():
    _w2 = [k for k, w in _kws if w >= 2]
    _w1 = [k for k, w in _kws if w == 1]
    if _w2:
        _THEME_W2_RE[_th] = _build_combined_pattern(_w2)
    if _w1:
        _THEME_W1_RE[_th] = _build_combined_pattern(_w1)

for _th, _excls in THEME_EXCLUSIONS.items():
    _THEME_EXCL_RE[_th] = re.compile("|".join(_excls), re.IGNORECASE)

# Ordered theme list for stable iteration
_THEME_LIST = list(THEME_KEYWORDS_WEIGHTED.keys())

# Keep old ONETECH/GENERIC as empty for backward compat (value chain uses _keyword_positive_hit)
ONETECH: Dict[str, List[str]] = {}
GENERIC: Dict[str, List[str]] = {}


def infer_themes_vectorized(text_series: pd.Series) -> pd.Series:
    """
    Vectorized theme inference on an entire pandas Series of text.
    Returns a Series of theme labels. ~50-100x faster than row-by-row apply.
    """
    s = text_series.fillna("").astype(str).str.lower()
    # Apply FR→EN aliases vectorized
    for patt, repl in TEXT_ALIAS_PATTERNS:
        s = s.str.replace(patt, repl, regex=True)

    n = len(s)
    # Score matrix: rows=documents, cols=themes
    score_w2 = np.zeros((n, len(_THEME_LIST)), dtype=np.int16)
    score_w1 = np.zeros((n, len(_THEME_LIST)), dtype=np.int16)
    excluded = np.zeros((n, len(_THEME_LIST)), dtype=bool)

    for j, theme in enumerate(_THEME_LIST):
        # Exclusions (vectorized)
        if theme in _THEME_EXCL_RE:
            excluded[:, j] = s.str.contains(_THEME_EXCL_RE[theme], na=False).values

        # Weight-2 keywords: count how many distinct groups match
        if theme in _THEME_W2_RE:
            score_w2[:, j] = s.str.contains(_THEME_W2_RE[theme], na=False).astype(np.int16).values * 2

        # Weight-1 keywords
        if theme in _THEME_W1_RE:
            score_w1[:, j] = s.str.contains(_THEME_W1_RE[theme], na=False).astype(np.int16).values

    # Total score (zero out excluded themes)
    total = score_w2 + score_w1
    total[excluded] = 0

    # Best theme per row
    best_idx = np.argmax(total, axis=1)
    best_score = total[np.arange(n), best_idx]

    result = np.where(
        best_score >= THEME_MIN_SCORE,
        np.array(_THEME_LIST)[best_idx],
        "Other",
    )
    return pd.Series(result, index=text_series.index)



VALUE_CHAIN_RULES: Dict[str, List[str]] = {
    "Resources & feedstock": [
        "critical raw material", "raw material", "lithium", "nickel", "cobalt", "graphite", "mining", "refining",
        "feedstock", "biomass", "supply chain", "recycling", "recycled content", "precursor", "cathode material",
        "anode material", "resource efficiency", "sourcing",
    ],
    "Components & core technology": [
        "electrolyser", "electrolyzer", "fuel cell", "reactor", "cell", "module", "stack", "turbine", "battery",
        "membrane", "electrode", "catalyst", "compressor", "inverter", "converter", "sensor", "power electronics",
        "hardware", "component", "subsystem",
    ],
    "Systems & infrastructure": [
        "grid", "microgrid", "pipeline", "network", "charging", "charging station", "storage system", "integration",
        "interoperability", "hub", "terminal", "facility", "plant", "district heating", "balance of plant", "bop",
        "infrastructure", "platform",
    ],
    "Deployment & operations": [
        "pilot", "demonstration", "demo", "deployment", "operation", "operations", "industrialisation", "industrialization",
        "scale-up", "scale up", "roll-out", "roll out", "retrofit", "commissioning", "field trial", "validation",
        "first-of-a-kind", "foak", "maintenance", "site implementation", "trl 6", "trl 7", "trl 8",
    ],
    "End-use & market": [
        "mobility", "aviation", "shipping", "manufacturing", "consumer", "commercialisation", "commercialization",
        "market uptake", "market adoption", "end-user", "customer", "offtake", "business model", "procurement",
        "bankable", "replication", "go-to-market", "go to market", "trl 9",
    ],
    "Research & concept": [
        "research", "r&d", "feasibility", "proof of concept", "proof-of-concept", "concept", "laboratory",
        "fundamental", "early-stage", "early stage", "methodology", "simulation", "modeling", "trl 1", "trl 2", "trl 3", "trl 4",
    ],
}
VALUE_CHAIN_PRIORITY = list(VALUE_CHAIN_RULES.keys())

NEGATION_TOKENS = [
    "not", "no", "without", "excluding", "exclude", "except",
    "non", "sans", "hors", "ne pas", "a exclure", "à exclure",
]
NEG_RE = re.compile(r"\b(" + "|".join([re.escape(x) for x in NEGATION_TOKENS]) + r")\b", flags=re.IGNORECASE)
EXCLUSION_CONTEXT_RE = re.compile(
    r"\b(not in scope|out of scope|excluded from scope|exclude this|to exclude|a exclure|à exclure|hors perimetre|hors périmètre|non retenu)\b",
    flags=re.IGNORECASE,
)
TEXT_ALIAS_PATTERNS = [
    (re.compile(r"\bhydrog[eè]ne\b", flags=re.IGNORECASE), "hydrogen"),
    (re.compile(r"\b[eé]lectrolyseur?s?\b", flags=re.IGNORECASE), "electrolyser"),
    (re.compile(r"\b(?:pile a combustible|pile à combustible)s?\b", flags=re.IGNORECASE), "fuel cell"),
    (re.compile(r"\bsolaire\b", flags=re.IGNORECASE), "solar"),
    (re.compile(r"\bphotovolta[iï]que?s?\b", flags=re.IGNORECASE), "photovoltaic"),
    (re.compile(r"\b[eé]olien(?:ne)?s?\b", flags=re.IGNORECASE), "wind"),
    (re.compile(r"\bbiocarburants?\b", flags=re.IGNORECASE), "biofuel"),
    (re.compile(r"\bbiomasse\b", flags=re.IGNORECASE), "biomass"),
    (re.compile(r"\bbiogaz\b", flags=re.IGNORECASE), "biogas"),
    (re.compile(r"\bbatteri(?:e|es)\b", flags=re.IGNORECASE), "battery"),
    (re.compile(r"\bstockage\b", flags=re.IGNORECASE), "storage"),
    (re.compile(r"\bintelligence artificielle\b", flags=re.IGNORECASE), "artificial intelligence"),
    (re.compile(r"\bnum[eé]rique\b", flags=re.IGNORECASE), "digital"),
    (re.compile(r"\b(?:jumeau numerique|jumeau numérique)s?\b", flags=re.IGNORECASE), "digital twin"),
    (re.compile(r"\bmat[eé]riaux?\b", flags=re.IGNORECASE), "materials"),
    (re.compile(r"\b(?:mobilite electrique|mobilité électrique)\b", flags=re.IGNORECASE), "e-mobility"),
    (re.compile(r"\b(?:capture du carbone|captage du carbone)\b", flags=re.IGNORECASE), "carbon capture"),
    (re.compile(r"\b(?:capture de co2|captage de co2)\b", flags=re.IGNORECASE), "co2 capture"),
    (re.compile(r"\bstockage du co2\b", flags=re.IGNORECASE), "co2 storage"),
    (re.compile(r"\bnucl[eé]aire\b", flags=re.IGNORECASE), "nuclear"),
    (re.compile(r"\bsant[eé]\b", flags=re.IGNORECASE), "health"),
    (re.compile(r"\bagriculture\b", flags=re.IGNORECASE), "agriculture"),
    (re.compile(r"\bespace\b", flags=re.IGNORECASE), "space"),
]


def _clean_text(*parts: str) -> str:
    txt = " ".join([(p or "") for p in parts]).lower()
    txt = re.sub(r"[\r\n\t]+", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    for patt, repl in TEXT_ALIAS_PATTERNS:
        txt = patt.sub(repl, txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _keyword_positive_hit(txt: str, keyword: str) -> bool:
    kw = str(keyword).strip().lower()
    if not kw:
        return False

    start = 0
    while True:
        idx = txt.find(kw, start)
        if idx < 0:
            return False
        left = txt[max(0, idx - 70):idx]
        right = txt[idx + len(kw): idx + len(kw) + 30]

        neg_left = NEG_RE.search(left) is not None
        explicit_excl = EXCLUSION_CONTEXT_RE.search(left + right) is not None
        # Keep positive occurrences unless they are clearly in an exclusion context.
        if not neg_left and not explicit_excl:
            return True
        start = idx + len(kw)


def infer_theme(*parts: str) -> str:
    """Single-row fallback (used for small batches / external connectors)."""
    txt = _clean_text(*parts)
    if not txt:
        return "Other"
    return infer_themes_vectorized(pd.Series([txt])).iloc[0]


def infer_value_chain_stage(*parts: str) -> str:
    txt = _clean_text(*parts)
    if not txt:
        return "Research & concept"

    scores: Dict[str, int] = {}
    for stage, keys in VALUE_CHAIN_RULES.items():
        scores[stage] = sum(1 for k in keys if _keyword_positive_hit(txt, k))

    non_research = {k: v for k, v in scores.items() if k != "Research & concept" and v > 0}
    if non_research:
        # Prefer the most concrete downstream stage if present, even if research terms also appear.
        best_non_research = sorted(non_research.items(), key=lambda x: (-x[1], VALUE_CHAIN_PRIORITY.index(x[0])))[0][0]
        return best_non_research

    if scores.get("Research & concept", 0) > 0:
        return "Research & concept"
    return "Research & concept"


# ============================================================
# Country helpers (minimal, robust)
# ============================================================
_FALLBACK_NAME = {
    # EU 27
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "HR": "Croatia",
    "CY": "Cyprus", "CZ": "Czechia", "DK": "Denmark", "EE": "Estonia",
    "FI": "Finland", "FR": "France", "DE": "Germany", "GR": "Greece", "EL": "Greece",
    "HU": "Hungary", "IE": "Ireland", "IT": "Italy", "LV": "Latvia",
    "LT": "Lithuania", "LU": "Luxembourg", "MT": "Malta", "NL": "Netherlands",
    "PL": "Poland", "PT": "Portugal", "RO": "Romania", "SK": "Slovakia",
    "SI": "Slovenia", "ES": "Spain", "SE": "Sweden",
    # Associated countries (Horizon Europe, as of 2025)
    "NO": "Norway", "IS": "Iceland", "CH": "Switzerland", "LI": "Liechtenstein",
    "UK": "United Kingdom", "GB": "United Kingdom",
    "TR": "Türkiye", "RS": "Serbia", "AL": "Albania", "ME": "Montenegro",
    "MK": "North Macedonia", "BA": "Bosnia and Herzegovina",
    "XK": "Kosovo", "MD": "Moldova", "UA": "Ukraine", "GE": "Georgia", "AM": "Armenia",
    "IL": "Israel", "TN": "Tunisia", "EG": "Egypt", "MA": "Morocco",
    "KR": "South Korea", "CA": "Canada", "NZ": "New Zealand",
    "FO": "Faroe Islands",
    # Other frequent participants
    "US": "United States", "JP": "Japan", "CN": "China", "IN": "India",
    "BR": "Brazil", "ZA": "South Africa", "AU": "Australia",
    "SG": "Singapore", "TW": "Taiwan", "CL": "Chile", "MX": "Mexico",
    "AR": "Argentina", "CO": "Colombia", "TH": "Thailand", "MY": "Malaysia",
    "ID": "Indonesia", "PH": "Philippines", "VN": "Vietnam",
    "NG": "Nigeria", "KE": "Kenya", "GH": "Ghana", "ET": "Ethiopia",
    "SN": "Senegal", "TZ": "Tanzania", "UG": "Uganda",
    "RU": "Russia", "BY": "Belarus",
}
_FALLBACK_A3 = {
    # EU 27
    "AT": "AUT", "BE": "BEL", "BG": "BGR", "HR": "HRV",
    "CY": "CYP", "CZ": "CZE", "DK": "DNK", "EE": "EST",
    "FI": "FIN", "FR": "FRA", "DE": "DEU", "GR": "GRC", "EL": "GRC",
    "HU": "HUN", "IE": "IRL", "IT": "ITA", "LV": "LVA",
    "LT": "LTU", "LU": "LUX", "MT": "MLT", "NL": "NLD",
    "PL": "POL", "PT": "PRT", "RO": "ROU", "SK": "SVK",
    "SI": "SVN", "ES": "ESP", "SE": "SWE",
    # Associated
    "NO": "NOR", "IS": "ISL", "CH": "CHE", "LI": "LIE",
    "UK": "GBR", "GB": "GBR",
    "TR": "TUR", "RS": "SRB", "AL": "ALB", "ME": "MNE",
    "MK": "MKD", "BA": "BIH",
    "XK": "XKX", "MD": "MDA", "UA": "UKR", "GE": "GEO", "AM": "ARM",
    "IL": "ISR", "TN": "TUN", "EG": "EGY", "MA": "MAR",
    "KR": "KOR", "CA": "CAN", "NZ": "NZL",
    "FO": "FRO",
    # Other
    "US": "USA", "JP": "JPN", "CN": "CHN", "IN": "IND",
    "BR": "BRA", "ZA": "ZAF", "AU": "AUS",
    "SG": "SGP", "TW": "TWN", "CL": "CHL", "MX": "MEX",
    "AR": "ARG", "CO": "COL", "TH": "THA", "MY": "MYS",
    "RU": "RUS", "BY": "BLR",
}


def country_name(alpha2: Any) -> str:
    a2 = str(alpha2).strip().upper()
    if not a2 or a2 in {"NAN", "NONE"}:
        return ""
    try:
        import pycountry  # optional
        c = pycountry.countries.get(alpha_2=a2)
        return c.name if c else _FALLBACK_NAME.get(a2, a2)
    except Exception:
        return _FALLBACK_NAME.get(a2, a2)


def country_alpha3(alpha2: Any) -> str:
    a2 = str(alpha2).strip().upper()
    if not a2 or a2 in {"NAN", "NONE"}:
        return ""
    try:
        import pycountry  # optional
        c = pycountry.countries.get(alpha_2=a2)
        return c.alpha_3 if c else _FALLBACK_A3.get(a2, a2)
    except Exception:
        return _FALLBACK_A3.get(a2, a2)


# ============================================================
# Entity type (CORDIS activityType)
# ============================================================
def classify_entity(activity: Any) -> str:
    if pd.isna(activity):
        return "Unknown"
    a = str(activity).strip().upper()
    if a == "PRC":
        return "Private company"
    if a in {"HES", "REC"}:
        return "Research & academia"
    if a in {"PUB", "GOV", "ADM"}:
        return "Public"
    return "Other"


# ============================================================
# Normalisation helpers (for actor_id)
# ============================================================
_WS = re.compile(r"\s+")
_NONALNUM = re.compile(r"[^A-Z0-9]+")
_COL_TOKEN = re.compile(r"[^a-z0-9]+")


def norm_name(x: Any) -> str:
    s = "" if x is None else str(x)
    s = s.strip().upper()
    s = _WS.sub(" ", s)
    s = s.replace("&", " AND ")
    s = _NONALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


def pick_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    cols_exact = set(df.columns)
    cols_norm = {_COL_TOKEN.sub("", str(c).strip().lower()): c for c in df.columns}
    for c in candidates:
        if c in cols_exact:
            return c
        key = _COL_TOKEN.sub("", str(c).strip().lower())
        if key in cols_norm:
            return cols_norm[key]
    return None


# ============================================================
# IO helpers
# ============================================================
def _read_cordis_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", on_bad_lines="skip", low_memory=False)


def _atomic_write_csv(df: pd.DataFrame, out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_csv.with_suffix(out_csv.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(out_csv)


def _atomic_write_parquet(df: pd.DataFrame, out_parquet: Path) -> None:
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_parquet.with_suffix(out_parquet.suffix + ".tmp")
    # requires pyarrow installed (Streamlit Cloud usually has it / you can add to requirements)
    df.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(out_parquet)


# ============================================================
# CORDIS loader
# ============================================================
def load_cordis_program(label: str, folder: Path) -> pd.DataFrame:
    proj_path = folder / "project.csv"
    org_path = folder / "organization.csv"
    if not proj_path.exists() or not org_path.exists():
        return pd.DataFrame(columns=_SCHEMA_COLS())

    proj = _read_cordis_csv(proj_path)
    org = _read_cordis_csv(org_path)

    # contribution numeric
    if "ecContribution" in org.columns:
        org["ecContribution"] = pd.to_numeric(
            org["ecContribution"].astype(str).str.replace(",", ".", regex=False),
            errors="coerce",
        )
    else:
        org["ecContribution"] = np.nan

    # org name
    name_col = pick_col(org, "name", "organisationName", "organizationName")
    org["org_name"] = org[name_col].astype("string").fillna("").str.strip() if name_col else ""

    # stable org id if present
    org_id_col = pick_col(org, "id", "organisationID", "organizationID", "orgID")
    org["_org_id"] = org[org_id_col].astype("string").fillna("").astype(str) if org_id_col else ""

    # country alpha2
    ccol = pick_col(org, "country", "countryCode")
    org["country_alpha2"] = (org[ccol].astype("string") if ccol else "").fillna("").astype(str).str.upper().str.strip()

    # drop rows without org name (avoid pollution)
    org = org[org["org_name"].astype(str).str.len() > 0].copy()

    # actor_id (CORDIS)
    org["org_name_norm"] = org["org_name"].apply(norm_name)
    org["actor_id"] = np.where(
        org["_org_id"].astype(str).str.len() > 0,
        "CORDIS:" + org["country_alpha2"].astype(str) + ":" + org["_org_id"].astype(str),
        "CORDIS:" + org["country_alpha2"].astype(str) + ":" + org["org_name_norm"].astype(str),
    )
    org["_pic_guess"] = org["_org_id"].astype(str).str.replace(r"\D+", "", regex=True).str.extract(r"([0-9]{8,10})", expand=False).fillna("")

    # project fields
    keep_proj = [c for c in [
        "id", "acronym", "title",
        "objective", "abstract", "summary", "content",
        "startDate", "endDate",
        "frameworkProgramme", "programmeDivisionTitle", "programmeDivision",
        "topic", "topics", "call"
    ] if c in proj.columns]
    proj2 = proj[keep_proj].copy()

    # abstract resolution
    abs_col = pick_col(proj2, "abstract", "summary", "content")
    proj2["abstract"] = proj2[abs_col].astype("string").fillna("").str.strip() if abs_col else ""
    proj2["objective"] = proj2["objective"].astype("string").fillna("").str.strip() if "objective" in proj2.columns else ""

    # merge org x project
    df = org.merge(proj2, left_on="projectID", right_on="id", how="left", suffixes=("", "_p"))

    # year
    df["year"] = pd.to_datetime(df.get("startDate"), errors="coerce").dt.year
    end_dt = pd.to_datetime(df.get("endDate"), errors="coerce")
    df["project_status"] = np.where(
        end_dt.isna(),
        "Unknown",
        np.where(end_dt >= pd.Timestamp(date.today()), "Open", "Closed"),
    )

    # country name/alpha3
    df["country_name"] = df["country_alpha2"].apply(country_name)
    df["country_alpha3"] = df["country_alpha2"].apply(country_alpha3)

    # entity type
    at = pick_col(df, "activityType")
    df["entity_type"] = df[at].apply(classify_entity) if at else "Unknown"

    # section (coalesce)
    section_cols = [c for c in ["programmeDivisionTitle", "programmeDivision", "topic", "topics", "call", "frameworkProgramme"] if c in df.columns]
    if section_cols:
        s = None
        for c in section_cols:
            col = df[c].astype("string").fillna("").astype(str).str.strip()
            col = col.where(col.str.len() > 0, np.nan)
            s = col if s is None else s.where(s.notna(), col)
        df["section"] = s.fillna(label)
    else:
        df["section"] = label

    # Theme uses title+acronym+objective+abstract and produces a single inferred
    # label per row. Because CORDIS is merged at participant level above, the
    # same project theme is repeated across participant rows, but projects are
    # not exploded into multiple rows with different themes.
    title_s = df.get("title", pd.Series([""] * len(df))).fillna("").astype(str)
    acr_s = df.get("acronym", pd.Series([""] * len(df))).fillna("").astype(str)
    obj_s = df.get("objective", pd.Series([""] * len(df))).fillna("").astype(str)
    abs_s = df.get("abstract", pd.Series([""] * len(df))).fillna("").astype(str)
    # Vectorized theme inference (single regex pass per theme — fast)
    combined_text = title_s + " " + acr_s + " " + obj_s + " " + abs_s
    df["theme"] = infer_themes_vectorized(combined_text).values
    df["value_chain_stage"] = [infer_value_chain_stage(t, a, o, ab) for t, a, o, ab in zip(title_s, acr_s, obj_s, abs_s)]
    pic_from_actor = df["actor_id"].astype(str).str.extract(r"([0-9]{8,10})$", expand=False).fillna("")
    df["pic"] = np.where(df["_pic_guess"].astype(str).str.len() > 0, df["_pic_guess"].astype(str), pic_from_actor)

    out = pd.DataFrame({
        "source": "CORDIS",
        "program": label,
        "section": df["section"].astype(str),
        "year": df["year"],
        "projectID": df.get("projectID").astype("string").fillna("").astype(str),
        "acronym": df.get("acronym", "").astype("string").fillna("").astype(str),
        "title": df.get("title", "").astype("string").fillna("").astype(str),
        "objective": df.get("objective", "").astype("string").fillna("").astype(str),
        "abstract": df.get("abstract", "").astype("string").fillna("").astype(str),
        "actor_id": df["actor_id"].astype(str),
        "pic": df["pic"].astype(str),
        "org_name": df["org_name"].astype(str),
        "entity_type": df["entity_type"].astype(str),
        "country_alpha2": df["country_alpha2"].astype(str),
        "country_alpha3": df["country_alpha3"].astype(str),
        "country_name": df["country_name"].astype(str),
        "amount_eur": df["ecContribution"],
        "theme": df["theme"].astype(str),
        "value_chain_stage": df["value_chain_stage"].astype(str),
        "project_status": df["project_status"].astype(str),
    })

    return out

# ============================================================
# External connectors loader (optional, via connectors_manifest.csv)
# ============================================================
def _read_connector_payload(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path, low_memory=False)
        if path.suffix.lower() in {".json", ".jsonl"}:
            txt = path.read_text(encoding="utf-8", errors="ignore").strip()
            if not txt:
                return pd.DataFrame()
            if path.suffix.lower() == ".jsonl":
                rows = [json.loads(line) for line in txt.splitlines() if line.strip()]
                return pd.json_normalize(rows) if rows else pd.DataFrame()
            js = json.loads(txt)
            if isinstance(js, list):
                return pd.json_normalize(js)
            if isinstance(js, dict):
                for k in ("items", "results", "data", "projects", "rows", "value"):
                    if isinstance(js.get(k), list):
                        return pd.json_normalize(js[k])
                return pd.json_normalize([js])
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _connector_frame_to_schema(df: pd.DataFrame, connector_id: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_SCHEMA_COLS())

    def pick_series(*cols: str) -> pd.Series:
        c = pick_col(df, *cols)
        if c:
            return df[c]
        return pd.Series([np.nan] * len(df))

    source = pick_series("source", "origin", "provider", "data_source").astype("string").fillna("").astype(str).str.strip()
    source = np.where(pd.Series(source).astype(str).str.len() > 0, source, str(connector_id).upper())
    program = pick_series("program", "programme", "framework", "call_programme", "programme_name").astype("string").fillna("").astype(str).str.strip()
    program = np.where(pd.Series(program).astype(str).str.len() > 0, program, str(connector_id))
    section = pick_series("section", "topic", "call", "type", "category").astype("string").fillna("").astype(str).str.strip()
    section = np.where(pd.Series(section).astype(str).str.len() > 0, section, "External connector")

    date_s = pick_series("date", "start_date", "publication_date", "updated_at", "created_at")
    year = pd.to_numeric(pick_series("year", "annee", "call_year"), errors="coerce")
    if year.isna().all():
        year = pd.to_datetime(date_s, errors="coerce").dt.year

    title = pick_series("title", "name", "project_title", "projectName", "objet", "intitule").astype("string").fillna("").astype(str).str.strip()
    objective = pick_series("objective", "project_objective", "description", "summary").astype("string").fillna("").astype(str).str.strip()
    abstract = pick_series("abstract", "project_abstract", "notes", "content").astype("string").fillna("").astype(str).str.strip()

    org_name = pick_series("org_name", "organization", "organisation", "beneficiary", "nomBeneficiaire", "participant_name").astype("string").fillna("").astype(str).str.strip()
    actor_id = pick_series("actor_id", "participant_id", "beneficiary_id", "organization_id", "org_id", "idBeneficiaire", "siret", "siren", "pic").astype("string").fillna("").astype(str).str.strip()
    org_norm = org_name.apply(norm_name)
    actor_id = np.where(
        pd.Series(actor_id).astype(str).str.len() > 0,
        pd.Series(actor_id).astype(str),
        str(connector_id).upper() + ":" + org_norm.astype(str),
    )

    pic = pick_series("pic", "organisation_pic", "organization_pic", "participant_pic").astype("string").fillna("").astype(str)
    pic = pic.str.replace(r"\D+", "", regex=True)

    country_name = pick_series("country_name", "country", "pays").astype("string").fillna("").astype(str).str.strip()
    country_alpha2 = pick_series("country_alpha2", "iso2", "country_code2").astype("string").fillna("").astype(str).str.strip().str.upper()
    country_alpha3 = pick_series("country_alpha3", "iso3", "country_code3").astype("string").fillna("").astype(str).str.strip().str.upper()
    country_alpha2 = np.where(pd.Series(country_alpha2).astype(str).str.len() > 0, country_alpha2, "UN")
    country_alpha3 = np.where(pd.Series(country_alpha3).astype(str).str.len() > 0, country_alpha3, "UNK")
    country_name = np.where(pd.Series(country_name).astype(str).str.len() > 0, country_name, "Unknown")

    amount = pick_series("amount_eur", "budget_eur", "amount", "grant_amount", "funding", "montant")
    amount_num = pd.to_numeric(amount.astype(str).str.replace(",", ".", regex=False), errors="coerce").fillna(0.0)

    project_id_src = pick_series("projectID", "project_id", "id", "reference", "projectRef", "proposal_id").astype("string").fillna("").astype(str).str.strip()
    pid_fallback = (
        str(connector_id).upper()
        + ":"
        + pd.Series(np.arange(len(df)), dtype="int64").astype(str)
    )
    project_id = np.where(project_id_src.str.len() > 0, project_id_src, pid_fallback)

    theme_src = pick_series("theme", "domain", "sector", "topic").astype("string").fillna("").astype(str).str.strip()
    stage_src = pick_series("value_chain_stage", "stage", "chain_stage").astype("string").fillna("").astype(str).str.strip()
    status_src = pick_series("project_status", "status").astype("string").fillna("").astype(str).str.strip()

    text_for_inference = (
        title.fillna("").astype(str)
        + " "
        + objective.fillna("").astype(str)
        + " "
        + abstract.fillna("").astype(str)
    )
    # External connectors keep one theme value per row, either provided by the
    # source or inferred as a single fallback label.
    inferred_themes = infer_themes_vectorized(text_for_inference)
    theme = np.where(pd.Series(theme_src).astype(str).str.len() > 0, theme_src, inferred_themes)
    value_chain_stage = np.where(pd.Series(stage_src).astype(str).str.len() > 0, stage_src, [infer_value_chain_stage(str(x)) for x in text_for_inference])
    project_status = np.where(pd.Series(status_src).astype(str).str.len() > 0, status_src, "Unknown")

    out = pd.DataFrame({
        "source": pd.Series(source).astype("string").fillna("").astype(str),
        "program": pd.Series(program).astype("string").fillna("").astype(str),
        "section": pd.Series(section).astype("string").fillna("").astype(str),
        "year": year,
        "projectID": pd.Series(project_id).astype("string").fillna("").astype(str),
        "acronym": "",
        "title": title,
        "objective": objective,
        "abstract": abstract,
        "actor_id": pd.Series(actor_id).astype("string").fillna("").astype(str),
        "pic": pd.Series(pic).astype("string").fillna("").astype(str),
        "org_name": org_name,
        "entity_type": pick_series("entity_type", "organization_type", "organisation_type").astype("string").fillna("Unknown").astype(str),
        "country_alpha2": pd.Series(country_alpha2).astype("string").fillna("UN").astype(str),
        "country_alpha3": pd.Series(country_alpha3).astype("string").fillna("UNK").astype(str),
        "country_name": pd.Series(country_name).astype("string").fillna("Unknown").astype(str),
        "amount_eur": amount_num,
        "theme": pd.Series(theme).astype("string").fillna("").astype(str),
        "value_chain_stage": pd.Series(value_chain_stage).astype("string").fillna("").astype(str),
        "project_status": pd.Series(project_status).astype("string").fillna("Unknown").astype(str),
    })
    return out


def load_external_connectors(data_dir: Path) -> pd.DataFrame:
    manifest = data_dir / "external" / "connectors_manifest.csv"
    if not manifest.exists():
        return pd.DataFrame(columns=_SCHEMA_COLS())
    try:
        m = pd.read_csv(manifest, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame(columns=_SCHEMA_COLS())
    if m.empty:
        return pd.DataFrame(columns=_SCHEMA_COLS())

    out_frames: List[pd.DataFrame] = []
    for _, row in m.iterrows():
        enabled = str(row.get("enabled", "false")).strip().lower() in {"1", "true", "yes", "y", "oui"}
        if not enabled:
            continue
        connector_id = str(row.get("connector_id", "")).strip() or f"connector_{_}"
        out_file = str(row.get("output_file", "")).strip()
        if not out_file:
            continue
        p = Path(out_file)
        if not p.is_absolute():
            p = (BASE_DIR / out_file).resolve()
        raw_df = _read_connector_payload(p)
        if raw_df.empty:
            continue
        out_frames.append(_connector_frame_to_schema(raw_df, connector_id))

    if not out_frames:
        return pd.DataFrame(columns=_SCHEMA_COLS())
    return pd.concat(out_frames, ignore_index=True)


# ============================================================
# Schema + cleaning
# ============================================================
def _SCHEMA_COLS() -> List[str]:
    return [
        "source", "program", "section", "year",
        "projectID", "acronym", "title", "objective", "abstract",
        "actor_id", "pic", "org_name", "entity_type",
        "country_alpha2", "country_alpha3", "country_name",
        "amount_eur", "theme", "value_chain_stage", "project_status",
    ]


def _enforce_schema(out: pd.DataFrame) -> pd.DataFrame:
    for c in _SCHEMA_COLS():
        if c not in out.columns:
            out[c] = ""

    # types
    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    out["amount_eur"] = pd.to_numeric(out["amount_eur"], errors="coerce")

    # trim strings
    for c in ["source", "program", "section", "projectID", "acronym", "title", "objective", "abstract",
              "actor_id", "pic", "org_name", "entity_type", "country_alpha2", "country_alpha3", "country_name",
              "theme", "value_chain_stage", "project_status"]:
        out[c] = out[c].astype("string").fillna("").astype(str).str.strip()

    # keep only valid rows
    out = out[out["projectID"].str.len() > 0].copy()
    out = out[out["actor_id"].str.len() > 0].copy()
    out = out[out["org_name"].str.len() > 0].copy()
    out = out[out["country_alpha3"].str.len() > 0].copy()

    out = out.dropna(subset=["year"]).copy()
    out["year"] = out["year"].astype(int)

    # amounts: keep >=0, fillna to 0
    out["amount_eur"] = out["amount_eur"].fillna(0.0)
    out = out[out["amount_eur"] >= 0].copy()

    # stable ordering (useful for diffs)
    out = out.sort_values(["source", "program", "year", "projectID", "actor_id"]).reset_index(drop=True)

    return out[_SCHEMA_COLS()].copy()


def _norm_col_name(x: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(x).strip().lower()).strip("_")


def _load_actor_group_map(path: Path) -> pd.DataFrame:
    cols = ["actor_id", "pic", "group_id", "group_name", "is_funder"]
    if not path.exists():
        return pd.DataFrame(columns=cols)

    try:
        raw = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame(columns=cols)

    raw.columns = [_norm_col_name(c) for c in raw.columns]

    aliases = {
        "actor_id": {"actor_id", "actorid", "participant_actor_id", "participant_id", "entity_actor_id"},
        "pic": {"pic", "participant_pic", "organisation_pic", "organization_pic"},
        "group_id": {"group_id", "group", "group_key", "parent_group_id", "company_group_id", "tic"},
        "group_name": {"group_name", "group_label", "parent_group", "company_group", "enterprise_group", "group_display"},
        "is_funder": {"is_funder", "funder", "is_financer", "financeur", "is_funding_body", "funding_body"},
    }

    out = pd.DataFrame(index=raw.index)
    for col, names in aliases.items():
        found = next((c for c in raw.columns if c in names), None)
        out[col] = raw[found].astype("string").fillna("").astype(str).str.strip() if found else ""

    out["pic"] = out["pic"].astype(str).str.replace(r"\D+", "", regex=True)
    out["is_funder"] = out["is_funder"].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y", "oui", "vrai", "t"})
    out = out[(out["actor_id"].astype(str).str.len() > 0) | (out["pic"].astype(str).str.len() > 0)].copy()
    if out.empty:
        return pd.DataFrame(columns=cols)

    out["group_id"] = np.where(
        out["group_id"].astype(str).str.len() > 0,
        out["group_id"].astype(str),
        np.where(
            out["group_name"].astype(str).str.len() > 0,
            out["group_name"].astype(str),
            np.where(out["pic"].astype(str).str.len() > 0, "PIC:" + out["pic"].astype(str), out["actor_id"].astype(str)),
        ),
    )
    out["group_name"] = np.where(out["group_name"].astype(str).str.len() > 0, out["group_name"].astype(str), out["group_id"].astype(str))

    return out[cols].drop_duplicates().reset_index(drop=True)


def _join_unique(series: pd.Series, sep: str = " | ", top: int = 20) -> str:
    vals = [str(x).strip() for x in series.dropna().astype(str).tolist() if str(x).strip()]
    if not vals:
        return ""
    uniq = sorted(set(vals))
    if len(uniq) > top:
        uniq = uniq[:top] + ["..."]
    return sep.join(uniq)


def build_master_actor_tables(base_df: pd.DataFrame, out_dir: Path, actor_group_map_path: Path) -> None:
    """
    Builds durable master tables used for actor/group/PIC analyses and network views.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    m = base_df.copy()
    m["pic"] = m["pic"].astype("string").fillna("").astype(str).str.replace(r"\D+", "", regex=True).str.extract(r"([0-9]{8,10})", expand=False).fillna("")

    map_df = _load_actor_group_map(actor_group_map_path)
    by_actor = map_df[map_df["actor_id"].astype(str).str.len() > 0][["actor_id", "group_id", "group_name", "is_funder"]].drop_duplicates("actor_id")
    by_pic = map_df[map_df["pic"].astype(str).str.len() > 0][["pic", "group_id", "group_name", "is_funder"]].drop_duplicates("pic")

    m = m.merge(by_actor, on="actor_id", how="left", suffixes=("", "_actor"))
    m = m.merge(by_pic, on="pic", how="left", suffixes=("", "_pic"))
    m["group_id"] = np.where(
        m["group_id"].astype("string").fillna("").astype(str).str.len() > 0,
        m["group_id"].astype(str),
        np.where(
            m["group_id_pic"].astype("string").fillna("").astype(str).str.len() > 0,
            m["group_id_pic"].astype(str),
            m["actor_id"].astype(str),
        ),
    )
    m["group_name"] = np.where(
        m["group_name"].astype("string").fillna("").astype(str).str.len() > 0,
        m["group_name"].astype(str),
        np.where(
            m["group_name_pic"].astype("string").fillna("").astype(str).str.len() > 0,
            m["group_name_pic"].astype(str),
            m["org_name"].astype(str),
        ),
    )
    m["is_funder"] = (
        m["is_funder"].astype("boolean").fillna(False).astype(bool)
        | m["is_funder_pic"].astype("boolean").fillna(False).astype(bool)
    )

    actor_master = (
        m.groupby(["actor_id", "pic", "org_name", "entity_type", "country_name", "group_id", "group_name", "is_funder"], as_index=False)
        .agg(
            n_projects=("projectID", "nunique"),
            budget_eur=("amount_eur", "sum"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            sources=("source", _join_unique),
            programs=("program", _join_unique),
        )
        .sort_values(["budget_eur", "n_projects"], ascending=False)
    )

    group_master = (
        m.groupby(["group_id", "group_name", "is_funder"], as_index=False)
        .agg(
            n_actor_ids=("actor_id", "nunique"),
            n_projects=("projectID", "nunique"),
            budget_eur=("amount_eur", "sum"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            countries=("country_name", _join_unique),
            entity_types=("entity_type", _join_unique),
        )
        .sort_values(["budget_eur", "n_projects"], ascending=False)
    )

    project_actor_links = (
        m[
            [
                "projectID", "year", "theme", "value_chain_stage", "project_status",
                "actor_id", "pic", "group_id", "group_name", "is_funder",
                "org_name", "entity_type", "country_name", "amount_eur",
            ]
        ]
        .copy()
        .sort_values(["year", "projectID", "group_id", "actor_id"])
        .reset_index(drop=True)
    )

    _atomic_write_csv(actor_master, out_dir / "actor_master.csv")
    _atomic_write_parquet(actor_master, out_dir / "actor_master.parquet")
    _atomic_write_csv(group_master, out_dir / "group_master.csv")
    _atomic_write_parquet(group_master, out_dir / "group_master.parquet")
    _atomic_write_csv(project_actor_links, out_dir / "project_actor_links.csv")
    _atomic_write_parquet(project_actor_links, out_dir / "project_actor_links.parquet")


# ============================================================
# Main build
# ============================================================
def build_processed_dataset(raw_dir: Path, out_csv: Path) -> None:
    cordis_root = raw_dir / "cordis"

    dfs: List[pd.DataFrame] = []

    he = cordis_root / "horizon_europe"
    h2 = cordis_root / "h2020"

    if he.exists():
        dfs.append(load_cordis_program("Horizon Europe", he))
    else:
        print("[WARN] Missing:", he)

    if h2.exists():
        dfs.append(load_cordis_program("Horizon 2020", h2))
    else:
        print("[WARN] Missing:", h2)

    # Optional connector outputs (API/MCP), if enabled and available.
    dfs.append(load_external_connectors(raw_dir.parent))

    out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=_SCHEMA_COLS())
    out = _enforce_schema(out)

    # write CSV + Parquet atomically
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    _atomic_write_csv(out, out_csv)
    out_parquet = out_csv.with_suffix(".parquet")
    _atomic_write_parquet(out, out_parquet)

    data_dir = out_csv.parent.parent
    actor_group_map_path = data_dir / "external" / "actor_groups.csv"
    build_master_actor_tables(out, out_csv.parent, actor_group_map_path)

    print(f"[OK] Wrote CSV: {out_csv}")
    print(f"[OK] Wrote Parquet: {out_parquet}")
    print(f"[OK] Wrote actor/group master tables in: {out_csv.parent}")
    print(f"[OK] Rows: {len(out):,}".replace(",", " "))


def main() -> None:
    base = Path(__file__).resolve().parent
    raw_dir = base / "data" / "raw"
    out_csv = base / "data" / "processed" / "subsidy_base.csv"
    build_processed_dataset(raw_dir=raw_dir, out_csv=out_csv)


if __name__ == "__main__":
    main()