from __future__ import annotations

from typing import Iterable, List, Optional
import html
import json
import re

from cordis_taxonomy import CORDIS_DOMAIN_UI_FR

THEME_EN_TO_FR = {
    "Climate Change and Environment": "Climat et environnement",
    "Digital Economy": "Économie numérique",
    "Energy": "Énergie",
    "Food and Natural Resources": "Alimentation et ressources naturelles",
    "Fundamental Research": "Recherche fondamentale",
    "Health": "Santé",
    "Industrial Technologies": "Technologies industrielles",
    "Security": "Sécurité",
    "Society": "Société",
    "Space": "Espace",
    "Transport and Mobility": "Transport et mobilité",
    "Hydrogen (H2)": "Hydrogène (H2)",
    "Solar (PV/CSP)": "Solaire (PV/CSP)",
    "Wind": "Éolien",
    "Bioenergy & SAF": "Bioénergies & SAF",
    "CCUS": "CCUS (carbone)",
    "Nuclear & SMR": "Nucléaire & SMR",
    "Batteries & Storage": "Batteries & stockage",
    "AI & Digital": "IA & numérique",
    "Advanced materials": "Matériaux avancés",
    "E-mobility": "Mobilité électrique",
    "Climate & Environment": "Climat & environnement",
    "Industry & Manufacturing": "Industrie & production",
    "Transport & Aviation": "Transport & aviation",
    "Health & Biotech": "Santé & biotechnologies",
    "Agriculture & Food": "Agriculture & alimentation",
    "Security & Resilience": "Sécurité & résilience",
    "Other": "Autres",
}

CORDIS_THEME_PREFIX_LABELS = [
    ("HORIZON-JU-EUROHPC-", ("EuroHPC JU", "EuroHPC JU")),
    ("HORIZON-JTI-CLEANH2-", ("Clean Hydrogen JU", "Clean Hydrogen JU")),
    ("HORIZON-JU-CLEANH2-", ("Clean Hydrogen JU", "Clean Hydrogen JU")),
    ("HORIZON-MSCA-", ("Actions Marie Sklodowska-Curie", "Marie Sklodowska-Curie Actions")),
    ("MSCA-IF-", ("Actions Marie Sklodowska-Curie", "Marie Sklodowska-Curie Actions")),
    ("ERC-", ("Conseil européen de la recherche", "European Research Council")),
    ("EIC-SMEINST-", ("Instrument PME EIC", "EIC SME Instrument")),
    ("HORIZON-EIC-", ("Conseil européen de l'innovation", "European Innovation Council")),
    ("HORIZON-HLTH-", ("Santé", "Health")),
    ("HORIZON-CL4-", ("Numérique et industrie", "Digital and industry")),
    ("HORIZON-CL5-", ("Climat, énergie et mobilité", "Climate, energy and mobility")),
    ("HORIZON-CL6-", ("Alimentation, bioéconomie et environnement", "Food, bioeconomy and environment")),
    ("HORIZON-CL3-", ("Sécurité civile", "Civil security")),
    ("INNOSUP-", ("Soutien à l'innovation", "Innovation support")),
    ("ICT-", ("ICT", "ICT")),
    ("NMBP-", ("Matériaux et production", "Materials and production")),
]

TOKEN_LABELS = {
    "ADG": ("Advanced Grant", "Advanced Grant"),
    "COG": ("Consolidator Grant", "Consolidator Grant"),
    "STG": ("Starting Grant", "Starting Grant"),
    "SYG": ("Synergy Grant", "Synergy Grant"),
    "POC": ("Proof of Concept", "Proof of Concept"),
    "AI": ("IA", "AI"),
    "HPC": ("HPC", "HPC"),
    "SMEINST": ("Instrument PME", "SME Instrument"),
    "EUROHPC": ("EuroHPC", "EuroHPC"),
    "CLEANH2": ("Hydrogène propre", "Clean Hydrogen"),
    "HLTH": ("Santé", "Health"),
    "ICT": ("ICT", "ICT"),
    "NMBP": ("Matériaux et production", "Materials and production"),
    "CL3": ("Cluster 3", "Cluster 3"),
    "CL4": ("Cluster 4", "Cluster 4"),
    "CL5": ("Cluster 5", "Cluster 5"),
    "CL6": ("Cluster 6", "Cluster 6"),
    "RIA": ("RIA", "RIA"),
    "IA": ("IA", "IA"),
    "CSA": ("CSA", "CSA"),
}

DROP_TOKENS = {
    "HORIZON", "JU", "JTI", "ERC", "MSCA", "EIC", "CALL", "TOPIC",
    "WORK", "PROGRAMME", "PROGRAM", "TWO", "STAGE", "SINGLE", "STAGES",
    "IBA", "FPA", "PCP", "PPI",
}

_CODE_RE = re.compile(r"[A-Z0-9]+(?:[-_][A-Z0-9]+){2,}")
_YEAR_RE = re.compile(r"(?:19|20)\d{2}")
_SPLIT_RE = re.compile(r"[-_/]+")


def _pick(labels: tuple[str, str], lang: str) -> str:
    return labels[0] if lang == "FR" else labels[1]


def _clean_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _title_token(token: str) -> str:
    token = token.replace('.', ' ').strip()
    token = re.sub(r"\s+", " ", token)
    if not token:
        return ""
    if token.isupper() and len(token) <= 5:
        return token
    return token.replace('  ', ' ').title()


def _token_label(token: str, lang: str) -> str:
    token = str(token or "").strip()
    if not token:
        return ""
    upper = token.upper()
    if upper in DROP_TOKENS:
        return ""
    if upper in TOKEN_LABELS:
        return _pick(TOKEN_LABELS[upper], lang)
    if _YEAR_RE.fullmatch(token):
        return token
    if re.fullmatch(r"[A-Z]\d+", upper):
        return upper
    if re.fullmatch(r"\d{1,2}", token):
        return ""
    if re.fullmatch(r"[A-Z]{2,8}", upper):
        return upper
    return _title_token(token)


def _tail_labels(tokens: List[str], lang: str) -> List[str]:
    labels: List[str] = []
    seen = set()
    pending_short_numbers: List[str] = []
    for token in tokens:
        token = str(token or "").strip()
        if not token:
            continue
        if _YEAR_RE.fullmatch(token):
            key = ("year", token)
            if key not in seen:
                labels.append(token)
                seen.add(key)
            continue
        if re.fullmatch(r"\d{1,2}", token):
            pending_short_numbers.append(token)
            continue
        label = _token_label(token, lang)
        if not label:
            continue
        key = ("label", label)
        if key not in seen:
            labels.append(label)
            seen.add(key)
    if not labels and pending_short_numbers:
        call_label = f"Appel {pending_short_numbers[0]}" if lang == "FR" else f"Call {pending_short_numbers[0]}"
        labels.append(call_label)
    return labels[:3]


def _erc_label(value: str, lang: str) -> str:
    parts = _SPLIT_RE.split(value)
    base = "Conseil européen de la recherche" if lang == "FR" else "European Research Council"
    scheme = next((_pick(TOKEN_LABELS[p.upper()], lang) for p in parts if p.upper() in {"ADG", "COG", "STG", "SYG", "POC"}), "")
    year = next((p for p in parts if _YEAR_RE.fullmatch(p)), "")
    kept = [base]
    if scheme:
        kept.append(scheme)
    if year:
        kept.append(year)
    return " · ".join(kept)


def _prefix_label(value: str, lang: str) -> str:
    upper = value.upper()
    for prefix, labels in CORDIS_THEME_PREFIX_LABELS:
        if upper.startswith(prefix):
            base = _pick(labels, lang)
            rest = value[len(prefix):]
            tails = _tail_labels(_SPLIT_RE.split(rest), lang)
            return base if not tails else f"{base} · {' · '.join(tails)}"
    return ""


def _humanize_code_like(value: str, lang: str) -> str:
    value = _clean_spaces(value)
    if not value:
        return ""
    upper = value.upper()
    if upper.startswith("ERC-"):
        return _erc_label(value, lang)
    prefixed = _prefix_label(value, lang)
    if prefixed:
        return prefixed
    if _CODE_RE.fullmatch(upper):
        tokens = _SPLIT_RE.split(value)
        lead = _token_label(tokens[0], lang)
        tails = _tail_labels(tokens[1:], lang)
        kept = [x for x in [lead, *tails] if x]
        return " · ".join(kept) if kept else value.replace('_', ' · ').replace('-', ' · ')
    return value


def domain_raw_to_display(raw: str, lang: str = "FR") -> str:
    value = _clean_spaces(raw)
    if not value:
        return ""
    if lang == "FR":
        return CORDIS_DOMAIN_UI_FR.get(value, THEME_EN_TO_FR.get(value, value))
    return value


def theme_raw_to_display(raw: str, lang: str = "FR", review_label: Optional[str] = None) -> str:
    value = _clean_spaces(raw)
    if not value:
        return ""
    if value in {"Other", "Multidisciplinary", "Multi-domain"}:
        return review_label or ("Multithématique" if lang == "FR" else "Multi-domain")
    if lang == "FR" and value in THEME_EN_TO_FR:
        return THEME_EN_TO_FR[value]
    human = _humanize_code_like(value, lang)
    return human or value


def scientific_subthemes_compact(raw: object, limit: int = 3) -> str:
    if raw is None:
        return ""
    values: List[str] = []
    if isinstance(raw, (list, tuple, set)):
        values = [str(x).strip() for x in raw if str(x).strip()]
    else:
        txt = str(raw).strip()
        if not txt or txt == "[]":
            return ""
        try:
            js = json.loads(txt)
            if isinstance(js, list):
                values = [str(x).strip() for x in js if str(x).strip()]
            else:
                values = [txt]
        except Exception:
            values = [x.strip() for x in re.split(r"\s*(?:\||;|,)\s*", txt) if x.strip()]
    if not values:
        return ""
    if len(values) <= limit:
        return ", ".join(values)
    return ", ".join(values[:limit]) + f" +{len(values) - limit}"


def format_dimension_value(dimension: str, raw: object, lang: str = "FR", review_label: Optional[str] = None) -> str:
    value = _clean_spaces(raw)
    if not value:
        return ""
    dim = str(dimension or "").strip()
    if dim in {"cordis_domain_ui", "domain"}:
        return domain_raw_to_display(value, lang)
    if dim in {"cordis_theme_primary", "theme", "section", "program"}:
        return theme_raw_to_display(value, lang, review_label=review_label)
    if dim in {"scientific_subthemes", "sub_theme"}:
        return scientific_subthemes_compact(value, limit=3)
    return value


def build_dimension_hover_html(
    dimension: str,
    raw: object,
    lang: str = "FR",
    value_line: Optional[str] = None,
    source: Optional[str] = None,
    review_label: Optional[str] = None,
    extra_lines: Optional[Iterable[str]] = None,
) -> str:
    raw_value = _clean_spaces(raw)
    display_value = format_dimension_value(dimension, raw_value, lang=lang, review_label=review_label)
    raw_label = "Code CORDIS" if lang == "FR" else "CORDIS code"
    source_label = "Source du thème principal" if lang == "FR" else "Primary theme source"
    parts = [f"<b>{html.escape(display_value or raw_value)}</b>"]
    if value_line:
        parts.append(html.escape(str(value_line)))
    if extra_lines:
        for line in extra_lines:
            if str(line or "").strip():
                parts.append(html.escape(str(line)))
    if raw_value and raw_value != (display_value or raw_value):
        parts.append(f"{html.escape(raw_label)}: {html.escape(raw_value)}")
    if source and str(source).strip() and str(dimension) in {"theme", "cordis_theme_primary"}:
        parts.append(f"{html.escape(source_label)}: {html.escape(str(source).strip())}")
    return "<br>".join(parts)
