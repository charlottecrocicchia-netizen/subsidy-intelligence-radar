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
    ("HORIZON-EUROHPC-JU-", ("EuroHPC JU", "EuroHPC JU")),
    ("EUROHPC-", ("EuroHPC", "EuroHPC")),
    ("HORIZON-KDT-JU-", ("Technologies numériques clés JU", "Key Digital Technologies JU")),
    ("HORIZON-JU-CHIPS-", ("Chips JU", "Chips JU")),
    ("HORIZON-CHIPS-", ("Chips", "Chips")),
    ("HORIZON-JU-CBE-", ("Europe biosourcée circulaire JU", "Circular Bio-based Europe JU")),
    ("SESAR-", ("SESAR", "SESAR")),
    ("IMI2-", ("Initiative médicaments innovants 2", "Innovative Medicines Initiative 2")),
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
    ("INFRIA-", ("Infrastructures de recherche", "Research infrastructures")),
    ("SGA-FETFLAG-HBP-", ("Human Brain Project", "Human Brain Project")),
    ("FETFLAG-", ("FET Flagships", "FET Flagships")),
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
    "IMI2": ("Initiative médicaments innovants 2", "Innovative Medicines Initiative 2"),
    "KDT": ("Technologies numériques clés", "Key Digital Technologies"),
    "CHIPS": ("Chips", "Chips"),
    "CBE": ("Europe biosourcée circulaire", "Circular Bio-based Europe"),
    "SESAR": ("SESAR", "SESAR"),
    "PF": ("Bourses postdoctorales", "Postdoctoral Fellowships"),
    "HUMAN": ("Humain", "Human"),
    "DIGITAL": ("Numérique", "Digital"),
    "EMERGING": ("Technologies émergentes", "Emerging technologies"),
    "TWIN": ("Jumeau numérique", "Digital twin"),
    "TRANSITION": ("Transition", "Transition"),
    "DN": ("Réseaux doctoraux", "Doctoral Networks"),
    "COFUND": ("Cofinancement", "Co-funding"),
    "INFRAIA": ("Infrastructures de recherche", "Research infrastructures"),
    "FETFLAG": ("FET Flagships", "FET Flagships"),
    "HBP": ("Human Brain Project", "Human Brain Project"),
    "SGA": ("Accord spécifique", "Specific grant agreement"),
    "HLTH": ("Santé", "Health"),
    "ICT": ("ICT", "ICT"),
    "NMBP": ("Matériaux et production", "Materials and production"),
    "CL3": ("Cluster 3", "Cluster 3"),
    "CL4": ("Cluster 4", "Cluster 4"),
    "CL5": ("Cluster 5", "Cluster 5"),
    "CL6": ("Cluster 6", "Cluster 6"),
    "RIA": ("Recherche et innovation", "Research and innovation"),
    "IA": ("IA", "IA"),
    "CSA": ("Coordination et soutien", "Coordination and support"),
}

DROP_TOKENS = {
    "HORIZON", "JU", "JTI", "ERC", "MSCA", "EIC", "CALL", "TOPIC",
    "WORK", "PROGRAMME", "PROGRAM", "TWO", "STAGE", "SINGLE", "STAGES",
    "IBA", "FPA", "PCP", "PPI",
}

_CODE_RE = re.compile(r"[A-Z0-9]+(?:[-_.][A-Z0-9]+){2,}")
_YEAR_RE = re.compile(r"(?:19|20)\d{2}")
_SPLIT_RE = re.compile(r"[-_/]+")
_DOT_SPLIT_RE = re.compile(r"[-_/.]+")

# ── Programme hierarchy codes → friendly names (FR, EN) ──
# Handles dot-separated codes (HORIZON.2.5) and H2020 hierarchy codes.
# Looked up by exact match first, then longest-prefix match.
PROGRAMME_HIERARCHY_LABELS: dict[str, tuple[str, str]] = {
    # Horizon Europe — Pillars
    "HORIZON.1":     ("Excellence scientifique", "Excellent Science"),
    "HORIZON.1.1":   ("ERC – Conseil européen de la recherche", "ERC – European Research Council"),
    "HORIZON.1.2":   ("Actions Marie Skłodowska-Curie", "Marie Skłodowska-Curie Actions"),
    "HORIZON.1.3":   ("Infrastructures de recherche", "Research Infrastructures"),
    "HORIZON.1.4":   ("Excellence scientifique – Autre", "Excellent Science – Other"),
    "HORIZON.2":     ("Défis mondiaux & compétitivité", "Global Challenges & Competitiveness"),
    "HORIZON.2.1":   ("Santé", "Health"),
    "HORIZON.2.2":   ("Culture, créativité & société inclusive", "Culture, Creativity & Inclusive Society"),
    "HORIZON.2.3":   ("Sécurité civile", "Civil Security for Society"),
    "HORIZON.2.4":   ("Numérique, industrie & espace", "Digital, Industry & Space"),
    "HORIZON.2.5":   ("Climat, énergie & mobilité", "Climate, Energy & Mobility"),
    "HORIZON.2.6":   ("Alimentation, bioéconomie & environnement", "Food, Bioeconomy & Environment"),
    "HORIZON.3":     ("Europe innovante", "Innovative Europe"),
    "HORIZON.3.1":   ("Conseil européen de l'innovation (EIC)", "European Innovation Council (EIC)"),
    "HORIZON.3.2":   ("Écosystèmes d'innovation européens", "European Innovation Ecosystems"),
    "HORIZON.3.3":   ("Institut européen d'innovation (EIT)", "European Institute of Innovation & Technology (EIT)"),
    "HORIZON.4":     ("Élargissement & renforcement de l'EER", "Widening & Strengthening ERA"),
    "HORIZON.4.1":   ("Élargissement de la participation", "Widening Participation"),
    "HORIZON.4.2":   ("Renforcement de l'EER", "Strengthening ERA"),
    # H2020 — Pillars & priorities
    "H2020-EU.1":    ("Excellence scientifique", "Excellent Science"),
    "H2020-EU.1.1":  ("ERC – Conseil européen de la recherche", "ERC – European Research Council"),
    "H2020-EU.1.2":  ("Technologies futures & émergentes (FET)", "Future & Emerging Technologies (FET)"),
    "H2020-EU.1.3":  ("Actions Marie Skłodowska-Curie", "Marie Skłodowska-Curie Actions"),
    "H2020-EU.1.4":  ("Infrastructures de recherche", "Research Infrastructures"),
    "H2020-EU.2":    ("Primauté industrielle", "Industrial Leadership"),
    "H2020-EU.2.1":  ("Technologies clés génériques (LEIT)", "Leadership in Enabling Technologies (LEIT)"),
    "H2020-EU.2.1.1": ("TIC – Technologies de l'information", "ICT – Information & Communication Technologies"),
    "H2020-EU.2.1.2": ("Nanotechnologies & matériaux avancés", "Nanotechnologies & Advanced Materials"),
    "H2020-EU.2.1.3": ("Fabrication & transformation avancées", "Advanced Manufacturing & Processing"),
    "H2020-EU.2.1.4": ("Biotechnologie", "Biotechnology"),
    "H2020-EU.2.1.5": ("Espace", "Space"),
    "H2020-EU.2.1.6": ("Technologies énergétiques bas-carbone", "Low-Carbon Energy Technologies"),
    "H2020-EU.2.3":  ("Innovation dans les PME", "Innovation in SMEs"),
    "H2020-EU.3":    ("Défis de société", "Societal Challenges"),
    "H2020-EU.3.1":  ("Santé & bien-être", "Health & Wellbeing"),
    "H2020-EU.3.2":  ("Sécurité alimentaire & bioéconomie", "Food Security & Bioeconomy"),
    "H2020-EU.3.3":  ("Énergie sûre, propre & efficace", "Secure, Clean & Efficient Energy"),
    "H2020-EU.3.4":  ("Transports intelligents & intégrés", "Smart, Green & Integrated Transport"),
    "H2020-EU.3.5":  ("Climat, environnement & ressources", "Climate, Environment & Resources"),
    "H2020-EU.3.6":  ("Sociétés inclusives & innovantes", "Inclusive & Innovative Societies"),
    "H2020-EU.3.7":  ("Sociétés sûres", "Secure Societies"),
    "H2020-EU.4":    ("Diffusion de l'excellence & élargissement", "Spreading Excellence & Widening"),
    "H2020-EU.5":    ("Science avec & pour la société", "Science with and for Society"),
    # H2020 — specific programmes
    "H2020-Euratom":  ("Recherche & formation Euratom", "Euratom Research & Training"),
    "H2020-EC":       ("Commission européenne – H2020", "European Commission – H2020"),
    "H2020-SEWP":     ("Diffusion de l'excellence", "Spreading Excellence & Widening Participation"),
    # Framework / instrument short codes
    "ERC":            ("ERC – Conseil européen de la recherche", "ERC – European Research Council"),
    "MSCA":           ("Actions Marie Skłodowska-Curie", "Marie Skłodowska-Curie Actions"),
    "FET":            ("Technologies futures & émergentes", "Future & Emerging Technologies"),
    "SMEInst":        ("Instrument PME", "SME Instrument"),
    "EIC":            ("Conseil européen de l'innovation (EIC)", "European Innovation Council (EIC)"),
    "EIT":            ("Institut européen d'innovation (EIT)", "European Institute of Innovation & Technology (EIT)"),
    "FCH":            ("Piles à combustible & hydrogène", "Fuel Cells & Hydrogen"),
    "FCH2":           ("Piles à combustible & hydrogène 2", "Fuel Cells & Hydrogen 2"),
    "CleanSky":       ("Aviation propre – Clean Sky", "Clean Sky"),
    "CleanSky2":      ("Aviation propre – Clean Sky 2", "Clean Sky 2"),
    "CleanH2":        ("Hydrogène propre", "Clean Hydrogen"),
    "CleanAviation":  ("Aviation propre", "Clean Aviation"),
    "SESAR":          ("SESAR – Gestion du trafic aérien", "SESAR – Air Traffic Management"),
    "S2R":            ("Shift2Rail – Innovation ferroviaire", "Shift2Rail – Railway Innovation"),
    "ERSR":           ("Europe's Rail", "Europe's Rail"),
    "ECSEL":          ("ECSEL – Composants & systèmes électroniques", "ECSEL – Electronic Components & Systems"),
    "KDT":            ("Technologies numériques clés", "Key Digital Technologies"),
    "BBI":            ("Industries biosourcées", "Bio-Based Industries"),
    "CBE":            ("Europe biosourcée circulaire", "Circular Bio-based Europe"),
    "IMI":            ("Initiative médicaments innovants", "Innovative Medicines Initiative"),
    "IMI2":           ("Initiative médicaments innovants 2", "Innovative Medicines Initiative 2"),
    "IHI":            ("Initiative pour la santé innovante", "Innovative Health Initiative"),
    "EDCTP":          ("EDCTP – Partenariat essais cliniques", "EDCTP – Clinical Trials Partnership"),
    "EDCTP3":         ("Santé mondiale EDCTP3", "Global Health EDCTP3"),
    "EuroHPC":        ("Calcul haute performance européen", "European High Performance Computing"),
    "SNS":            ("Réseaux & services intelligents", "Smart Networks & Services"),
    "LIFE":           ("Programme LIFE – Environnement & climat", "LIFE Programme – Environment & Climate"),
    "CEF":            ("Mécanisme pour l'interconnexion en Europe", "Connecting Europe Facility"),
    "DEP":            ("Programme Europe numérique", "Digital Europe Programme"),
    "RFCS":           ("Fonds de recherche charbon & acier", "Research Fund for Coal & Steel"),
    "Euratom":        ("Recherche & formation Euratom", "Euratom Research & Training"),
    "InnovFund":      ("Fonds pour l'innovation", "Innovation Fund"),
    # FP7
    "FP7":            ("7e programme-cadre (FP7)", "Framework Programme 7 (FP7)"),
    "FP7-ENERGY":     ("FP7 – Énergie", "FP7 – Energy"),
    "FP7-NMP":        ("FP7 – Nanosciences & nouveaux matériaux", "FP7 – Nanosciences & New Materials"),
    "FP7-TRANSPORT":  ("FP7 – Transport", "FP7 – Transport"),
    "FP7-ENV":        ("FP7 – Environnement", "FP7 – Environment"),
    "FP7-HEALTH":     ("FP7 – Santé", "FP7 – Health"),
    "FP7-ICT":        ("FP7 – TIC", "FP7 – ICT"),
    "FP7-KBBE":       ("FP7 – Alimentation & biotechnologie", "FP7 – Food & Biotechnology"),
    "FP7-PEOPLE":     ("FP7 – Marie Curie", "FP7 – People (Marie Curie)"),
    "FP7-IDEAS":      ("FP7 – Idées (ERC)", "FP7 – Ideas (ERC)"),
    "FP7-SPA":        ("FP7 – Espace", "FP7 – Space"),
    "FP7-SEC":        ("FP7 – Sécurité", "FP7 – Security"),
    "FP7-SSH":        ("FP7 – Sciences sociales & humaines", "FP7 – Social Sciences & Humanities"),
    "FP7-INFRASTRUCTURES": ("FP7 – Infrastructures", "FP7 – Infrastructures"),
    "FP7-JTI":        ("FP7 – Initiatives technologiques conjointes", "FP7 – Joint Technology Initiatives"),
    "FP7-EURATOM":    ("FP7 – Euratom", "FP7 – Euratom"),
    # Horizon Europe topic-level prefixes
    "HORIZON-CL1":    ("Santé", "Health"),
    "HORIZON-CL2":    ("Culture, créativité & société inclusive", "Culture, Creativity & Inclusive Society"),
    "HORIZON-CL3":    ("Sécurité civile", "Civil Security for Society"),
    "HORIZON-CL4":    ("Numérique, industrie & espace", "Digital, Industry & Space"),
    "HORIZON-CL5":    ("Climat, énergie & mobilité", "Climate, Energy & Mobility"),
    "HORIZON-CL6":    ("Alimentation, bioéconomie & environnement", "Food, Bioeconomy & Environment"),
    "HORIZON-ERC":    ("ERC – Conseil européen de la recherche", "ERC – European Research Council"),
    "HORIZON-MSCA":   ("Actions Marie Skłodowska-Curie", "Marie Skłodowska-Curie Actions"),
    "HORIZON-INFRA":  ("Infrastructures de recherche", "Research Infrastructures"),
    "HORIZON-EIC":    ("Conseil européen de l'innovation (EIC)", "European Innovation Council (EIC)"),
    "HORIZON-EIE":    ("Écosystèmes d'innovation européens", "European Innovation Ecosystems"),
    "HORIZON-WIDERA": ("Élargissement de la participation", "Widening Participation"),
    "HORIZON-JU":     ("Entreprises communes", "Joint Undertakings"),
    "HORIZON-MISS":   ("Missions de l'UE", "EU Missions"),
    "HORIZON-AG":     ("Autres actions Horizon Europe", "Other Horizon Europe Actions"),
}


def _programme_hierarchy_lookup(value: str, lang: str) -> str:
    """Look up a programme code in the hierarchy mapping.
    Tries exact match first, then longest prefix match."""
    if not value:
        return ""
    # exact match
    entry = PROGRAMME_HIERARCHY_LABELS.get(value)
    if entry:
        return _pick(entry, lang)
    # longest-prefix match (must be at least 3 chars)
    best_prefix = ""
    best_entry: tuple[str, str] | None = None
    for code, labels in PROGRAMME_HIERARCHY_LABELS.items():
        if value.startswith(code) and len(code) > len(best_prefix):
            best_prefix = code
            best_entry = labels
    if best_entry and len(best_prefix) >= 3:
        return _pick(best_entry, lang)
    return ""


def _pick(labels: tuple[str, str], lang: str) -> str:
    return labels[0] if lang == "FR" else labels[1]


def _clean_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _apply_display_mode(display_value: str, raw_value: str, display_mode: str = "friendly") -> str:
    mode = str(display_mode or "friendly").strip().lower()
    display = _clean_spaces(display_value)
    raw = _clean_spaces(raw_value)
    if mode == "code":
        return raw or display
    if mode == "both" and raw and display and raw != display:
        return f"{display} [{raw}]"
    return display or raw


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


def _is_administrative_label(label: str) -> bool:
    value = _clean_spaces(label)
    return (not value) or bool(re.fullmatch(r"(?:19|20)\d{2}(?:-(?:19|20)\d{2})?", value)) or bool(re.fullmatch(r"(?:Appel|Call)\s+\d+", value))


def _tail_labels(tokens: List[str], lang: str) -> List[str]:
    labels: List[str] = []
    years: List[str] = []
    seen = set()
    pending_short_numbers: List[str] = []
    for token in tokens:
        token = str(token or "").strip()
        if not token:
            continue
        if _YEAR_RE.fullmatch(token):
            if token not in years:
                years.append(token)
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
    if years and labels:
        labels = [years[0], *labels]
    elif years and not labels:
        labels = [years[0] if len(years) == 1 else f"{years[0]}-{years[-1]}"]
    if not labels and pending_short_numbers:
        call_label = f"Appel {pending_short_numbers[0]}" if lang == "FR" else f"Call {pending_short_numbers[0]}"
        labels.append(call_label)
    return labels[:3]


def _erc_label(value: str, lang: str) -> str:
    parts = _SPLIT_RE.split(value)
    base = "Conseil européen de la recherche" if lang == "FR" else "European Research Council"
    scheme = next((_pick(TOKEN_LABELS[p.upper()], lang) for p in parts if p.upper() in {"ADG", "COG", "STG", "SYG", "POC"}), "")
    kept = [base]
    if scheme:
        kept.append(scheme)
    return " · ".join(kept)


def _prefix_label(value: str, lang: str) -> str:
    upper = value.upper()
    for prefix, labels in CORDIS_THEME_PREFIX_LABELS:
        if upper.startswith(prefix):
            base = _pick(labels, lang)
            rest = value[len(prefix):]
            tails = _tail_labels(_SPLIT_RE.split(rest), lang)
            semantic_tails = [t for t in tails if not _is_administrative_label(str(t))]
            if semantic_tails:
                return f"{base} · {' · '.join(semantic_tails[:2])}"
            return base
    return ""


def _humanize_code_like(value: str, lang: str) -> str:
    value = _clean_spaces(value)
    if not value:
        return ""
    # 1) Try programme hierarchy lookup (handles HORIZON.2.5, H2020-EU.3.3, etc.)
    hierarchy_label = _programme_hierarchy_lookup(value, lang)
    if hierarchy_label:
        return hierarchy_label
    upper = value.upper()
    if upper.startswith("ERC-"):
        return _erc_label(value, lang)
    prefixed = _prefix_label(value, lang)
    if prefixed:
        return prefixed
    if _CODE_RE.fullmatch(upper):
        tokens = _DOT_SPLIT_RE.split(value)
        lead = _token_label(tokens[0], lang)
        tails = _tail_labels(tokens[1:], lang)
        semantic_tails = [t for t in tails if not _is_administrative_label(str(t))]
        kept = [x for x in [lead, *(semantic_tails[:2] if semantic_tails else [])] if x]
        return " · ".join(kept) if kept else (lead or value.replace('_', ' · ').replace('-', ' · '))
    return value


def domain_raw_to_display(raw: str, lang: str = "FR", display_mode: str = "friendly") -> str:
    value = _clean_spaces(raw)
    if not value:
        return ""
    display = CORDIS_DOMAIN_UI_FR.get(value, THEME_EN_TO_FR.get(value, value)) if lang == "FR" else value
    return _apply_display_mode(display, value, display_mode=display_mode)


def theme_raw_to_display(raw: str, lang: str = "FR", review_label: Optional[str] = None, display_mode: str = "friendly") -> str:
    value = _clean_spaces(raw)
    if not value:
        return ""
    if value in {"Other", "Multidisciplinary", "Multi-domain"}:
        display = review_label or ("Multithématique" if lang == "FR" else "Multi-domain")
        return _apply_display_mode(display, value, display_mode=display_mode)
    if lang == "FR" and value in THEME_EN_TO_FR:
        display = THEME_EN_TO_FR[value]
        return _apply_display_mode(display, value, display_mode=display_mode)
    human = _humanize_code_like(value, lang)
    return _apply_display_mode(human or value, value, display_mode=display_mode)


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


def format_dimension_value(dimension: str, raw: object, lang: str = "FR", review_label: Optional[str] = None, display_mode: str = "friendly") -> str:
    value = _clean_spaces(raw)
    if not value:
        return ""
    dim = str(dimension or "").strip()
    if dim in {"cordis_domain_ui", "domain"}:
        return domain_raw_to_display(value, lang, display_mode=display_mode)
    if dim in {"cordis_theme_primary", "theme", "section", "program"}:
        return theme_raw_to_display(value, lang, review_label=review_label, display_mode=display_mode)
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
    display_mode: str = "friendly",
) -> str:
    raw_value = _clean_spaces(raw)
    display_value = format_dimension_value(dimension, raw_value, lang=lang, review_label=review_label, display_mode=display_mode)
    raw_label = "Code programme" if lang == "FR" else "Programme code"
    source_label = "Source du thème" if lang == "FR" else "Theme source"
    parts = [f"<b>{html.escape(display_value or raw_value)}</b>"]
    if value_line:
        parts.append(html.escape(str(value_line)))
    if extra_lines:
        for line in extra_lines:
            if str(line or "").strip():
                parts.append(html.escape(str(line)))
    if str(display_mode or "friendly").strip().lower() == "friendly" and raw_value and raw_value != (display_value or raw_value):
        parts.append(f"{html.escape(raw_label)}: {html.escape(raw_value)}")
    if source and str(source).strip() and str(dimension) in {"theme", "cordis_theme_primary"}:
        parts.append(f"{html.escape(source_label)}: {html.escape(str(source).strip())}")
    return "<br>".join(parts)