from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple, List
import subprocess
import sys

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go


# ============================================================
# Paths (reproductible)
# ============================================================
BASE_DIR = Path(__file__).resolve().parent  # .../Script

DATA_DIR = BASE_DIR / "data"
DATA_PATH = DATA_DIR / "processed" / "subsidy_base.csv"
EVENTS_PATH = DATA_DIR / "external" / "events.csv"

# Offline scripts (ONLY on refresh click)
BUILD_EVENTS_SCRIPT = BASE_DIR / "build_events.py"
PROCESS_BUILD_SCRIPT = BASE_DIR / "process_build.py"  # underscore
PIPELINE_SCRIPT = BASE_DIR / "pipeline.py"

# Use the current interpreter (works with .venv + Streamlit Cloud)
EVENTS_PYTHON = sys.executable


# ============================================================
# Page config + style
# ============================================================
st.set_page_config(page_title="Subsidy Intelligence Radar", layout="wide", initial_sidebar_state="expanded")

st.markdown(
    """
<style>
  .main { background-color: #0f1622; }
  section[data-testid="stSidebar"] { background-color: #111a28; }
  h1,h2,h3,h4 { color: #eaf2ff; }
  .stCaption { color: rgba(234,242,255,0.75) !important; }

  div[data-testid="metric-container"] {
    background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.10);
    padding: 14px;
    border-radius: 14px;
    box-shadow: 0 8px 18px rgba(0,0,0,0.25);
  }

  .stTabs [data-baseweb="tab"]{
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.10);
    border-radius: 12px;
    padding: 10px 14px;
    color: rgba(234,242,255,0.88);
  }
  .stTabs [aria-selected="true"]{
    background: rgba(255,255,255,0.10);
    border: 1px solid rgba(255,255,255,0.18);
  }

  div[data-baseweb="select"] span { color: rgba(234,242,255,0.92); }
  div[data-baseweb="tag"] { background: rgba(255,255,255,0.10) !important; }

  .stDataFrame { background: rgba(255,255,255,0.02); border-radius: 10px; }
</style>
""",
    unsafe_allow_html=True,
)


# ============================================================
# Colors
# ============================================================
R2G = [
    (0.00, "rgba(220,20,60,0.25)"),
    (0.50, "rgba(255,165,0,0.60)"),
    (1.00, "rgba(0,128,0,1.00)"),
]


# ============================================================
# Taxonomy + translations
# ============================================================
ONETECH_THEMES_EN = {
    "Hydrogen (H2)",
    "Solar (PV/CSP)",
    "Wind",
    "Bioenergy & SAF",
    "CCUS",
    "Nuclear & SMR",
    "Batteries & Storage",
    "AI & Digital",
    "Advanced materials",
    "E-mobility",
}

THEME_EN_TO_FR = {
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
    "Space": "Espace",
    "Agriculture & Food": "Agriculture & alimentation",
    "Security & Resilience": "Sécurité & résilience",
    "Other": "Autres",
}

ENTITY_EN_TO_FR = {
    "Private company": "Entreprise (privé)",
    "Research & academia": "Recherche & académique",
    "Public": "Public",
    "Other": "Autre",
    "Unknown": "Inconnu",
}

TAG_TO_THEMES = {
    "H2": {"Hydrogen (H2)", "Hydrogène (H2)"},
    "CCUS": {"CCUS", "CCUS (carbone)"},
    "BAT": {"Batteries & Storage", "Batteries & stockage"},
    "NUC": {"Nuclear & SMR", "Nucléaire & SMR"},
    "SOL": {"Solar (PV/CSP)", "Solaire (PV/CSP)"},
    "WND": {"Wind", "Éolien"},
    "BIO": {"Bioenergy & SAF", "Bioénergies & SAF"},
    "AI": {"AI & Digital", "IA & numérique"},
    "MAT": {"Advanced materials", "Matériaux avancés"},
    "EMOB": {"E-mobility", "Mobilité électrique"},
    "REG": set(),
    "GEO": set(),
    "FIN": set(),
    "IND": set(),
}

I18N: Dict[str, Dict[str, str]] = {
    "FR": {
        "language": "Langue",
        "title": "🛰️ Subsidy Intelligence Radar",
        "subtitle": "UE (CORDIS) — filtres, benchmarks, géographie, tendances, comparaison, macro & actualités.",
        "reset": "🧹 Réinitialiser",
        "refresh": "🔄 Rafraîchir",
        "refresh_hint": "Met à jour CORDIS + events (offline), puis recharge l’app.",
        "filters": "🧩 Filtres",
        "sources": "Sources",
        "onetech_only": "Limiter au périmètre OneTech",
        "programmes": "Programmes",
        "period": "Période (année de démarrage)",
        "use_section": "Filtrer par section",
        "section": "Section (UE / Programme / Topic)",
        "themes": "Thématiques",
        "entity": "Type d’entité",
        "countries": "Pays",
        "kpis": "📊 Indicateurs clés",
        "budget_total": "Budget total",
        "n_projects": "Nombre de projets",
        "n_actors": "Acteurs uniques",
        "avg_ticket": "Ticket moyen / projet",
        "median_ticket": "Ticket médian / projet",
        "top10_share": "Part Top10 acteurs",
        "hhi": "Concentration (HHI)",
        "no_data": "Aucune donnée pour cette sélection. Élargis les filtres.",
        "tab_overview": "📌 Vue d’ensemble",
        "tab_geo": "🌍 Géographie",
        "tab_comp": "🏆 Benchmark acteurs",
        "tab_trends": "📈 Tendances",
        "tab_compare": "🆚 Comparaison",
        "tab_macro": "🧭 Macro & actualités",
        "tab_actor": "👤 Fiche acteur",
        "tab_data": "🔎 Données",
        "tab_quality": "🧪 Qualité",
        "tab_help": "💬 Aide",
        "tab_guide": "📘 Guide",
        "zoom_on": "Zoom",
        "projection": "Projection",
        "borders": "Frontières & côtes",
        "labels": "Libellés continents",
        "top_countries": "Top 15 pays",
        "benchmark_mode": "Vue benchmark",
        "bm_scatter": "Positionnement (log/log)",
        "bm_treemap": "Treemap lisible",
        "bm_top": "Classements",
        "pct_threshold": "Seuil budget (percentile)",
        "topn": "Top N (après seuil)",
        "search_actor": "Recherche texte (contient…)",
        "actor_picker": "Sélection d’un acteur",
        "actor_picker_hint": "Tape pour chercher dans la liste.",
        "legend_tip": "Astuce : clique sur la légende pour masquer/afficher une série.",
        "scatter_explain": (
            "- Chaque point = **un acteur** (organisation), agrégé sur le périmètre filtré.\n"
            "- Axe X = **nombre de projets distincts** où cet acteur apparaît.\n"
            "- Axe Y = **budget total capté** par cet acteur.\n\n"
            "**Pourquoi un projet compte pour plusieurs acteurs ?**\n"
            "Un projet a plusieurs participants : il est compté pour **chaque** acteur participant.\n"
        ),
        "dimension": "Dimension d’analyse",
        "dim_theme": "Thématique",
        "dim_section": "Section",
        "mode": "Mode",
        "mode_abs": "Budget (absolu)",
        "mode_share": "Part (% par année)",
        "drivers": "Top moteurs (période)",
        "compare_title": "Comparaison de périodes (Δ part de budget)",
        "period_a": "Période A",
        "period_b": "Période B",
        "compare_caption": "Comparaison en **% du budget total** de chaque période, Δ en **points de %**.",
        "actor_profile": "Fiche acteur",
        "actor_trend": "Évolution (budget & projets)",
        "actor_mix_theme": "Mix thématique",
        "actor_mix_country": "Mix géographique",
        "actor_partners": "Top co-participants",
        "download": "⬇️ Télécharger CSV (filtres actuels)",
        "quality_title": "Qualité des données",
        "help_title": "Aide (FAQ + interprétation)",
        "guide_title": "Guide de lecture",
        "data_warning": "Dataset volumineux : affichage paginé pour éviter MessageSizeError.",
        "columns": "Colonnes affichées",
        "filter_text": "Filtre texte (sur colonnes affichées)",
        "rows_per_page": "Lignes / page",
        "page": "Page",
        "last_update": "Dernière MAJ",
        "last_update_data": "Données",
        "last_update_events": "Événements",
        "macro_title": "Macro & actualités — analyse approfondie",
        "macro_subtitle": "Onglet indépendant : filtres macro internes (ne dépend pas de la sidebar).",
        "macro_match": "Correspondance des événements",
        "macro_match_theme": "Par thématique (theme)",
        "macro_match_tag": "Par tag (tag → thématiques)",
        "macro_pick_theme": "Thématique",
        "macro_pick_tag": "Tag",
        "macro_metric": "Indicateur",
        "macro_overlay": "Afficher les événements sur le graphe",
        "macro_event_select": "Sélection d’un événement",
        "macro_window": "Fenêtre autour de l’événement (± années)",
        "macro_signal": "Signal autour de l’événement",
        "macro_examples": "Exemples de projets dans la fenêtre",
        "macro_no_events": "Aucun événement disponible pour cette sélection.",
        "macro_events": "Événements clés",
        "rebuild_ok": "Mise à jour terminée.",
        "rebuild_fail": "Mise à jour incomplète / erreur.",
        "logs": "Logs",
        "macro_filters": "Filtres Macro (indépendants)",
    },
    "EN": {
        "language": "Language",
        "title": "🛰️ Subsidy Intelligence Radar",
        "subtitle": "EU (CORDIS) — filters, benchmarks, geography, trends, comparison, macro & news.",
        "reset": "🧹 Reset",
        "refresh": "🔄 Refresh (rebuild)",
        "refresh_hint": "Updates CORDIS + events (offline), then reloads the app.",
        "filters": "🧩 Filters",
        "sources": "Sources",
        "onetech_only": "Restrict to OneTech scope",
        "programmes": "Programmes",
        "period": "Period (start year)",
        "use_section": "Filter by section",
        "section": "Section (EU / Programme / Topic)",
        "themes": "Themes",
        "entity": "Entity type",
        "countries": "Countries",
        "kpis": "📊 Key indicators",
        "budget_total": "Total budget",
        "n_projects": "Projects",
        "n_actors": "Unique actors",
        "avg_ticket": "Avg ticket / project",
        "median_ticket": "Median ticket / project",
        "top10_share": "Top10 actors share",
        "hhi": "Concentration (HHI)",
        "no_data": "No data for this selection. Broaden the filters.",
        "tab_overview": "📌 Overview",
        "tab_geo": "🌍 Geography",
        "tab_comp": "🏆 Actor benchmark",
        "tab_trends": "📈 Trends",
        "tab_compare": "🆚 Compare",
        "tab_macro": "🧭 Macro & news",
        "tab_actor": "👤 Actor profile",
        "tab_data": "🔎 Data",
        "tab_quality": "🧪 Quality",
        "tab_help": "💬 Help",
        "tab_guide": "📘 Guide",
        "zoom_on": "Zoom",
        "projection": "Projection",
        "borders": "Borders & coastlines",
        "labels": "Continent labels",
        "top_countries": "Top 15 countries",
        "benchmark_mode": "Benchmark view",
        "bm_scatter": "Positioning (log/log)",
        "bm_treemap": "Readable treemap",
        "bm_top": "Rankings",
        "pct_threshold": "Budget threshold (percentile)",
        "topn": "Top N (after threshold)",
        "search_actor": "Text search (contains…)",
        "actor_picker": "Pick an actor",
        "actor_picker_hint": "Type to search in the list.",
        "legend_tip": "Tip: click the legend to hide/show a series.",
        "scatter_explain": (
            "- Each point = **one actor** (organisation), aggregated over current filters.\n"
            "- X axis = **# of distinct projects** where the actor appears.\n"
            "- Y axis = **total funding captured** by the actor.\n\n"
            "**Why can one project count for multiple actors?**\n"
            "A project has multiple participants: it is counted for **each** participating actor.\n"
        ),
        "dimension": "Analysis dimension",
        "dim_theme": "Theme",
        "dim_section": "Section",
        "mode": "Mode",
        "mode_abs": "Budget (absolute)",
        "mode_share": "Share (% per year)",
        "drivers": "Top drivers (period)",
        "compare_title": "Period comparison (Δ budget share)",
        "period_a": "Period A",
        "period_b": "Period B",
        "compare_caption": "Comparison as **% of total budget** in each period, Δ in **percentage points**.",
        "actor_profile": "Actor profile",
        "actor_trend": "Trend (budget & projects)",
        "actor_mix_theme": "Theme mix",
        "actor_mix_country": "Geography mix",
        "actor_partners": "Top co-participants",
        "download": "⬇️ Download CSV (current filters)",
        "quality_title": "Data quality",
        "help_title": "Help (FAQ + interpretation)",
        "guide_title": "Reading guide",
        "data_warning": "Large dataset: paginated display to avoid MessageSizeError.",
        "columns": "Displayed columns",
        "filter_text": "Text filter (on displayed columns)",
        "rows_per_page": "Rows / page",
        "page": "Page",
        "last_update": "Last update",
        "last_update_data": "Data",
        "last_update_events": "Events",
        "macro_title": "Macro & news — deep dive",
        "macro_subtitle": "Independent tab: internal macro filters (does not depend on sidebar).",
        "macro_match": "Event matching",
        "macro_match_theme": "By theme (theme)",
        "macro_match_tag": "By tag (tag → themes)",
        "macro_pick_theme": "Theme",
        "macro_pick_tag": "Tag",
        "macro_metric": "Metric",
        "macro_overlay": "Show events on chart",
        "macro_event_select": "Select an event",
        "macro_window": "Window around the event (± years)",
        "macro_signal": "Signal around the event",
        "macro_examples": "Example projects in the window",
        "macro_no_events": "No events for this selection.",
        "macro_events": "Key events",
        "rebuild_ok": "Update completed.",
        "rebuild_fail": "Update incomplete / failed.",
        "logs": "Logs",
        "macro_filters": "Macro filters (independent)",
    },
}


def t(lang: str, key: str) -> str:
    return I18N[lang].get(key, key)


# ============================================================
# Formatting
# ============================================================
def fmt_money(x: float, lang: str) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    x = float(x)
    ax = abs(x)
    big = "Md€" if lang == "FR" else "B€"
    if ax >= 1e9:
        return f"{x/1e9:,.2f} {big}".replace(",", " ")
    if ax >= 1e6:
        return f"{x/1e6:,.1f} M€".replace(",", " ")
    if ax >= 1e3:
        return f"{x/1e3:,.0f} k€".replace(",", " ")
    return f"{x:,.0f} €".replace(",", " ")


def fmt_pct(x: float, digits: int = 1) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{100.0 * float(x):.{digits}f}%"


def fmt_pp(delta_share: float, digits: int = 2, lang: str = "FR") -> str:
    if delta_share is None or (isinstance(delta_share, float) and np.isnan(delta_share)):
        return "—"
    return f"{100.0 * float(delta_share):+.{digits}f} {'pts' if lang=='FR' else 'pp'}"


def compute_top10_share_and_hhi(actor_budget: pd.Series) -> Tuple[float, float]:
    total = float(actor_budget.sum())
    if total <= 0:
        return 0.0, 0.0
    shares = (actor_budget / total).values
    hhi = float(np.sum(shares**2))
    top10 = float(actor_budget.sort_values(ascending=False).head(10).sum() / total)
    return top10, hhi


# ============================================================
# Loaders (cached) — NO rebuild here
# ============================================================
def _clean_text_series(s: pd.Series) -> pd.Series:
    s = s.astype("string").fillna("").str.strip()
    return s.replace({"nan": "", "None": "", "<NA>": ""})


@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    if not DATA_PATH.exists():
        return pd.DataFrame()

    df = pd.read_csv(DATA_PATH, low_memory=False)

    required = [
        "source", "program", "section", "year", "projectID",
        "actor_id", "org_name", "entity_type",
        "country_name", "country_alpha3",
        "theme", "title", "abstract", "amount_eur",
    ]
    for c in required:
        if c not in df.columns:
            df[c] = ""

    for c in [
        "source", "program", "section", "projectID",
        "actor_id", "org_name", "entity_type",
        "country_name", "country_alpha3",
        "theme", "title", "abstract"
    ]:
        df[c] = _clean_text_series(df[c])

    df["year"] = pd.to_numeric(df.get("year", np.nan), errors="coerce")
    df["amount_eur"] = pd.to_numeric(df.get("amount_eur", np.nan), errors="coerce")
    df = df.dropna(subset=["year", "amount_eur"]).copy()
    df["year"] = df["year"].astype(int)
    df = df[df["amount_eur"] >= 0].copy()
    df = df[df["projectID"] != ""].copy()
    df = df[df["country_alpha3"] != ""].copy()
    df = df[df["country_name"] != ""].copy()

    df["onetech_scope"] = df["theme"].astype(str).isin(ONETECH_THEMES_EN)
    return df


@st.cache_data(show_spinner=False)
def load_events() -> pd.DataFrame:
    cols = ["date", "theme", "tag", "title", "source", "impact_direction", "notes"]
    if not EVENTS_PATH.exists():
        return pd.DataFrame(columns=cols)

    ev = pd.read_csv(EVENTS_PATH)
    for c in cols:
        if c not in ev.columns:
            ev[c] = ""
    ev = ev[cols].copy()

    ev["date"] = pd.to_datetime(ev["date"], errors="coerce")
    ev = ev.dropna(subset=["date"]).copy()
    for c in ["theme", "tag", "title", "source", "impact_direction", "notes"]:
        ev[c] = ev[c].astype("string").fillna("").str.strip()

    ev["year"] = ev["date"].dt.year.astype(int)
    ev["event_id"] = (
        ev["tag"].astype(str).fillna("").str.strip()
        + "::" + ev["year"].astype(str)
        + "::" + ev["title"].astype(str).fillna("").str.strip()
    )
    return ev.sort_values(["tag", "date", "title"]).reset_index(drop=True)


# ============================================================
# Offline rebuild (ONLY on refresh click)
# ============================================================
def _run_script(script_path: Path, timeout_sec: int = 1800) -> Tuple[bool, str]:
    if not script_path.exists():
        return False, f"Script not found: {script_path}"

    py = EVENTS_PYTHON if script_path.name == "build_events.py" else sys.executable

    try:
        res = subprocess.run(
            [py, str(script_path)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout_sec,
        )
        out = (res.stdout or "").strip()
        err = (res.stderr or "").strip()
        msg = out if out else "OK"
        if err:
            msg = msg + "\n\n[stderr]\n" + err
        return True, msg
    except subprocess.TimeoutExpired:
        return False, f"Timeout after {timeout_sec}s: {script_path.name}"
    except subprocess.CalledProcessError as e:
        out = (e.stdout or "").strip()
        err = (e.stderr or "").strip()
        msg = "[FAILED]\n" + (out if out else "")
        if err:
            msg = msg + "\n\n[stderr]\n" + err
        return False, msg


def rebuild_all() -> Tuple[bool, Dict[str, str]]:
    logs: Dict[str, str] = {}

    # Prefer pipeline.py: it downloads + rebuilds the processed dataset.
    if PIPELINE_SCRIPT.exists():
        ok, msg = _run_script(PIPELINE_SCRIPT, timeout_sec=1800)
        logs["pipeline.py"] = msg
        if not ok:
            return False, logs
    elif PROCESS_BUILD_SCRIPT.exists():
        ok, msg = _run_script(PROCESS_BUILD_SCRIPT, timeout_sec=1800)
        logs["process_build.py"] = msg
        if not ok:
            return False, logs
    else:
        logs["data"] = "No pipeline.py / process_build.py found."
        return False, logs

    # Events (RSS + EUR-Lex)
    if BUILD_EVENTS_SCRIPT.exists():
        ok, msg = _run_script(BUILD_EVENTS_SCRIPT, timeout_sec=1800)
        logs["build_events.py"] = msg
        if not ok:
            return False, logs
    else:
        logs["build_events.py"] = "build_events.py not found."
        return False, logs

    return True, logs


# ============================================================
# UI mapping (raw <-> display) to keep filters stable FR/EN
# ============================================================
def theme_raw_to_display(raw: str, lang: str) -> str:
    if lang == "FR":
        return THEME_EN_TO_FR.get(raw, raw)
    return raw


def entity_raw_to_display(raw: str, lang: str) -> str:
    if lang == "FR":
        return ENTITY_EN_TO_FR.get(raw, raw)
    return raw


def display_to_theme_raw(display: str, lang: str, theme_disp_to_raw: Dict[str, str]) -> str:
    # using dict built from df ensures reversibility
    return theme_disp_to_raw.get(display, display)


def display_to_entity_raw(display: str, lang: str, entity_disp_to_raw: Dict[str, str]) -> str:
    return entity_disp_to_raw.get(display, display)


def _fmt_mtime(p: Path) -> str:
    from datetime import datetime
    try:
        ts = p.stat().st_mtime
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "—"


def reset_filters() -> None:
    for k in list(st.session_state.keys()):
        if k.startswith("f_") or k.startswith("macro_"):
            del st.session_state[k]


# ============================================================
# Sidebar: language + reset + refresh + last update + logs
# ============================================================
with st.sidebar:
    lang = st.radio("", ["FR", "EN"], index=0, horizontal=True, label_visibility="collapsed", key="ui_lang")
    st.caption(t(lang, "language"))

    st.caption(f"{t(lang,'last_update')} — {t(lang,'last_update_data')}: {_fmt_mtime(DATA_PATH)}")
    st.caption(f"{t(lang,'last_update')} — {t(lang,'last_update_events')}: {_fmt_mtime(EVENTS_PATH)}")

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        if st.button(t(lang, "reset"), use_container_width=True):
            reset_filters()
            st.cache_data.clear()
            st.rerun()

    with c2:
        if st.button(t(lang, "refresh"), use_container_width=True, help=t(lang, "refresh_hint")):
            with st.spinner("Mise à jour en cours (CORDIS + events)..." if lang == "FR" else "Updating (CORDIS + events)..."):
                ok, logs = rebuild_all()
            st.session_state["last_rebuild_ok"] = ok
            st.session_state["last_rebuild_logs"] = logs
            st.cache_data.clear()
            st.rerun()

    if "last_rebuild_logs" in st.session_state:
        st.divider()

        if st.session_state.get("last_rebuild_ok"):
            st.success(t(lang, "rebuild_ok"))
        else:
            st.warning(t(lang, "rebuild_fail"))

        with st.expander(t(lang, "logs"), expanded=False):
            for k, v in st.session_state["last_rebuild_logs"].items():
                st.markdown(f"**{k}**")
                st.code(v or "", language="text")


# ============================================================
# Load data
# ============================================================
df = load_data()

st.title(t(lang, "title"))
st.caption(t(lang, "subtitle"))

if df.empty:
    st.warning(f"Missing dataset: `{DATA_PATH}`")
    st.stop()

# Display columns
df["theme_display"] = df["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))
df["entity_display"] = df["entity_type"].astype(str).map(lambda x: entity_raw_to_display(x, lang))
df["program_display"] = df["program"].astype(str)

# build mapping for reversibility
theme_disp_to_raw: Dict[str, str] = {}
entity_disp_to_raw: Dict[str, str] = {}

for raw in df["theme"].dropna().astype(str).unique():
    disp = theme_raw_to_display(raw, lang)
    theme_disp_to_raw[disp] = raw

for raw in df["entity_type"].dropna().astype(str).unique():
    disp = entity_raw_to_display(raw, lang)
    entity_disp_to_raw[disp] = raw

# actor label disambiguation
dup = df.groupby("org_name")["actor_id"].nunique()
dups = set(dup[dup > 1].index.astype(str).tolist())
df["actor_label"] = np.where(
    df["org_name"].astype(str).isin(dups) & (df["org_name"] != ""),
    df["org_name"] + " — " + df["country_name"],
    df["org_name"],
)
df["actor_label"] = df["actor_label"].where(df["actor_label"] != "", df["actor_id"])


# ============================================================
# Default filters (raw) — only set once (stable across FR/EN)
# ============================================================
def _init_defaults_once(df_all: pd.DataFrame) -> None:
    if st.session_state.get("_defaults_inited", False):
        return

    st.session_state["_defaults_inited"] = True

    # sources/programmes default: all
    st.session_state["f_sources_raw"] = sorted([s for s in df_all["source"].astype(str).unique() if s.strip()])
    st.session_state["f_programmes_raw"] = sorted([p for p in df_all["program"].astype(str).unique() if p.strip()])

    # years default: full range
    st.session_state["f_years_raw"] = (int(df_all["year"].min()), int(df_all["year"].max()))

    # section filter off by default
    st.session_state["f_use_section"] = False
    st.session_state["f_sections_raw"] = []

    # onetech_only default = True
    st.session_state["f_onetech_only"] = True

    # themes default = OneTech themes present
    themes_present = sorted(set(df_all["theme"].astype(str).unique()))
    one_present = [x for x in themes_present if x in ONETECH_THEMES_EN]
    st.session_state["f_themes_raw"] = one_present if one_present else themes_present

    # entity default = all
    st.session_state["f_entity_raw"] = sorted([x for x in df_all["entity_type"].astype(str).unique() if x.strip()])

    # countries default = important list (fallback to top by budget if missing)
    important = [
        "France", "Germany", "Netherlands", "Spain", "Italy",
        "Belgium", "Sweden", "Denmark", "Finland", "Austria",
        "Poland", "Portugal", "Norway", "Switzerland", "United Kingdom",
        "Czech Republic", "Ireland", "Greece"
    ]
    countries_all = sorted([c for c in df_all["country_name"].astype(str).unique() if c.strip()])
    default_c = [c for c in important if c in countries_all]

    if not default_c:
        # fallback: top 10 countries by budget
        g = df_all.groupby("country_name")["amount_eur"].sum().sort_values(ascending=False)
        default_c = [c for c in g.head(10).index.astype(str).tolist() if c in countries_all]

    st.session_state["f_countries_raw"] = default_c if default_c else countries_all[:10]


_init_defaults_once(df)


# ============================================================
# Sidebar filters (UI shows display, state stores raw)
# ============================================================
with st.sidebar:
    st.header(t(lang, "filters"))

    # Sources
    sources = sorted([s for s in df["source"].astype(str).unique() if s.strip()])
    # set display state from raw (raw==display here)
    if "f_sources" not in st.session_state:
        st.session_state["f_sources"] = st.session_state.get("f_sources_raw", sources)
    sel_sources = st.multiselect(t(lang, "sources"), sources, default=st.session_state["f_sources"], key="f_sources")
    st.session_state["f_sources_raw"] = sel_sources

    # OneTech scope
    if "f_onetech_only" not in st.session_state:
        st.session_state["f_onetech_only"] = True
    onetech_only = st.checkbox(t(lang, "onetech_only"), value=st.session_state["f_onetech_only"], key="f_onetech_only")

    # Programmes
    programmes = sorted([p for p in df["program"].astype(str).unique() if p.strip()])
    if "f_prog" not in st.session_state:
        st.session_state["f_prog"] = st.session_state.get("f_programmes_raw", programmes)
    sel_programmes = st.multiselect(t(lang, "programmes"), programmes, default=st.session_state["f_prog"], key="f_prog")
    st.session_state["f_programmes_raw"] = sel_programmes

    # Year range (based on current scope sources+programmes)
    df_opt = df.copy()
    if sel_sources:
        df_opt = df_opt[df_opt["source"].astype(str).isin(sel_sources)]
    if sel_programmes:
        df_opt = df_opt[df_opt["program"].astype(str).isin(sel_programmes)]

    min_y = int(df_opt["year"].min())
    max_y = int(df_opt["year"].max())
    if "f_years" not in st.session_state:
        st.session_state["f_years"] = st.session_state.get("f_years_raw", (min_y, max_y))
    sel_years = st.slider(t(lang, "period"), min_y, max_y, st.session_state["f_years"], key="f_years")
    st.session_state["f_years_raw"] = sel_years

    # Section
    if "f_use_section" not in st.session_state:
        st.session_state["f_use_section"] = False
    use_section = st.checkbox(t(lang, "use_section"), value=st.session_state["f_use_section"], key="f_use_section")

    df_opt = df_opt[df_opt["year"].between(sel_years[0], sel_years[1])].copy()
    sections = sorted([s for s in df_opt["section"].astype(str).unique() if s.strip()])

    if "f_sections" not in st.session_state:
        st.session_state["f_sections"] = st.session_state.get("f_sections_raw", [])
    sel_sections = st.multiselect(t(lang, "section"), sections, default=st.session_state["f_sections"], key="f_sections") if use_section else []
    st.session_state["f_sections_raw"] = sel_sections

    # Themes: show display strings but store raw in f_themes_raw
    themes_raw_available = sorted([x for x in df_opt["theme"].astype(str).unique() if x.strip()])
    # default raw themes
    if "f_themes_raw" not in st.session_state:
        # should have been set by defaults init
        st.session_state["f_themes_raw"] = themes_raw_available

    # if onetech_only, restrict available themes in UI (but keep raw stable)
    if onetech_only:
        themes_raw_available_ui = [x for x in themes_raw_available if x in ONETECH_THEMES_EN]
        if not themes_raw_available_ui:
            themes_raw_available_ui = themes_raw_available
    else:
        themes_raw_available_ui = themes_raw_available

    theme_options_disp = [theme_raw_to_display(x, lang) for x in themes_raw_available_ui]
    # compute display default from stored raw intersect available
    raw_selected = [x for x in st.session_state["f_themes_raw"] if x in themes_raw_available_ui]
    if not raw_selected:
        # fallback (avoid empty)
        raw_selected = themes_raw_available_ui[: min(10, len(themes_raw_available_ui))]
        st.session_state["f_themes_raw"] = raw_selected

    default_disp = [theme_raw_to_display(x, lang) for x in raw_selected]

    # write into widget state so it remains stable across lang changes
    if "f_themes_disp" not in st.session_state:
        st.session_state["f_themes_disp"] = default_disp
    else:
        # if language changed, or options changed, recompute
        st.session_state["f_themes_disp"] = [d for d in st.session_state["f_themes_disp"] if d in theme_options_disp]
        if not st.session_state["f_themes_disp"]:
            st.session_state["f_themes_disp"] = default_disp

    sel_themes_disp = st.multiselect(t(lang, "themes"), theme_options_disp, default=st.session_state["f_themes_disp"], key="f_themes_disp")
    # update raw selection
    st.session_state["f_themes_raw"] = [display_to_theme_raw(d, lang, theme_disp_to_raw) for d in sel_themes_disp]

    # Entity: display but store raw
    entity_raw_available = sorted([x for x in df_opt["entity_type"].astype(str).unique() if x.strip()])
    if "f_entity_raw" not in st.session_state:
        st.session_state["f_entity_raw"] = entity_raw_available

    entity_options_disp = [entity_raw_to_display(x, lang) for x in entity_raw_available]
    raw_selected_e = [x for x in st.session_state["f_entity_raw"] if x in entity_raw_available]
    if not raw_selected_e:
        raw_selected_e = entity_raw_available
        st.session_state["f_entity_raw"] = raw_selected_e
    default_disp_e = [entity_raw_to_display(x, lang) for x in raw_selected_e]

    if "f_entity_disp" not in st.session_state:
        st.session_state["f_entity_disp"] = default_disp_e
    else:
        st.session_state["f_entity_disp"] = [d for d in st.session_state["f_entity_disp"] if d in entity_options_disp]
        if not st.session_state["f_entity_disp"]:
            st.session_state["f_entity_disp"] = default_disp_e

    sel_entity_disp = st.multiselect(t(lang, "entity"), entity_options_disp, default=st.session_state["f_entity_disp"], key="f_entity_disp")
    st.session_state["f_entity_raw"] = [display_to_entity_raw(d, lang, entity_disp_to_raw) for d in sel_entity_disp]

    # Countries: raw==display
    countries_all = sorted([c for c in df_opt["country_name"].astype(str).unique() if c.strip()])
    if "f_countries" not in st.session_state:
        st.session_state["f_countries"] = [c for c in st.session_state.get("f_countries_raw", []) if c in countries_all]
        if not st.session_state["f_countries"]:
            st.session_state["f_countries"] = countries_all[:10]
    sel_countries = st.multiselect(t(lang, "countries"), countries_all, default=st.session_state["f_countries"], key="f_countries")
    st.session_state["f_countries_raw"] = sel_countries


# ============================================================
# Apply filters (main scope)
# ============================================================
df_f = df.copy()

sel_sources = st.session_state.get("f_sources_raw", [])
sel_programmes = st.session_state.get("f_programmes_raw", [])
sel_years = st.session_state.get("f_years_raw", (int(df["year"].min()), int(df["year"].max())))
use_section = st.session_state.get("f_use_section", False)
sel_sections = st.session_state.get("f_sections_raw", [])
sel_themes_raw = st.session_state.get("f_themes_raw", [])
sel_entity_raw = st.session_state.get("f_entity_raw", [])
sel_countries = st.session_state.get("f_countries_raw", [])
onetech_only = st.session_state.get("f_onetech_only", True)

if sel_sources:
    df_f = df_f[df_f["source"].astype(str).isin(sel_sources)]
if sel_programmes:
    df_f = df_f[df_f["program"].astype(str).isin(sel_programmes)]
df_f = df_f[df_f["year"].between(sel_years[0], sel_years[1])]

if use_section and sel_sections:
    df_f = df_f[df_f["section"].astype(str).isin(sel_sections)]

if onetech_only:
    df_f = df_f[df_f["onetech_scope"] == True]

if sel_themes_raw:
    df_f = df_f[df_f["theme"].astype(str).isin(sel_themes_raw)]
if sel_entity_raw:
    df_f = df_f[df_f["entity_type"].astype(str).isin(sel_entity_raw)]
if sel_countries:
    df_f = df_f[df_f["country_name"].astype(str).isin(sel_countries)]


# ============================================================
# KPIs
# ============================================================
st.subheader(t(lang, "kpis"))
if df_f.empty:
    st.warning(t(lang, "no_data"))
    st.stop()

total_budget = float(df_f["amount_eur"].sum())
project_budget = df_f.groupby("projectID")["amount_eur"].sum()

df_actor = df_f[df_f["actor_id"].astype(str) != ""].copy()
actor_budget = df_actor.groupby("actor_id")["amount_eur"].sum()

nb_projects = int(project_budget.shape[0])
nb_actors = int(actor_budget.shape[0])
avg_ticket = total_budget / nb_projects if nb_projects else 0.0
median_ticket = float(project_budget.median()) if nb_projects else 0.0
top10_share, hhi = compute_top10_share_and_hhi(actor_budget) if nb_actors else (0.0, 0.0)

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric(t(lang, "budget_total"), fmt_money(total_budget, lang))
k2.metric(t(lang, "n_projects"), f"{nb_projects:,}".replace(",", " "))
k3.metric(t(lang, "n_actors"), f"{nb_actors:,}".replace(",", " "))
k4.metric(t(lang, "avg_ticket"), fmt_money(avg_ticket, lang))
k5.metric(t(lang, "median_ticket"), fmt_money(median_ticket, lang))
k6.metric(t(lang, "top10_share"), fmt_pct(top10_share, 1))
st.caption(f"{t(lang, 'hhi')}: {hhi:.3f}")
st.divider()


# ============================================================
# Tabs
# ============================================================
tab_overview, tab_geo, tab_comp, tab_trends, tab_compare, tab_macro, tab_actor, tab_data, tab_quality, tab_help, tab_guide = st.tabs(
    [
        t(lang, "tab_overview"),
        t(lang, "tab_geo"),
        t(lang, "tab_comp"),
        t(lang, "tab_trends"),
        t(lang, "tab_compare"),
        t(lang, "tab_macro"),
        t(lang, "tab_actor"),
        t(lang, "tab_data"),
        t(lang, "tab_quality"),
        t(lang, "tab_help"),
        t(lang, "tab_guide"),
    ]
)


# ============================================================
# TAB OVERVIEW
# ============================================================
with tab_overview:
    df_f["theme_display"] = df_f["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))
    df_f["entity_display"] = df_f["entity_type"].astype(str).map(lambda x: entity_raw_to_display(x, lang))

    st.markdown("### " + ("Allocation du budget par type d’entité" if lang == "FR" else "Budget allocation by entity type"))
    alloc = df_f.groupby("entity_display", as_index=False)["amount_eur"].sum().sort_values("amount_eur", ascending=False)
    fig_alloc = px.bar(
        alloc.iloc[::-1],
        x="amount_eur",
        y="entity_display",
        orientation="h",
        color="amount_eur",
        color_continuous_scale=R2G,
        height=420,
        labels={"amount_eur": "Budget (€)", "entity_display": ""},
    )
    fig_alloc.update_traces(
        customdata=np.stack([alloc["amount_eur"].apply(lambda v: fmt_money(v, lang)).iloc[::-1]], axis=-1),
        hovertemplate="<b>%{y}</b><br>Budget: %{customdata[0]}<extra></extra>",
    )
    fig_alloc.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
    st.plotly_chart(fig_alloc, use_container_width=True)

    st.divider()
    st.markdown("### " + ("Distribution des tickets (échelle log)" if lang == "FR" else "Ticket distribution (log scale)"))
    tb = project_budget[project_budget > 0]
    if tb.empty:
        st.info(t(lang, "no_data"))
    else:
        logv = np.log10(tb.values)
        fig_hist = px.histogram(x=logv, nbins=60, height=420, labels={"x": "log10(Budget €)"})
        fig_hist.update_layout(showlegend=False, xaxis_title="log10(Budget €)", yaxis_title="Count")
        st.plotly_chart(fig_hist, use_container_width=True)

    st.divider()
    st.markdown("### " + ("Courbe de Lorenz" if lang == "FR" else "Lorenz curve"))
    ab = actor_budget.sort_values()
    if ab.sum() <= 0 or len(ab) < 2:
        st.info(t(lang, "no_data"))
    else:
        cum = ab.cumsum() / ab.sum()
        x = np.arange(1, len(ab) + 1) / len(ab)
        lor = pd.DataFrame({"actors_share": x, "budget_share": cum.values})
        fig_l = px.line(lor, x="actors_share", y="budget_share", height=420)
        fig_l.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode="lines", name="Equality", line=dict(dash="dash")))
        fig_l.update_layout(xaxis_title="Actors (cumulative share)", yaxis_title="Budget (cumulative share)")
        st.plotly_chart(fig_l, use_container_width=True)


# ============================================================
# TAB GEO
# ============================================================
with tab_geo:
    geo = df_f.groupby(["country_alpha3", "country_name"], as_index=False)["amount_eur"].sum()
    geo["budget_str"] = geo["amount_eur"].apply(lambda v: fmt_money(v, lang))

    zoom_opts = ["Auto", "Europe", "World", "Africa", "Asia", "North America", "South America", "Oceania"]
    a, b, c, d = st.columns([1.2, 1.1, 1.3, 1.4])
    with a:
        zoom = st.selectbox(t(lang, "zoom_on"), zoom_opts, index=1)
    with b:
        projection = st.selectbox(t(lang, "projection"), ["natural earth", "mercator"], index=0)
    with c:
        show_borders = st.checkbox(t(lang, "borders"), value=True)
    with d:
        show_labels = st.checkbox(t(lang, "labels"), value=False)

    fig_map = px.choropleth(
        geo,
        locations="country_alpha3",
        color="amount_eur",
        hover_name="country_name",
        color_continuous_scale=R2G,
        height=640,
        labels={"amount_eur": "Budget (€)"},
    )
    fig_map.update_traces(
        customdata=np.stack([geo["budget_str"]], axis=-1),
        hovertemplate="<b>%{hovertext}</b><br>Budget: %{customdata[0]}<extra></extra>",
    )

    geo_kwargs = dict(
        projection_type=projection,
        showframe=False,
        bgcolor="rgba(0,0,0,0)",
        showland=True,
        landcolor="rgba(255,255,255,0.04)",
        showocean=True,
        oceancolor="rgba(40,90,140,0.10)",
        showlakes=True,
        lakecolor="rgba(40,90,140,0.10)",
    )
    if show_borders:
        geo_kwargs.update(
            dict(
                showcoastlines=True,
                coastlinecolor="rgba(255,255,255,0.25)",
                coastlinewidth=0.8,
                showcountries=True,
                countrycolor="rgba(255,255,255,0.25)",
                countrywidth=0.7,
            )
        )
    fig_map.update_geos(**geo_kwargs)

    if zoom == "Auto":
        try:
            fig_map.update_geos(fitbounds="locations")
        except Exception:
            pass
    elif zoom == "Europe":
        fig_map.update_geos(scope="europe")
    elif zoom == "World":
        fig_map.update_geos(scope="world")
    elif zoom == "Africa":
        fig_map.update_geos(scope="africa")
    elif zoom == "Asia":
        fig_map.update_geos(scope="asia")
    elif zoom == "North America":
        fig_map.update_geos(scope="north america")
    elif zoom == "South America":
        fig_map.update_geos(scope="south america")
    elif zoom == "Oceania":
        fig_map.update_geos(center=dict(lat=-25, lon=140), projection_scale=2.2, lataxis_range=[-55, 10], lonaxis_range=[105, 180])

    if show_labels:
        labels = [
            ("North America", -105, 48),
            ("South America", -60, -20),
            ("Europe", 15, 55),
            ("Africa", 20, 5),
            ("Asia", 90, 40),
            ("Oceania", 140, -25),
        ]
        fig_map.add_trace(
            go.Scattergeo(
                lon=[x[1] for x in labels],
                lat=[x[2] for x in labels],
                text=[x[0] for x in labels],
                mode="text",
                textfont=dict(size=12, color="rgba(234,242,255,0.75)"),
                showlegend=False,
                hoverinfo="skip",
            )
        )

    fig_map.update_layout(margin=dict(l=0, r=0, t=0, b=0), coloraxis_colorbar=dict(title="Budget (€)", len=0.7))
    st.plotly_chart(fig_map, use_container_width=True, config={"scrollZoom": True})

    st.markdown(f"#### {t(lang, 'top_countries')}")
    top_c = geo.sort_values("amount_eur", ascending=False).head(15).copy()
    fig_bar = px.bar(
        top_c,
        x="amount_eur",
        y="country_name",
        orientation="h",
        color="amount_eur",
        color_continuous_scale=R2G,
        height=520,
        labels={"amount_eur": "Budget (€)", "country_name": ""},
    )
    fig_bar.update_traces(
        customdata=np.stack([top_c["amount_eur"].apply(lambda v: fmt_money(v, lang))], axis=-1),
        hovertemplate="<b>%{y}</b><br>Budget: %{customdata[0]}<extra></extra>",
    )
    fig_bar.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
    st.plotly_chart(fig_bar, use_container_width=True)


# ============================================================
# TAB COMP (Benchmark)
# ============================================================
with tab_comp:
    bm_view = st.radio(
        t(lang, "benchmark_mode"),
        [t(lang, "bm_scatter"), t(lang, "bm_treemap"), t(lang, "bm_top")],
        index=0,
        horizontal=True,
    )

    m = df_actor.groupby(["actor_id", "actor_label", "entity_type"], as_index=False).agg(
        budget_eur=("amount_eur", "sum"),
        n_projects=("projectID", "nunique"),
    )
    m["ticket_eur"] = m["budget_eur"] / m["n_projects"].clip(lower=1)
    m["budget_str"] = m["budget_eur"].apply(lambda v: fmt_money(v, lang))
    m["ticket_str"] = m["ticket_eur"].apply(lambda v: fmt_money(v, lang))

    a, b, c = st.columns([1.1, 1.0, 1.9])
    with a:
        pct = st.slider(t(lang, "pct_threshold"), 0, 99, 90)
        thr = float(np.nanpercentile(m["budget_eur"].values, pct)) if len(m) else 0.0
        st.caption(f"≥ {fmt_money(thr, lang)}")
    with b:
        topn = st.number_input(t(lang, "topn"), min_value=20, max_value=5000, value=200, step=10)
    with c:
        query = st.text_input(t(lang, "search_actor"), value="")

    m2 = m[m["budget_eur"] >= thr].copy()
    if query.strip():
        m2 = m2[m2["actor_label"].astype(str).str.contains(query.strip(), case=False, na=False)]
    m2 = m2.sort_values("budget_eur", ascending=False).head(int(topn))

    all_label = "Tous les acteurs" if lang == "FR" else "All actors"
    actor_options = [all_label] + m2["actor_label"].astype(str).tolist()
    picked_label = st.selectbox(t(lang, "actor_picker"), actor_options, index=0, help=t(lang, "actor_picker_hint"))
    if picked_label != all_label:
        m2 = m2[m2["actor_label"].astype(str) == picked_label].copy()

    if bm_view == t(lang, "bm_scatter"):
        st.subheader(t(lang, "bm_scatter"))
        st.info(t(lang, "scatter_explain"), icon="ℹ️")

        if m2.empty:
            st.info(t(lang, "no_data"))
        else:
            fig1 = px.scatter(
                m2,
                x="n_projects",
                y="budget_eur",
                size="budget_eur",
                color="entity_type",
                hover_name="actor_label",
                size_max=55,
                log_x=True,
                log_y=True,
                height=580,
                labels={"n_projects": "Projets" if lang == "FR" else "Projects", "budget_eur": "Budget (€)"},
            )
            fig1.update_traces(
                customdata=np.stack([m2["budget_str"], m2["ticket_str"]], axis=-1),
                hovertemplate="<b>%{hovertext}</b><br>Budget: %{customdata[0]}<br>Ticket: %{customdata[1]}<extra></extra>",
                marker=dict(line=dict(width=1, color="rgba(255,255,255,0.22)")),
            )
            st.plotly_chart(fig1, use_container_width=True)
            st.caption(t(lang, "legend_tip"))

    elif bm_view == t(lang, "bm_treemap"):
        st.subheader(t(lang, "bm_treemap"))

        with st.expander("Paramètres treemap" if lang == "FR" else "Treemap settings", expanded=False):
            tm_top_themes = st.slider("# thématiques" if lang == "FR" else "# themes", 3, 20, 10)
            tm_top_countries = st.slider("# pays / thématique" if lang == "FR" else "# countries per theme", 2, 20, 8)
            tm_top_actors = st.slider("# acteurs / pays" if lang == "FR" else "# actors per country", 2, 25, 8)
            tm_group_others = st.checkbox("Grouper le reste en « Autres »" if lang == "FR" else "Group the rest as Others", value=True)

        base = df_actor.copy()
        base["theme_display"] = base["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))

        g_theme = base.groupby("theme_display", as_index=False)["amount_eur"].sum().sort_values("amount_eur", ascending=False)
        top_themes = g_theme["theme_display"].head(tm_top_themes).astype(str).tolist()
        base = base[base["theme_display"].astype(str).isin(top_themes)].copy()

        tc = base.groupby(["theme_display", "country_name"], as_index=False)["amount_eur"].sum()
        tc["rk"] = tc.groupby("theme_display")["amount_eur"].rank(method="first", ascending=False)
        tc_keep = tc[tc["rk"] <= tm_top_countries][["theme_display", "country_name"]]
        base = base.merge(tc_keep, on=["theme_display", "country_name"], how="inner")

        tca = base.groupby(["theme_display", "country_name", "actor_label"], as_index=False)["amount_eur"].sum()
        tca["rk"] = tca.groupby(["theme_display", "country_name"])["amount_eur"].rank(method="first", ascending=False)

        if tm_group_others:
            other_label = "Autres" if lang == "FR" else "Others"
            tca["actor_label_2"] = np.where(tca["rk"] <= tm_top_actors, tca["actor_label"].astype(str), other_label)
            agg = (
                tca.groupby(["theme_display", "country_name", "actor_label_2"], as_index=False)["amount_eur"]
                .sum()
                .rename(columns={"actor_label_2": "actor_label"})
            )
        else:
            agg = tca[tca["rk"] <= tm_top_actors][["theme_display", "country_name", "actor_label", "amount_eur"]].copy()

        agg["budget_str"] = agg["amount_eur"].apply(lambda v: fmt_money(v, lang))

        if agg.empty:
            st.info(t(lang, "no_data"))
        else:
            fig_tree = px.treemap(
                agg,
                path=[px.Constant("Tous" if lang == "FR" else "All"), "theme_display", "country_name", "actor_label"],
                values="amount_eur",
                color="amount_eur",
                color_continuous_scale=R2G,
                height=720,
            )
            fig_tree.update_traces(
                marker=dict(line=dict(width=0.8, color="rgba(255,255,255,0.10)")),
                customdata=np.stack([agg["budget_str"]], axis=-1),
                hovertemplate="<b>%{label}</b><br>Budget: %{customdata[0]}<br>%{percentEntry:.1%} of parent<extra></extra>",
                texttemplate="%{label}<br>%{percentEntry:.0%}",
            )
            fig_tree.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                uniformtext=dict(minsize=12, mode="hide"),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_tree, use_container_width=True)

    else:
        st.subheader(t(lang, "bm_top"))
        c1, c2, c3 = st.columns(3)

        def top_bar(entity_code: str, col, title: str):
            sel = m[m["entity_type"].astype(str) == entity_code].sort_values("budget_eur", ascending=False).head(20)
            if sel.empty:
                col.caption("—")
                return
            fig = px.bar(
                sel.iloc[::-1],
                x="budget_eur",
                y="actor_label",
                orientation="h",
                color="budget_eur",
                color_continuous_scale=R2G,
                height=560,
                labels={"budget_eur": "Budget (€)", "actor_label": ""},
            )
            fig.update_traces(
                customdata=np.stack([sel["budget_str"].iloc[::-1]], axis=-1),
                hovertemplate="<b>%{y}</b><br>Budget: %{customdata[0]}<extra></extra>",
            )
            fig.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
            col.markdown(f"#### {title}")
            col.plotly_chart(fig, use_container_width=True)

        top_bar("Private company", c1, "Top 20 industriels" if lang == "FR" else "Top 20 industrials")
        top_bar("Research & academia", c2, "Top 20 recherche" if lang == "FR" else "Top 20 research")
        top_bar("Public", c3, "Top 20 public" if lang == "FR" else "Top 20 public")


# ============================================================
# TAB TRENDS
# ============================================================
with tab_trends:
    dim_choice = st.radio(
        t(lang, "dimension"),
        [t(lang, "dim_theme"), t(lang, "dim_section")],
        index=0,
        horizontal=True,
    )

    tmp = df_f.copy()
    tmp["dim"] = tmp["section"].astype(str) if dim_choice == t(lang, "dim_section") else tmp["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))

    dim_budget = tmp.groupby("dim", as_index=False)["amount_eur"].sum().sort_values("amount_eur", ascending=False)
    top_default = dim_budget["dim"].head(min(12, len(dim_budget))).tolist()

    selected_dims = st.multiselect("Séries" if lang == "FR" else "Series", dim_budget["dim"].astype(str).tolist(), default=top_default)
    if not selected_dims:
        st.info(t(lang, "no_data"))
    else:
        tmp2 = tmp[tmp["dim"].astype(str).isin(selected_dims)].copy()

        mode = st.radio(t(lang, "mode"), [t(lang, "mode_abs"), t(lang, "mode_share")], index=1, horizontal=True, key="tr_mode")
        tdf = tmp2.groupby(["year", "dim"], as_index=False)["amount_eur"].sum()

        if mode == t(lang, "mode_share"):
            yearly = tdf.groupby("year")["amount_eur"].transform("sum").replace(0, np.nan)
            tdf["value"] = (tdf["amount_eur"] / yearly).fillna(0.0) * 100.0
            ylab = "Part (%)" if lang == "FR" else "Share (%)"
            hover = lambda v: f"{v:.1f}%"
        else:
            tdf["value"] = tdf["amount_eur"]
            ylab = "Budget (€)"
            hover = lambda v: fmt_money(v, lang)

        fig_area = px.area(
            tdf.sort_values("year"),
            x="year",
            y="value",
            color="dim",
            markers=True,
            height=520,
            labels={"value": ylab, "year": "Année" if lang == "FR" else "Year", "dim": ""},
        )
        fig_area.update_traces(
            customdata=np.stack([tdf["value"].apply(hover)], axis=-1),
            hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>%{customdata[0]}<extra></extra>",
        )
        st.plotly_chart(fig_area, use_container_width=True)

        st.divider()
        st.markdown(f"#### {t(lang, 'drivers')}")
        drivers = tmp2.groupby("dim", as_index=False)["amount_eur"].sum().sort_values("amount_eur", ascending=False).head(20)
        fig_drv = px.bar(
            drivers.iloc[::-1],
            x="amount_eur",
            y="dim",
            orientation="h",
            color="amount_eur",
            color_continuous_scale=R2G,
            height=520,
            labels={"amount_eur": "Budget (€)", "dim": ""},
        )
        fig_drv.update_traces(
            customdata=np.stack([drivers["amount_eur"].apply(lambda v: fmt_money(v, lang)).iloc[::-1]], axis=-1),
            hovertemplate="<b>%{y}</b><br>Budget: %{customdata[0]}<extra></extra>",
        )
        fig_drv.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
        st.plotly_chart(fig_drv, use_container_width=True)


# ============================================================
# TAB COMPARE
# ============================================================
with tab_compare:
    st.markdown(f"### {t(lang, 'compare_title')}")
    st.caption(t(lang, "compare_caption"))

    min_year = int(df["year"].min())
    max_year = int(df["year"].max())

    a1, a2 = st.columns(2)
    with a1:
        period_a = st.slider(t(lang, "period_a"), min_year, max_year, (max(min_year, max_year - 8), max_year - 5))
    with a2:
        period_b = st.slider(t(lang, "period_b"), min_year, max_year, (max_year - 3, max_year))

    dim_choice = st.radio(t(lang, "dimension"), [t(lang, "dim_theme"), t(lang, "dim_section")], index=0, horizontal=True, key="cmp_dim")
    base = df_f.copy()
    base["dim"] = base["section"].astype(str) if dim_choice == t(lang, "dim_section") else base["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))

    def share_by_period(df_in: pd.DataFrame, y0: int, y1: int) -> pd.Series:
        sub = df_in[df_in["year"].between(y0, y1)]
        g = sub.groupby("dim")["amount_eur"].sum()
        tot = float(g.sum())
        return (g / tot) if tot > 0 else (g * 0.0)

    sA = share_by_period(base, period_a[0], period_a[1])
    sB = share_by_period(base, period_b[0], period_b[1])

    all_dims = sorted(set(sA.index.astype(str)).union(set(sB.index.astype(str))))
    delta = pd.Series({d: float(sB.get(d, 0.0) - sA.get(d, 0.0)) for d in all_dims}).sort_values(ascending=False)

    view = pd.DataFrame({
        "dim": delta.index.astype(str),
        "share_A": [float(sA.get(d, 0.0)) for d in delta.index],
        "share_B": [float(sB.get(d, 0.0)) for d in delta.index],
        "delta_share": delta.values
    })

    topk = st.slider("Top K", 10, 60, 25)
    view2 = pd.concat([view.head(topk), view.tail(topk)]).drop_duplicates().sort_values("delta_share")

    fig = px.bar(
        view2,
        x=(view2["delta_share"] * 100.0),
        y=view2["dim"],
        orientation="h",
        height=680,
        labels={"x": "Δ (points de %)" if lang == "FR" else "Δ (pp)", "y": ""},
    )
    st.plotly_chart(fig, use_container_width=True)

    table = view.head(60).copy()
    table["Part A (%)" if lang == "FR" else "Share A (%)"] = table["share_A"].map(lambda x: f"{100*x:.1f}%")
    table["Part B (%)" if lang == "FR" else "Share B (%)"] = table["share_B"].map(lambda x: f"{100*x:.1f}%")
    table["Δ"] = table["delta_share"].map(lambda x: fmt_pp(x, 2, lang))
    table = table.drop(columns=["share_A", "share_B", "delta_share"])
    st.dataframe(table, use_container_width=True, height=520)


# ============================================================
# TAB MACRO & NEWS — INDEPENDENT of sidebar filters
# ============================================================
with tab_macro:
    st.markdown(f"### {t(lang,'macro_title')}")
    st.caption(t(lang, "macro_subtitle"))

    ev = load_events()
    if ev.empty:
        st.warning("events.csv est introuvable ou vide." if lang == "FR" else "events.csv is missing or empty.")
        st.stop()

    with st.expander(t(lang, "macro_filters"), expanded=True):
        # Macro dataset is full df (not df_f) by design
        macro_onetech = st.checkbox(t(lang, "onetech_only"), value=True, key="macro_onetech_only")
        macro_years = st.slider(
            t(lang, "period"),
            int(df["year"].min()),
            int(df["year"].max()),
            (int(df["year"].min()), int(df["year"].max())),
            key="macro_years"
        )
        macro_mode = st.radio(t(lang, "macro_metric"), [t(lang, "mode_abs"), t(lang, "mode_share")], index=0, horizontal=True, key="macro_metric_mode")
        match_mode = st.radio(
            t(lang, "macro_match"),
            [t(lang, "macro_match_theme"), t(lang, "macro_match_tag")],
            index=1,
            horizontal=True,
            key="macro_match_mode"
        )
        show_overlay = st.checkbox(t(lang, "macro_overlay"), value=True, key="macro_overlay")
        window = st.slider(t(lang, "macro_window"), 0, 3, 1, 1, key="macro_window")

    df_macro = df.copy()
    if macro_onetech:
        df_macro = df_macro[df_macro["onetech_scope"] == True]
    df_macro = df_macro[df_macro["year"].between(macro_years[0], macro_years[1])].copy()

    # Choose theme/tag inside macro tab
    if match_mode == t(lang, "macro_match_tag"):
        tags = sorted([x for x in ev["tag"].astype(str).unique().tolist() if x.strip()])
        chosen_tag = st.selectbox(t(lang, "macro_pick_tag"), tags, index=0)
        candidate_themes = TAG_TO_THEMES.get(str(chosen_tag), set())

        # choose theme among macro themes (display)
        df_macro["theme_display"] = df_macro["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))
        themes_df = sorted([x for x in df_macro["theme_display"].unique().tolist() if str(x).strip()])
        if candidate_themes:
            themes_df = [x for x in themes_df if x in candidate_themes]

        if not themes_df:
            st.info(t(lang, "no_data"))
            st.stop()

        chosen_theme_disp = st.selectbox(t(lang, "macro_pick_theme"), themes_df, index=0)
        df_theme = df_macro[df_macro["theme_display"].astype(str) == str(chosen_theme_disp)].copy()
        ev_sel = ev[ev["tag"].astype(str) == str(chosen_tag)].copy()

    else:
        themes_ev = sorted([x for x in ev["theme"].astype(str).unique().tolist() if x.strip()])
        df_macro["theme_display"] = df_macro["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))
        themes_df = sorted([x for x in df_macro["theme_display"].unique().tolist() if str(x).strip()])
        all_themes = sorted(set(themes_ev).union(set(themes_df)))

        chosen_theme_disp = st.selectbox(t(lang, "macro_pick_theme"), all_themes, index=0)
        df_theme = df_macro[df_macro["theme_display"].astype(str) == str(chosen_theme_disp)].copy()
        ev_sel = ev[ev["theme"].astype(str) == str(chosen_theme_disp)].copy()

    if df_theme.empty:
        st.info(t(lang, "no_data"))
        st.stop()

    agg = df_theme.groupby("year", as_index=False).agg(
        budget_total=("amount_eur", "sum"),
        n_projects=("projectID", "nunique"),
    ).sort_values("year")

    if macro_mode == t(lang, "mode_share"):
        tot = float(agg["budget_total"].sum())
        agg["value"] = (agg["budget_total"] / tot * 100.0) if tot > 0 else 0.0
        ylab = "Part du budget (%)" if lang == "FR" else "Budget share (%)"
        hover_val = lambda v: f"{v:.1f}%"
    else:
        agg["value"] = agg["budget_total"]
        ylab = "Budget (€)"
        hover_val = lambda v: fmt_money(v, lang)

    fig = px.line(
        agg,
        x="year",
        y="value",
        markers=True,
        height=520,
        labels={"year": "Année" if lang == "FR" else "Year", "value": ylab},
    )
    fig.update_traces(
        customdata=np.stack([
            agg["budget_total"].apply(lambda v: fmt_money(v, lang)).values,
            agg["n_projects"].astype(int).values,
            agg["value"].apply(hover_val).values
        ], axis=-1),
        hovertemplate="<b>%{x}</b><br>Value: %{customdata[2]}<br>Budget: %{customdata[0]}<br>Projects: %{customdata[1]}<extra></extra>",
    )

    if show_overlay and not ev_sel.empty:
        for _, r in ev_sel.iterrows():
            fig.add_vline(x=int(r["year"]), line_width=1, line_dash="dot", opacity=0.35)

    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("#### " + t(lang, "macro_events"))

    if ev_sel.empty:
        st.caption(t(lang, "macro_no_events"))
        st.stop()

    ev_sel = ev_sel.copy()
    ev_sel["label"] = ev_sel.apply(lambda r: f"{int(r['year'])} — {r['title']}", axis=1)
    picked_event_id = st.selectbox(
        t(lang, "macro_event_select"),
        ev_sel["event_id"].tolist(),
        format_func=lambda eid: ev_sel.loc[ev_sel["event_id"] == eid, "label"].iloc[0],
    )
    e = ev_sel[ev_sel["event_id"] == picked_event_id].iloc[0].to_dict()
    ey = int(e["year"])
    y0, y1 = ey - window, ey + window

    with st.expander("Détails" if lang == "FR" else "Details", expanded=True):
        st.write(f"**Year**: {ey}")
        if e.get("tag", ""):
            st.write(f"**Tag**: {e.get('tag')}")
        if e.get("source", ""):
            st.write(f"**Source**: {e.get('source')}")
        if e.get("impact_direction", ""):
            st.write(f"**Impact**: {e.get('impact_direction')}")
        if e.get("notes", ""):
            st.write(e.get("notes"))

    st.markdown("#### " + t(lang, "macro_signal"))
    inside = agg[(agg["year"] >= y0) & (agg["year"] <= y1)]
    outside = agg[(agg["year"] < y0) | (agg["year"] > y1)]
    c1m, c2m, c3m, c4m = st.columns(4)
    c1m.metric("Budget (fenêtre)" if lang == "FR" else "Budget (window)", fmt_money(float(inside["budget_total"].sum()), lang))
    c2m.metric("Projets (fenêtre)" if lang == "FR" else "Projects (window)", int(inside["n_projects"].sum()))
    c3m.metric("Budget (hors fenêtre)" if lang == "FR" else "Budget (outside)", fmt_money(float(outside["budget_total"].sum()), lang))
    c4m.metric("Projets (hors fenêtre)" if lang == "FR" else "Projects (outside)", int(outside["n_projects"].sum()))

    st.markdown("#### " + t(lang, "macro_examples"))
    df_w = df_theme[df_theme["year"].between(y0, y1)].copy()
    if df_w.empty:
        st.info("Aucun projet dans la fenêtre." if lang == "FR" else "No projects in the window.")
    else:
        proj = (
            df_w.groupby(["projectID", "title"], as_index=False)
            .agg(budget_eur=("amount_eur", "sum"), year=("year", "min"))
            .sort_values("budget_eur", ascending=False)
        )
        proj["budget"] = proj["budget_eur"].apply(lambda v: fmt_money(v, lang))
        st.dataframe(proj[["year", "projectID", "title", "budget"]].head(25), use_container_width=True, height=520)


# ============================================================
# TAB ACTOR PROFILE
# ============================================================
with tab_actor:
    st.markdown(f"### {t(lang, 'actor_profile')}")

    actors = (
        df_actor.groupby(["actor_id", "actor_label"], as_index=False)["amount_eur"]
        .sum()
        .sort_values("amount_eur", ascending=False)
    )
    if actors.empty:
        st.info(t(lang, "no_data"))
        st.stop()

    picked_label = st.selectbox(t(lang, "actor_picker"), actors["actor_label"].astype(str).tolist(), index=0)
    picked_id = actors.loc[actors["actor_label"].astype(str) == picked_label, "actor_id"].iloc[0]
    df_a = df_actor[df_actor["actor_id"].astype(str) == str(picked_id)].copy()

    st.markdown(f"#### {t(lang, 'actor_trend')}")
    byy = df_a.groupby("year", as_index=False).agg(
        budget_eur=("amount_eur", "sum"),
        n_projects=("projectID", "nunique"),
    )
    c1, c2 = st.columns(2)
    with c1:
        figb = px.bar(byy, x="year", y="budget_eur", height=360, labels={"budget_eur": "Budget (€)"})
        st.plotly_chart(figb, use_container_width=True)
    with c2:
        fign = px.line(byy, x="year", y="n_projects", markers=True, height=360,
                       labels={"n_projects": "Projets" if lang == "FR" else "Projects"})
        st.plotly_chart(fign, use_container_width=True)

    st.divider()
    st.markdown(f"#### {t(lang, 'actor_mix_theme')}")
    df_a["theme_display"] = df_a["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))
    mix_t = df_a.groupby("theme_display", as_index=False)["amount_eur"].sum().sort_values("amount_eur", ascending=False).head(15)
    figt = px.bar(mix_t.iloc[::-1], x="amount_eur", y="theme_display", orientation="h", height=420,
                  color="amount_eur", color_continuous_scale=R2G, labels={"amount_eur": "Budget (€)", "theme_display": ""})
    st.plotly_chart(figt, use_container_width=True)

    st.markdown(f"#### {t(lang, 'actor_mix_country')}")
    mix_c = df_a.groupby("country_name", as_index=False)["amount_eur"].sum().sort_values("amount_eur", ascending=False).head(15)
    figc = px.bar(mix_c.iloc[::-1], x="amount_eur", y="country_name", orientation="h", height=420,
                  color="amount_eur", color_continuous_scale=R2G, labels={"amount_eur": "Budget (€)", "country_name": ""})
    st.plotly_chart(figc, use_container_width=True)

    st.divider()
    st.markdown(f"#### {t(lang, 'actor_partners')}")
    projects = df_a["projectID"].astype(str).unique().tolist()
    df_p = df_actor[df_actor["projectID"].astype(str).isin(projects)].copy()

    partners = (
        df_p[df_p["actor_id"].astype(str) != str(picked_id)]
        .groupby(["actor_label"], as_index=False)
        .agg(n_projects=("projectID", "nunique"), budget_eur=("amount_eur", "sum"))
        .sort_values(["n_projects", "budget_eur"], ascending=False)
        .head(25)
    )
    partners["budget_eur"] = partners["budget_eur"].apply(lambda v: fmt_money(v, lang))
    st.dataframe(partners, use_container_width=True, height=520)


# ============================================================
# TAB DATA (paginated)
# ============================================================
with tab_data:
    st.caption(t(lang, "data_warning"))

    df_f["theme_display"] = df_f["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))
    df_f["entity_display"] = df_f["entity_type"].astype(str).map(lambda x: entity_raw_to_display(x, lang))
    df_f["program_display"] = df_f["program"].astype(str)

    all_cols = [
        "source", "program_display", "section", "year", "country_name",
        "actor_label", "entity_display", "title", "theme_display", "amount_eur"
    ]
    if "abstract" in df_f.columns:
        all_cols.insert(8, "abstract")

    all_cols = [c for c in all_cols if c in df_f.columns]
    default_cols = [c for c in all_cols if c != "abstract"]

    selected_cols = st.multiselect(t(lang, "columns"), all_cols, default=default_cols)
    if not selected_cols:
        selected_cols = default_cols

    view = df_f[selected_cols].copy()
    if "amount_eur" in view.columns:
        view["amount_eur"] = view["amount_eur"].apply(lambda v: fmt_money(v, lang))

    q = st.text_input(t(lang, "filter_text"), value="")
    if q.strip():
        ql = q.strip().lower()
        mask = np.zeros(len(view), dtype=bool)
        for col in selected_cols:
            if col == "amount_eur":
                continue
            s = view[col].astype(str).str.lower()
            mask |= s.str.contains(ql, na=False)
        view = view.loc[mask].copy()

    c1, c2 = st.columns([1, 1])
    with c1:
        rows_per_page = st.selectbox(t(lang, "rows_per_page"), [100, 250, 500, 1000], index=1)
    with c2:
        n_pages = max(1, int(np.ceil(len(view) / rows_per_page)))
        page = st.number_input(t(lang, "page"), min_value=1, max_value=n_pages, value=1, step=1)

    start = (page - 1) * rows_per_page
    end = start + rows_per_page
    st.dataframe(view.iloc[start:end], use_container_width=True, height=560)

    st.download_button(
        t(lang, "download"),
        df_f.to_csv(index=False).encode("utf-8"),
        file_name="export_filtered.csv",
        mime="text/csv",
    )


# ============================================================
# TAB QUALITY
# ============================================================
with tab_quality:
    st.markdown(f"### {t(lang, 'quality_title')}")
    qd = {
        "rows": int(len(df_f)),
        "missing_actor_id_%": float((df_f["actor_id"].astype(str) == "").mean() * 100),
        "missing_org_name_%": float((df_f["org_name"].astype(str) == "").mean() * 100),
        "missing_title_%": float((df_f["title"].astype(str) == "").mean() * 100),
        "amount_zero_%": float((df_f["amount_eur"] <= 0).mean() * 100),
    }
    st.dataframe(pd.DataFrame([qd]), use_container_width=True)


# ============================================================
# TAB HELP
# ============================================================
with tab_help:
    st.title(t(lang, "help_title"))

    with st.expander("Périmètre & finalité" if lang == "FR" else "Scope & purpose", expanded=True):
        if lang == "FR":
            st.markdown(
                """
**But** : analyser les projets lauréats (H2020 / Horizon Europe) et comparer thématiques, acteurs, pays, programmes.  
La couche **Macro & actualités** (events.csv) sert à **contextualiser** : repérer des coïncidences temporelles, formuler des hypothèses.

**Important** : l’outil est descriptif. Il ne prouve pas de causalité.
                """
            )
        else:
            st.markdown(
                """
**Goal**: analyze awarded projects (H2020 / Horizon Europe) and compare themes, actors, countries, programmes.  
The **Macro & news** layer (events.csv) provides **context**: spot temporal alignments and frame hypotheses.

**Important**: this is descriptive; it does not prove causality.
                """
            )

    with st.expander("Mise à jour (offline)" if lang == "FR" else "Update (offline)", expanded=False):
        if lang == "FR":
            st.markdown(
                f"""
- L’app **ne scrape pas** au runtime : elle lit `subsidy_base.csv` et `events.csv`.
- Le bouton **Rafraîchir** exécute :
  - `process_build.py` ou `pipeline.py` (rebuild data)
  - `build_events.py` (rebuild events)
- `build_events.py` est lancé avec : `{EVENTS_PYTHON}`
                """
            )
        else:
            st.markdown(
                f"""
- The app does **not** scrape at runtime: it reads `subsidy_base.csv` and `events.csv`.
- The **Refresh** button runs:
  - `process_build.py` or `pipeline.py` (rebuild data)
  - `build_events.py` (rebuild events)
- `build_events.py` is executed with: `{EVENTS_PYTHON}`
                """
            )


# ============================================================
# TAB GUIDE (expanders)
# ============================================================
with tab_guide:
    st.title(t(lang, "guide_title"))

    if lang == "FR":
        st.markdown("Guide pour lire les vues correctement, sans sur-interpréter. Tout dépend du **périmètre filtré** (sidebar).")

        with st.expander("1) Vue d’ensemble : KPIs, allocation, distribution, Lorenz", expanded=True):
            st.markdown(
                """
- KPIs = ordres de grandeur (budget, projets, acteurs, tickets).
- Histogramme log = lecture robuste du long-tail (évite les “méga-projets” qui écrasent tout).
- Lorenz + HHI = concentration / dépendance à quelques acteurs.
                """
            )
        with st.expander("2) Géographie : carte + top pays", expanded=False):
            st.markdown(
                """
- Carte = agrégation budget par pays.
- “Top pays” = lecture rapide.
- Attention : biais consortium / coordinations.
                """
            )
        with st.expander("3) Benchmark acteurs : scatter log/log, treemap, rankings", expanded=False):
            st.markdown(
                """
- Scatter log/log : volume (#projets) vs budget.
- Treemap : lisibilité via Top + Autres.
- Rankings : top acteurs par type.
                """
            )
        with st.expander("4) Tendances : part % vs absolu", expanded=False):
            st.markdown(
                """
- Part (%) = structure (robuste quand le total varie).
- Absolu = volume (attention outliers).
                """
            )
        with st.expander("5) Comparaison : Δ points de % entre périodes", expanded=False):
            st.markdown(
                """
- Compare A vs B en points de %.
- Sert à détecter des bascules.
                """
            )
        with st.expander("6) Macro & actualités : indépendant des filtres sidebar", expanded=False):
            st.markdown(
                """
- Onglet indépendant : utile pour éviter les “no data” causés par la sidebar.
- Matching par tag = plus robuste que par libellé.
- Overlay = contexte (corrélation, pas causalité).
                """
            )
        with st.expander("7) Données : pagination + export", expanded=False):
            st.markdown(
                """
- Pagination = évite MessageSizeError.
- Export = CSV complet du périmètre filtré.
                """
            )
    else:
        st.markdown("Guide to interpret the views correctly without over-claiming. Everything depends on the **filtered scope** (sidebar).")

        with st.expander("1) Overview: KPIs, allocation, distribution, Lorenz", expanded=True):
            st.markdown(
                """
- KPIs = orders of magnitude.
- Log histogram = robust long-tail reading.
- Lorenz + HHI = concentration / dependency risk.
                """
            )
        with st.expander("2) Geography: map + top countries", expanded=False):
            st.markdown(
                """
- Map aggregates budget by country.
- Top countries provides quick reading.
                """
            )
        with st.expander("3) Actor benchmark: log/log scatter, treemap, rankings", expanded=False):
            st.markdown(
                """
- Log/log scatter: volume vs budget.
- Treemap: readability via Top + Others.
                """
            )
        with st.expander("4) Trends: share % vs absolute", expanded=False):
            st.markdown(
                """
- Share (%) = structural reading.
- Absolute = volume reading (watch outliers).
                """
            )
        with st.expander("5) Compare: Δ percentage points between periods", expanded=False):
            st.markdown(
                """
- Compares A vs B in percentage points.
                """
            )
        with st.expander("6) Macro & news: independent from sidebar filters", expanded=False):
            st.markdown(
                """
- Independent tab avoids “no data” caused by sidebar.
- Tag matching is more robust.
                """
            )
        with st.expander("7) Data: pagination + export", expanded=False):
            st.markdown(
                """
- Pagination avoids MessageSizeError.
- Export downloads full CSV for current scope.
                """
            )