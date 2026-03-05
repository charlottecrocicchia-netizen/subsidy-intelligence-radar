from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple, List, Optional
import subprocess
import sys
import tempfile
import re
import os

import numpy as np
import pandas as pd
import streamlit as st

import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

pio.templates.default = "plotly_dark"

import duckdb
from filelock import FileLock, Timeout


# ============================================================
# Paths (reproductible)
# ============================================================
BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
PARQUET_PATH = DATA_DIR / "processed" / "subsidy_base.parquet"
CSV_PATH = DATA_DIR / "processed" / "subsidy_base.csv"  # optionnel (export)
EVENTS_PATH = DATA_DIR / "external" / "events.csv"
ACTOR_GROUPS_PATH = DATA_DIR / "external" / "actor_groups.csv"
REQUIREMENTS_PATH = BASE_DIR / "requirements.txt"

# Offline scripts (ONLY on refresh click)
BUILD_EVENTS_SCRIPT = BASE_DIR / "build_events.py"
PROCESS_BUILD_SCRIPT = BASE_DIR / "process_build.py"
PIPELINE_SCRIPT = BASE_DIR / "pipeline.py"

PYTHON_BIN = sys.executable

# Global lock (works on Streamlit Cloud)
LOCK_PATH = Path(tempfile.gettempdir()) / "subsidy_radar_refresh.lock"


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
# Taxonomy + translations (inchangé)
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
        "actor_grouping": "Regrouper entités juridiques (PIC/groupe)",
        "exclude_funders": "Exclure financeurs / agences",
        "actor_groups_ready": "Mapping groupes chargé",
        "actor_groups_missing": "Mapping groupes absent (`data/external/actor_groups.csv`).",
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
        "tab_network": "🧬 Chaîne & réseau",
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
        "download_page": "⬇️ Télécharger la page CSV",
        "download_full": "⬇️ Télécharger le CSV complet (filtres actuels)",
        "prepare_full_export": "Préparer l’export complet",
        "quality_title": "Qualité des données",
        "help_title": "Aide (FAQ + interprétation)",
        "guide_title": "Guide de lecture",
        "data_warning": "Dataset volumineux : affichage paginé (DuckDB) pour éviter les crashes.",
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
        "macro_use_global": "Utiliser les filtres globaux (sidebar)",
        "cloud_persistence_note": "Mode Streamlit Cloud : les mises à jour de fichiers via ce bouton ne sont pas durables. Utiliser le workflow GitHub « Refresh Data » pour une persistance automatique.",
        "missing_stage_col": "La colonne `value_chain_stage` n’est pas encore disponible. Lance un rafraîchissement des données.",
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
        "actor_grouping": "Group legal entities (PIC/group)",
        "exclude_funders": "Exclude funders / agencies",
        "actor_groups_ready": "Group mapping loaded",
        "actor_groups_missing": "Group mapping missing (`data/external/actor_groups.csv`).",
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
        "tab_network": "🧬 Value chain & network",
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
        "download_page": "⬇️ Download page CSV",
        "download_full": "⬇️ Download full CSV (current filters)",
        "prepare_full_export": "Prepare full export",
        "quality_title": "Data quality",
        "help_title": "Help (FAQ + interpretation)",
        "guide_title": "Reading guide",
        "data_warning": "Large dataset: paginated display (DuckDB) to avoid crashes.",
        "columns": "Displayed columns",
        "filter_text": "Text filter (on displayed columns)",
        "rows_per_page": "Rows / page",
        "page": "Page",
        "last_update": "Last update",
        "last_update_data": "Data",
        "last_update_events": "Events",
        "macro_title": "Macro & news — deep dive",
        "macro_subtitle": "Independent tab: internal macro filters (does not depend on the sidebar).",
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
        "macro_use_global": "Use global filters (sidebar)",
        "cloud_persistence_note": "Streamlit Cloud mode: file updates from this button are not durable. Use the GitHub workflow \"Refresh Data\" for persistent automation.",
        "missing_stage_col": "Column `value_chain_stage` is not available yet. Run a data refresh.",
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


# ============================================================
# UI mapping
# ============================================================
def theme_raw_to_display(raw: str, lang: str) -> str:
    if lang == "FR":
        return THEME_EN_TO_FR.get(raw, raw)
    return raw


def entity_raw_to_display(raw: str, lang: str) -> str:
    if lang == "FR":
        return ENTITY_EN_TO_FR.get(raw, raw)
    return raw


def _fmt_mtime(p: Path) -> str:
    from datetime import datetime
    try:
        ts = p.stat().st_mtime
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "—"


def is_streamlit_cloud_runtime() -> bool:
    if os.getenv("STREAMLIT_SERVER_HEADLESS") == "true":
        return True
    if os.getenv("STREAMLIT_RUNTIME"):
        return True
    if os.getenv("SUBSIDY_RADAR_CLOUD") == "1":
        return True
    return False


def reset_filters() -> None:
    for k in list(st.session_state.keys()):
        if k.startswith("f_") or k.startswith("macro_"):
            del st.session_state[k]


# ============================================================
# Optional actor grouping (PIC / group mapping)
# ============================================================
_TRUE_VALUES = {"1", "true", "yes", "y", "oui", "vrai", "t"}


def _empty_actor_map_actor() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "actor_id": pd.Series(dtype="string"),
            "group_id": pd.Series(dtype="string"),
            "group_name": pd.Series(dtype="string"),
            "pic": pd.Series(dtype="string"),
            "is_funder": pd.Series(dtype="bool"),
        }
    )


def _empty_actor_map_pic() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "pic": pd.Series(dtype="string"),
            "group_id": pd.Series(dtype="string"),
            "group_name": pd.Series(dtype="string"),
            "is_funder": pd.Series(dtype="bool"),
        }
    )


def _norm_col_name(x: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(x).strip().lower()).strip("_")


@st.cache_data(show_spinner=False)
def load_actor_group_tables() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Optional CSV mapping to group legal entities under one parent (PIC/group).
    Expected columns (flexible names): actor_id OR pic, plus group_id/group_name and optional is_funder.
    """
    if not ACTOR_GROUPS_PATH.exists():
        return _empty_actor_map_actor(), _empty_actor_map_pic()

    try:
        raw = pd.read_csv(ACTOR_GROUPS_PATH, dtype=str).fillna("")
    except Exception:
        return _empty_actor_map_actor(), _empty_actor_map_pic()

    raw.columns = [_norm_col_name(c) for c in raw.columns]

    aliases = {
        "actor_id": {"actor_id", "actorid", "participant_actor_id", "participant_id", "entity_actor_id"},
        "pic": {"pic", "participant_pic", "organisation_pic", "organization_pic"},
        "group_id": {"group_id", "group", "group_key", "parent_group_id", "company_group_id", "tic"},
        "group_name": {"group_name", "group_label", "parent_group", "company_group", "enterprise_group", "group_display"},
        "is_funder": {"is_funder", "funder", "is_financer", "financeur", "is_funding_body", "funding_body"},
    }

    data = pd.DataFrame(index=raw.index)
    for out_col, names in aliases.items():
        found = next((c for c in raw.columns if c in names), None)
        data[out_col] = raw[found].astype("string").fillna("").astype(str).str.strip() if found else ""

    data["pic"] = data["pic"].astype(str).str.replace(r"\D+", "", regex=True)
    data["is_funder"] = data["is_funder"].astype(str).str.strip().str.lower().isin(_TRUE_VALUES)

    data = data[(data["actor_id"].astype(str).str.len() > 0) | (data["pic"].astype(str).str.len() > 0)].copy()
    if data.empty:
        return _empty_actor_map_actor(), _empty_actor_map_pic()

    data["group_id"] = np.where(
        data["group_id"].astype(str).str.len() > 0,
        data["group_id"].astype(str),
        np.where(
            data["group_name"].astype(str).str.len() > 0,
            data["group_name"].astype(str),
            np.where(data["pic"].astype(str).str.len() > 0, "PIC:" + data["pic"].astype(str), data["actor_id"].astype(str)),
        ),
    )
    data["group_name"] = np.where(
        data["group_name"].astype(str).str.len() > 0,
        data["group_name"].astype(str),
        data["group_id"].astype(str),
    )

    by_actor = (
        data[data["actor_id"].astype(str).str.len() > 0][["actor_id", "group_id", "group_name", "pic", "is_funder"]]
        .drop_duplicates(subset=["actor_id"], keep="first")
        .reset_index(drop=True)
    )
    by_pic = (
        data[data["pic"].astype(str).str.len() > 0][["pic", "group_id", "group_name", "is_funder"]]
        .drop_duplicates(subset=["pic"], keep="first")
        .reset_index(drop=True)
    )

    return by_actor, by_pic


# ============================================================
# DuckDB engine (critical: no full pandas load)
# ============================================================
@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")
    return con


def rel() -> str:
    return f"read_parquet('{PARQUET_PATH.as_posix()}')"


@st.cache_data(show_spinner=False)
def base_schema_columns() -> List[str]:
    df = get_con().execute(f"SELECT * FROM {rel()} LIMIT 0").fetchdf()
    return [str(c) for c in df.columns]


def register_actor_group_tables() -> Dict[str, int]:
    by_actor, by_pic = load_actor_group_tables()

    con = get_con()
    con.register("actor_groups_by_actor", by_actor if not by_actor.empty else _empty_actor_map_actor())
    con.register("actor_groups_by_pic", by_pic if not by_pic.empty else _empty_actor_map_pic())

    return {
        "available": bool((not by_actor.empty) or (not by_pic.empty)),
        "rows_actor": int(len(by_actor)),
        "rows_pic": int(len(by_pic)),
    }


def rel_analytics(use_actor_groups: bool, exclude_funders: bool, actor_map_available: bool) -> str:
    base = rel()
    cols = set(base_schema_columns())
    pic_expr = "b.pic" if "pic" in cols else "regexp_extract(b.actor_id, '([0-9]{8,10})$', 1)"
    stage_expr = "b.value_chain_stage" if "value_chain_stage" in cols else "'Unspecified'"
    status_expr = "b.project_status" if "project_status" in cols else "'Unknown'"

    actor_expr = (
        "COALESCE(NULLIF(TRIM(ga.group_id), ''), NULLIF(TRIM(gp.group_id), ''), b.actor_id)"
        if use_actor_groups
        else "b.actor_id"
    )
    org_expr = (
        "COALESCE(NULLIF(TRIM(ga.group_name), ''), NULLIF(TRIM(gp.group_name), ''), b.org_name)"
        if use_actor_groups
        else "b.org_name"
    )
    funder_expr = "COALESCE(ga.is_funder, gp.is_funder, FALSE)"
    where_funder = f"WHERE {funder_expr} = FALSE" if exclude_funders else ""

    return f"""
    (
      SELECT
        b.source,
        b.program,
        b.section,
        b.year,
        b.projectID,
        b.acronym,
        b.title,
        b.objective,
        b.abstract,
        {actor_expr} AS actor_id,
        {pic_expr} AS pic,
        {org_expr} AS org_name,
        b.entity_type,
        b.country_alpha2,
        b.country_alpha3,
        b.country_name,
        b.amount_eur,
        b.theme,
        {stage_expr} AS value_chain_stage,
        {status_expr} AS project_status
      FROM {base} b
      LEFT JOIN actor_groups_by_actor ga ON b.actor_id = ga.actor_id
      LEFT JOIN actor_groups_by_pic gp
        ON (ga.actor_id IS NULL AND regexp_extract(b.actor_id, '([0-9]{{8,10}})$', 1) = gp.pic)
      {where_funder}
    )
    """


def fetch_df(sql: str) -> pd.DataFrame:
    return get_con().execute(sql).fetchdf()


def list_str(sql: str) -> List[str]:
    df = fetch_df(sql)
    if df.empty:
        return []
    return [str(x) for x in df.iloc[:, 0].tolist() if str(x).strip()]


def in_list(values: List[str]) -> str:
    esc = [str(v).replace("'", "''") for v in values if v is not None and str(v).strip()]
    if not esc:
        return "(NULL)"
    return "(" + ",".join([f"'{v}'" for v in esc]) + ")"


@st.cache_data(show_spinner=False)
def export_query_csv_bytes(sql_query: str) -> bytes:
    tmp = None
    with tempfile.NamedTemporaryFile(prefix="subsidy_export_", suffix=".csv", delete=False) as fh:
        tmp = Path(fh.name)
    tmp_sql = tmp.as_posix().replace("'", "''")
    try:
        get_con().execute(f"COPY ({sql_query}) TO '{tmp_sql}' (HEADER, DELIMITER ',');")
        return tmp.read_bytes()
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def where_clause(
    sources: List[str],
    programmes: List[str],
    years: Tuple[int, int],
    use_section: bool,
    sections: List[str],
    onetech_only: bool,
    themes: List[str],
    entities: List[str],
    countries: List[str],
) -> str:
    w = []
    if sources:
        w.append(f"source IN {in_list(sources)}")
    if programmes:
        w.append(f"program IN {in_list(programmes)}")
    w.append(f"year BETWEEN {int(years[0])} AND {int(years[1])}")
    if use_section and sections:
        w.append(f"section IN {in_list(sections)}")
    if onetech_only:
        w.append(f"theme IN {in_list(sorted(list(ONETECH_THEMES_EN)))}")
    if themes:
        w.append(f"theme IN {in_list(themes)}")
    if entities:
        w.append(f"entity_type IN {in_list(entities)}")
    if countries:
        w.append(f"country_name IN {in_list(countries)}")
    return " AND ".join(w) if w else "TRUE"


# ============================================================
# Events loader (small, OK in pandas)
# ============================================================
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
# Offline rebuild (ONLY on refresh click) + lock
# ============================================================
def _run_script(script_path: Path, timeout_sec: int = 3600) -> Tuple[bool, str]:
    if not script_path.exists():
        return False, f"Script not found: {script_path}"

    try:
        res = subprocess.run(
            [PYTHON_BIN, str(script_path)],
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
        combo = f"{out}\n{err}".lower()
        if script_path.name == "build_events.py" and ("no module named 'feedparser'" in combo or "no module named \"feedparser\"" in combo):
            msg += (
                "\n\n[hint]\nMissing dependency `feedparser` in the Python environment used by Streamlit.\n"
                f"Interpreter: {PYTHON_BIN}\n"
                f"Install with:\n`{PYTHON_BIN} -m pip install -r {REQUIREMENTS_PATH}`\n"
                "If you use conda, start Streamlit with that same conda environment."
            )
        if script_path.name == "build_events.py" and ("no module named 'sparqlwrapper'" in combo or "no module named \"sparqlwrapper\"" in combo):
            msg += (
                "\n\n[hint]\nMissing dependency `SPARQLWrapper` in the Python environment used by Streamlit.\n"
                f"Install with:\n`{PYTHON_BIN} -m pip install -r {REQUIREMENTS_PATH}`"
            )
        return False, msg


def rebuild_all() -> Tuple[bool, Dict[str, str]]:
    logs: Dict[str, str] = {}

    if PIPELINE_SCRIPT.exists():
        ok, msg = _run_script(PIPELINE_SCRIPT, timeout_sec=3600)
        logs["pipeline.py"] = msg
        if not ok:
            return False, logs
    elif PROCESS_BUILD_SCRIPT.exists():
        ok, msg = _run_script(PROCESS_BUILD_SCRIPT, timeout_sec=3600)
        logs["process_build.py"] = msg
        if not ok:
            return False, logs
    else:
        logs["data"] = "No pipeline.py / process_build.py found."
        return False, logs

    if BUILD_EVENTS_SCRIPT.exists():
        ok, msg = _run_script(BUILD_EVENTS_SCRIPT, timeout_sec=3600)
        logs["build_events.py"] = msg
        if not ok:
            return False, logs
    else:
        logs["build_events.py"] = "build_events.py not found."
        return False, logs

    return True, logs


def refresh_with_lock() -> Tuple[bool, Dict[str, str]]:
    lock = FileLock(str(LOCK_PATH))
    try:
        lock.acquire(timeout=0)
    except Timeout:
        return False, {"lock": "Refresh already running. Try again in 1–2 minutes."}

    try:
        ok, logs = rebuild_all()
        # Important: clear caches
        st.cache_data.clear()
        st.cache_resource.clear()
        return ok, logs
    finally:
        lock.release()


# ============================================================
# Sidebar: language + reset + refresh + last update + logs
# ============================================================
with st.sidebar:
    lang = st.radio("Language", ["FR", "EN"], index=0, horizontal=True, label_visibility="collapsed", key="ui_lang")
    st.caption(t(lang, "language"))
    cloud_runtime = is_streamlit_cloud_runtime()

    st.caption(f"{t(lang,'last_update')} — {t(lang,'last_update_data')}: {_fmt_mtime(PARQUET_PATH)}")
    st.caption(f"{t(lang,'last_update')} — {t(lang,'last_update_events')}: {_fmt_mtime(EVENTS_PATH)}")

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        if st.button(t(lang, "reset"), width="stretch"):
            reset_filters()
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

    with c2:
        if st.button(t(lang, "refresh"), width="stretch", help=t(lang, "refresh_hint")):
            with st.spinner("Mise à jour en cours (CORDIS + events)..." if lang == "FR" else "Updating (CORDIS + events)..."):
                ok, logs = refresh_with_lock()
            st.session_state["last_rebuild_ok"] = ok
            st.session_state["last_rebuild_logs"] = logs
            st.rerun()

    if cloud_runtime:
        st.caption(t(lang, "cloud_persistence_note"))

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
# Guard
# ============================================================
st.title(t(lang, "title"))
st.caption(t(lang, "subtitle"))

if not PARQUET_PATH.exists():
    st.error(
        f"Base Parquet manquante : `{PARQUET_PATH}`.\n\n"
        f"➡️ Clique sur **{t(lang,'refresh')}** pour la générer (via process_build / pipeline)."
    )
    st.stop()


# ============================================================
# Metadata lists + ranges (cheap)
# ============================================================
@st.cache_data(show_spinner=False)
def get_meta() -> dict:
    R = rel()
    yr = fetch_df(f"SELECT MIN(year) AS miny, MAX(year) AS maxy FROM {R}")
    miny = int(yr["miny"].iloc[0])
    maxy = int(yr["maxy"].iloc[0])

    return {
        "miny": miny,
        "maxy": maxy,
        "sources": list_str(f"SELECT DISTINCT source FROM {R} WHERE source IS NOT NULL AND TRIM(source)<>'' ORDER BY source"),
        "programmes": list_str(f"SELECT DISTINCT program FROM {R} WHERE program IS NOT NULL AND TRIM(program)<>'' ORDER BY program"),
        "sections": list_str(f"SELECT DISTINCT section FROM {R} WHERE section IS NOT NULL AND TRIM(section)<>'' ORDER BY section"),
        "themes": list_str(f"SELECT DISTINCT theme FROM {R} WHERE theme IS NOT NULL AND TRIM(theme)<>'' ORDER BY theme"),
        "entities": list_str(f"SELECT DISTINCT entity_type FROM {R} WHERE entity_type IS NOT NULL AND TRIM(entity_type)<>'' ORDER BY entity_type"),
        "countries": list_str(f"SELECT DISTINCT country_name FROM {R} WHERE country_name IS NOT NULL AND TRIM(country_name)<>'' ORDER BY country_name"),
    }

meta = get_meta()
actor_map_info = register_actor_group_tables()


# ============================================================
# Default filters (set once)
# ============================================================
def _init_defaults_once() -> None:
    if st.session_state.get("_defaults_inited", False):
        return
    st.session_state["_defaults_inited"] = True

    st.session_state["f_sources"] = meta["sources"]
    st.session_state["f_programmes"] = meta["programmes"]
    st.session_state["f_years"] = (meta["miny"], meta["maxy"])
    st.session_state["f_use_section"] = False
    st.session_state["f_sections"] = []
    st.session_state["f_onetech_only"] = False
    st.session_state["f_themes_raw"] = meta["themes"]

    st.session_state["f_entity_raw"] = meta["entities"]
    st.session_state["f_countries"] = meta["countries"]
    st.session_state["f_use_actor_groups"] = bool(actor_map_info.get("available", False))
    st.session_state["f_exclude_funders"] = bool(actor_map_info.get("available", False))

_init_defaults_once()


# ============================================================
# Sidebar filters (display mapping FR/EN, raw stored)
# ============================================================
with st.sidebar:
    st.header(t(lang, "filters"))

    src_default = [x for x in st.session_state["f_sources"] if x in meta["sources"]]
    prg_default = [x for x in st.session_state["f_programmes"] if x in meta["programmes"]]
    ctry_default = [x for x in st.session_state["f_countries"] if x in meta["countries"]]

    st.session_state["f_sources"] = st.multiselect(t(lang, "sources"), meta["sources"], default=src_default or meta["sources"])
    st.session_state["f_onetech_only"] = st.checkbox(t(lang, "onetech_only"), value=st.session_state["f_onetech_only"])
    st.session_state["f_programmes"] = st.multiselect(t(lang, "programmes"), meta["programmes"], default=prg_default or meta["programmes"])
    st.session_state["f_years"] = st.slider(t(lang, "period"), meta["miny"], meta["maxy"], st.session_state["f_years"])

    st.session_state["f_use_section"] = st.checkbox(t(lang, "use_section"), value=st.session_state["f_use_section"])
    if st.session_state["f_use_section"]:
        sec_default = [x for x in st.session_state["f_sections"] if x in meta["sections"]]
        st.session_state["f_sections"] = st.multiselect(t(lang, "section"), meta["sections"], default=sec_default)
    else:
        st.session_state["f_sections"] = []

    # Themes and entities are stored in raw values, labels are translated on display.
    themes_ui = [x for x in meta["themes"] if (not st.session_state["f_onetech_only"]) or (x in ONETECH_THEMES_EN)]
    themes_default = [x for x in st.session_state["f_themes_raw"] if x in themes_ui]
    st.session_state["f_themes_raw"] = st.multiselect(
        t(lang, "themes"),
        themes_ui,
        default=themes_default or themes_ui,
        format_func=lambda x: theme_raw_to_display(str(x), lang),
    )

    entities_default = [x for x in st.session_state["f_entity_raw"] if x in meta["entities"]]
    st.session_state["f_entity_raw"] = st.multiselect(
        t(lang, "entity"),
        meta["entities"],
        default=entities_default or meta["entities"],
        format_func=lambda x: entity_raw_to_display(str(x), lang),
    )

    st.session_state["f_countries"] = st.multiselect(t(lang, "countries"), meta["countries"], default=ctry_default or meta["countries"])

    st.divider()
    if actor_map_info.get("available", False):
        st.caption(
            f"{t(lang, 'actor_groups_ready')}: "
            f"{actor_map_info.get('rows_actor', 0)} actor_id / {actor_map_info.get('rows_pic', 0)} PIC"
        )
        st.checkbox(t(lang, "actor_grouping"), key="f_use_actor_groups")
        st.checkbox(t(lang, "exclude_funders"), key="f_exclude_funders")
    else:
        st.caption(t(lang, "actor_groups_missing"))
        st.checkbox(t(lang, "actor_grouping"), value=False, disabled=True)
        st.checkbox(t(lang, "exclude_funders"), value=False, disabled=True)
        st.session_state["f_use_actor_groups"] = False
        st.session_state["f_exclude_funders"] = False


# ============================================================
# Main WHERE
# ============================================================
W = where_clause(
    sources=st.session_state["f_sources"],
    programmes=st.session_state["f_programmes"],
    years=st.session_state["f_years"],
    use_section=st.session_state["f_use_section"],
    sections=st.session_state["f_sections"],
    onetech_only=st.session_state["f_onetech_only"],
    themes=st.session_state["f_themes_raw"],
    entities=st.session_state["f_entity_raw"],
    countries=st.session_state["f_countries"],
)
R = rel_analytics(
    use_actor_groups=bool(st.session_state.get("f_use_actor_groups", False)),
    exclude_funders=bool(st.session_state.get("f_exclude_funders", False)),
    actor_map_available=bool(actor_map_info.get("available", False)),
)


# ============================================================
# KPIs (DuckDB)
# ============================================================
st.subheader(t(lang, "kpis"))

kpi = fetch_df(f"""
SELECT
  SUM(amount_eur) AS total_budget,
  COUNT(DISTINCT projectID) AS n_projects,
  COUNT(DISTINCT actor_id) FILTER (WHERE actor_id IS NOT NULL AND TRIM(actor_id) <> '') AS n_actors
FROM {R}
WHERE {W}
""")

total_budget = float(kpi["total_budget"].iloc[0] or 0.0)
nb_projects = int(kpi["n_projects"].iloc[0] or 0)
nb_actors = int(kpi["n_actors"].iloc[0] or 0)

if nb_projects == 0:
    st.warning(t(lang, "no_data"))

proj_stats = fetch_df(f"""
SELECT
  AVG(proj_budget) AS avg_ticket,
  MEDIAN(proj_budget) AS median_ticket
FROM (
  SELECT projectID, SUM(amount_eur) AS proj_budget
  FROM {R}
  WHERE {W}
  GROUP BY projectID
) t
""")
avg_ticket = float(proj_stats["avg_ticket"].iloc[0] or 0.0)
median_ticket = float(proj_stats["median_ticket"].iloc[0] or 0.0)

actor_b = fetch_df(f"""
SELECT actor_id, SUM(amount_eur) AS b
FROM {R}
WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
GROUP BY actor_id
""")
top10_share = 0.0
hhi = 0.0
if not actor_b.empty and float(actor_b["b"].sum()) > 0:
    b = actor_b["b"].astype(float)
    tot = float(b.sum())
    shares = (b / tot).to_numpy()
    hhi = float(np.sum(shares**2))
    top10_share = float(b.sort_values(ascending=False).head(10).sum() / tot)

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
# Tabs (same)
# ============================================================
tab_overview, tab_geo, tab_comp, tab_trends, tab_compare, tab_macro, tab_actor, tab_network, tab_data, tab_quality, tab_help, tab_guide = st.tabs(
    [
        t(lang, "tab_overview"),
        t(lang, "tab_geo"),
        t(lang, "tab_comp"),
        t(lang, "tab_trends"),
        t(lang, "tab_compare"),
        t(lang, "tab_macro"),
        t(lang, "tab_actor"),
        t(lang, "tab_network"),
        t(lang, "tab_data"),
        t(lang, "tab_quality"),
        t(lang, "tab_help"),
        t(lang, "tab_guide"),
    ]
)


# ============================================================
# TAB OVERVIEW (DuckDB)
# ============================================================
with tab_overview:
    st.markdown("### " + ("Allocation du budget par type d’entité" if lang == "FR" else "Budget allocation by entity type"))
    alloc = fetch_df(f"""
    SELECT entity_type, SUM(amount_eur) AS amount_eur
    FROM {R}
    WHERE {W}
    GROUP BY entity_type
    ORDER BY amount_eur DESC
    """)
    if alloc.empty:
        st.info(t(lang, "no_data"))
    else:
        alloc["entity_display"] = alloc["entity_type"].map(lambda x: entity_raw_to_display(str(x), lang))
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
            customdata=np.stack([alloc["amount_eur"].apply(lambda v: fmt_money(float(v), lang)).iloc[::-1]], axis=-1),
            hovertemplate="<b>%{y}</b><br>Budget: %{customdata[0]}<extra></extra>",
        )
        fig_alloc.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
        st.plotly_chart(fig_alloc, use_container_width=True)

    st.divider()
    st.markdown("### " + ("Distribution des tickets (échelle log)" if lang == "FR" else "Ticket distribution (log scale)"))
    tb = fetch_df(f"""
    SELECT projectID, SUM(amount_eur) AS proj_budget
    FROM {R}
    WHERE {W}
    GROUP BY projectID
    HAVING SUM(amount_eur) > 0
    """)
    if tb.empty:
        st.info(t(lang, "no_data"))
    else:
        logv = np.log10(tb["proj_budget"].astype(float).to_numpy())
        fig_hist = px.histogram(x=logv, nbins=60, height=420, labels={"x": "log10(Budget €)"})
        fig_hist.update_layout(showlegend=False, xaxis_title="log10(Budget €)", yaxis_title="Count")
        st.plotly_chart(fig_hist, use_container_width=True)

    st.divider()
    st.markdown("### " + ("Courbe de Lorenz" if lang == "FR" else "Lorenz curve"))
    ab = actor_b.copy()
    if ab.empty or float(ab["b"].sum()) <= 0 or len(ab) < 2:
        st.info(t(lang, "no_data"))
    else:
        ab = ab.sort_values("b")
        cum = ab["b"].cumsum() / float(ab["b"].sum())
        x = np.arange(1, len(ab) + 1) / len(ab)
        lor = pd.DataFrame({"actors_share": x, "budget_share": cum.values})
        fig_l = px.line(lor, x="actors_share", y="budget_share", height=420)
        fig_l.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode="lines", name="Equality", line=dict(dash="dash")))
        fig_l.update_layout(xaxis_title="Actors (cumulative share)", yaxis_title="Budget (cumulative share)")
        st.plotly_chart(fig_l, use_container_width=True)


# ============================================================
# TAB GEO (DuckDB)
# ============================================================
with tab_geo:
    geo = fetch_df(f"""
    SELECT country_alpha3, country_name, SUM(amount_eur) AS amount_eur
    FROM {R}
    WHERE {W} AND country_alpha3 IS NOT NULL AND TRIM(country_alpha3) <> ''
    GROUP BY country_alpha3, country_name
    ORDER BY amount_eur DESC
    """)
    if geo.empty:
        st.info(t(lang, "no_data"))
    else:
        geo["budget_str"] = geo["amount_eur"].apply(lambda v: fmt_money(float(v), lang))

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
        top_c = geo.head(15).copy()
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
            customdata=np.stack([top_c["amount_eur"].apply(lambda v: fmt_money(float(v), lang))], axis=-1),
            hovertemplate="<b>%{y}</b><br>Budget: %{customdata[0]}<extra></extra>",
        )
        fig_bar.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
        st.plotly_chart(fig_bar, use_container_width=True)


# ============================================================
# TAB COMP (Benchmark) — scatter, treemap, top (DuckDB)
# ============================================================
with tab_comp:
    bm_view = st.radio(
        t(lang, "benchmark_mode"),
        [t(lang, "bm_scatter"), t(lang, "bm_treemap"), t(lang, "bm_top")],
        index=0,
        horizontal=True,
    )

    m = fetch_df(f"""
    SELECT
      actor_id,
      COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS org_name2,
      COALESCE(NULLIF(TRIM(country_name), ''), 'Unknown') AS country_name2,
      COALESCE(NULLIF(TRIM(entity_type), ''), 'Unknown') AS entity_type,
      SUM(amount_eur) AS budget_eur,
      COUNT(DISTINCT projectID) AS n_projects
    FROM {R}
    WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
    GROUP BY actor_id, org_name2, country_name2, entity_type
    """)

    if m.empty:
        st.info(t(lang, "no_data"))

    # Disambiguate label like your pandas logic (org + country)
    m["actor_label"] = np.where(
        m["org_name2"].astype(str) == m["actor_id"].astype(str),
        m["actor_id"].astype(str),
        (m["org_name2"].astype(str) + " — " + m["country_name2"].astype(str)),
    )
    m["ticket_eur"] = m["budget_eur"].astype(float) / m["n_projects"].astype(float).clip(lower=1.0)
    m["budget_str"] = m["budget_eur"].apply(lambda v: fmt_money(float(v), lang))
    m["ticket_str"] = m["ticket_eur"].apply(lambda v: fmt_money(float(v), lang))

    a, b, c = st.columns([1.1, 1.0, 1.9])
    with a:
        pct = st.slider(t(lang, "pct_threshold"), 0, 99, 90)
        thr = float(np.nanpercentile(m["budget_eur"].astype(float).values, pct)) if len(m) else 0.0
        st.caption(f"≥ {fmt_money(thr, lang)}")
    with b:
        topn = st.number_input(t(lang, "topn"), min_value=20, max_value=5000, value=200, step=10)
    with c:
        query = st.text_input(t(lang, "search_actor"), value="")

    m2 = m[m["budget_eur"].astype(float) >= thr].copy()
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

        # Build treemap base by SQL (theme, country, actor)
        base = fetch_df(f"""
        SELECT theme, country_name, 
               COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS org_name2,
               actor_id,
               SUM(amount_eur) AS amount_eur
        FROM {R}
        WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
        GROUP BY theme, country_name, org_name2, actor_id
        """)
        if base.empty:
            st.info(t(lang, "no_data"))
        else:
            base["theme_display"] = base["theme"].astype(str).map(lambda x: theme_raw_to_display(x, lang))
            base["actor_label"] = np.where(
                base["org_name2"].astype(str) == base["actor_id"].astype(str),
                base["actor_id"].astype(str),
                (base["org_name2"].astype(str) + " — " + base["country_name"].astype(str)),
            )

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

            agg["budget_str"] = agg["amount_eur"].apply(lambda v: fmt_money(float(v), lang))

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
# TAB TRENDS (DuckDB)
# ============================================================
with tab_trends:
    dim_choice = st.radio(
        t(lang, "dimension"),
        [t(lang, "dim_theme"), t(lang, "dim_section")],
        index=0,
        horizontal=True,
    )
    dim_col = "section" if dim_choice == t(lang, "dim_section") else "theme"

    dim_budget = fetch_df(f"""
    SELECT {dim_col} AS dim, SUM(amount_eur) AS amount_eur
    FROM {R}
    WHERE {W}
    GROUP BY dim
    ORDER BY amount_eur DESC
    """)
    if dim_budget.empty:
        st.info(t(lang, "no_data"))
    else:
        dims_all_raw = [str(x) for x in dim_budget["dim"].tolist() if str(x).strip()]
        dims_all_disp = [theme_raw_to_display(x, lang) if dim_col == "theme" else x for x in dims_all_raw]
        top_default = dims_all_disp[: min(12, len(dims_all_disp))]

        selected_dims = st.multiselect("Séries" if lang == "FR" else "Series", dims_all_disp, default=top_default)
        if not selected_dims:
            st.info(t(lang, "no_data"))
        else:
            # translate back to raw if needed
            if dim_col == "theme":
                disp_to_raw = {theme_raw_to_display(x, lang): x for x in dims_all_raw}
                selected_raw = [disp_to_raw.get(d, d) for d in selected_dims]
            else:
                selected_raw = selected_dims

            mode = st.radio(t(lang, "mode"), [t(lang, "mode_abs"), t(lang, "mode_share")], index=1, horizontal=True, key="tr_mode")

            tdf = fetch_df(f"""
            SELECT year, {dim_col} AS dim, SUM(amount_eur) AS amount_eur
            FROM {R}
            WHERE {W} AND {dim_col} IN {in_list(selected_raw)}
            GROUP BY year, dim
            ORDER BY year
            """)

            if dim_col == "theme":
                tdf["dim"] = tdf["dim"].map(lambda x: theme_raw_to_display(str(x), lang))

            if mode == t(lang, "mode_share"):
                yearly = tdf.groupby("year")["amount_eur"].transform("sum").replace(0, np.nan)
                tdf["value"] = (tdf["amount_eur"] / yearly).fillna(0.0) * 100.0
                ylab = "Part (%)" if lang == "FR" else "Share (%)"
                hover = lambda v: f"{v:.1f}%"
            else:
                tdf["value"] = tdf["amount_eur"]
                ylab = "Budget (€)"
                hover = lambda v: fmt_money(float(v), lang)

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
            drivers = tdf.groupby("dim", as_index=False)["amount_eur"].sum().sort_values("amount_eur", ascending=False).head(20)
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
                customdata=np.stack([drivers["amount_eur"].apply(lambda v: fmt_money(float(v), lang)).iloc[::-1]], axis=-1),
                hovertemplate="<b>%{y}</b><br>Budget: %{customdata[0]}<extra></extra>",
            )
            fig_drv.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
            st.plotly_chart(fig_drv, use_container_width=True)


# ============================================================
# TAB COMPARE (DuckDB)
# ============================================================
with tab_compare:
    st.markdown(f"### {t(lang, 'compare_title')}")
    st.caption(t(lang, "compare_caption"))

    min_year = meta["miny"]
    max_year = meta["maxy"]

    a1, a2 = st.columns(2)
    with a1:
        period_a = st.slider(t(lang, "period_a"), min_year, max_year, (max(min_year, max_year - 8), max_year - 5))
    with a2:
        period_b = st.slider(t(lang, "period_b"), min_year, max_year, (max_year - 3, max_year))

    dim_choice = st.radio(t(lang, "dimension"), [t(lang, "dim_theme"), t(lang, "dim_section")], index=0, horizontal=True, key="cmp_dim")
    dim_col = "section" if dim_choice == t(lang, "dim_section") else "theme"

    def share_df(y0: int, y1: int) -> pd.DataFrame:
        return fetch_df(f"""
        WITH g AS (
          SELECT {dim_col} AS dim, SUM(amount_eur) AS b
          FROM {R}
          WHERE {W} AND year BETWEEN {int(y0)} AND {int(y1)}
          GROUP BY dim
        ),
        tot AS (SELECT SUM(b) AS t FROM g)
        SELECT g.dim, CASE WHEN tot.t > 0 THEN g.b / tot.t ELSE 0 END AS s
        FROM g, tot
        """)

    sA = share_df(period_a[0], period_a[1])
    sB = share_df(period_b[0], period_b[1])
    view = pd.merge(sA, sB, on="dim", how="outer", suffixes=("_A", "_B")).fillna(0.0)
    view["delta_share"] = view["s_B"] - view["s_A"]
    view = view.sort_values("delta_share", ascending=False)

    if dim_col == "theme":
        view["dim_disp"] = view["dim"].map(lambda x: theme_raw_to_display(str(x), lang))
    else:
        view["dim_disp"] = view["dim"].astype(str)

    topk = st.slider("Top K variations" if lang == "FR" else "Top K shifts", 10, 60, 25)
    view2 = pd.concat([view.head(topk), view.tail(topk)]).drop_duplicates().sort_values("delta_share")

    fig = px.bar(
        view2,
        x=(view2["delta_share"] * 100.0),
        y=view2["dim_disp"],
        orientation="h",
        height=680,
        labels={"x": "Δ (points de %)" if lang == "FR" else "Δ (pp)", "y": ""},
    )
    st.plotly_chart(fig, use_container_width=True)

    table = view.head(60).copy()
    table["Part A (%)" if lang == "FR" else "Share A (%)"] = table["s_A"].map(lambda x: f"{100*x:.1f}%")
    table["Part B (%)" if lang == "FR" else "Share B (%)"] = table["s_B"].map(lambda x: f"{100*x:.1f}%")
    table["Δ"] = table["delta_share"].map(lambda x: fmt_pp(float(x), 2, lang))
    table = table[["dim_disp", "Part A (%)" if lang == "FR" else "Share A (%)", "Part B (%)" if lang == "FR" else "Share B (%)", "Δ"]]
    table = table.rename(columns={"dim_disp": "dim"})
    st.dataframe(table, use_container_width=True, height=520)


# ============================================================
# TAB MACRO & NEWS — independent (DuckDB + events.csv)
# ============================================================
with tab_macro:
    st.markdown(f"### {t(lang,'macro_title')}")
    st.caption(t(lang, "macro_subtitle"))

    ev = load_events()
    if ev.empty:
        st.warning("events.csv est introuvable ou vide." if lang == "FR" else "events.csv is missing or empty.")
    else:
        with st.expander(t(lang, "macro_filters"), expanded=True):
            macro_use_global = st.checkbox(t(lang, "macro_use_global"), value=False, key="macro_use_global")
            macro_onetech = st.checkbox(t(lang, "onetech_only"), value=True, key="macro_onetech_only")
            macro_years = st.slider(
                t(lang, "period"),
                meta["miny"],
                meta["maxy"],
                (meta["miny"], meta["maxy"]),
                key="macro_years",
            )
            macro_mode = st.radio(t(lang, "macro_metric"), [t(lang, "mode_abs"), t(lang, "mode_share")], index=0, horizontal=True, key="macro_metric_mode")
            match_mode = st.radio(
                t(lang, "macro_match"),
                [t(lang, "macro_match_theme"), t(lang, "macro_match_tag")],
                index=1,
                horizontal=True,
                key="macro_match_mode",
            )
            show_overlay = st.checkbox(t(lang, "macro_overlay"), value=True, key="macro_overlay")
            window = st.slider(t(lang, "macro_window"), 0, 3, 1, 1, key="macro_window")

        macro_parts = [f"year BETWEEN {int(macro_years[0])} AND {int(macro_years[1])}"]
        if macro_use_global:
            macro_parts.append(f"({W})")
        if macro_onetech:
            macro_parts.append(f"theme IN {in_list(sorted(list(ONETECH_THEMES_EN)))}")
        macro_W = " AND ".join(macro_parts)

        themes_raw_macro = list_str(f"SELECT DISTINCT theme FROM {R} WHERE {macro_W} ORDER BY theme")
        chosen_theme_raw: Optional[str] = None
        ev_sel = pd.DataFrame(columns=ev.columns)

        if match_mode == t(lang, "macro_match_tag"):
            tags = sorted([x for x in ev["tag"].astype(str).unique().tolist() if x.strip()])
            if not tags:
                st.info(t(lang, "macro_no_events"))
            else:
                chosen_tag = st.selectbox(t(lang, "macro_pick_tag"), tags, index=0)
                candidate_themes = TAG_TO_THEMES.get(str(chosen_tag), set())
                themes_candidates = themes_raw_macro
                if candidate_themes:
                    themes_candidates = [
                        raw for raw in themes_raw_macro
                        if (raw in candidate_themes) or (theme_raw_to_display(raw, lang) in candidate_themes)
                    ]
                if not themes_candidates:
                    st.info(t(lang, "no_data"))
                else:
                    chosen_theme_raw = st.selectbox(
                        t(lang, "macro_pick_theme"),
                        themes_candidates,
                        index=0,
                        format_func=lambda x: theme_raw_to_display(str(x), lang),
                    )
                    ev_sel = ev[ev["tag"].astype(str) == str(chosen_tag)].copy()
        else:
            if not themes_raw_macro:
                st.info(t(lang, "no_data"))
            else:
                chosen_theme_raw = st.selectbox(
                    t(lang, "macro_pick_theme"),
                    themes_raw_macro,
                    index=0,
                    format_func=lambda x: theme_raw_to_display(str(x), lang),
                )
                th_disp = theme_raw_to_display(str(chosen_theme_raw), lang)
                ev_sel = ev[
                    (ev["theme"].astype(str) == str(chosen_theme_raw))
                    | (ev["theme"].astype(str) == th_disp)
                ].copy()

        if chosen_theme_raw:
            agg = fetch_df(f"""
            SELECT year, SUM(amount_eur) AS budget_total, COUNT(DISTINCT projectID) AS n_projects
            FROM {R}
            WHERE {macro_W} AND theme = '{chosen_theme_raw.replace("'", "''")}'
            GROUP BY year
            ORDER BY year
            """)

            if agg.empty:
                st.info(t(lang, "no_data"))
            else:
                if macro_mode == t(lang, "mode_share"):
                    tot = float(agg["budget_total"].sum())
                    agg["value"] = (agg["budget_total"] / tot * 100.0) if tot > 0 else 0.0
                    ylab = "Part du budget (%)" if lang == "FR" else "Budget share (%)"
                    hover_val = lambda v: f"{v:.1f}%"
                else:
                    agg["value"] = agg["budget_total"]
                    ylab = "Budget (€)"
                    hover_val = lambda v: fmt_money(float(v), lang)

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
                        agg["budget_total"].apply(lambda v: fmt_money(float(v), lang)).values,
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
                else:
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
                        st.write(f"**{'Année' if lang == 'FR' else 'Year'}**: {ey}")
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
                    proj = fetch_df(f"""
                    SELECT projectID, title, MIN(year) AS year, SUM(amount_eur) AS budget_eur
                    FROM {R}
                    WHERE {macro_W} AND theme = '{chosen_theme_raw.replace("'", "''")}' AND year BETWEEN {int(y0)} AND {int(y1)}
                    GROUP BY projectID, title
                    ORDER BY budget_eur DESC
                    LIMIT 25
                    """)
                    if proj.empty:
                        st.info("Aucun projet dans la fenêtre." if lang == "FR" else "No projects in the window.")
                    else:
                        proj["budget"] = proj["budget_eur"].apply(lambda v: fmt_money(float(v), lang))
                        st.dataframe(proj[["year", "projectID", "title", "budget"]], use_container_width=True, height=520)


# ============================================================
# TAB ACTOR PROFILE (DuckDB)
# ============================================================
with tab_actor:
    st.markdown(f"### {t(lang, 'actor_profile')}")

    actors = fetch_df(f"""
    SELECT actor_id,
           COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS org_name2,
           COALESCE(NULLIF(TRIM(country_name), ''), 'Unknown') AS country_name2,
           SUM(amount_eur) AS budget_eur
    FROM {R}
    WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
    GROUP BY actor_id, org_name2, country_name2
    ORDER BY budget_eur DESC
    LIMIT 5000
    """)
    if actors.empty:
        st.info(t(lang, "no_data"))
    else:
        actors["actor_label"] = np.where(
            actors["org_name2"].astype(str) == actors["actor_id"].astype(str),
            actors["actor_id"].astype(str),
            (actors["org_name2"].astype(str) + " — " + actors["country_name2"].astype(str)),
        )

        picked_label = st.selectbox(t(lang, "actor_picker"), actors["actor_label"].astype(str).tolist(), index=0)
        picked_id = actors.loc[actors["actor_label"].astype(str) == picked_label, "actor_id"].iloc[0]

        st.markdown(f"#### {t(lang, 'actor_trend')}")
        byy = fetch_df(f"""
        SELECT year, SUM(amount_eur) AS budget_eur, COUNT(DISTINCT projectID) AS n_projects
        FROM {R}
        WHERE {W} AND actor_id = '{str(picked_id).replace("'", "''")}'
        GROUP BY year
        ORDER BY year
        """)
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
        mix_t = fetch_df(f"""
        SELECT theme, SUM(amount_eur) AS budget_eur
        FROM {R}
        WHERE {W} AND actor_id = '{str(picked_id).replace("'", "''")}'
        GROUP BY theme
        ORDER BY budget_eur DESC
        LIMIT 15
        """)
        mix_t["theme_display"] = mix_t["theme"].map(lambda x: theme_raw_to_display(str(x), lang))
        figt = px.bar(mix_t.iloc[::-1], x="budget_eur", y="theme_display", orientation="h", height=420,
                      color="budget_eur", color_continuous_scale=R2G, labels={"budget_eur": "Budget (€)", "theme_display": ""})
        st.plotly_chart(figt, use_container_width=True)

        st.markdown(f"#### {t(lang, 'actor_mix_country')}")
        mix_c = fetch_df(f"""
        SELECT country_name, SUM(amount_eur) AS budget_eur
        FROM {R}
        WHERE {W} AND actor_id = '{str(picked_id).replace("'", "''")}'
        GROUP BY country_name
        ORDER BY budget_eur DESC
        LIMIT 15
        """)
        figc = px.bar(mix_c.iloc[::-1], x="budget_eur", y="country_name", orientation="h", height=420,
                      color="budget_eur", color_continuous_scale=R2G, labels={"budget_eur": "Budget (€)", "country_name": ""})
        st.plotly_chart(figc, use_container_width=True)

        st.divider()
        st.markdown(f"#### {t(lang, 'actor_partners')}")
        partners = fetch_df(f"""
        WITH my_projects AS (
          SELECT DISTINCT projectID
          FROM {R}
          WHERE {W} AND actor_id = '{str(picked_id).replace("'", "''")}'
        )
        SELECT
          COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS org_name2,
          COALESCE(NULLIF(TRIM(country_name), ''), 'Unknown') AS country_name2,
          actor_id,
          COUNT(DISTINCT r.projectID) AS n_projects,
          SUM(r.amount_eur) AS budget_eur
        FROM {R} r
        JOIN my_projects p ON r.projectID = p.projectID
        WHERE {W} AND r.actor_id IS NOT NULL AND TRIM(r.actor_id) <> '' AND r.actor_id <> '{str(picked_id).replace("'", "''")}'
        GROUP BY org_name2, country_name2, actor_id
        ORDER BY n_projects DESC, budget_eur DESC
        LIMIT 25
        """)
        if partners.empty:
            st.info(t(lang, "no_data"))
        else:
            partners["actor_label"] = np.where(
                partners["org_name2"].astype(str) == partners["actor_id"].astype(str),
                partners["actor_id"].astype(str),
                (partners["org_name2"].astype(str) + " — " + partners["country_name2"].astype(str)),
            )
            partners["budget_eur"] = partners["budget_eur"].apply(lambda v: fmt_money(float(v), lang))
            st.dataframe(partners[["actor_label", "n_projects", "budget_eur"]], use_container_width=True, height=520)


# ============================================================
# TAB NETWORK & VALUE CHAIN (DuckDB)
# ============================================================
with tab_network:
    st.markdown("### " + ("Chaîne de valeur & réseau de collaboration" if lang == "FR" else "Value chain & collaboration network"))

    # ---------- Value chain ----------
    st.markdown("#### " + ("Chaîne de valeur (budget -> acteurs)" if lang == "FR" else "Value chain (budget -> actors)"))
    vc_dim = fetch_df(f"""
    SELECT theme, value_chain_stage, SUM(amount_eur) AS budget_eur
    FROM {R}
    WHERE {W}
    GROUP BY theme, value_chain_stage
    ORDER BY budget_eur DESC
    """)

    if vc_dim.empty:
        st.info(t(lang, "missing_stage_col") if "value_chain_stage" not in set(base_schema_columns()) else t(lang, "no_data"))
    else:
        top_themes = (
            vc_dim.groupby("theme", as_index=False)["budget_eur"]
            .sum()
            .sort_values("budget_eur", ascending=False)["theme"]
            .head(20)
            .astype(str)
            .tolist()
        )
        default_themes = top_themes[: min(6, len(top_themes))]

        cvc1, cvc2 = st.columns([2, 1])
        with cvc1:
            picked_themes = st.multiselect(
                "Thématiques" if lang == "FR" else "Themes",
                top_themes,
                default=default_themes,
                format_func=lambda x: theme_raw_to_display(str(x), lang),
            )
        with cvc2:
            vc_top_actors = st.slider(
                "Top acteurs" if lang == "FR" else "Top actors",
                8,
                50,
                20,
                1,
            )

        if picked_themes:
            vc = fetch_df(f"""
            SELECT
              value_chain_stage,
              actor_id,
              COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
              SUM(amount_eur) AS budget_eur
            FROM {R}
            WHERE {W} AND theme IN {in_list(picked_themes)}
            GROUP BY value_chain_stage, actor_id, actor_label
            """)

            if vc.empty:
                st.info(t(lang, "no_data"))
            else:
                rank_actors = (
                    vc.groupby(["actor_id", "actor_label"], as_index=False)["budget_eur"]
                    .sum()
                    .sort_values("budget_eur", ascending=False)
                    .head(int(vc_top_actors))
                )
                vc = vc.merge(rank_actors[["actor_id"]], on="actor_id", how="inner")

                stage_order = (
                    vc.groupby("value_chain_stage", as_index=False)["budget_eur"]
                    .sum()
                    .sort_values("budget_eur", ascending=False)["value_chain_stage"]
                    .astype(str)
                    .tolist()
                )
                actor_order = rank_actors["actor_label"].astype(str).tolist()
                node_labels = stage_order + actor_order
                node_idx = {k: i for i, k in enumerate(node_labels)}

                links = (
                    vc.groupby(["value_chain_stage", "actor_label"], as_index=False)["budget_eur"]
                    .sum()
                    .sort_values("budget_eur", ascending=False)
                )
                source = [node_idx[str(s)] for s in links["value_chain_stage"].astype(str)]
                target = [node_idx[str(a)] for a in links["actor_label"].astype(str)]
                value = links["budget_eur"].astype(float).tolist()

                fig_sankey = go.Figure(
                    data=[
                        go.Sankey(
                            node=dict(
                                pad=14,
                                thickness=14,
                                line=dict(color="rgba(255,255,255,0.22)", width=0.5),
                                label=node_labels,
                            ),
                            link=dict(source=source, target=target, value=value),
                        )
                    ]
                )
                fig_sankey.update_layout(height=620, margin=dict(l=10, r=10, t=20, b=10))
                st.plotly_chart(fig_sankey, use_container_width=True)

                stage_tbl = (
                    vc.groupby("value_chain_stage", as_index=False)["budget_eur"]
                    .sum()
                    .sort_values("budget_eur", ascending=False)
                )
                stage_tbl["budget"] = stage_tbl["budget_eur"].map(lambda x: fmt_money(float(x), lang))
                stage_tbl = stage_tbl.rename(columns={"value_chain_stage": ("Étape chaîne de valeur" if lang == "FR" else "Value-chain stage")})
                st.dataframe(stage_tbl[[("Étape chaîne de valeur" if lang == "FR" else "Value-chain stage"), "budget"]], use_container_width=True, height=260)
        else:
            st.info("Sélectionne au moins une thématique." if lang == "FR" else "Select at least one theme.")

    st.divider()

    # ---------- Collaboration graph ----------
    st.markdown("#### " + ("Graphe de collaboration (acteur focal)" if lang == "FR" else "Collaboration graph (focal actor)"))
    actor_rank = fetch_df(f"""
    SELECT
      actor_id,
      COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
      SUM(amount_eur) AS budget_eur,
      COUNT(DISTINCT projectID) AS n_projects
    FROM {R}
    WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
    GROUP BY actor_id, actor_label
    ORDER BY budget_eur DESC
    LIMIT 400
    """)

    if actor_rank.empty:
        st.info(t(lang, "no_data"))
    else:
        dup = actor_rank["actor_label"].astype(str).duplicated(keep=False)
        actor_rank["actor_display"] = np.where(
            dup,
            actor_rank["actor_label"].astype(str) + " [" + actor_rank["actor_id"].astype(str).str.slice(0, 18) + "]",
            actor_rank["actor_label"].astype(str),
        )

        nc1, nc2 = st.columns([2, 1])
        with nc1:
            focal_display = st.selectbox(
                "Acteur focal" if lang == "FR" else "Focal actor",
                actor_rank["actor_display"].astype(str).tolist(),
                index=0,
            )
        with nc2:
            net_top = st.slider(
                "Top partenaires" if lang == "FR" else "Top partners",
                5,
                40,
                16,
                1,
            )

        focal_row = actor_rank[actor_rank["actor_display"].astype(str) == focal_display].iloc[0]
        focal_id = str(focal_row["actor_id"]).replace("'", "''")
        focal_label = str(focal_row["actor_label"])

        partners = fetch_df(f"""
        WITH part AS (
          SELECT
            projectID,
            actor_id,
            COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
            SUM(amount_eur) AS actor_budget
          FROM {R}
          WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
          GROUP BY projectID, actor_id, actor_label
        ),
        focus_projects AS (
          SELECT DISTINCT projectID FROM part WHERE actor_id = '{focal_id}'
        )
        SELECT
          p.actor_id,
          p.actor_label,
          COUNT(DISTINCT p.projectID) AS shared_projects,
          SUM(p.actor_budget) AS partner_budget
        FROM part p
        JOIN focus_projects f ON p.projectID = f.projectID
        WHERE p.actor_id <> '{focal_id}'
        GROUP BY p.actor_id, p.actor_label
        ORDER BY shared_projects DESC, partner_budget DESC
        LIMIT {int(net_top)}
        """)

        if partners.empty:
            st.info("Aucun partenaire dans le périmètre." if lang == "FR" else "No partners in scope.")
        else:
            n = len(partners)
            angles = np.linspace(0, 2 * np.pi, n, endpoint=False)
            rx, ry = np.cos(angles), np.sin(angles)

            fig_net = go.Figure()
            for i, (_, r) in enumerate(partners.iterrows()):
                w = 1.0 + 3.5 * (float(r["shared_projects"]) / float(max(1, partners["shared_projects"].max())))
                fig_net.add_trace(
                    go.Scatter(
                        x=[0.0, float(rx[i])],
                        y=[0.0, float(ry[i])],
                        mode="lines",
                        line=dict(width=w, color="rgba(120,180,255,0.45)"),
                        hoverinfo="skip",
                        showlegend=False,
                    )
                )

            partner_size = 14 + 22 * (partners["shared_projects"].astype(float) / float(max(1, partners["shared_projects"].max())))
            fig_net.add_trace(
                go.Scatter(
                    x=[0.0],
                    y=[0.0],
                    mode="markers+text",
                    marker=dict(size=34, color="rgba(255,180,80,0.95)", line=dict(width=1, color="rgba(255,255,255,0.5)")),
                    text=[focal_label[:44]],
                    textposition="bottom center",
                    hovertemplate=f"<b>{focal_label}</b><extra></extra>",
                    showlegend=False,
                )
            )
            fig_net.add_trace(
                go.Scatter(
                    x=rx,
                    y=ry,
                    mode="markers+text",
                    marker=dict(size=partner_size, color="rgba(120,180,255,0.9)", line=dict(width=0.8, color="rgba(255,255,255,0.35)")),
                    text=[str(x)[:34] for x in partners["actor_label"].astype(str).tolist()],
                    textposition="top center",
                    customdata=np.stack(
                        [
                            partners["shared_projects"].astype(int).values,
                            partners["partner_budget"].astype(float).apply(lambda x: fmt_money(float(x), lang)).values,
                        ],
                        axis=-1,
                    ),
                    hovertemplate="<b>%{text}</b><br>Shared projects: %{customdata[0]}<br>Budget: %{customdata[1]}<extra></extra>",
                    showlegend=False,
                )
            )
            fig_net.update_layout(
                height=640,
                xaxis=dict(visible=False),
                yaxis=dict(visible=False, scaleanchor="x", scaleratio=1),
                margin=dict(l=10, r=10, t=10, b=10),
            )
            st.plotly_chart(fig_net, use_container_width=True)

            ptab = partners.copy()
            ptab["budget"] = ptab["partner_budget"].apply(lambda x: fmt_money(float(x), lang))
            ptab = ptab.rename(
                columns={
                    "actor_label": ("Partenaire" if lang == "FR" else "Partner"),
                    "shared_projects": ("Projets communs" if lang == "FR" else "Shared projects"),
                }
            )
            st.dataframe(ptab[[("Partenaire" if lang == "FR" else "Partner"), ("Projets communs" if lang == "FR" else "Shared projects"), "budget"]], use_container_width=True, height=320)


# ============================================================
# TAB DATA (paginated, DuckDB) + export
# ============================================================
with tab_data:
    st.caption(t(lang, "data_warning"))

    # Column choices (raw names)
    all_cols = [
        "source", "program", "section", "year", "country_name",
        "actor_id", "pic", "org_name", "entity_type", "project_status",
        "title", "abstract", "theme", "value_chain_stage", "amount_eur", "projectID"
    ]
    default_cols = [
        "source", "program", "section", "year", "country_name",
        "org_name", "entity_type", "project_status", "title",
        "theme", "value_chain_stage", "amount_eur", "projectID",
    ]

    selected_cols = st.multiselect(t(lang, "columns"), all_cols, default=default_cols)
    if not selected_cols:
        selected_cols = default_cols

    qtxt = st.text_input(t(lang, "filter_text"), value="")
    where_extra = ""
    if qtxt.strip():
        q = qtxt.strip().replace("'", "''").lower()
        ors = []
        for c in selected_cols:
            if c == "amount_eur":
                continue
            ors.append(f"LOWER(CAST({c} AS VARCHAR)) LIKE '%{q}%'")
        if ors:
            where_extra = " AND (" + " OR ".join(ors) + ")"

    c1, c2 = st.columns([1, 1])
    with c1:
        rows_per_page = st.selectbox(t(lang, "rows_per_page"), [100, 250, 500, 1000], index=1)
    with c2:
        page = st.number_input(t(lang, "page"), min_value=1, value=1, step=1)

    offset = (int(page) - 1) * int(rows_per_page)
    base_select_sql = f"SELECT {', '.join(selected_cols)} FROM {R} WHERE {W} {where_extra}"

    page_df_raw = fetch_df(f"{base_select_sql} LIMIT {int(rows_per_page)} OFFSET {int(offset)}")
    page_df = page_df_raw.copy()

    # Pretty display
    if "theme" in page_df.columns:
        page_df["theme"] = page_df["theme"].map(lambda x: theme_raw_to_display(str(x), lang))
    if "entity_type" in page_df.columns:
        page_df["entity_type"] = page_df["entity_type"].map(lambda x: entity_raw_to_display(str(x), lang))
    if "amount_eur" in page_df.columns:
        page_df["amount_eur"] = page_df["amount_eur"].apply(lambda v: fmt_money(float(v) if v is not None else np.nan, lang))

    st.dataframe(page_df, use_container_width=True, height=560)

    export_query_key = str(abs(hash(base_select_sql)))
    if st.session_state.get("full_export_query_key") != export_query_key:
        st.session_state.pop("full_export_bytes", None)
        st.session_state["full_export_query_key"] = export_query_key

    d1, d2 = st.columns(2)
    with d1:
        st.download_button(
            t(lang, "download_page"),
            page_df_raw.to_csv(index=False).encode("utf-8"),
            file_name="export_page.csv",
            mime="text/csv",
        )
    with d2:
        if st.button(t(lang, "prepare_full_export"), width="stretch"):
            with st.spinner("Préparation de l’export..." if lang == "FR" else "Preparing export..."):
                st.session_state["full_export_bytes"] = export_query_csv_bytes(base_select_sql)

    if "full_export_bytes" in st.session_state:
        st.download_button(
            t(lang, "download_full"),
            st.session_state["full_export_bytes"],
            file_name="export_full_filtered.csv",
            mime="text/csv",
        )


# ============================================================
# TAB QUALITY (DuckDB)
# ============================================================
with tab_quality:
    st.markdown(f"### {t(lang, 'quality_title')}")
    qd = fetch_df(f"""
    SELECT
      COUNT(*) AS rows,
      AVG(CASE WHEN actor_id IS NULL OR TRIM(actor_id)='' THEN 1 ELSE 0 END) * 100 AS missing_actor_id_pct,
      AVG(CASE WHEN org_name IS NULL OR TRIM(org_name)='' THEN 1 ELSE 0 END) * 100 AS missing_org_name_pct,
      AVG(CASE WHEN title IS NULL OR TRIM(title)='' THEN 1 ELSE 0 END) * 100 AS missing_title_pct,
      AVG(CASE WHEN amount_eur <= 0 THEN 1 ELSE 0 END) * 100 AS amount_zero_pct
    FROM {R}
    WHERE {W}
    """)
    if qd.empty:
        st.info(t(lang, "no_data"))
    else:
        out = {
            "rows": int(qd["rows"].iloc[0]),
            "missing_actor_id_%": float(qd["missing_actor_id_pct"].iloc[0] or 0.0),
            "missing_org_name_%": float(qd["missing_org_name_pct"].iloc[0] or 0.0),
            "missing_title_%": float(qd["missing_title_pct"].iloc[0] or 0.0),
            "amount_zero_%": float(qd["amount_zero_pct"].iloc[0] or 0.0),
        }
        st.dataframe(pd.DataFrame([out]), use_container_width=True)


# ============================================================
# TAB HELP (as before)
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
- L’app **ne scrape pas** au runtime : elle lit `subsidy_base.parquet` (+ events.csv).
- Le bouton **Rafraîchir** exécute :
  - `process_build.py` ou `pipeline.py` (rebuild data)
  - `build_events.py` (rebuild events)
- Connecteurs externes incrémentaux optionnels via `data/external/connectors_manifest.csv`.
- Si `data/external/actor_groups.csv` est présent : regroupement entités (PIC/groupe) + exclusion financeurs disponibles dans la sidebar.
- Python utilisé : `{PYTHON_BIN}`
                """
            )
        else:
            st.markdown(
                f"""
- The app does **not** scrape at runtime: it reads `subsidy_base.parquet` (+ events.csv).
- The **Refresh** button runs:
  - `process_build.py` or `pipeline.py` (rebuild data)
  - `build_events.py` (rebuild events)
- Optional incremental external connectors via `data/external/connectors_manifest.csv`.
- If `data/external/actor_groups.csv` exists: entity grouping (PIC/group) + funder exclusion are available in the sidebar.
- Python used: `{PYTHON_BIN}`
                """
            )


# ============================================================
# TAB GUIDE (same)
# ============================================================
with tab_guide:
    st.title(t(lang, "guide_title"))

    if lang == "FR":
        st.markdown("Guide pour lire les vues correctement, sans sur-interpréter. Tout dépend du **périmètre filtré** (sidebar).")
        with st.expander("1) Vue d’ensemble : KPIs, allocation, distribution, Lorenz", expanded=True):
            st.markdown(
                """
- KPIs = ordres de grandeur (budget, projets, acteurs, tickets).
- Histogramme log = lecture robuste du long-tail.
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
- Onglet indépendant.
- Matching par tag = plus robuste que par libellé.
- Overlay = contexte (corrélation, pas causalité).
                """
            )
        with st.expander("7) Données : pagination + export", expanded=False):
            st.markdown(
                """
- Pagination = évite MessageSizeError et OOM.
- Export = page courante (safe).
                """
            )
        with st.expander("8) Chaîne & réseau : lecture opérationnelle", expanded=False):
            st.markdown(
                """
- Sankey = budget par étape de chaîne de valeur puis acteurs.
- Graphe étoile = collaborations autour d’un acteur focal.
- Utiliser le mode regroupé (PIC/groupe) pour une lecture “groupe industriel”.
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
- Independent tab.
- Tag matching is more robust.
                """
            )
        with st.expander("7) Data: pagination + export", expanded=False):
            st.markdown(
                """
- Pagination avoids OOM.
- Export downloads the current page.
                """
            )
        with st.expander("8) Value chain & network: operational reading", expanded=False):
            st.markdown(
                """
- Sankey = budget by value-chain stage and actors.
- Star graph = collaborations around one focal actor.
- Use grouped mode (PIC/group) for industrial-group level reading.
                """
            )
