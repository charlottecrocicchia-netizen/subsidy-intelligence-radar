from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple, List, Optional
import subprocess
import sys
import tempfile
import re
import os
import time
import json
import html

import numpy as np
import pandas as pd
import streamlit as st
from streamlit import config as st_config
try:
    import pycountry
except Exception:
    pycountry = None

import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

import duckdb
from filelock import FileLock, Timeout

from cordis_taxonomy import (
    CORDIS_DOMAIN_UI_FR,
    CORDIS_DOMAIN_UI_ORDER,
    LEGACY_THEME_TO_DOMAIN_UI,
    SCIENTIFIC_SUBTHEMES_BY_DOMAIN,
)

# Interactive Sankey clicks can trigger rerun loops on Streamlit Cloud.
# Keep click mode disabled for stability; isolation is controlled via selectors.
ENABLE_SANKEY_CLICK = False
plotly_events = None
HAS_PLOTLY_EVENTS = False

WIP_SECTIONS = {
    "free_text_search": False,
    "actor_grouping": False,
    "value_chain": False,
}

GUIDED_INTENT_ORDER = [
    "projects",
    "actors",
    "countries",
    "trends",
    "value_chain",
    "macro",
]


# ============================================================
# Paths (reproductible)
# ============================================================
BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
PARQUET_PATH = DATA_DIR / "processed" / "subsidy_base.parquet"
CSV_PATH = DATA_DIR / "processed" / "subsidy_base.csv"  # optionnel (export)
EVENTS_PATH = DATA_DIR / "external" / "events.csv"
EVENTS_META_PATH = DATA_DIR / "external" / "events_meta.json"
ACTOR_GROUPS_PATH = DATA_DIR / "external" / "actor_groups.csv"
ACTOR_GROUPS_TEMPLATE_PATH = DATA_DIR / "external" / "actor_groups.template.csv"
CONNECTORS_MANIFEST_PATH = DATA_DIR / "external" / "connectors_manifest.csv"
SCIENTIFIC_SUBTHEMES_PARQUET_PATH = DATA_DIR / "processed" / "project_scientific_subthemes.parquet"
PIPELINE_STATE_PATH = DATA_DIR / "processed" / "_state.json"
REQUIREMENTS_PATH = BASE_DIR / "requirements.txt"

# Offline scripts (ONLY on refresh click)
BUILD_EVENTS_SCRIPT = BASE_DIR / "build_events.py"
PROCESS_BUILD_SCRIPT = BASE_DIR / "process_build.py"
PIPELINE_SCRIPT = BASE_DIR / "pipeline.py"

PYTHON_BIN = sys.executable

# Global lock (works on Streamlit Cloud)
LOCK_PATH = Path(tempfile.gettempdir()) / "subsidy_radar_refresh.lock"
REFRESH_LOCK_STALE_SEC = int(os.environ.get("SUBSIDY_REFRESH_LOCK_STALE_SEC", "7200"))  # 2h


# ============================================================
# Page config + style
# ============================================================
STREAMLIT_THEME_OVERRIDES = {
    "theme.base": "dark",
    "theme.primaryColor": "#4F7CAC",
    "theme.backgroundColor": "#0B1220",
    "theme.secondaryBackgroundColor": "#111827",
    "theme.textColor": "#F8FAFC",
    "theme.dataframeHeaderBackgroundColor": "#162033",
    "theme.dataframeBorderColor": "#243145",
}
for _theme_key, _theme_value in STREAMLIT_THEME_OVERRIDES.items():
    try:
        st_config.set_option(_theme_key, _theme_value)
    except Exception:
        pass

st.set_page_config(page_title="Subsidy Intelligence Radar", layout="wide", initial_sidebar_state="collapsed")

st.markdown(
    """
<style>
  :root {
    --primary-color: #4F7CAC;
    --background-color: #0B1220;
    --secondary-background-color: #111827;
    --text-color: #F8FAFC;
    --sir-bg: #0B1220;
    --sir-sidebar: #111827;
    --sir-surface: #0F172A;
    --sir-surface-soft: #162033;
    --sir-border: #243145;
    --sir-border-strong: #334155;
    --sir-text: #F8FAFC;
    --sir-text-secondary: #D0D8E4;
    --sir-text-muted: #94A3B8;
    --sir-blue: #4F7CAC;
    --sir-cyan: #22D3EE;
    --sir-teal: #14B8A6;
    --sir-green: #22C55E;
    --sir-orange: #F97316;
    --sir-coral: #FB7185;
    --sir-yellow: #FACC15;
    --sir-blue-soft: rgba(79, 124, 172, 0.14);
    --sir-cyan-soft: rgba(34, 211, 238, 0.14);
    --sir-teal-soft: rgba(20, 184, 166, 0.14);
    --sir-green-soft: rgba(34, 197, 94, 0.18);
    --sir-orange-soft: rgba(249, 115, 22, 0.10);
    --sir-accent: #22D3EE;
    --sir-accent-pale: rgba(34, 211, 238, 0.12);
    --sir-success: #22C55E;
    --sir-warning: #FACC15;
    --sir-danger: #FB7185;
    --sir-shadow: 0 20px 48px rgba(2, 6, 23, 0.42);
    --sir-font: system-ui, -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display", "Segoe UI", sans-serif;
  }

  html, body, [data-testid="stAppViewContainer"], .main {
    background: var(--sir-bg);
    color: var(--sir-text);
    color-scheme: dark;
    font-family: var(--sir-font) !important;
  }

  [data-testid="stAppViewContainer"] > .main .block-container {
    padding-top: 1.2rem;
    padding-bottom: 3rem;
    max-width: 1500px;
  }

  .sir-wip-badge-wrap {
    display: flex;
    justify-content: flex-end;
    margin: 0 0 0.35rem 0;
  }

  .sir-wip-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    padding: 0.2rem 0.52rem;
    border-radius: 999px;
    border: 1px solid rgba(249, 115, 22, 0.30);
    background: rgba(249, 115, 22, 0.14);
    color: #FDBA74;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.01em;
    line-height: 1;
    white-space: nowrap;
  }

  section[data-testid="stSidebar"] {
    background: var(--sir-sidebar);
    border-right: 1px solid var(--sir-border);
    color-scheme: dark;
  }

  section[data-testid="stSidebar"] .block-container {
    padding-top: 1rem;
  }

  section[data-testid="stSidebar"] [data-testid="stWidgetLabel"],
  section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] *,
  section[data-testid="stSidebar"] .stSelectbox label,
  section[data-testid="stSidebar"] .stMultiSelect label,
  section[data-testid="stSidebar"] .stTextInput label,
  section[data-testid="stSidebar"] .stTextArea label,
  section[data-testid="stSidebar"] .stNumberInput label,
  section[data-testid="stSidebar"] .stRadio label,
  section[data-testid="stSidebar"] .stCheckbox label,
  section[data-testid="stSidebar"] .stSlider label {
    display: block !important;
    width: 100% !important;
    white-space: normal !important;
    overflow-wrap: anywhere !important;
    word-break: break-word !important;
    line-height: 1.46 !important;
    margin-bottom: 0.12rem !important;
  }

  section[data-testid="stSidebar"] details[data-testid="stExpander"] summary,
  section[data-testid="stSidebar"] details[data-testid="stExpander"] summary * {
    white-space: normal !important;
    overflow-wrap: anywhere !important;
    word-break: break-word !important;
    line-height: 1.42 !important;
  }

  section[data-testid="stSidebar"] details[data-testid="stExpander"] summary {
    position: relative !important;
    list-style: none !important;
    padding-top: 0.78rem !important;
    padding-right: 0.9rem !important;
    padding-bottom: 0.78rem !important;
    min-height: 2.9rem !important;
  }

  section[data-testid="stSidebar"] details[data-testid="stExpander"] summary::-webkit-details-marker {
    display: none !important;
  }

  section[data-testid="stSidebar"] details[data-testid="stExpander"] summary [data-testid="stExpanderToggleIcon"],
  section[data-testid="stSidebar"] details[data-testid="stExpander"] summary .material-symbols-rounded,
  section[data-testid="stSidebar"] details[data-testid="stExpander"] summary .material-symbols-outlined,
  section[data-testid="stSidebar"] details[data-testid="stExpander"] summary .material-icons {
    color: var(--sir-cyan) !important;
    font-size: 1rem !important;
    margin-right: 0.5rem !important;
    flex: 0 0 auto !important;
  }

  section[data-testid="stSidebar"] .stSelectbox,
  section[data-testid="stSidebar"] .stMultiSelect,
  section[data-testid="stSidebar"] .stSlider,
  section[data-testid="stSidebar"] .stRadio,
  section[data-testid="stSidebar"] .stCheckbox,
  section[data-testid="stSidebar"] .stTextInput,
  section[data-testid="stSidebar"] .stNumberInput {
    margin-bottom: 0.5rem;
  }

  section[data-testid="stSidebar"] div[data-baseweb="select"] > div,
  section[data-testid="stSidebar"] div[data-baseweb="base-input"] > div {
    min-height: 52px !important;
    height: auto !important;
    padding-top: 6px !important;
    padding-bottom: 6px !important;
    align-items: flex-start !important;
  }

  section[data-testid="stSidebar"] div[data-baseweb="select"] > div > div {
    align-items: flex-start !important;
    align-content: flex-start !important;
    flex-wrap: wrap !important;
    row-gap: 6px !important;
  }

  section[data-testid="stSidebar"] div[data-baseweb="select"] span,
  section[data-testid="stSidebar"] div[data-baseweb="select"] input,
  section[data-testid="stSidebar"] div[data-baseweb="base-input"] input {
    line-height: 1.42 !important;
  }

  section[data-testid="stSidebar"] div[data-baseweb="tag"] {
    display: inline-flex !important;
    align-items: center !important;
    max-width: 100%;
    margin-top: 0 !important;
    margin-right: 6px !important;
    margin-bottom: 6px !important;
    padding: 3px 8px !important;
    background-color: rgba(79, 124, 172, 0.22) !important;
    background-image: linear-gradient(135deg, rgba(79, 124, 172, 0.24), rgba(34, 211, 238, 0.12)) !important;
    border: 1px solid rgba(34, 211, 238, 0.30) !important;
    color: var(--sir-text) !important;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.02) !important;
  }

  section[data-testid="stSidebar"] div[data-baseweb="tag"] span {
    display: block !important;
    white-space: normal !important;
    overflow-wrap: anywhere !important;
    word-break: break-word !important;
    line-height: 1.34 !important;
    color: var(--sir-text) !important;
  }

  section[data-testid="stSidebar"] div[data-baseweb="tag"] *,
  section[data-testid="stSidebar"] div[data-baseweb="tag"] span {
    color: var(--sir-text) !important;
    background: transparent !important;
  }

  section[data-testid="stSidebar"] div[data-baseweb="tag"] svg,
  section[data-testid="stSidebar"] div[data-baseweb="tag"] path {
    fill: rgba(208, 216, 228, 0.88) !important;
  }

  section[data-testid="stSidebar"] [role="slider"] {
    min-height: 20px;
  }

  h1, h2, h3, h4, h5 {
    color: var(--sir-text);
    letter-spacing: -0.02em;
    font-weight: 650;
    line-height: 1.18;
  }

  h1 {
    margin-bottom: 0.18rem;
    font-size: clamp(1.8rem, 2.5vw, 2.35rem);
  }

  h2 {
    font-size: clamp(1.34rem, 1.9vw, 1.7rem);
  }

  h3 {
    margin-top: 0.8rem;
    font-size: clamp(1.08rem, 1.4vw, 1.28rem);
  }

  h4 {
    font-size: clamp(0.98rem, 1.2vw, 1.1rem);
  }

  .stCaption,
  [data-testid="stCaptionContainer"] {
    color: var(--sir-text-muted) !important;
    font-size: 0.84rem !important;
    line-height: 1.45 !important;
  }

  [data-testid="stAlert"] {
    background: linear-gradient(135deg, rgba(22, 32, 51, 0.96), rgba(15, 23, 42, 0.98)) !important;
    border: 1px solid var(--sir-border) !important;
    border-radius: 14px !important;
    box-shadow: 0 14px 32px rgba(2, 6, 23, 0.24) !important;
  }

  [data-testid="stAlert"] * {
    color: var(--sir-text) !important;
  }

  p, li, label, button, input, textarea, select {
    color: inherit;
    font-family: var(--sir-font) !important;
  }

  .material-symbols-rounded,
  .material-symbols-outlined,
  .material-icons,
  [data-testid="stExpanderToggleIcon"],
  [data-testid="stExpanderToggleIcon"] * {
    font-family: "Material Symbols Rounded", "Material Symbols Outlined", "Material Icons" !important;
    font-style: normal !important;
    font-weight: 400 !important;
    letter-spacing: normal !important;
    text-transform: none !important;
    line-height: 1 !important;
    white-space: nowrap !important;
    word-wrap: normal !important;
    -webkit-font-feature-settings: "liga" !important;
    -webkit-font-smoothing: antialiased !important;
  }

  [data-testid="stMarkdownContainer"],
  [data-testid="stMarkdownContainer"] p,
  [data-testid="stMarkdownContainer"] li,
  [data-testid="stWidgetLabel"],
  [data-testid="stWidgetLabel"] *,
  .stSelectbox label,
  .stMultiSelect label,
  .stTextInput label,
  .stTextArea label,
  .stNumberInput label,
  .stRadio label,
  .stCheckbox label,
  .stSlider label {
    color: var(--sir-text) !important;
    font-size: 0.92rem !important;
    line-height: 1.4 !important;
  }

  .stTextInput input::placeholder,
  .stTextArea textarea::placeholder,
  .stNumberInput input::placeholder {
    color: var(--sir-text-muted) !important;
    opacity: 1 !important;
  }

  a, a:visited {
    color: var(--sir-blue);
  }

  hr {
    border-color: var(--sir-border) !important;
  }

  [data-testid="stToolbar"],
  [data-testid="stDecoration"],
  [data-testid="stStatusWidget"] {
    background: transparent !important;
  }

  div[data-testid="metric-container"] {
    background: var(--sir-surface);
    border: 1px solid var(--sir-border);
    border-top: 3px solid rgba(91, 192, 235, 0.78);
    padding: 12px 14px;
    border-radius: 16px;
    box-shadow: var(--sir-shadow);
    min-height: 112px;
    height: 100%;
  }

  div[data-testid="metric-container"] [data-testid="stMetricLabel"] {
    color: var(--sir-text-secondary) !important;
    font-weight: 580;
    font-size: 0.76rem !important;
    line-height: 1.4 !important;
    white-space: normal !important;
    overflow-wrap: anywhere !important;
    word-break: break-word !important;
    min-height: 2.5em;
  }

  div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: var(--sir-text) !important;
    font-weight: 630;
    font-size: clamp(1.02rem, 1.45vw, 1.38rem) !important;
    line-height: 1.14 !important;
    letter-spacing: -0.02em;
    white-space: normal !important;
    overflow-wrap: anywhere !important;
    word-break: break-word !important;
  }

  div[data-testid="metric-container"] [data-testid="stMetricDelta"] {
    font-size: 0.78rem !important;
    line-height: 1.3 !important;
    white-space: normal !important;
    overflow-wrap: anywhere !important;
  }

  .stTabs [data-baseweb="tab-list"] {
    gap: 0.45rem;
    margin-bottom: 0.6rem;
  }

  .stTabs [data-baseweb="tab"] {
    background: var(--sir-surface);
    border: 1px solid var(--sir-border);
    border-radius: 12px;
    padding: 9px 13px;
    color: var(--sir-text-secondary);
    font-weight: 580;
    font-size: 0.92rem;
    line-height: 1.2;
  }

  .stTabs [data-baseweb="tab"] *,
  .stTabs [data-baseweb="tab"] span {
    color: inherit !important;
  }

  .stTabs [data-baseweb="tab"]:hover {
    background: var(--sir-surface-soft);
    border-color: var(--sir-border-strong);
    color: var(--sir-text);
  }

  .stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, rgba(79, 124, 172, 0.18), rgba(34, 211, 238, 0.10));
    border: 1px solid rgba(34, 211, 238, 0.32);
    color: var(--sir-text);
    box-shadow: 0 10px 28px rgba(79, 124, 172, 0.14);
  }

  .stTabs [aria-selected="true"] *,
  .stTabs [aria-selected="true"] span {
    color: var(--sir-text) !important;
  }

  .stButton > button,
  .stDownloadButton > button {
    background: var(--sir-surface);
    color: var(--sir-text);
    border: 1px solid var(--sir-border-strong);
    border-radius: 12px;
    font-weight: 600;
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.04);
  }

  .stButton > button:hover,
  .stDownloadButton > button:hover {
    border-color: rgba(34, 211, 238, 0.34);
    background: var(--sir-blue-soft);
    color: var(--sir-text);
  }

  .stButton > button:focus,
  .stDownloadButton > button:focus {
    border-color: var(--sir-cyan);
    box-shadow: 0 0 0 0.2rem rgba(34, 211, 238, 0.18);
  }

  .stTextInput input,
  .stTextArea textarea,
  .stNumberInput input,
  div[data-baseweb="select"] > div,
  div[data-baseweb="base-input"] > div {
    background: var(--sir-surface) !important;
    color: var(--sir-text) !important;
    border: 1px solid var(--sir-border) !important;
    border-radius: 12px !important;
    box-shadow: none !important;
  }

  .stTextInput input:focus,
  .stTextArea textarea:focus,
  .stNumberInput input:focus,
  div[data-baseweb="select"]:focus-within > div,
  div[data-baseweb="base-input"]:focus-within > div {
    border-color: var(--sir-blue) !important;
    box-shadow: 0 0 0 3px rgba(79, 124, 172, 0.12) !important;
  }

  div[data-baseweb="select"] span,
  div[data-baseweb="select"] input,
  div[data-baseweb="select"] svg,
  div[data-baseweb="base-input"] input {
    color: var(--sir-text) !important;
    fill: var(--sir-text-secondary) !important;
  }

  div[data-baseweb="tag"] {
    background: linear-gradient(135deg, rgba(79, 124, 172, 0.18), rgba(34, 211, 238, 0.10)) !important;
    border: 1px solid rgba(34, 211, 238, 0.24) !important;
    color: var(--sir-text) !important;
    border-radius: 10px !important;
    box-shadow: none !important;
  }

  div[data-baseweb="tag"] span,
  div[data-baseweb="tag"] svg,
  div[data-baseweb="tag"] path {
    color: var(--sir-text) !important;
    fill: rgba(208, 216, 228, 0.86) !important;
  }

  .stRadio > div,
  .stMultiSelect > div,
  .stSelectbox > div {
    color: var(--sir-text) !important;
  }

  [data-baseweb="popover"],
  [data-baseweb="menu"],
  [data-baseweb="select-dropdown"],
  [role="dialog"],
  body [id^="portal"],
  [data-baseweb="popover"] * ,
  [data-baseweb="menu"] *,
  [data-baseweb="select-dropdown"] *,
  [role="dialog"] *,
  body [id^="portal"] *,
  [role="listbox"],
  [role="option"] {
    color: var(--sir-text) !important;
  }

  [data-baseweb="popover"],
  [data-baseweb="menu"],
  [data-baseweb="select-dropdown"],
  [role="dialog"],
  body [id^="portal"],
  [role="listbox"] {
    background: var(--sir-surface-soft) !important;
    border: 1px solid var(--sir-border) !important;
    box-shadow: 0 20px 36px rgba(2, 6, 23, 0.42) !important;
    color-scheme: dark !important;
  }

  [role="option"] {
    background: var(--sir-surface-soft) !important;
  }

  [role="option"][aria-selected="true"],
  [role="option"]:hover {
    background: var(--sir-blue-soft) !important;
    color: var(--sir-text) !important;
  }

  [aria-selected="true"][role="option"] * {
    color: var(--sir-text) !important;
  }

  .stCheckbox label,
  .stRadio label {
    color: var(--sir-text) !important;
  }

  input[type="radio"],
  input[type="checkbox"] {
    accent-color: var(--sir-blue) !important;
  }

  [role="radio"][aria-checked="true"],
  [role="checkbox"][aria-checked="true"] {
    border-color: var(--sir-blue) !important;
    box-shadow: 0 0 0 1px rgba(79, 124, 172, 0.18) !important;
  }

  [role="slider"] {
    accent-color: var(--sir-blue);
  }

  .stSlider [data-baseweb="slider"] * {
    color: var(--sir-text) !important;
  }

  .stSlider [data-baseweb="slider"] [role="slider"] {
    background: var(--sir-blue) !important;
    border-color: var(--sir-blue) !important;
  }

  .stSlider [data-baseweb="slider"] [role="slider"] > div {
    background: var(--sir-blue) !important;
  }

  .stSlider [data-baseweb="slider"] > div > div {
    background: rgba(148, 163, 184, 0.18) !important;
  }

  .stSlider [data-baseweb="slider"] > div > div > div {
    background: linear-gradient(90deg, rgba(79, 124, 172, 0.95), rgba(34, 211, 238, 0.72)) !important;
  }

  details[data-testid="stExpander"] {
    background: var(--sir-surface);
    border: 1px solid var(--sir-border);
    border-radius: 14px;
    box-shadow: 0 16px 34px rgba(2, 6, 23, 0.28);
    overflow: hidden;
  }

  details[data-testid="stExpander"] summary {
    background: var(--sir-surface-soft);
    color: var(--sir-text) !important;
  }

  details[data-testid="stExpander"] summary *,
  details[data-testid="stExpander"] p,
  details[data-testid="stExpander"] label {
    color: var(--sir-text) !important;
  }

  details[data-testid="stExpander"][open] summary {
    border-bottom: 1px solid var(--sir-border);
  }

  [data-testid="stDataFrame"],
  .stDataFrame {
    background: var(--sir-surface);
    border: 1px solid var(--sir-border);
    border-radius: 14px;
    --gdg-bg-cell: #0F172A;
    --gdg-bg-cell-medium: #111827;
    --gdg-bg-header: #162033;
    --gdg-bg-header-has-focus: #1B2940;
    --gdg-bg-header-hovered: #1B2940;
    --gdg-bg-search-result: rgba(34, 211, 238, 0.14);
    --gdg-bg-bubble: rgba(37, 99, 235, 0.18);
    --gdg-bg-bubble-selected: rgba(34, 211, 238, 0.18);
    --gdg-bg-row-hover: #162033;
    --gdg-bg-cell-selected: rgba(37, 99, 235, 0.18);
    --gdg-bg-cell-selected-padded: rgba(34, 211, 238, 0.16);
    --gdg-accent-color: #22D3EE;
    --gdg-accent-fg: #F8FAFC;
    --gdg-accent-light: rgba(34, 211, 238, 0.16);
    --gdg-border-color: #243145;
    --gdg-horizontal-border-color: rgba(208, 216, 228, 0.10);
    --gdg-vertical-border-color: rgba(208, 216, 228, 0.06);
    --gdg-text-dark: #F8FAFC;
    --gdg-text-medium: #D0D8E4;
    --gdg-text-light: #94A3B8;
    --gdg-link-color: #22D3EE;
    --gdg-selection-ring: #22D3EE;
  }

  [data-testid="stDataFrame"] *,
  .stDataFrame * {
    --gdg-bg-cell: #0F172A;
    --gdg-bg-cell-medium: #111827;
    --gdg-bg-header: #162033;
    --gdg-bg-header-has-focus: #1B2940;
    --gdg-bg-header-hovered: #1B2940;
    --gdg-bg-row-hover: #162033;
    --gdg-bg-cell-selected: rgba(37, 99, 235, 0.18);
    --gdg-bg-cell-selected-padded: rgba(34, 211, 238, 0.16);
    --gdg-accent-color: #22D3EE;
    --gdg-accent-fg: #F8FAFC;
    --gdg-accent-light: rgba(34, 211, 238, 0.16);
    --gdg-border-color: #243145;
    --gdg-text-dark: #F8FAFC;
    --gdg-text-medium: #D0D8E4;
    --gdg-text-light: #94A3B8;
  }

  [data-testid="stDataFrame"] [role="columnheader"],
  [data-testid="stDataFrame"] [role="gridcell"],
  [data-testid="stDataFrame"] [role="rowheader"] {
    color: var(--sir-text) !important;
  }

  [data-testid="stDataFrame"] [role="columnheader"] {
    background: var(--sir-surface-soft) !important;
  }

  [data-testid="stDataFrame"] [role="gridcell"] {
    background: var(--sir-surface) !important;
  }

  [data-testid="stDataFrame"] canvas {
    background: var(--sir-surface) !important;
  }

  [data-testid="stDataFrame"] [class*="glide"],
  [data-testid="stDataFrame"] [class*="gdg"],
  [data-testid="stDataFrame"] [data-testid*="Glide"],
  [data-testid="stDataFrame"] [data-testid*="DataFrame"] {
    background: var(--sir-surface) !important;
    color: var(--sir-text) !important;
  }

  [data-testid="stDataFrame"] > div,
  [data-testid="stDataFrame"] section,
  [data-testid="stDataFrame"] button,
  [data-testid="stDataFrame"] input,
  [data-testid="stDataFrame"] svg {
    color: var(--sir-text) !important;
    fill: var(--sir-text-secondary) !important;
    background-color: transparent !important;
  }

  [data-testid="stDataFrame"] button:hover {
    background: var(--sir-blue-soft) !important;
  }

  [data-testid="stDataFrameColumnVisibilityMenu"] > div,
  [data-testid="stDataFrameTooltipContent"],
  [data-testid="stDataFrameTooltipTarget"],
  .stDataFrame .gdg-d19meir1,
  .stDataFrame .gdg-seveqep,
  .stDataFrame .gdg-phbadu4 > div,
  .stDataFrame .gdg-p13nj8j0 > div {
    background: var(--sir-surface-soft) !important;
    color: var(--sir-text) !important;
    border-color: var(--sir-border) !important;
  }

  table {
    background: var(--sir-surface);
    color: var(--sir-text);
    border-color: var(--sir-border);
  }

  thead tr th {
    background: var(--sir-surface-soft) !important;
    color: var(--sir-text) !important;
    font-size: 0.82rem !important;
    line-height: 1.35 !important;
    white-space: normal !important;
    overflow-wrap: anywhere !important;
    word-break: break-word !important;
  }

  tbody tr td {
    background: var(--sir-surface) !important;
    color: var(--sir-text) !important;
  }

  tbody tr:hover td {
    background: var(--sir-surface-soft) !important;
  }

  .sir-chip-row {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 8px 0 18px 0;
  }

  .sir-chip {
    display: inline-flex;
    align-items: center;
    padding: 5px 9px;
    border-radius: 10px;
    background: linear-gradient(135deg, rgba(91, 192, 235, 0.10), rgba(20, 184, 166, 0.07));
    border: 1px solid rgba(91, 192, 235, 0.20);
    color: var(--sir-text-secondary);
    font-size: 0.8rem;
    font-weight: 540;
    line-height: 1.3;
    white-space: normal;
    overflow-wrap: anywhere;
  }

  .sir-search-wrap {
    background: linear-gradient(135deg, rgba(79, 124, 172, 0.14), rgba(20, 184, 166, 0.08));
    border: 1px solid var(--sir-border);
    border-radius: 18px;
    padding: 16px 18px 8px 18px;
    margin: 10px 0 12px 0;
  }

  .sir-hero {
    position: relative;
    overflow: hidden;
    background:
      radial-gradient(circle at top right, rgba(34, 211, 238, 0.16), transparent 36%),
      radial-gradient(circle at bottom left, rgba(20, 184, 166, 0.10), transparent 34%),
      linear-gradient(135deg, rgba(22, 32, 51, 0.98), rgba(11, 18, 32, 0.98));
    border: 1px solid var(--sir-border);
    border-radius: 22px;
    padding: 22px 24px;
    margin: 4px 0 16px 0;
    box-shadow: 0 24px 56px rgba(2, 6, 23, 0.34);
  }

  .sir-hero__eyebrow {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 5px 10px;
    border-radius: 999px;
    background: rgba(34, 211, 238, 0.10);
    border: 1px solid rgba(34, 211, 238, 0.14);
    color: var(--sir-cyan);
    font-size: 0.76rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .sir-hero__title {
    margin: 12px 0 8px 0;
    color: var(--sir-text);
    font-size: clamp(1.85rem, 2.7vw, 2.25rem);
    line-height: 1.1;
    letter-spacing: -0.03em;
    font-weight: 690;
  }

  .sir-hero__subtitle {
    margin: 0;
    color: var(--sir-text-secondary);
    font-size: 0.97rem;
    line-height: 1.55;
    max-width: 72ch;
  }

  .sir-section-head {
    background: linear-gradient(135deg, rgba(79, 124, 172, 0.12), rgba(20, 184, 166, 0.07));
    border: 1px solid var(--sir-border);
    border-radius: 18px;
    padding: 16px 18px;
    margin: 10px 0 14px 0;
    box-shadow: 0 18px 40px rgba(2, 6, 23, 0.24);
  }

  .sir-section-head__row {
    display: flex;
    gap: 14px;
    align-items: flex-start;
  }

  .sir-section-head__icon {
    flex: 0 0 38px;
    width: 38px;
    height: 38px;
    border-radius: 12px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    color: var(--sir-text);
    background: linear-gradient(135deg, rgba(79, 124, 172, 0.20), rgba(34, 211, 238, 0.12));
    border: 1px solid rgba(34, 211, 238, 0.18);
    font-size: 1rem;
    font-weight: 700;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
  }

  .sir-section-head__eyebrow {
    color: var(--sir-cyan);
    font-size: 0.74rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 4px;
  }

  .sir-section-head__title {
    color: var(--sir-text);
    font-size: clamp(1.02rem, 1.35vw, 1.18rem);
    line-height: 1.28;
    font-weight: 650;
    margin: 0;
    letter-spacing: -0.02em;
    overflow-wrap: anywhere;
  }

  .sir-section-head__desc {
    color: var(--sir-text-secondary);
    font-size: 0.92rem;
    line-height: 1.5;
    margin: 6px 0 0 0;
    overflow-wrap: anywhere;
  }

  .sir-inline-note {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 5px 10px;
    border-radius: 999px;
    background: rgba(34, 211, 238, 0.12);
    border: 1px solid rgba(34, 211, 238, 0.14);
    color: var(--sir-text-secondary);
    font-size: 0.8rem;
    font-weight: 560;
    line-height: 1.3;
    white-space: normal;
    overflow-wrap: anywhere;
  }

  .sir-guided-pill-row {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 8px 0 16px 0;
  }

  .sir-guided-pill {
    display: inline-flex;
    align-items: center;
    padding: 7px 11px;
    border-radius: 999px;
    background: rgba(79, 124, 172, 0.14);
    border: 1px solid rgba(79, 124, 172, 0.20);
    color: var(--sir-text-secondary);
    font-size: 0.82rem;
    font-weight: 560;
    line-height: 1.35;
  }

  .sir-guided-card-title {
    color: var(--sir-text);
    font-size: 1rem;
    font-weight: 640;
    letter-spacing: -0.02em;
    margin: 0 0 4px 0;
  }

  .sir-guided-card-desc {
    color: var(--sir-text-secondary);
    font-size: 0.9rem;
    line-height: 1.5;
    margin: 0 0 10px 0;
  }

  .sir-guided-question-copy {
    min-height: 7.2rem;
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    gap: 0.35rem;
  }

  .sir-guided-question-title {
    color: var(--sir-text);
    font-size: 0.98rem;
    font-weight: 650;
    line-height: 1.35;
    margin: 0;
  }

  .sir-guided-question-desc {
    color: var(--sir-text-secondary);
    font-size: 0.86rem;
    line-height: 1.45;
    margin: 0;
  }

  .sir-guided-theme-copy {
    min-height: 3.7rem;
    display: flex;
    align-items: flex-start;
  }

  .sir-guided-theme-title {
    color: var(--sir-text);
    font-size: 0.94rem;
    font-weight: 620;
    line-height: 1.35;
    margin: 0;
  }

  .sir-guided-theme-select-note {
    color: var(--sir-text-muted);
    font-size: 0.76rem;
    margin: 0.1rem 0 0.2rem 0;
  }

  .sir-guided-next {
    background: linear-gradient(135deg, rgba(79, 124, 172, 0.10), rgba(20, 184, 166, 0.06));
    border: 1px solid var(--sir-border);
    border-radius: 18px;
    padding: 16px 18px;
    margin-top: 10px;
  }

  .sir-guided-next__title {
    color: var(--sir-text);
    font-size: 1rem;
    font-weight: 650;
    margin: 0 0 8px 0;
  }

  .sir-guided-next ul {
    margin: 0;
    padding-left: 18px;
    color: var(--sir-text-secondary);
  }

  .sir-guided-next li {
    margin: 0 0 6px 0;
  }


  @media (max-width: 1200px) {
    div[data-testid="metric-container"] {
      padding: 11px 12px;
      min-height: 108px;
    }

    div[data-testid="metric-container"] [data-testid="stMetricLabel"] {
      min-height: 2.7em;
    }

    div[data-testid="metric-container"] [data-testid="stMetricValue"] {
      font-size: clamp(0.98rem, 1.38vw, 1.24rem) !important;
    }

    .sir-section-head {
      padding: 14px 16px;
    }
  }

  @media (max-width: 900px) {
    .sir-hero {
      padding: 18px 18px;
      border-radius: 18px;
    }

    .sir-hero__title {
      font-size: clamp(1.62rem, 6vw, 1.95rem);
    }

    .sir-section-head__row {
      gap: 10px;
    }

    .sir-section-head__icon {
      flex: 0 0 34px;
      width: 34px;
      height: 34px;
      border-radius: 10px;
      font-size: 0.92rem;
    }

    .stTabs [data-baseweb="tab"] {
      padding: 8px 11px;
      font-size: 0.88rem;
    }
  }

  [data-testid="stPlotlyChart"] {
    background: var(--sir-surface);
    border: 1px solid var(--sir-border);
    border-radius: 14px;
    padding: 6px;
  }

  [data-testid="stPlotlyChart"] .js-plotly-plot,
  [data-testid="stPlotlyChart"] .plot-container,
  [data-testid="stPlotlyChart"] .svg-container,
  [data-testid="stPlotlyChart"] .main-svg {
    background: transparent !important;
  }

  [data-testid="stPlotlyChart"] .modebar {
    background: rgba(15, 23, 42, 0.92) !important;
    border: 1px solid var(--sir-border) !important;
    border-radius: 10px !important;
  }

  [data-testid="stPlotlyChart"] .modebar-btn path,
  [data-testid="stPlotlyChart"] .modebar-btn svg {
    fill: var(--sir-text-secondary) !important;
  }
</style>
""",
    unsafe_allow_html=True,
)


# ============================================================
# Colors
# ============================================================
TOTALE_COLORWAY = [
    "#4F7CAC",
    "#22D3EE",
    "#14B8A6",
    "#22C55E",
    "#F97316",
    "#FB7185",
    "#FACC15",
]

R2G = [
    (0.00, "#4F7CAC"),
    (0.20, "#22D3EE"),
    (0.45, "#14B8A6"),
    (0.70, "#22C55E"),
    (0.88, "#FACC15"),
    (1.00, "#F97316"),
]

APP_BG = "#0B1220"
SIDEBAR_BG = "#111827"
PANEL_BG = "#0F172A"
PANEL_BG_SOFT = "#162033"
BORDER = "#243145"
BORDER_STRONG = "#334155"
TEXT_PRIMARY = "#F8FAFC"
TEXT_SECONDARY = "#D0D8E4"
TEXT_MUTED = "#94A3B8"
GRID_COLOR = "rgba(208, 216, 228, 0.10)"
GRID_COLOR_SOFT = "rgba(208, 216, 228, 0.06)"
MAP_LABEL_COLOR = "rgba(208, 216, 228, 0.52)"
LEGEND_BG = "rgba(15, 23, 42, 0.88)"
HOVER_BG = "#162033"

# ============================================================
# Country mapping (ISO alpha-2 -> full name)
# ============================================================
COUNTRY_CODE_TO_NAME = {
    # EU 27
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "HR": "Croatia",
    "CY": "Cyprus", "CZ": "Czechia", "DK": "Denmark", "EE": "Estonia",
    "FI": "Finland", "FR": "France", "DE": "Germany", "GR": "Greece", "EL": "Greece",
    "HU": "Hungary", "IE": "Ireland", "IT": "Italy", "LV": "Latvia",
    "LT": "Lithuania", "LU": "Luxembourg", "MT": "Malta", "NL": "Netherlands",
    "PL": "Poland", "PT": "Portugal", "RO": "Romania", "SK": "Slovakia",
    "SI": "Slovenia", "ES": "Spain", "SE": "Sweden",
    # Associated countries (Horizon Europe, as of Oct 2025)
    "NO": "Norway", "IS": "Iceland", "CH": "Switzerland", "LI": "Liechtenstein",
    "UK": "United Kingdom", "GB": "United Kingdom",
    "TR": "Türkiye", "RS": "Serbia", "AL": "Albania", "ME": "Montenegro",
    "MK": "North Macedonia", "BA": "Bosnia and Herzegovina",
    "XK": "Kosovo", "MD": "Moldova", "UA": "Ukraine", "GE": "Georgia", "AM": "Armenia",
    "IL": "Israel", "TN": "Tunisia", "EG": "Egypt", "MA": "Morocco",
    "KR": "South Korea", "CA": "Canada", "NZ": "New Zealand",
    "FO": "Faroe Islands",
    # Other frequent participants (not associated but appear in CORDIS)
    "US": "United States", "JP": "Japan", "CN": "China", "IN": "India",
    "BR": "Brazil", "ZA": "South Africa", "AU": "Australia",
    "SG": "Singapore", "TW": "Taiwan", "CL": "Chile", "MX": "Mexico",
    "AR": "Argentina", "CO": "Colombia", "TH": "Thailand", "MY": "Malaysia",
    "ID": "Indonesia", "PH": "Philippines", "VN": "Vietnam",
    "NG": "Nigeria", "KE": "Kenya", "GH": "Ghana", "ET": "Ethiopia",
    "SN": "Senegal", "TZ": "Tanzania", "UG": "Uganda",
    "RU": "Russia", "BY": "Belarus",
}

# Alpha-2 → Alpha-3 mapping (needed for choropleth map)
COUNTRY_CODE_TO_ALPHA3 = {
    "AT": "AUT", "BE": "BEL", "BG": "BGR", "HR": "HRV", "CY": "CYP", "CZ": "CZE",
    "DK": "DNK", "EE": "EST", "FI": "FIN", "FR": "FRA", "DE": "DEU", "GR": "GRC",
    "EL": "GRC", "HU": "HUN", "IE": "IRL", "IT": "ITA", "LV": "LVA", "LT": "LTU",
    "LU": "LUX", "MT": "MLT", "NL": "NLD", "PL": "POL", "PT": "PRT", "RO": "ROU",
    "SK": "SVK", "SI": "SVN", "ES": "ESP", "SE": "SWE",
    "NO": "NOR", "IS": "ISL", "CH": "CHE", "LI": "LIE",
    "UK": "GBR", "GB": "GBR",
    "TR": "TUR", "RS": "SRB", "AL": "ALB", "ME": "MNE", "MK": "MKD", "BA": "BIH",
    "XK": "XKX", "MD": "MDA", "UA": "UKR", "GE": "GEO", "AM": "ARM",
    "IL": "ISR", "TN": "TUN", "EG": "EGY", "MA": "MAR",
    "KR": "KOR", "CA": "CAN", "NZ": "NZL", "FO": "FRO",
    "US": "USA", "JP": "JPN", "CN": "CHN", "IN": "IND",
    "BR": "BRA", "ZA": "ZAF", "AU": "AUS",
    "SG": "SGP", "TW": "TWN", "CL": "CHL", "MX": "MEX",
    "AR": "ARG", "CO": "COL", "TH": "THA", "MY": "MYS",
    "ID": "IDN", "PH": "PHL", "VN": "VNM",
    "NG": "NGA", "KE": "KEN", "GH": "GHA", "ET": "ETH",
    "SN": "SEN", "TZ": "TZA", "UG": "UGA",
    "RU": "RUS", "BY": "BLR",
}

EU27_COUNTRIES = [
    "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia",
    "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary",
    "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta",
    "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia",
    "Spain", "Sweden",
]

ASSOCIATED_COUNTRIES_HORIZON_EUROPE = [
    "Albania", "Armenia", "Bosnia and Herzegovina", "Canada", "Egypt",
    "Faroe Islands", "Georgia", "Iceland", "Israel", "Kosovo",
    "Moldova", "Montenegro", "New Zealand", "North Macedonia", "Norway",
    "South Korea", "Serbia", "Switzerland", "Türkiye", "Tunisia",
    "Ukraine", "United Kingdom",
]

EUROPE_DEFAULT_COUNTRIES = EU27_COUNTRIES + ["Norway", "Switzerland", "United Kingdom", "Iceland"]

# World Bank/UN style rounded values (inhabitants). Used only for budget-per-population normalization in map.
POPULATION_BY_ALPHA3 = {
    "AUT": 9130000, "BEL": 11700000, "BGR": 6440000, "HRV": 3870000, "CYP": 1250000, "CZE": 10900000,
    "DNK": 5960000, "EST": 1370000, "FIN": 5600000, "FRA": 68400000, "DEU": 84500000, "GRC": 10300000,
    "HUN": 9580000, "IRL": 5320000, "ITA": 58900000, "LVA": 1880000, "LTU": 2860000, "LUX": 673000,
    "MLT": 564000, "NLD": 18000000, "POL": 37700000, "PRT": 10500000, "ROU": 19000000, "SVK": 5430000,
    "SVN": 2120000, "ESP": 48800000, "SWE": 10600000, "NOR": 5560000, "CHE": 8920000, "GBR": 68200000,
    "ISL": 394000, "SRB": 6650000, "ALB": 2780000, "MNE": 620000, "MKD": 1840000,
    "BIH": 3210000, "XKX": 1780000, "MDA": 2540000, "UKR": 37000000,
    "GEO": 3690000, "ARM": 2780000, "TUN": 12500000, "EGY": 112000000,
    "MAR": 37900000, "NZL": 5280000, "FRO": 54000, "LIE": 40000,
    "USA": 340000000, "CAN": 41000000, "AUS": 27000000, "JPN": 124000000, "CHN": 1410000000, "IND": 1430000000,
    "BRA": 216000000, "ZAF": 62000000, "KOR": 51800000, "ISR": 10000000, "TUR": 85700000, "UKR": 37000000,
}

VALUE_CHAIN_ORDER = [
    "Resources & feedstock",
    "Components & core technology",
    "Systems & infrastructure",
    "Deployment & operations",
    "End-use & market",
    "Research & concept",
    "Unspecified",
]
STAGE_COLORS = {
    "Resources & feedstock": "rgba(79, 124, 172, 0.82)",
    "Components & core technology": "rgba(34, 211, 238, 0.84)",
    "Systems & infrastructure": "rgba(20, 184, 166, 0.82)",
    "Deployment & operations": "rgba(34, 197, 94, 0.82)",
    "End-use & market": "rgba(249, 115, 22, 0.56)",
    "Research & concept": "rgba(167, 199, 231, 0.72)",
    "Unspecified": "rgba(148, 163, 184, 0.56)",
}

_totale_dark = go.layout.Template(pio.templates["plotly_dark"])
_totale_dark.layout.update(
    font=dict(family="system-ui, -apple-system, BlinkMacSystemFont, SF Pro Text, SF Pro Display, Segoe UI, sans-serif", size=13, color=TEXT_PRIMARY),
    title=dict(font=dict(size=18, color=TEXT_PRIMARY)),
    paper_bgcolor=PANEL_BG,
    plot_bgcolor=PANEL_BG,
    colorway=TOTALE_COLORWAY,
    hoverlabel=dict(
        bgcolor=HOVER_BG,
        bordercolor=BORDER,
        font=dict(color=TEXT_PRIMARY),
    ),
    legend=dict(
        bgcolor=LEGEND_BG,
        bordercolor=BORDER,
        borderwidth=1,
        font=dict(color=TEXT_SECONDARY),
    ),
    margin=dict(l=36, r=24, t=42, b=36),
    bargap=0.18,
    xaxis=dict(
        showline=True,
        linecolor=BORDER,
        gridcolor=GRID_COLOR,
        zerolinecolor=BORDER,
        tickfont=dict(color=TEXT_SECONDARY),
        title=dict(font=dict(color=TEXT_SECONDARY)),
    ),
    yaxis=dict(
        showline=False,
        gridcolor=GRID_COLOR,
        zerolinecolor=BORDER,
        tickfont=dict(color=TEXT_SECONDARY),
        title=dict(font=dict(color=TEXT_SECONDARY)),
    ),
    coloraxis=dict(
        colorbar=dict(
            outlinecolor=BORDER,
            tickcolor=TEXT_MUTED,
            bgcolor=LEGEND_BG,
            title=dict(font=dict(color=TEXT_SECONDARY)),
        )
    ),
    geo=dict(
        bgcolor=PANEL_BG,
        lakecolor=APP_BG,
        oceancolor=APP_BG,
        landcolor="#18263D",
        showland=True,
        showocean=True,
        showlakes=True,
        showcountries=True,
        countrycolor="rgba(208, 216, 228, 0.18)",
        coastlinecolor="rgba(208, 216, 228, 0.16)",
    ),
)
pio.templates["totale_dark"] = _totale_dark
pio.templates.default = "totale_dark"
px.defaults.template = "totale_dark"
px.defaults.color_discrete_sequence = TOTALE_COLORWAY


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
    "Space": "Espace",
    "Agriculture & Food": "Agriculture & alimentation",
    "Security & Resilience": "Sécurité & résilience",
    "Other": "Autres",
}

GUIDED_DOMAIN_SUBCATEGORIES = dict(SCIENTIFIC_SUBTHEMES_BY_DOMAIN)

SUBTOPIC_TERM_OVERRIDES = {
    "Electrolysis": ["electrolysis", "electrolyser", "electrolyzer"],
    "Fuel cells": ["fuel cells", "fuel cell"],
    "Direct air capture (DAC)": ["direct air capture", "DAC"],
    "Concentrated solar power (CSP)": ["concentrated solar power", "CSP"],
    "Sustainable aviation fuel (SAF)": ["sustainable aviation fuel", "SAF"],
    "SMR technologies": ["SMR", "small modular reactor", "small modular reactors"],
    "Artificial intelligence & machine learning": ["artificial intelligence", "machine learning", "AI"],
    "Digital twins": ["digital twin", "digital twins"],
    "Vehicle-to-grid (V2G)": ["vehicle-to-grid", "vehicle to grid", "V2G"],
    "CO2 capture": ["CO2 capture", "carbon capture"],
    "CO2 transport": ["CO2 transport", "carbon transport"],
    "CO2 storage": ["CO2 storage", "carbon storage"],
    "CO2 utilization": ["CO2 utilization", "carbon utilization", "carbon use"],
    "Catalysts, membranes & safety": ["catalysts", "membranes", "hydrogen safety"],
    "PV cells & architectures": ["PV cells", "photovoltaic cells"],
    "Charging infrastructure": ["charging infrastructure", "charging station", "charging stations"],
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
        "title": "Subsidy Intelligence Radar",
        "subtitle": "Recherche guidée sur les subventions UE : résultats, acteurs, géographie, tendances et événements.",
        "guided_home_title": "Commencer par une question",
        "guided_home_caption": "Choisis d’abord le type de réponse attendu, puis affine le sujet, les pays et la période avant d’ouvrir l’analyse complète.",
        "guided_home_intro_title": "Parcours guidé",
        "guided_home_intro": "Cette page sert à formuler une intention simple. L’analyse complète s’ouvrira ensuite déjà cadrée sur ta question.",
        "guided_home_question_title": "1. Choisir une question",
        "guided_home_question_caption": "Sélectionne le parcours qui correspond le mieux à ce que tu veux comprendre.",
        "guided_home_intent_projects": "Trouver les projets sur un sujet",
        "guided_home_intent_projects_desc": "Ouvre les résultats détaillés et la table de preuve sur le périmètre choisi.",
        "guided_home_intent_actors": "Identifier les principaux acteurs",
        "guided_home_intent_actors_desc": "Met l’accent sur les acteurs dominants du périmètre courant.",
        "guided_home_intent_countries": "Comparer des pays",
        "guided_home_intent_countries_desc": "Ouvre la géographie et le classement pays sur le périmètre choisi.",
        "guided_home_intent_trends": "Voir l’évolution dans le temps",
        "guided_home_intent_trends_desc": "Ouvre d’abord la tendance annuelle, puis la comparaison de périodes si besoin.",
        "guided_home_intent_value_chain": "Explorer la chaîne de valeur",
        "guided_home_intent_value_chain_desc": "Ouvre l’outil expert centré sur les étapes et les acteurs.",
        "guided_home_intent_macro": "Comprendre le contexte macro",
        "guided_home_intent_macro_desc": "Ouvre la lecture macro comme couche de contexte des tendances.",
        "guided_home_selected_question": "Question retenue",
        "guided_home_selected_question_desc": "Cette intention orientera la première vue ouverte dans l’analyse complète.",
        "guided_home_choose_question": "Choisir cette question",
        "guided_home_selected_question_button": "Question retenue",
        "guided_home_search": "Sujet ou mots-clés",
        "guided_home_search_help": "Exemples : hydrogène Allemagne, batteries France, IA industrie.",
        "guided_home_topics": "Domaines CORDIS à suivre",
        "guided_home_topics_help": "Laisse vide si tu veux ouvrir l’analyse sur tous les domaines CORDIS.",
        "guided_home_subtopics_note": "Les sous-thématiques détaillées ne sont pas encore structurées dans le référentiel. Cette entrée guidée travaille donc au niveau des grandes thématiques.",
        "guided_home_countries_help": "Commence par un périmètre pays simple ; tu pourras l’affiner ensuite dans les filtres complets.",
        "guided_home_period_help": "La période définit le cadrage de départ avant l’analyse détaillée.",
        "guided_home_open": "Ouvrir l’analyse complète",
        "guided_home_back": "Retour au cadrage",
        "guided_home_analysis_note": "Tu es dans l’analyse complète. Reviens au cadrage guidé si tu veux changer rapidement le sujet de départ.",
        "guided_home_examples_label": "Exemples de demandes",
        "guided_home_example_1": "Hydrogène en Allemagne depuis 2021",
        "guided_home_example_2": "Acteurs batteries en France",
        "guided_home_example_3": "IA & numérique en Europe",
        "guided_home_topic_card": "1. Définir le sujet",
        "guided_home_topic_card_desc": "Commence par les mots-clés et les grandes thématiques qui t’intéressent vraiment.",
        "guided_home_scope_card": "2. Définir le périmètre",
        "guided_home_scope_card_desc": "Cadre ensuite le point de départ avec les pays et la période.",
        "guided_home_metric_themes": "Domaines",
        "guided_home_metric_countries": "Pays",
        "guided_home_metric_period": "Période",
        "guided_home_next_title": "Ce que l’analyse complète va ouvrir",
        "guided_home_next_1": "les résultats détaillés du périmètre choisi",
        "guided_home_next_2": "les acteurs, la géographie et les tendances déjà préfiltrés",
        "guided_home_next_3": "tous les filtres avancés si tu veux aller plus loin",
        "guided_home_theme_cards_help": "Choisis un ou plusieurs domaines CORDIS. Tu pourras ensuite affiner avec le thème principal et les sous-thèmes scientifiques.",
        "guided_home_theme_select_note": "Coche pour inclure ce domaine",
        "guided_home_theme_select_action": "Sélectionner",
        "guided_home_selected_themes": "Domaines retenus",
        "guided_home_subtopics": "Sous-thèmes scientifiques",
        "guided_home_subtopics_help": "Choisis des sous-thèmes scientifiques si tu veux affiner l’exploration. Ils filtreront ensuite l’analyse détaillée sans recomposer les totaux globaux.",
        "guided_terms": "Sous-thèmes guidés",
        "guided_terms_applied": "Sous-thèmes guidés appliqués",
        "reset": "Réinitialiser",
        "refresh": "Rafraîchir les données",
        "refresh_hint": "Met à jour CORDIS + events (offline), puis recharge l’app.",
        "filters": "Filtres",
        "basic_filters": "Filtres métier",
        "advanced_filters": "Plus de filtres métier",
        "analysis_options": "Comportement d'analyse",
        "sources": "Sources",
        "onetech_only": "Limiter au périmètre OneTech",
        "programmes": "Programmes",
        "period": "Période (année de démarrage)",
        "use_section": "Filtrer par section",
        "section": "Section (UE / Programme / Topic)",
        "project_status": "Statut projet",
        "status_open": "Ouvert",
        "status_closed": "Fermé",
        "status_unknown": "Inconnu",
        "domains": "Domaines CORDIS",
        "themes": "Thème principal CORDIS",
        "scientific_subthemes": "Sous-thèmes scientifiques",
        "entity": "Type d’entité",
        "countries": "Pays",
        "country_preset_eu27": "UE 27",
        "country_preset_associated": "UE + Associés",
        "country_preset_all": "Tous",
        "quick_search": "Recherche rapide",
        "quick_search_hint": "Recherche dans acteur, projet, acronyme ou titre",
        "main_search_support": "Recherche libre dans les projets et acteurs. Utilise ensuite les filtres pour préciser pays, période et programme.",
        "search_help_more": "Comment fonctionne cette recherche",
        "main_search_examples": "Exemples : « hydrogène en Allemagne depuis 2021 », « principaux acteurs IA en France », « compare les batteries entre France et Allemagne ».",
        "main_search_exploratory": "Recherche libre exploratoire : fonctionne mieux avec des mots-clés simples. Utilise les filtres pour cadrer le pays, la période et le programme.",
        "search_literal_note": "La recherche libre reste littérale. Les synonymes et exclusions servent surtout à la classification thématique, pas encore à une vraie recherche sémantique.",
        "search_interpretation_title": "Question comprise",
        "search_interpretation_caption": "Voici ce que l’app utilise actuellement pour construire le périmètre.",
        "search_interpretation_intent": "Intention",
        "search_interpretation_scope": "Lecture active",
        "search_interpretation_search": "Recherche libre",
        "search_interpretation_none": "Aucun terme libre",
        "filters_advanced_hint": "Programme, source, type d’entité et options d’analyse restent disponibles en Recherche avancée.",
        "search_simplified_notice": "La recherche a été simplifiée pour éviter une erreur. Essaie un mot-clé simple puis affine avec les filtres.",
        "search_ignored_notice": "La recherche n’a pas pu être appliquée avec cette saisie. Les filtres sont conservés ; essaie un mot-clé plus simple.",
        "view_recover_hint": "Essaie une recherche plus simple ou ajuste les filtres. Les autres vues restent disponibles.",
        "chart_render_unavailable": "Ce graphique n’a pas pu être affiché pour ce périmètre.",
        "results_view_unavailable": "Cette vue de résultats n’a pas pu être affichée pour ce périmètre.",
        "results_scope_partial_warning": "Certaines informations de synthèse n’ont pas pu être calculées complètement pour ce périmètre.",
        "geo_view_unavailable": "La vue géographique n’a pas pu être affichée pour ce périmètre.",
        "benchmark_view_unavailable": "Cette vue de comparaison n’a pas pu être affichée pour ce périmètre.",
        "value_chain_view_unavailable": "La vue étapes et acteurs n’a pas pu être affichée pour ce périmètre.",
        "partnership_view_unavailable": "La vue partenariats n’a pas pu être affichée pour ce périmètre.",
        "concentration_view_unavailable": "La vue de concentration n’a pas pu être affichée pour ce périmètre.",
        "geo_empty_hint": "Essaie d’élargir la période, le périmètre géographique ou de revenir à une lecture pays plus large.",
        "geo_country_empty_hint": "Essaie un autre pays, une période plus large, ou reviens au classement global.",
        "advanced_empty_hint": "Essaie d’élargir le périmètre ou commence par la vue tableau/classement avant d’ouvrir les vues expertes.",
        "benchmark_empty_hint": "Essaie d’élargir le périmètre ou reviens au classement simple des acteurs.",
        "value_chain_empty_hint": "Essaie d’élargir les thématiques, de réactiver toutes les étapes, ou de revenir au résumé des étapes.",
        "partnership_empty_hint": "Choisis un acteur plus central, augmente le nombre de partenaires affichés, ou élargis le périmètre.",
        "concentration_empty_hint": "Élargis le périmètre pour comparer davantage d’acteurs ou reviens à la vue acteurs.",
        "net_no_partners": "Aucun partenaire dans le périmètre actuel.",
        "explore_overview_title": "Ce que vous pouvez faire ici",
        "explore_overview_1": "Trouver des projets par thématique, pays et période",
        "explore_overview_2": "Comparer les principaux acteurs d’un domaine",
        "explore_overview_3": "Voir quels pays reçoivent le plus de financement",
        "explore_overview_4": "Relier tendances de financement et événements macro",
        "explore_overview_5": "Ouvrir ensuite les vues avancées si besoin",
        "explore_overview_tip": "Commence par la recherche libre, puis affine avec les filtres.",
        "actor_grouping": "Regrouper entités juridiques (PIC/groupe)",
        "exclude_funders": "Exclure financeurs / agences",
        "actor_groups_ready": "Mapping groupes chargé",
        "actor_groups_source": "Source mapping",
        "mapping_low_coverage": "Couverture faible: complète `actor_groups.csv` pour un regroupement fiable.",
        "mapping_pic_fallback": "Fallback actif: regroupement automatique par PIC quand le mapping n'est pas renseigné.",
        "exclude_funders_heuristic": "Exclusion basée aussi sur heuristique nom d'organisation (EIT/CINEA/etc.).",
        "actor_groups_missing": "Mapping groupes absent (`actor_groups.csv` / template).",
        "mapping_summary": "Résumé mapping groupes",
        "mapping_loaded_count": "Lignes mapping",
        "mapping_match_rate": "Taux de correspondance",
        "mapping_mode_explicit": "Mode regroupement: mapping explicite + fallback PIC",
        "mapping_mode_fallback": "Mode regroupement: fallback PIC (mapping partiel)",
        "mapping_mode_pic_only": "Mode regroupement: fallback PIC uniquement",
        "mapping_keys_matched": "Clés mapping reconnues",
        "mapping_keys_issue": "Le mapping ne correspond pas aux IDs/PIC présents dans la base.",
        "mapping_keys_partial": "Une partie des clés mapping ne correspond pas à la base.",
        "mapping_global_impact": "Acteurs impactés (périmètre actuel)",
        "mapping_status_ready": "Mapping groupes actif.",
        "mapping_status_partial": "Mapping groupes partiel (fallback PIC utilisé).",
        "mapping_status_missing_short": "Mapping groupes absent (fallback PIC).",
        "mapping_diag_toggle": "Diagnostic mapping (optionnel)",
        "refresh_cloud_cta": "Ouvrir GitHub Actions « Refresh Data »",
        "kpis": "Indicateurs clés",
        "insights_title": "Insights automatiques (périmètre courant)",
        "budget_total": "Budget total",
        "n_projects": "Nombre de projets",
        "n_actors": "Acteurs uniques",
        "avg_ticket": "Ticket moyen / projet",
        "median_ticket": "Ticket médian / projet",
        "top10_share": "Part Top10 acteurs",
        "hhi": "Concentration (HHI)",
        "no_data": "Aucune donnée pour cette sélection. Élargis les filtres.",
        "tab_overview": "Vue d’ensemble",
        "tab_geo": "Géographie",
        "tab_comp": "Comparer les acteurs",
        "tab_trends": "Tendances",
        "tab_compare": "Comparaison",
        "tab_macro": "Macro & actualités",
        "tab_actor": "Fiche acteur",
        "tab_network": "Chaîne & réseau",
        "tab_data": "Données",
        "tab_quality": "Qualité",
        "tab_help": "Aide",
        "tab_guide": "Guide",
        "zoom_on": "Zoom",
        "projection": "Type de carte",
        "borders": "Frontières & côtes",
        "labels": "Libellés continents",
        "top_countries": "Top 15 pays",
        "geo_metric": "Indicateur carte",
        "geo_metric_total": "Budget total (€)",
        "geo_metric_per_million": "Budget / million hab. (€)",
        "geo_pop_missing": "Population manquante pour certains pays: normalisation partielle.",
        "geo_caption": "Lecture géographique du périmètre actif: concentration, rang pays et détail pays.",
        "geo_perimeter_default": "Périmètre par défaut : pays européens présents dans la base, avec États membres de l’UE et quelques pays associés lorsqu’ils sont disponibles.",
        "geo_perimeter_custom": "Le classement ci-dessous suit tes filtres pays actuels. Par défaut, l’app démarre sur l’Europe présente dans la base, y compris quelques pays associés lorsqu’ils sont disponibles.",
        "geo_country_picker": "Pays à détailler",
        "geo_advanced_options": "Réglages de carte",
        "geo_selected_summary": "Pays sélectionné",
        "geo_rank": "Rang",
        "geo_scope_share": "Part du périmètre",
        "geo_country_detail": "Détail pays",
        "geo_country_actors": "Acteurs principaux",
        "geo_country_themes": "Thèmes principaux",
        "geo_country_projects": "Projets principaux",
        "benchmark_mode": "Vue de comparaison",
        "bm_scatter": "Comparer volume et budget",
        "bm_treemap": "Vue en blocs",
        "bm_top": "Classement des acteurs",
        "bm_caption": "Commence par un classement simple, puis ouvre les vues expertes si besoin.",
        "bm_default_caption": "Vue par défaut : lecture simple du périmètre courant, avec tableau en premier.",
        "bm_expert_caption": "Vue experte: utile pour explorer des positionnements ou hiérarchies plus complexes.",
        "bm_compare_scope": "Réglages de comparaison",
        "bm_overall_rank": "Classement global",
        "bm_breakdown_entity": "Par type d'entité",
        "pct_threshold": "Seuil de budget",
        "topn": "Nombre d'acteurs affichés",
        "search_actor": "Recherche texte (contient…)",
        "actor_picker": "Acteur à comparer",
        "actor_picker_hint": "Tape pour chercher dans la liste.",
        "legend_tip": "Astuce : clique sur la légende pour masquer/afficher une série.",
        "scatter_explain": (
            "- Chaque point = **un acteur** (organisation), agrégé sur le périmètre filtré.\n"
            "- Axe X = **nombre de projets distincts** où cet acteur apparaît.\n"
            "- Axe Y = **budget total capté** par cet acteur.\n\n"
            "**Pourquoi un projet compte pour plusieurs acteurs ?**\n"
            "Un projet a plusieurs participants : il est compté pour **chaque** acteur participant.\n"
        ),
        "dimension": "Regrouper par",
        "dim_theme": "Thématique",
        "dim_program": "Programme",
        "mode": "Mode",
        "mode_abs": "Budget (absolu)",
        "mode_share": "Part (% par année)",
        "drivers": "Principaux moteurs",
        "compare_title": "Comparer deux périodes",
        "period_a": "Période A",
        "period_b": "Période B",
        "compare_caption": "Compare les écarts de budget entre deux périodes sur les thèmes ou les programmes du périmètre courant.",
        "compare_normalize_annual": "Ramener à la moyenne annuelle",
        "compare_period_years": "Période A : {years_a} ans, Période B : {years_b} ans",
        "compare_period_years_normalized": "Période A : {years_a} ans, Période B : {years_b} ans · lecture ramenée à une moyenne annuelle.",
        "compare_budget_a": "Budget A",
        "compare_budget_b": "Budget B",
        "compare_delta_budget": "Écart de budget (B - A)",
        "compare_budget_reading": "Lecture : à droite, la période B finance davantage ; à gauche, elle finance moins. La comparaison porte sur le budget, pas sur une part relative.",
        "budget_envelope_note": "Les budgets sont lus comme des enveloppes de projet rattachées à l’année de démarrage. Ce ne sont pas des montants effectivement versés chaque année.",
        "theme_method_note": "Les thématiques sont inférées par règles de mots-clés FR/EN avec gestion de quelques exclusions. Chaque projet reçoit aujourd’hui un thème principal.",
        "theme_review_label": "Multithématique",
        "theme_review_note": "« Multithématique » regroupe les projets transversaux sans correspondance unique dans le référentiel thématique actuel.",
        "actor_grouping_note": "Le regroupement d’entités dépend d’un mapping groupes et d’un fallback PIC. Il reste partiel pour certaines filiales et structures corporate.",
        "value_chain_method_note": "Les étapes de chaîne de valeur sont inférées à partir du texte projet. Cette lecture reste indicative et ne remplace pas une qualification TRL auditée.",
        "partnership_stage_note": "La lecture des partenaires n’est pas encore découpée directement par étape de chaîne de valeur. Utilise d’abord le thème et le périmètre actif pour cadrer l’analyse.",
        "actor_profile": "Fiche acteur",
        "actor_group_mode_caption": "Vue groupe active: les fiches et graphes peuvent agréger plusieurs entités juridiques via mapping ou PIC.",
        "actor_profile_caption": "Choisis un acteur, puis commence par son profil, son évolution et ses projets avant d’ouvrir les lectures plus expertes.",
        "actor_opened_from_results": "Ouvert depuis un projet sélectionné dans Résultats.",
        "actor_trend": "Évolution (budget & projets)",
        "actor_mix_theme": "Mix thématique",
        "actor_mix_country": "Mix géographique",
        "actor_partners": "Partenaires principaux",
        "actor_partners_caption": "Lis d’abord le tableau des partenaires. Le réseau détaillé reste dans Analyse avancée.",
        "actor_partners_mode": "Lecture des partenaires",
        "actor_partners_mode_scope": "Dans le périmètre actif",
        "actor_partners_mode_matched": "Tous les partenaires sur les projets retenus",
        "actor_partners_mode_scope_caption": "Lecture stricte : les partenaires restent classés sur le périmètre actif, mais le filtre type d’entité n’est pas appliqué à cette lecture.",
        "actor_partners_mode_matched_caption": "Lecture élargie : les projets sont sélectionnés avec le périmètre actif, puis tous les co-participants enregistrés sur ces projets sont pris en compte, le filtre type d’entité restant ignoré.",
        "actor_partners_scope_note_extra": "Des partenaires supplémentaires existent sur les projets retenus mais ne respectent pas tous les filtres actifs. Ouvre la lecture élargie pour les voir.",
        "partners_entity_filter_note": "Les partenaires sont affichés toutes typologies confondues, indépendamment du filtre entité.",
        "actor_tab_profile": "Profil",
        "actor_tab_partners": "Partenaires",
        "actor_tab_peers": "Acteurs comparables",
        "actor_top_theme": "Thème principal",
        "actor_entity_type": "Type d'entité",
        "actor_rank_overall": "Rang global",
        "actor_rank_peer": "Rang dans le groupe",
        "actor_avg_ticket": "Ticket moyen",
        "actor_peer_group": "Groupe de pairs",
        "actor_peer_caption": "Comparaison simple dans le périmètre actif. Les visuels avancés restent dans « Comparer les acteurs ».",
        "actor_peer_table": "Acteurs comparables",
        "scope_caption": "Périmètre actif",
        "scope_group_on": "vue groupes",
        "scope_group_off": "vue entités juridiques",
        "scope_funders_off": "financeurs exclus",
        "scope_funders_on": "financeurs inclus",
        "status_budget_title": "Budget par statut projet",
        "status_projects_title": "Projets par statut",
        "tab_explorer": "⌕ Recherche & résultats",
        "tab_actors_hub": "◈ Acteurs",
        "tab_markets": "◎ Géographie",
        "tab_trends_events": "↗ Tendances & événements",
        "tab_advanced": "◇ Outils experts",
        "tab_admin": "⋯ Données, méthode & exports",
        "admin_title": "Données, méthode & exports",
        "admin_caption": "Exports, qualité, méthode et diagnostic. Cette zone reste secondaire pour l’exploration standard.",
        "sub_results": "Résultats",
        "sub_overview": "Vue d’ensemble",
        "overview_caption": "Lecture secondaire : commence par Résultats pour répondre à la question, puis utilise cette page pour un résumé compact du périmètre.",
        "overview_support_note": "Cette page sert de synthèse d’appui. Pour la lecture principale, les comparaisons et la table de preuve, reste d’abord dans Résultats.",
        "overview_more_context": "Plus de contexte",
        "overview_yearly_extra": "Complément annuel : budgets et ticket médian",
        "sub_benchmark": "Comparer les acteurs",
        "sub_network": "Chaîne & réseau",
        "sub_value_chain": "Étapes et acteurs",
        "sub_collaboration": "Partenariats",
        "sub_concentration": "Concentration du financement",
        "sub_data": "Exports & données",
        "sub_quality": "Qualité & méthode",
        "sub_debug": "Diagnostic technique",
        "advanced_title": "Outils experts",
        "advanced_caption": "Vues expertes pour aller plus loin après la lecture principale. Commence d’abord par Recherche, Acteurs, Géographie ou Tendances.",
        "advanced_overview_title": "Quand ouvrir l’analyse avancée",
        "advanced_overview_1": "Comparer les acteurs quand un classement simple ne suffit plus",
        "advanced_overview_2": "Voir les étapes et acteurs pour lire une chaîne de valeur",
        "advanced_overview_3": "Explorer les partenariats autour d’un acteur focal",
        "advanced_overview_4": "Mesurer si le financement est réparti ou concentré",
        "advanced_overview_tip": "Commence par les tableaux et classements ci-dessous ; les graphes experts restent un second niveau de lecture.",
        "adv_benchmark_helper": "Repère rapidement quels acteurs dominent le périmètre, puis ouvre les vues expertes si besoin.",
        "adv_value_chain_helper": "Vois à quelle étape interviennent les acteurs et quels projets sont liés à chaque étape.",
        "adv_collaboration_helper": "Identifie les partenaires clés d’un acteur avant d’ouvrir la carte réseau.",
        "adv_concentration_helper": "Vois si le financement est réparti entre beaucoup d’acteurs ou concentré sur quelques-uns.",
        "debug_title": "Diagnostic technique",
        "debug_caption": "Surfaces techniques déplacées hors de la sidebar pour garder l'exploration lisible.",
        "results_title": "Résultats du périmètre",
        "results_caption": "Commence ici : définis une question, lis la réponse, puis ouvre les autres lectures seulement si besoin.",
        "results_view": "Vue principale",
        "results_table": "Table projets",
        "results_trend": "Tendance",
        "results_map": "Carte",
        "results_actors": "Acteurs",
        "main_search_label": "Que veux-tu explorer ?",
        "main_search_help": "Recherche libre dans acteur, projet, acronyme ou titre",
        "main_search_placeholder": "Ex. AI, hydrogène, CNRS, batteries",
        "active_filters": "Filtres actifs",
        "clear_search": "Effacer la recherche",
        "no_results_title": "Aucun résultat pour ce périmètre.",
        "no_results_hint": "Essaie d’élargir le pays, la période ou la thématique.",
        "no_results_reset": "Réinitialiser les filtres",
        "no_results_clear_search": "Effacer la recherche",
        "no_results_broaden": "Essaie d’élargir le pays, la période ou la thématique, ou de simplifier la recherche.",
        "results_summary_title": "Réponse synthétique",
        "results_summary_headline": "Le périmètre courant couvre {projects} projets pour {budget}, avec {actors} acteurs dans {countries} pays.",
        "results_summary_headline_single_country": "Le périmètre courant couvre {projects} projets pour {budget}, avec {actors} acteurs en {country}.",
        "results_summary_country_lead": "Le budget le plus élevé se situe en {country}.",
        "results_summary_actor_lead": "L’acteur le plus financé est {actor}.",
        "results_summary_theme_lead": "La thématique dominante est {theme}.",
        "theme_counting_note": "Note : dans le build actuel, chaque ligne reçoit une seule thématique inférée. Un projet n’apparaît donc pas dans plusieurs thèmes ici.",
        "results_summary_fallback": "Le périmètre courant couvre {projects} projets pour {budget} et {actors} acteurs uniques.",
        "results_primary_visual": "Lecture principale",
        "results_primary_trend": "Budget annuel",
        "results_primary_countries": "Pays dominants",
        "results_primary_actors": "Acteurs dominants",
        "results_next_steps": "Étapes suivantes suggérées",
        "results_next_geo": "Voir la géographie",
        "results_next_actors": "Voir les acteurs",
        "results_next_trends": "Voir les tendances",
        "results_other_views": "Autres lectures du périmètre",
        "results_projects_table": "Projets trouvés",
        "results_projects_table_caption": "Cette table est la preuve du périmètre courant : elle montre les projets derrière la réponse et le graphique principal.",
        "results_actor_table": "Acteurs dominants",
        "results_other_views_caption": "Ces lectures restent utiles, mais elles complètent la lecture principale plutôt qu’elles ne la remplacent.",
        "results_budget_year": "Budget par année",
        "results_projects_year": "Projets par année",
        "results_country_rank": "Classement pays",
        "geo_summary_title": "Lecture rapide",
        "geo_summary_single": "Le périmètre géographique actuel se concentre surtout sur {first}.",
        "geo_summary_multi": "Le financement est surtout concentré en {first}, puis {second}, sur {count} pays dans le périmètre courant.",
        "geo_open_results": "Ouvrir les résultats de ce pays",
        "geo_open_trends": "Ouvrir les tendances de ce pays",
        "trends_scope_summary_title": "Lecture rapide",
        "trends_scope_summary_up": "Le budget annuel progresse entre {start_year} ({start_budget}) et {end_year} ({end_budget}).",
        "trends_scope_summary_down": "Le budget annuel recule entre {start_year} ({start_budget}) et {end_year} ({end_budget}).",
        "trends_scope_summary_flat": "Le budget annuel reste globalement stable entre {start_year} ({start_budget}) et {end_year} ({end_budget}).",
        "actor_open_results": "Ouvrir les résultats de cet acteur",
        "actor_open_geo": "Ouvrir la géographie de ce pays",
        "actor_open_trends": "Ouvrir les tendances de ce thème",
        "docs_title": "Aide, guide et méthode",
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
        "macro_title": "Macro & actualités — lecture de contexte",
        "macro_subtitle": "Commence par les tendances, puis utilise cette vue pour ajouter du contexte événementiel si utile.",
        "macro_exploratory_note": "Lecture exploratoire : les événements servent de contexte indicatif et ne couvrent pas nécessairement tous les signaux pertinents.",
        "trends_title": "Tendances du périmètre",
        "trends_caption": "Lis d’abord l’évolution annuelle et les principaux moteurs. Les réglages plus denses restent secondaires.",
        "trends_summary_title": "En bref",
        "trends_summary_abs": "Le périmètre est surtout porté par {dim} sur la période sélectionnée.",
        "trends_summary_share": "{dim} représente la plus grande part du budget sur la période sélectionnée.",
        "trends_empty_hint": "Essaie d’élargir la période, de sélectionner moins de séries, ou reviens à la vue Résultats.",
        "compare_intro": "Commence par les plus fortes hausses et baisses de budget pour repérer rapidement où l’effort financier s’est déplacé.",
        "geo_primary_reading": "La carte sert de repère. Utilise surtout le classement pour comparer précisément les pays dans le périmètre courant.",
        "geo_rank_table": "Classement pays",
        "actor_answer_title": "En bref",
        "actor_top_projects": "Projets principaux",
        "actor_empty_hint": "Essaie d’élargir le périmètre, ou ouvre d’abord Résultats pour repartir d’une sélection plus large.",
        "support_overview_title": "À quoi sert cette zone",
        "support_overview_1": "Exporter les données du périmètre courant",
        "support_overview_2": "Vérifier qualité, couverture et méthode",
        "support_overview_3": "Ouvrir les diagnostics techniques si besoin",
        "support_overview_tip": "Cette zone reste utile pour l’exploitation interne, mais elle n’est pas nécessaire pour répondre à une question métier courante.",
        "macro_match": "Correspondance des événements",
        "macro_match_theme": "Par thématique (theme)",
        "macro_match_tag": "Par tag (tag → thématiques)",
        "macro_pick_theme": "Thématique",
        "macro_theme_scope": "Portée thématique",
        "macro_all_themes": "Toutes les thématiques (tag)",
        "macro_theme_not_mapped": "Ce tag n'a pas de mapping thématique strict: affichage multi-thèmes.",
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
        "macro_filters": "Options de contexte macro",
        "macro_use_global": "Utiliser les filtres globaux (sidebar)",
        "cloud_persistence_note": "Mode Streamlit Cloud : les mises à jour de fichiers via ce bouton ne sont pas durables. Utiliser le workflow GitHub « Refresh Data » pour une persistance automatique.",
        "refresh_cloud_skip": "Mode Streamlit Cloud : ce bouton ne lance pas de refresh durable. Lance le workflow GitHub « Refresh Data » (Actions) pour mettre à jour les données en ligne.",
        "refresh_cloud_disabled": "En mode Streamlit Cloud, utilise GitHub Actions pour un rafraîchissement persistant.",
        "missing_stage_col": "La colonne `value_chain_stage` n’est pas encore disponible. Lance un rafraîchissement des données.",
        "build_sha": "Version code",
        "mapping_coverage": "Couverture mapping",
        "include_unspecified": "Inclure « Unspecified »",
        "kpi_scope": "Périmètre actif",
        "ticket_shape_title": "Budgets et ticket médian par année",
        "ticket_shape_caption": "Graphique 1 = budget annuel total. Graphique 2 = ticket médian par projet.",
        "ticket_shape_median": "Ticket médian",
        "ticket_shape_total": "Budget annuel",
        "ticket_shape_projects": "Projets / an",
        "concentration_title": "Concentration du financement",
        "concentration_caption": "Lecture simple: barres = budget par acteur, courbe = part cumulée.",
        "concentration_budget": "Budget acteur",
        "concentration_cum": "Part cumulée (%)",
        "bm_treemap_help": "Lecture treemap: taille case = budget, hiérarchie = thème > pays > acteur, pourcentage affiché = part dans le parent.",
        "bm_treemap_settings_help": "Réduit les niveaux pour simplifier la lecture (moins de thèmes/pays/acteurs).",
        "bm_treemap_detail": "Niveau de détail",
        "bm_detail_simple": "Simple",
        "bm_detail_standard": "Standard",
        "bm_detail_detailed": "Détaillé",
        "macro_event_labels": "Afficher libellé court des événements sur le graphe",
        "macro_scope_caption": "Les valeurs ci-dessous portent sur la thématique sélectionnée (et les filtres macro), pas sur le budget global total.",
        "macro_event_count": "Événements associés",
        "macro_low_coverage": "Peu d'événements détectés pour ce tag/thème dans `events.csv`.",
        "macro_source_link": "Lien source",
        "vc_stage_filter": "Étapes de chaîne à afficher",
        "vc_stage_mode": "Affichage des étapes",
        "vc_stage_mode_all": "Toutes les étapes",
        "vc_stage_mode_custom": "Sélection personnalisée",
        "vc_stage_focus": "Étape à explorer",
        "vc_actor_focus": "Acteur sur cette étape",
        "vc_projects_focus": "Projets liés (étape + acteur)",
        "vc_top_actors_stage": "Top acteurs (étape)",
        "vc_single_stage_warn": "La sélection ne contient qu'une étape de chaîne de valeur. Lance un refresh pour recalculer la classification si besoin.",
        "vc_flow_help": "Choisis des thématiques puis des étapes pour voir quels acteurs opèrent à chaque maillon.",
        "vc_default_caption": "Commence par les étapes et les top acteurs ; la vue Sankey reste disponible plus bas.",
        "vc_expert_caption": "Vue experte : explore les flux et l’isolation visuelle entre étapes et acteurs.",
        "vc_stage_summary": "Résumé des étapes",
        "vc_flow_expert": "Flux entre étapes et acteurs",
        "vc_highlight_stage": "Étape à mettre en avant",
        "vc_all_stages": "Toutes les étapes",
        "vc_isolate_stage": "Isoler uniquement l'étape sélectionnée",
        "vc_highlight_actor": "Acteur à mettre en avant",
        "vc_all_actors": "Tous les acteurs",
        "vc_isolate_actor": "Isoler uniquement l'acteur sélectionné",
        "vc_isolation_help": "Mise en avant visuelle: couleur forte sur l'étape ciblée, le reste est atténué.",
        "vc_query_error": "Impossible de calculer la chaîne de valeur avec cette combinaison de filtres. Essaie 'Reset filters' ou réduis les filtres.",
        "vc_click_hint": "Astuce: clique un nœud du Sankey pour isoler automatiquement l'étape ou l'acteur.",
        "vc_click_unavailable": "Interaction au clic désactivée pour stabilité (mode sans clic).",
        "macro_same_year_events": "Événements de la même année",
        "net_focus_partner": "Partenaire à mettre en avant",
        "net_all_partners": "Tous les partenaires",
        "net_isolate_partner": "Isoler le partenaire sélectionné",
        "net_focus_help": "La mise en avant partenaire facilite la lecture des collaborations clés.",
        "net_default_caption": "Commence par le tableau des partenaires ; le graphe réseau reste disponible plus bas.",
        "net_expert_caption": "Vue experte : utile pour explorer visuellement les liens autour de l’acteur focal.",
        "net_focal_actor": "Acteur focal",
        "net_top_partners": "Nombre de partenaires affichés",
        "net_partner_table": "Partenaires",
        "net_graph_expert": "Carte des partenariats",
        "net_shared_projects_total": "Projets partagés",
        "net_partner_budget_total": "Budget partenaires",
        "actor_geo_single_country": "Acteur concentré sur un seul pays dans le périmètre actuel.",
        "actor_countries": "Pays couverts",
        "actor_main_country": "Pays principal",
        "app_mode_label": "Mode",
        "app_mode_simple": "Vue d'ensemble",
        "app_mode_advanced": "Recherche avancée",
        "simple_mode_filters_note": "Cette vue applique le périmètre par défaut. Passe en Recherche avancée pour afficher et modifier les filtres.",
        "diag_snapshot": "Diagnostic global",
        "diag_snapshot_hint": "Ces valeurs sont globales (hors filtres sidebar).",
        "diag_rows": "Lignes base",
        "diag_budget": "Budget base",
        "diag_projects": "Projets base",
        "diag_actors": "Acteurs base",
        "diag_years": "Plage années base",
        "diag_events": "Événements macro",
        "diag_events_ai": "Événements macro tag AI",
        "diag_connectors": "Connecteurs configurés",
        "diag_connectors_ready": "Connecteurs prêts (env + URL)",
        "diag_connectors_last": "Dernier statut connecteurs",
        "diag_events_policy": "Politique refresh events",
        "diag_events_policy_value": "minimum {hours:.0f}h entre rebuilds",
        "actor_query_fallback": "Certaines étiquettes acteur ne sont pas lisibles dans la source actuelle. Affichage de secours basé sur actor_id.",
    },
    "EN": {
        "language": "Language",
        "title": "Subsidy Intelligence Radar",
        "subtitle": "Guided search across EU subsidies: results, actors, geography, trends, and events.",
        "guided_home_title": "Start with a question",
        "guided_home_caption": "Pick the kind of answer you want first, then refine the topic, countries, and period before opening the full analysis.",
        "guided_home_intro_title": "Guided start",
        "guided_home_intro": "This page is meant to capture a simple intent first. The full analysis will then open already framed around your question.",
        "guided_home_question_title": "1. Choose a question",
        "guided_home_question_caption": "Select the path that best matches what you want to understand.",
        "guided_home_intent_projects": "Find projects on a topic",
        "guided_home_intent_projects_desc": "Open detailed results and the evidence table for the chosen scope.",
        "guided_home_intent_actors": "Identify the leading actors",
        "guided_home_intent_actors_desc": "Focus the first reading on the leading actors in the current scope.",
        "guided_home_intent_countries": "Compare countries",
        "guided_home_intent_countries_desc": "Open geography and the country ranking for the chosen scope.",
        "guided_home_intent_trends": "See how funding changes over time",
        "guided_home_intent_trends_desc": "Start with the annual trend, then open period comparison if needed.",
        "guided_home_intent_value_chain": "Explore the value chain",
        "guided_home_intent_value_chain_desc": "Open the expert tool focused on stages and actors.",
        "guided_home_intent_macro": "Understand the macro context",
        "guided_home_intent_macro_desc": "Open the macro reading as a contextual layer for the trends.",
        "guided_home_selected_question": "Selected question",
        "guided_home_selected_question_desc": "This intent will steer the first view opened in the full analysis.",
        "guided_home_choose_question": "Choose this question",
        "guided_home_selected_question_button": "Selected question",
        "guided_home_search": "Topic or keywords",
        "guided_home_search_help": "Examples: hydrogen Germany, batteries France, AI industry.",
        "guided_home_topics": "CORDIS domains to follow",
        "guided_home_topics_help": "Leave empty if you want to open the analysis across all CORDIS domains.",
        "guided_home_subtopics_note": "Detailed sub-themes are not structured yet in the reference model. This guided entry therefore works at the main-theme level for now.",
        "guided_home_countries_help": "Start with a simple country perimeter; you can refine it later in the full filters.",
        "guided_home_period_help": "The period sets your starting perimeter before deeper analysis.",
        "guided_home_open": "Open full analysis",
        "guided_home_back": "Back to guided start",
        "guided_home_analysis_note": "You are in the full analysis view. Use the button on the left if you want to quickly reframe the starting topic.",
        "guided_home_examples_label": "Example prompts",
        "guided_home_example_1": "Hydrogen in Germany since 2021",
        "guided_home_example_2": "Battery actors in France",
        "guided_home_example_3": "AI & digital in Europe",
        "guided_home_topic_card": "1. Define the topic",
        "guided_home_topic_card_desc": "Start with the keywords and major themes you actually want to follow.",
        "guided_home_scope_card": "2. Define the perimeter",
        "guided_home_scope_card_desc": "Then set the starting geography and time range.",
        "guided_home_metric_themes": "Domains",
        "guided_home_metric_countries": "Countries",
        "guided_home_metric_period": "Period",
        "guided_home_next_title": "What the full analysis will open",
        "guided_home_next_1": "detailed results for the chosen scope",
        "guided_home_next_2": "actors, geography, and trends already prefiltered",
        "guided_home_next_3": "all advanced filters if you want to go deeper",
        "guided_home_theme_cards_help": "Choose one or more CORDIS domains first. You can then refine with primary official themes and scientific sub-themes.",
        "guided_home_theme_select_note": "Check to include this domain",
        "guided_home_theme_select_action": "Select",
        "guided_home_selected_themes": "Selected domains",
        "guided_home_subtopics": "Scientific sub-themes",
        "guided_home_subtopics_help": "Choose scientific sub-themes to refine the exploration. They will filter the detailed analysis without redefining the global totals.",
        "guided_terms": "Guided sub-themes",
        "guided_terms_applied": "Guided sub-themes applied",
        "reset": "Reset",
        "refresh": "Refresh data",
        "refresh_hint": "Updates CORDIS + events (offline), then reloads the app.",
        "filters": "Filters",
        "basic_filters": "Business filters",
        "advanced_filters": "More business filters",
        "analysis_options": "Analysis behavior",
        "sources": "Sources",
        "onetech_only": "Restrict to OneTech scope",
        "programmes": "Programmes",
        "period": "Period (start year)",
        "use_section": "Filter by section",
        "section": "Section (EU / Programme / Topic)",
        "project_status": "Project status",
        "status_open": "Open",
        "status_closed": "Closed",
        "status_unknown": "Unknown",
        "domains": "CORDIS domains",
        "themes": "Primary CORDIS theme",
        "scientific_subthemes": "Scientific sub-themes",
        "entity": "Entity type",
        "countries": "Countries",
        "country_preset_eu27": "EU 27",
        "country_preset_associated": "EU + Associated",
        "country_preset_all": "All",
        "quick_search": "Quick search",
        "quick_search_hint": "Search actor, project, acronym or title",
        "main_search_support": "Free-text search across projects and actors. Then use filters to narrow country, time period, and programme.",
        "search_help_more": "How this search works",
        "main_search_examples": "Examples: “hydrogen in Germany since 2021”, “top AI actors in France”, “compare batteries across France and Germany”.",
        "main_search_exploratory": "Exploratory free-text search works best with simple keywords. Use filters to narrow country, time period, and programme.",
        "search_literal_note": "Free-text search remains literal. Synonyms and exclusions currently feed theme classification more than true semantic search.",
        "search_interpretation_title": "Question currently interpreted",
        "search_interpretation_caption": "This is what the app is currently using to build the scope.",
        "search_interpretation_intent": "Intent",
        "search_interpretation_scope": "Active reading",
        "search_interpretation_search": "Free-text search",
        "search_interpretation_none": "No free-text term",
        "filters_advanced_hint": "Programme, source, entity type, and analysis options remain available in Advanced search.",
        "search_simplified_notice": "Search was simplified to avoid an error. Try a simpler keyword, then refine with filters.",
        "search_ignored_notice": "Search could not be applied safely for this input. Filters are still active; try a simpler keyword.",
        "view_recover_hint": "Try a simpler search or adjust the filters. Other views remain available.",
        "chart_render_unavailable": "This chart could not be displayed for the current scope.",
        "results_view_unavailable": "This results view could not be displayed for the current scope.",
        "results_scope_partial_warning": "Some scope summary information could not be fully computed for the current selection.",
        "geo_view_unavailable": "The geography view could not be displayed for the current scope.",
        "benchmark_view_unavailable": "This comparison view could not be displayed for the current scope.",
        "value_chain_view_unavailable": "The stages and actors view could not be displayed for the current scope.",
        "partnership_view_unavailable": "The partnerships view could not be displayed for the current scope.",
        "concentration_view_unavailable": "The funding concentration view could not be displayed for the current scope.",
        "geo_empty_hint": "Try widening the time range, broadening geography, or returning to a wider country-level view.",
        "geo_country_empty_hint": "Try another country, a wider time range, or go back to the overall country ranking.",
        "advanced_empty_hint": "Try widening the scope or start with the table/ranking view before opening expert visuals.",
        "benchmark_empty_hint": "Try widening the scope or return to the simple actor ranking.",
        "value_chain_empty_hint": "Try broadening themes, re-enabling all stages, or returning to the stage summary.",
        "partnership_empty_hint": "Choose a more central actor, show more partners, or widen the scope.",
        "concentration_empty_hint": "Widen the scope to compare more actors or return to the actors view.",
        "net_no_partners": "No partners are available in the current scope.",
        "explore_overview_title": "What you can do here",
        "explore_overview_1": "Find projects by theme, country, and time period",
        "explore_overview_2": "Compare the main actors in a domain",
        "explore_overview_3": "See which countries receive the most funding",
        "explore_overview_4": "Relate funding trends to macro events",
        "explore_overview_5": "Open advanced views only if needed",
        "explore_overview_tip": "Start with free-text search, then refine with filters.",
        "actor_grouping": "Group legal entities (PIC/group)",
        "exclude_funders": "Exclude funders / agencies",
        "actor_groups_ready": "Group mapping loaded",
        "actor_groups_source": "Mapping source",
        "mapping_low_coverage": "Low coverage: complete `actor_groups.csv` for reliable grouping.",
        "mapping_pic_fallback": "Fallback active: automatic grouping by PIC when mapping is not provided.",
        "exclude_funders_heuristic": "Exclusion also uses org-name heuristics (EIT/CINEA/etc.).",
        "actor_groups_missing": "Group mapping missing (`actor_groups.csv` / template).",
        "mapping_summary": "Group mapping summary",
        "mapping_loaded_count": "Mapping rows",
        "mapping_match_rate": "Match rate",
        "mapping_mode_explicit": "Grouping mode: explicit mapping + PIC fallback",
        "mapping_mode_fallback": "Grouping mode: PIC fallback (partial mapping)",
        "mapping_mode_pic_only": "Grouping mode: PIC fallback only",
        "mapping_keys_matched": "Matched mapping keys",
        "mapping_keys_issue": "Mapping keys do not match IDs/PIC present in the dataset.",
        "mapping_keys_partial": "Some mapping keys do not match the dataset.",
        "mapping_global_impact": "Impacted actors (current scope)",
        "mapping_status_ready": "Group mapping active.",
        "mapping_status_partial": "Partial group mapping (PIC fallback in use).",
        "mapping_status_missing_short": "No group mapping (PIC fallback).",
        "mapping_diag_toggle": "Mapping diagnostics (optional)",
        "refresh_cloud_cta": "Open GitHub Actions \"Refresh Data\"",
        "kpis": "Key indicators",
        "insights_title": "Automatic insights (current scope)",
        "budget_total": "Total budget",
        "n_projects": "Projects",
        "n_actors": "Unique actors",
        "avg_ticket": "Avg ticket / project",
        "median_ticket": "Median ticket / project",
        "top10_share": "Top10 actors share",
        "hhi": "Concentration (HHI)",
        "no_data": "No data for this selection. Broaden the filters.",
        "tab_overview": "Overview",
        "tab_geo": "Geography",
        "tab_comp": "Compare actors",
        "tab_trends": "Trends",
        "tab_compare": "Compare",
        "tab_macro": "Macro & news",
        "tab_actor": "Actor profile",
        "tab_network": "Value chain & network",
        "tab_data": "Data",
        "tab_quality": "Quality",
        "tab_help": "Help",
        "tab_guide": "Guide",
        "zoom_on": "Zoom",
        "projection": "Map type",
        "borders": "Borders & coastlines",
        "labels": "Continent labels",
        "top_countries": "Top 15 countries",
        "geo_metric": "Map metric",
        "geo_metric_total": "Total budget (€)",
        "geo_metric_per_million": "Budget / million inhabitants (€)",
        "geo_pop_missing": "Population missing for some countries: partial normalization.",
        "geo_caption": "Geographic reading of the active scope: concentration, country ranking, and country detail.",
        "geo_perimeter_default": "Default perimeter: European countries present in the dataset, including EU member states and a few associated countries when available.",
        "geo_perimeter_custom": "The ranking below follows your current country filters. By default, the app starts from the European countries present in the dataset, including a few associated countries when available.",
        "geo_country_picker": "Country to inspect",
        "geo_advanced_options": "Map settings",
        "geo_selected_summary": "Selected country",
        "geo_rank": "Rank",
        "geo_scope_share": "Share of scope",
        "geo_country_detail": "Country detail",
        "geo_country_actors": "Leading actors",
        "geo_country_themes": "Leading themes",
        "geo_country_projects": "Leading projects",
        "benchmark_mode": "Comparison view",
        "bm_scatter": "Compare volume and budget",
        "bm_treemap": "Block view",
        "bm_top": "Actor rankings",
        "bm_caption": "Start with a simple ranking, then open the expert views if needed.",
        "bm_default_caption": "Default view: simple, table-first reading of the current scope.",
        "bm_expert_caption": "Expert view: useful for exploring more complex positioning and hierarchy patterns.",
        "bm_compare_scope": "Comparison settings",
        "bm_overall_rank": "Overall ranking",
        "bm_breakdown_entity": "By entity type",
        "pct_threshold": "Budget threshold",
        "topn": "Actors shown",
        "search_actor": "Text search (contains…)",
        "actor_picker": "Actor to compare",
        "actor_picker_hint": "Type to search in the list.",
        "legend_tip": "Tip: click the legend to hide/show a series.",
        "scatter_explain": (
            "- Each point = **one actor** (organisation), aggregated over current filters.\n"
            "- X axis = **# of distinct projects** where the actor appears.\n"
            "- Y axis = **total funding captured** by the actor.\n\n"
            "**Why can one project count for multiple actors?**\n"
            "A project has multiple participants: it is counted for **each** participating actor.\n"
        ),
        "dimension": "Group by",
        "dim_theme": "Theme",
        "dim_program": "Programme",
        "mode": "Mode",
        "mode_abs": "Budget (absolute)",
        "mode_share": "Share (% per year)",
        "drivers": "Main drivers",
        "compare_title": "Compare two periods",
        "period_a": "Period A",
        "period_b": "Period B",
        "compare_caption": "Compare budget gaps across two periods for themes or programmes in the current scope.",
        "compare_normalize_annual": "Normalize to annual average",
        "compare_period_years": "Period A: {years_a} years, Period B: {years_b} years",
        "compare_period_years_normalized": "Period A: {years_a} years, Period B: {years_b} years · normalized to annual average.",
        "compare_budget_a": "Budget A",
        "compare_budget_b": "Budget B",
        "compare_delta_budget": "Budget change (B - A)",
        "compare_budget_reading": "Reading: bars to the right mean period B funds more; bars to the left mean it funds less. This compares budget, not relative share.",
        "budget_envelope_note": "Budgets are read as total project envelopes attached to the project start year. They are not actual yearly disbursements.",
        "theme_method_note": "Themes are inferred from controlled FR/EN keyword rules with a few exclusion patterns. Each project currently receives one main theme.",
        "theme_review_label": "Multi-domain",
        "theme_review_note": "“Multi-domain” groups cross-disciplinary projects without a single dominant theme in the current reference set.",
        "actor_grouping_note": "Entity grouping depends on a group mapping plus PIC fallback. It remains partial for some subsidiaries and corporate structures.",
        "value_chain_method_note": "Value-chain stages are inferred from project text. This is indicative and should not be read as an audited TRL classification.",
        "partnership_stage_note": "The partnership view is not yet sliced directly by value-chain stage. Use theme filters and active scope first to narrow the reading.",
        "actor_profile": "Actor profile",
        "actor_group_mode_caption": "Group view is active: profiles and charts may aggregate several legal entities through mapping or PIC.",
        "actor_profile_caption": "Pick an actor, then start with their profile, evolution, and projects before opening deeper expert reads.",
        "actor_opened_from_results": "Opened from a selected project in Results.",
        "actor_trend": "Trend (budget & projects)",
        "actor_mix_theme": "Theme mix",
        "actor_mix_country": "Geography mix",
        "actor_partners": "Leading partners",
        "actor_partners_caption": "Start with the partner table. The detailed network remains in Advanced.",
        "actor_partners_mode": "Partner reading",
        "actor_partners_mode_scope": "Within the active scope",
        "actor_partners_mode_matched": "All partners on matched projects",
        "actor_partners_mode_scope_caption": "Strict read: partners are still ranked within the active scope, but the entity-type filter is not applied to this partner read.",
        "actor_partners_mode_matched_caption": "Expanded read: projects are selected with the active scope, then all co-participants recorded on those matched projects are included, with the entity-type filter still ignored.",
        "actor_partners_scope_note_extra": "Additional partners exist on the matched projects but do not pass every active filter. Open the expanded read to include them.",
        "partners_entity_filter_note": "Partners shown across all entity types, regardless of entity filter.",
        "actor_tab_profile": "Profile",
        "actor_tab_partners": "Partners",
        "actor_tab_peers": "Comparable actors",
        "actor_top_theme": "Main theme",
        "actor_entity_type": "Entity type",
        "actor_rank_overall": "Overall rank",
        "actor_rank_peer": "Rank in peer group",
        "actor_avg_ticket": "Average ticket",
        "actor_peer_group": "Peer group",
        "actor_peer_caption": "Simple comparison within the active scope. Advanced visuals stay in “Compare actors”.",
        "actor_peer_table": "Comparable actors",
        "scope_caption": "Active scope",
        "scope_group_on": "group view",
        "scope_group_off": "legal-entity view",
        "scope_funders_off": "funders excluded",
        "scope_funders_on": "funders included",
        "status_budget_title": "Budget by project status",
        "status_projects_title": "Projects by status",
        "tab_explorer": "⌕ Search & results",
        "tab_actors_hub": "◈ Actors",
        "tab_markets": "◎ Geography",
        "tab_trends_events": "↗ Trends & events",
        "tab_advanced": "◇ Expert tools",
        "tab_admin": "⋯ Data, method & exports",
        "admin_title": "Data, method & exports",
        "admin_caption": "Exports, quality, method, and diagnostics. This area stays secondary for standard exploration.",
        "sub_results": "Results",
        "sub_overview": "Overview",
        "overview_caption": "Secondary read: start with Results to answer the question, then use this page for a compact scope summary.",
        "overview_support_note": "This page is a supporting summary. For the main reading flow, comparisons, and the evidence table, start with Results.",
        "overview_more_context": "More context",
        "overview_yearly_extra": "Yearly add-on: budgets and median ticket",
        "sub_benchmark": "Compare actors",
        "sub_network": "Value chain & network",
        "sub_value_chain": "Stages and actors",
        "sub_collaboration": "Partnerships",
        "sub_concentration": "Funding concentration",
        "sub_data": "Exports & data",
        "sub_quality": "Quality & method",
        "sub_debug": "Technical diagnostics",
        "advanced_title": "Expert tools",
        "advanced_caption": "Expert views for going deeper after the main reading flow. Start with Search, Actors, Geography, or Trends first.",
        "advanced_overview_title": "When to open advanced analysis",
        "advanced_overview_1": "Compare actors when a simple ranking is no longer enough",
        "advanced_overview_2": "Read stages and actors to understand a value chain",
        "advanced_overview_3": "Explore partnerships around a focal actor",
        "advanced_overview_4": "Measure whether funding is spread or concentrated",
        "advanced_overview_tip": "Start with the rankings and tables below; expert charts remain a second layer of reading.",
        "adv_benchmark_helper": "Use this to spot the leading actors in the current scope before opening the expert charts.",
        "adv_value_chain_helper": "Use this to see which actors are active at each stage and which projects sit behind them.",
        "adv_collaboration_helper": "Use this to identify an actor’s key partners before opening the network map.",
        "adv_concentration_helper": "Use this to see whether funding is spread across many actors or concentrated in a few.",
        "debug_title": "Technical diagnostics",
        "debug_caption": "Technical surfaces moved out of the sidebar to keep exploration readable.",
        "results_title": "Results in scope",
        "results_caption": "Start here: define the question, read the answer, then open other readings only if needed.",
        "results_view": "Primary view",
        "results_table": "Project table",
        "results_trend": "Trend",
        "results_map": "Map",
        "results_actors": "Actors",
        "main_search_label": "What do you want to explore?",
        "main_search_help": "Free-text search across actor, project, acronym, or title",
        "main_search_placeholder": "E.g. AI, hydrogen, CNRS, batteries",
        "active_filters": "Active filters",
        "clear_search": "Clear search",
        "no_results_title": "No results for this scope.",
        "no_results_hint": "Try widening country, time, or theme.",
        "no_results_reset": "Reset filters",
        "no_results_clear_search": "Clear search",
        "no_results_broaden": "Try widening country, time, or theme filters, or use a simpler search.",
        "results_summary_title": "In brief",
        "results_summary_headline": "The current scope contains {projects} projects worth {budget}, with {actors} actors across {countries} countries.",
        "results_summary_headline_single_country": "The current scope contains {projects} projects worth {budget}, with {actors} actors in {country}.",
        "results_summary_country_lead": "The largest budget is in {country}.",
        "results_summary_actor_lead": "The leading funded actor is {actor}.",
        "results_summary_theme_lead": "The leading theme is {theme}.",
        "theme_counting_note": "Note: in the current build, each row receives a single inferred theme. A project therefore does not appear in multiple themes here.",
        "results_summary_fallback": "The current scope contains {projects} projects worth {budget} and {actors} unique actors.",
        "results_primary_visual": "Primary reading",
        "results_primary_trend": "Annual budget",
        "results_primary_countries": "Leading countries",
        "results_primary_actors": "Leading actors",
        "results_next_steps": "Suggested next steps",
        "results_next_geo": "Open geography",
        "results_next_actors": "Open actors",
        "results_next_trends": "Open trends",
        "results_other_views": "Other ways to read this scope",
        "results_projects_table": "Matching projects",
        "results_projects_table_caption": "This table is the evidence layer for the current scope: it shows the projects behind the answer and the main chart.",
        "results_actor_table": "Leading actors",
        "results_other_views_caption": "These views remain useful, but they complement the primary reading instead of replacing it.",
        "results_budget_year": "Budget by year",
        "results_projects_year": "Projects by year",
        "results_country_rank": "Country ranking",
        "geo_summary_title": "Quick read",
        "geo_summary_single": "The current geographic scope is mainly concentrated in {first}.",
        "geo_summary_multi": "Funding is mainly concentrated in {first}, then {second}, across {count} countries in the current scope.",
        "geo_open_results": "Open results for this country",
        "geo_open_trends": "Open trends for this country",
        "trends_scope_summary_title": "Quick read",
        "trends_scope_summary_up": "Annual funding rises between {start_year} ({start_budget}) and {end_year} ({end_budget}).",
        "trends_scope_summary_down": "Annual funding falls between {start_year} ({start_budget}) and {end_year} ({end_budget}).",
        "trends_scope_summary_flat": "Annual funding stays broadly stable between {start_year} ({start_budget}) and {end_year} ({end_budget}).",
        "actor_open_results": "Open results for this actor",
        "actor_open_geo": "Open geography for this country",
        "actor_open_trends": "Open trends for this theme",
        "docs_title": "Help, guide, and method",
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
        "macro_title": "Macro & news — context read",
        "macro_subtitle": "Start with trends, then use this view to add event context when useful.",
        "macro_exploratory_note": "Exploratory read: events are indicative context and may not cover every relevant macro signal.",
        "trends_title": "Scope trends",
        "trends_caption": "Start with annual evolution and key drivers. Denser controls stay secondary.",
        "trends_summary_title": "In brief",
        "trends_summary_abs": "The current scope is mainly driven by {dim} over the selected period.",
        "trends_summary_share": "{dim} holds the largest share of funding over the selected period.",
        "trends_empty_hint": "Try widening the time range, selecting fewer series, or go back to Results.",
        "compare_intro": "Start with the strongest budget gains and declines to quickly see where funding moved.",
        "geo_primary_reading": "Use the map as orientation. Use the ranking to compare countries precisely within the current scope.",
        "geo_rank_table": "Country ranking",
        "actor_answer_title": "In brief",
        "actor_top_projects": "Top projects",
        "actor_empty_hint": "Try widening the scope, or start from Results to rebuild a broader selection.",
        "support_overview_title": "What this area is for",
        "support_overview_1": "Export data from the current scope",
        "support_overview_2": "Check quality, coverage, and method",
        "support_overview_3": "Open technical diagnostics when needed",
        "support_overview_tip": "This area remains useful for internal operations, but it is not needed for a standard business question.",
        "macro_match": "Event matching",
        "macro_match_theme": "By theme (theme)",
        "macro_match_tag": "By tag (tag → themes)",
        "macro_pick_theme": "Theme",
        "macro_theme_scope": "Theme scope",
        "macro_all_themes": "All themes (tag)",
        "macro_theme_not_mapped": "This tag has no strict theme mapping: multi-theme display is used.",
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
        "macro_filters": "Macro context options",
        "macro_use_global": "Use global filters (sidebar)",
        "cloud_persistence_note": "Streamlit Cloud mode: file updates from this button are not durable. Use the GitHub workflow \"Refresh Data\" for persistent automation.",
        "refresh_cloud_skip": "Streamlit Cloud mode: this button does not run a durable refresh. Run the GitHub \"Refresh Data\" workflow (Actions) to update online data.",
        "refresh_cloud_disabled": "In Streamlit Cloud mode, use GitHub Actions for persistent refresh.",
        "missing_stage_col": "Column `value_chain_stage` is not available yet. Run a data refresh.",
        "build_sha": "Code version",
        "mapping_coverage": "Mapping coverage",
        "include_unspecified": "Include \"Unspecified\"",
        "kpi_scope": "Active scope",
        "ticket_shape_title": "Budgets and median ticket by year",
        "ticket_shape_caption": "Chart 1 = total annual budget. Chart 2 = median project ticket.",
        "ticket_shape_median": "Median ticket",
        "ticket_shape_total": "Annual budget",
        "ticket_shape_projects": "Projects / year",
        "concentration_title": "Funding concentration",
        "concentration_caption": "Simple reading: bars = budget by actor, line = cumulative share.",
        "concentration_budget": "Actor budget",
        "concentration_cum": "Cumulative share (%)",
        "bm_treemap_help": "Treemap reading: tile size = budget, hierarchy = theme > country > actor, displayed percentage = share in parent.",
        "bm_treemap_settings_help": "Reduce levels to simplify reading (fewer themes/countries/actors).",
        "bm_treemap_detail": "Detail level",
        "bm_detail_simple": "Simple",
        "bm_detail_standard": "Standard",
        "bm_detail_detailed": "Detailed",
        "macro_event_labels": "Show short event labels on chart",
        "macro_scope_caption": "Values below apply to the selected theme (and macro filters), not to the global total budget.",
        "macro_event_count": "Matched events",
        "macro_low_coverage": "Few events detected for this tag/theme in `events.csv`.",
        "macro_source_link": "Source link",
        "vc_stage_filter": "Value-chain stages to display",
        "vc_stage_mode": "Stage display",
        "vc_stage_mode_all": "All stages",
        "vc_stage_mode_custom": "Custom selection",
        "vc_stage_focus": "Stage to explore",
        "vc_actor_focus": "Actor on this stage",
        "vc_projects_focus": "Related projects (stage + actor)",
        "vc_top_actors_stage": "Top actors (stage)",
        "vc_single_stage_warn": "This selection contains a single value-chain stage. Run refresh to recompute stage classification if needed.",
        "vc_flow_help": "Pick themes then stages to see which actors operate on each link of the chain.",
        "vc_default_caption": "Start with stages and top actors; the Sankey view stays available lower on the page.",
        "vc_expert_caption": "Expert view: explore flow patterns and visual isolation between stages and actors.",
        "vc_stage_summary": "Stage summary",
        "vc_flow_expert": "Flows between stages and actors",
        "vc_highlight_stage": "Stage to highlight",
        "vc_all_stages": "All stages",
        "vc_isolate_stage": "Show only selected stage",
        "vc_highlight_actor": "Actor to highlight",
        "vc_all_actors": "All actors",
        "vc_isolate_actor": "Show only selected actor",
        "vc_isolation_help": "Visual focus: strong color on selected stage, other links are faded.",
        "vc_query_error": "Unable to compute value-chain view with this filter combination. Try 'Reset filters' or reduce filters.",
        "vc_click_hint": "Tip: click a Sankey node to automatically isolate the stage or actor.",
        "vc_click_unavailable": "Click interaction disabled for stability (non-click mode).",
        "macro_same_year_events": "Events in the same year",
        "net_focus_partner": "Partner to highlight",
        "net_all_partners": "All partners",
        "net_isolate_partner": "Isolate selected partner",
        "net_focus_help": "Partner highlight helps read key collaborations.",
        "net_default_caption": "Start with the partner table; the network graph stays available lower on the page.",
        "net_expert_caption": "Expert view: useful for visually exploring links around the focal actor.",
        "net_focal_actor": "Focal actor",
        "net_top_partners": "Partners shown",
        "net_partner_table": "Partners",
        "net_graph_expert": "Partnership map",
        "net_shared_projects_total": "Shared projects",
        "net_partner_budget_total": "Partner budget",
        "actor_geo_single_country": "Actor concentrated in a single country in the current scope.",
        "actor_countries": "Countries covered",
        "actor_main_country": "Main country",
        "app_mode_label": "Mode",
        "app_mode_simple": "Overview",
        "app_mode_advanced": "Advanced search",
        "simple_mode_filters_note": "This view applies the default scope. Switch to Advanced search to show and edit filters.",
        "diag_snapshot": "Snapshot diagnostics",
        "diag_snapshot_hint": "These values are global (independent from sidebar filters).",
        "diag_rows": "Dataset rows",
        "diag_budget": "Dataset budget",
        "diag_projects": "Dataset projects",
        "diag_actors": "Dataset actors",
        "diag_years": "Dataset year range",
        "diag_events": "Macro events",
        "diag_events_ai": "Macro events tagged AI",
        "diag_connectors": "Configured connectors",
        "diag_connectors_ready": "Ready connectors (env + URL)",
        "diag_connectors_last": "Last connector status",
        "diag_events_policy": "Events refresh policy",
        "diag_events_policy_value": "minimum {hours:.0f}h between rebuilds",
        "actor_query_fallback": "Some actor labels are not readable in the current source. Fallback display uses actor_id.",
    },
}


def t(lang: str, key: str) -> str:
    return I18N[lang].get(key, key)


def guided_intent_title(lang: str, intent: str) -> str:
    return t(lang, f"guided_home_intent_{str(intent)}")


def guided_intent_desc(lang: str, intent: str) -> str:
    return t(lang, f"guided_home_intent_{str(intent)}_desc")


def guided_intent_mode(intent: str) -> str:
    return "advanced" if str(intent) == "value_chain" else "simple"


def guided_intent_primary_view(intent: str) -> str:
    mapping = {
        "actors": "actors",
        "countries": "countries",
        "trends": "trend",
    }
    return mapping.get(str(intent), "")


def apply_guided_intent_navigation(lang: str) -> None:
    intent = str(st.session_state.get("guided_intent", "projects") or "projects")
    st.session_state["app_mode"] = guided_intent_mode(intent)
    st.session_state["guided_intent_active"] = intent
    if intent == "countries":
        queue_tab_navigation(top_target=t(lang, "tab_markets"))
    elif intent == "trends":
        queue_tab_navigation(top_target=t(lang, "tab_trends_events"), trends_sub_target=t(lang, "tab_trends"))
    elif intent == "macro":
        queue_tab_navigation(top_target=t(lang, "tab_trends_events"), trends_sub_target=t(lang, "tab_macro"))
    elif intent == "value_chain":
        queue_tab_navigation(top_target=t(lang, "tab_advanced"), advanced_sub_target=t(lang, "sub_value_chain"))
    else:
        queue_tab_navigation(top_target=t(lang, "tab_explorer"))


def wip_badge(lang: str) -> str:
    label = "⚠ En cours" if lang == "FR" else "⚠ WIP"
    return f"<span class='sir-wip-badge'>{html.escape(label)}</span>"


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
    raw = str(raw)
    if raw == "Other":
        return t(lang, "theme_review_label")
    if lang == "FR":
        return THEME_EN_TO_FR.get(raw, raw)
    return raw


def domain_raw_to_display(raw: str, lang: str) -> str:
    raw = str(raw or "").strip()
    if not raw:
        return ""
    if lang == "FR":
        return CORDIS_DOMAIN_UI_FR.get(raw, THEME_EN_TO_FR.get(raw, raw))
    return raw


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


def entity_raw_to_display(raw: str, lang: str) -> str:
    if lang == "FR":
        return ENTITY_EN_TO_FR.get(raw, raw)
    return raw

def country_value_labels(raw: str) -> List[str]:
    value = str(raw or "").strip()
    if not value:
        return []
    parts = [p.strip() for p in re.split(r"[;,/]+", value) if p.strip()]
    labels: List[str] = []
    for part in parts or [value]:
        part_clean = str(part).strip()
        mapped = COUNTRY_CODE_TO_NAME.get(part_clean.upper(), part_clean)
        if mapped == part_clean and pycountry is not None:
            try:
                if len(part_clean) == 2:
                    country = pycountry.countries.get(alpha_2=part_clean.upper())
                    if country is not None:
                        mapped = str(country.name)
                elif len(part_clean) == 3:
                    country = pycountry.countries.get(alpha_3=part_clean.upper())
                    if country is not None:
                        mapped = str(country.name)
            except Exception:
                mapped = part_clean
        if mapped not in labels:
            labels.append(mapped)
    return labels


def country_raw_to_display(raw: str) -> str:
    labels = country_value_labels(raw)
    if not labels:
        return ""
    return " / ".join(labels)


def normalized_country_options(values: List[str]) -> List[str]:
    ordered: List[str] = []
    for raw in values or []:
        labels = country_value_labels(raw) or [str(raw).strip()]
        for label in labels:
            clean = str(label).strip()
            if clean and clean not in ordered:
                ordered.append(clean)
    return ordered


def normalize_country_selection(values: List[str], available: List[str]) -> List[str]:
    allowed = set(str(x).strip() for x in available or [] if str(x).strip())
    normalized: List[str] = []
    for raw in values or []:
        labels = country_value_labels(raw) or [str(raw).strip()]
        for label in labels:
            clean = str(label).strip()
            if clean and clean in allowed and clean not in normalized:
                normalized.append(clean)
    return normalized


def _country_values_matching(countries: List[str], allowed_names: List[str]) -> List[str]:
    allowed = set(str(x) for x in allowed_names if str(x).strip())
    matched: List[str] = []
    for raw in countries or []:
        labels = set(country_value_labels(raw))
        if labels & allowed and raw not in matched:
            matched.append(raw)
    return matched


def status_raw_to_display(raw: str, lang: str) -> str:
    mapping_fr = {"Open": "Ouvert", "Closed": "Fermé", "Unknown": "Inconnu"}
    if lang == "FR":
        return mapping_fr.get(str(raw), str(raw))
    return str(raw)


def sql_contains_expr(column_sql: str, query: str) -> str:
    safe = str(query).replace("\x00", "").replace("\r", " ").replace("\n", " ").strip().lower()
    safe = safe.replace("'", "''")
    if not safe:
        return "TRUE"
    return f"lower(COALESCE({column_sql}, '')) LIKE '%{safe}%'"


def quick_search_clause(prefix: str, query: str, columns: Optional[List[str]] = None) -> str:
    cols = columns or [
        "projectID",
        "acronym",
        "title",
        "org_name",
        "actor_id",
        "cordis_domain_ui",
        "cordis_theme_primary",
        "sub_theme",
        "keywords",
    ]
    return "(" + " OR ".join(sql_contains_expr(f"{prefix}{col}", query) for col in cols) + ")"


def _normalize_quick_search(query: str) -> str:
    raw = str(query or "").replace("\x00", " ").replace("\r", " ").replace("\n", " ")
    return re.sub(r"\s+", " ", raw).strip()


def _validate_where_query(relation_sql: str, where_sql: str, table_alias: Optional[str] = None) -> None:
    alias_sql = f" {str(table_alias).strip()}" if str(table_alias or "").strip() else ""
    get_con().execute(f"SELECT 1 FROM {relation_sql}{alias_sql} WHERE {where_sql} LIMIT 1").fetchone()


def build_safe_where_pair(
    relation_sql: str,
    *,
    sources: List[str],
    programmes: List[str],
    years: Tuple[int, int],
    use_section: bool,
    sections: List[str],
    onetech_only: bool,
    domains: List[str],
    statuses: List[str],
    themes: List[str],
    subthemes: List[str],
    entities: List[str],
    countries: List[str],
    quick_search: str,
    extra_search_terms: Optional[List[str]] = None,
) -> Tuple[str, str, Optional[str]]:
    normalized_search = _normalize_quick_search(quick_search)
    base_kwargs = dict(
        sources=sources,
        programmes=programmes,
        years=years,
        use_section=use_section,
        sections=sections,
        onetech_only=onetech_only,
        domains=domains,
        statuses=statuses,
        themes=themes,
        subthemes=subthemes,
        entities=entities,
        countries=countries,
        extra_search_terms=extra_search_terms,
    )

    w = where_clause(**base_kwargs, quick_search=normalized_search)
    w_r = where_clause(**base_kwargs, quick_search=normalized_search, table_alias="r")
    if not normalized_search:
        return w, w_r, None

    try:
        _validate_where_query(relation_sql, w)
        _validate_where_query(relation_sql, w_r, table_alias="r")
        return w, w_r, None
    except duckdb.Error:
        text_only_cols = ["acronym", "title", "org_name"]
        try:
            w_text = where_clause(**base_kwargs, quick_search=normalized_search, quick_search_columns=text_only_cols)
            w_r_text = where_clause(**base_kwargs, quick_search=normalized_search, table_alias="r", quick_search_columns=text_only_cols)
            _validate_where_query(relation_sql, w_text)
            _validate_where_query(relation_sql, w_r_text, table_alias="r")
            return w_text, w_r_text, "search_simplified_notice"
        except duckdb.Error:
            w_plain = where_clause(**base_kwargs, quick_search="")
            w_r_plain = where_clause(**base_kwargs, quick_search="", table_alias="r")
            return w_plain, w_r_plain, "search_ignored_notice"


def _compact_filter_values(values: List[str], formatter=None, limit: int = 3) -> str:
    vals = [str(v).strip() for v in values if str(v).strip()]
    if formatter is not None:
        vals = [str(formatter(v)) for v in vals]
    if not vals:
        return ""
    if len(vals) <= limit:
        return ", ".join(vals)
    return ", ".join(vals[:limit]) + f" +{len(vals) - limit}"


def active_filter_labels(meta: dict, lang: str) -> List[str]:
    labels: List[str] = []
    q = str(st.session_state.get("f_quick_search", "")).strip()
    if q:
        labels.append(f"{t(lang, 'quick_search')}: {q}")

    countries = [x for x in st.session_state.get("f_countries", []) if x in meta.get("countries", [])]
    if countries and len(countries) < len(meta.get("countries", [])):
        labels.append(f"{t(lang, 'countries')}: {_compact_filter_values(countries, country_raw_to_display)}")

    domains = [x for x in st.session_state.get("f_domains_raw", []) if x in meta.get("domains", [])]
    if domains and len(domains) < len(meta.get("domains", [])):
        labels.append(f"{t(lang, 'domains')}: {_compact_filter_values(domains, lambda x: domain_raw_to_display(x, lang))}")

    themes = [x for x in st.session_state.get("f_themes_raw", []) if x in meta.get("themes", [])]
    if themes and len(themes) < len(meta.get("themes", [])):
        labels.append(f"{t(lang, 'themes')}: {_compact_filter_values(themes, lambda x: theme_raw_to_display(x, lang))}")

    scientific_subthemes = [x for x in st.session_state.get("f_scientific_subthemes", []) if x in meta.get("scientific_subthemes", [])]
    if scientific_subthemes and len(scientific_subthemes) < len(meta.get("scientific_subthemes", [])):
        labels.append(f"{t(lang, 'scientific_subthemes')}: {_compact_filter_values(scientific_subthemes, limit=2)}")

    entities = [x for x in st.session_state.get("f_entity_raw", []) if x in meta.get("entities", [])]
    if entities and len(entities) < len(meta.get("entities", [])):
        labels.append(f"{t(lang, 'entity')}: {_compact_filter_values(entities, lambda x: entity_raw_to_display(x, lang))}")

    statuses = [x for x in st.session_state.get("f_statuses", []) if x in meta.get("statuses", [])]
    if statuses and len(statuses) < len(meta.get("statuses", [])):
        labels.append(f"{t(lang, 'project_status')}: {_compact_filter_values(statuses, lambda x: status_raw_to_display(x, lang))}")

    guided_subtopics = [x for x in st.session_state.get("f_guided_subtopics", []) if str(x).strip()]
    if guided_subtopics:
        labels.append(f"{t(lang, 'guided_terms')}: {_compact_filter_values(guided_subtopics, limit=2)}")

    if bool(st.session_state.get("f_onetech_only", False)):
        labels.append(t(lang, "onetech_only"))
    if bool(st.session_state.get("f_use_actor_groups", False)):
        labels.append(t(lang, "actor_grouping"))
    if bool(st.session_state.get("f_exclude_funders", False)):
        labels.append(t(lang, "exclude_funders"))
    return labels


def render_active_filter_chips(meta: dict, lang: str) -> None:
    labels = active_filter_labels(meta, lang)
    if not labels:
        return
    chips = "".join(
        f"<span class='sir-chip'>{html.escape(label)}</span>"
        for label in labels
    )
    st.markdown(
        f"<div><strong>{html.escape(t(lang, 'active_filters'))}</strong></div><div class='sir-chip-row'>{chips}</div>",
        unsafe_allow_html=True,
    )


def render_search_interpretation(meta: dict, lang: str, *, compact: bool = False) -> None:
    domains = [x for x in st.session_state.get("f_domains_raw", []) if x in meta.get("domains", [])]
    themes = [x for x in st.session_state.get("f_themes_raw", []) if x in meta.get("themes", [])]
    countries = [x for x in st.session_state.get("f_countries", []) if x in meta.get("countries", [])]
    scientific_subthemes = [x for x in st.session_state.get("f_scientific_subthemes", []) if x in meta.get("scientific_subthemes", [])]
    search_txt = str(st.session_state.get("f_quick_search", "")).strip()
    intent = str(st.session_state.get("guided_intent_active", st.session_state.get("guided_intent", "projects")) or "projects")
    all_label = "Tous" if lang == "FR" else "All"
    scope_bits = [
        f"{t(lang, 'period')}: {int(st.session_state['f_years'][0])}-{int(st.session_state['f_years'][1])}",
        f"{t(lang, 'domains')}: {_compact_filter_values(domains, lambda x: domain_raw_to_display(x, lang)) or all_label}",
        f"{t(lang, 'themes')}: {_compact_filter_values(themes, lambda x: theme_raw_to_display(x, lang)) or all_label}",
        f"{t(lang, 'countries')}: {_compact_filter_values(countries, country_raw_to_display) or all_label}",
    ]
    if scientific_subthemes:
        scope_bits.append(
            f"{t(lang, 'scientific_subthemes')}: {_compact_filter_values(scientific_subthemes, limit=3)}"
        )
    if st.session_state.get("f_guided_subtopics"):
        scope_bits.append(
            f"{t(lang, 'guided_terms')}: {_compact_filter_values(st.session_state.get('f_guided_subtopics', []), limit=3)}"
        )
    ctx = st.container(border=(not compact))
    with ctx:
        st.markdown("**" + t(lang, "search_interpretation_title") + "**")
        st.caption(t(lang, "search_interpretation_caption"))
        st.write(f"**{t(lang, 'search_interpretation_intent')}**: {guided_intent_title(lang, intent)}")
        st.write(f"**{t(lang, 'search_interpretation_scope')}**: " + " · ".join(scope_bits))
        st.write(
            f"**{t(lang, 'search_interpretation_search')}**: "
            + (search_txt if search_txt else t(lang, "search_interpretation_none"))
        )


def render_empty_state(lang: str) -> None:
    st.warning(t(lang, "no_results_title"))
    st.caption(t(lang, "no_results_hint"))
    st.caption(t(lang, "no_results_broaden"))
    c1, c2 = st.columns(2)
    with c1:
        has_search = bool(str(st.session_state.get("f_quick_search", "")).strip())
        st.button(
            t(lang, "no_results_clear_search"),
            key="empty_state_clear_search",
            on_click=clear_search,
            disabled=not has_search,
            width="stretch",
        )
    with c2:
        st.button(t(lang, "no_results_reset"), key="empty_state_reset", on_click=reset_filters, width="stretch")


def render_section_header(icon: str, title: str, desc: str = "", eyebrow: str = "") -> None:
    eyebrow_html = (
        f"<div class='sir-section-head__eyebrow'>{html.escape(eyebrow)}</div>"
        if str(eyebrow).strip()
        else ""
    )
    desc_html = (
        f"<p class='sir-section-head__desc'>{html.escape(desc)}</p>"
        if str(desc).strip()
        else ""
    )
    st.markdown(
        (
            "<div class='sir-section-head'>"
            "<div class='sir-section-head__row'>"
            f"<div class='sir-section-head__icon'>{html.escape(icon)}</div>"
            "<div class='sir-section-head__content'>"
            f"{eyebrow_html}"
            f"<h3 class='sir-section-head__title'>{html.escape(title)}</h3>"
            f"{desc_html}"
            "</div></div></div>"
        ),
        unsafe_allow_html=True,
    )


SAFE_VIEW_EXCEPTIONS = (duckdb.Error, ValueError, KeyError, IndexError, TypeError)
PLOTLY_RENDER_EXCEPTIONS = (ValueError, TypeError, RuntimeError)


def render_view_warning(lang: str, warning_key: str) -> None:
    st.warning(t(lang, warning_key))
    st.caption(t(lang, "view_recover_hint"))


def render_guided_empty_state(lang: str, hint_key: str) -> None:
    st.info(t(lang, "no_data"))
    st.caption(t(lang, hint_key))


def render_guided_message(lang: str, message: str, hint_key: str) -> None:
    st.info(message)
    st.caption(t(lang, hint_key))


def safe_fetch_df(sql: str, *, columns: List[str], lang: str, warning_key: str) -> pd.DataFrame:
    try:
        return fetch_df(sql)
    except duckdb.Error:
        render_view_warning(lang, warning_key)
        return pd.DataFrame(columns=columns)


def safe_fetch_df_quiet(sql: str, *, columns: List[str]) -> Tuple[pd.DataFrame, bool]:
    try:
        return fetch_df(sql), False
    except duckdb.Error:
        return pd.DataFrame(columns=columns), True


def render_plotly_chart(fig: go.Figure, **kwargs):
    kwargs.setdefault("theme", None)
    try:
        return st.plotly_chart(fig, **kwargs)
    except PLOTLY_RENDER_EXCEPTIONS:
        current_lang = str(globals().get("lang", "EN"))
        st.warning(t(current_lang, "chart_render_unavailable"))
        st.caption(t(current_lang, "view_recover_hint"))
        return None


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
    if os.getenv("STREAMLIT_SHARING_MODE"):
        return True
    if os.getenv("IS_STREAMLIT_CLOUD") == "1":
        return True
    base = str(BASE_DIR).replace("\\", "/").lower()
    if base.startswith("/mount/src/") or ("/mount/src/" in base):
        return True
    if os.getenv("SUBSIDY_RADAR_CLOUD") == "1":
        return True
    return False


def european_countries_present(countries: List[str]) -> List[str]:
    return _country_values_matching(countries, EUROPE_DEFAULT_COUNTRIES)


def eu27_countries_present(countries: List[str]) -> List[str]:
    return _country_values_matching(countries, EU27_COUNTRIES)


def associated_countries_present(countries: List[str]) -> List[str]:
    return _country_values_matching(countries, ASSOCIATED_COUNTRIES_HORIZON_EUROPE)


@st.cache_data(show_spinner=False)
def current_git_sha() -> str:
    try:
        res = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            check=True,
            timeout=2,
        )
        return (res.stdout or "").strip() or "—"
    except Exception:
        return "—"


@st.cache_data(show_spinner=False)
def github_actions_refresh_url() -> str:
    try:
        res = subprocess.run(
            ["git", "config", "--get", "remote.origin.url"],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            check=True,
            timeout=2,
        )
        raw = (res.stdout or "").strip()
        if not raw:
            return ""
        if raw.startswith("git@github.com:"):
            base = "https://github.com/" + raw[len("git@github.com:"):]
        else:
            base = raw
        if base.endswith(".git"):
            base = base[:-4]
        if not base.startswith("https://github.com/"):
            return ""
        return base.rstrip("/") + "/actions/workflows/refresh-data.yml"
    except Exception:
        return ""


def reset_filters() -> None:
    for k in list(st.session_state.keys()):
        if k.startswith("f_") or k.startswith("macro_"):
            del st.session_state[k]


def _default_countries_from_meta(meta: dict) -> List[str]:
    eu_default = european_countries_present(meta["countries"])
    return eu_default if eu_default else list(meta["countries"])


def _default_statuses_from_meta(meta: dict) -> List[str]:
    default_statuses = [s for s in ["Open", "Closed", "Unknown"] if s in meta["statuses"]]
    return default_statuses if default_statuses else list(meta["statuses"])


def sync_guided_entry_from_filters(meta: dict) -> None:
    default_countries = _default_countries_from_meta(meta)
    current_domains = [x for x in st.session_state.get("f_domains_raw", []) if x in meta["domains"]]
    current_themes = [x for x in st.session_state.get("f_themes_raw", []) if x in meta["themes"]]
    current_countries = [x for x in st.session_state.get("f_countries", []) if x in meta["countries"]]
    current_years = tuple(st.session_state.get("f_years", (meta["miny"], meta["maxy"])))
    if len(current_years) != 2:
        current_years = (meta["miny"], meta["maxy"])

    st.session_state["guided_search"] = str(st.session_state.get("f_quick_search", ""))
    st.session_state["guided_themes_raw"] = [] if set(current_domains) == set(meta["domains"]) else current_domains
    st.session_state["guided_countries"] = current_countries or default_countries
    st.session_state["guided_countries_widget"] = list(st.session_state["guided_countries"])
    st.session_state["guided_years"] = current_years
    st.session_state["guided_subtopics_by_theme"] = {
        theme: values
        for theme, values in _clean_guided_subtopics_by_theme().items()
        if theme in st.session_state["guided_themes_raw"]
    }
    st.session_state["guided_subtopics"] = _selected_guided_subtopics(st.session_state["guided_themes_raw"])
    selected_set = set(st.session_state["guided_themes_raw"])
    for theme in meta.get("domains", []):
        st.session_state[f"guided_theme_selected::{theme}"] = theme in selected_set


def apply_guided_entry_to_filters(meta: dict) -> None:
    default_countries = _default_countries_from_meta(meta)
    default_statuses = _default_statuses_from_meta(meta)
    guided_domains = [x for x in st.session_state.get("guided_themes_raw", []) if x in meta["domains"]]
    guided_countries = [x for x in st.session_state.get("guided_countries_widget", st.session_state.get("guided_countries", [])) if x in meta["countries"]]
    st.session_state["guided_countries"] = guided_countries
    guided_years = tuple(st.session_state.get("guided_years", (meta["miny"], meta["maxy"])))
    if len(guided_years) != 2:
        guided_years = (meta["miny"], meta["maxy"])

    selected_subtopics = _selected_guided_subtopics(guided_domains)
    guided_topic_terms: List[str] = []
    for subtopic in selected_subtopics:
        for term in _subtopic_search_terms(subtopic):
            if term not in guided_topic_terms:
                guided_topic_terms.append(term)

    selected_primary_themes: List[str] = []
    if guided_domains:
        for domain in guided_domains:
            for theme in meta.get("themes_by_domain", {}).get(domain, []):
                if theme not in selected_primary_themes:
                    selected_primary_themes.append(theme)
    else:
        selected_primary_themes = list(meta["themes"])

    st.session_state["f_quick_search"] = str(st.session_state.get("guided_search", "")).strip()
    st.session_state["f_domains_raw"] = guided_domains if guided_domains else list(meta["domains"])
    st.session_state["f_themes_raw"] = selected_primary_themes if selected_primary_themes else list(meta["themes"])
    st.session_state["f_countries"] = guided_countries if guided_countries else default_countries
    st.session_state["f_years"] = guided_years
    st.session_state["f_guided_subtopics"] = selected_subtopics
    st.session_state["f_guided_topic_terms"] = guided_topic_terms
    st.session_state["f_scientific_subthemes"] = []
    st.session_state["f_sources"] = list(meta["sources"])
    st.session_state["f_programmes"] = list(meta["programmes"])
    st.session_state["f_statuses"] = default_statuses
    st.session_state["f_entity_raw"] = list(meta["entities"])
    st.session_state["f_onetech_only"] = False
    st.session_state["f_use_actor_groups"] = False
    st.session_state["f_exclude_funders"] = True


def clear_search() -> None:
    st.session_state["f_quick_search"] = ""


def _clean_guided_subtopics_by_theme() -> Dict[str, List[str]]:
    raw = st.session_state.get("guided_subtopics_by_theme", {})
    if not isinstance(raw, dict):
        return {}
    cleaned: Dict[str, List[str]] = {}
    for theme, subtopics in raw.items():
        theme_key = str(theme)
        allowed = [str(x) for x in GUIDED_DOMAIN_SUBCATEGORIES.get(theme_key, [])]
        if not allowed:
            continue
        values: List[str] = []
        if isinstance(subtopics, (list, tuple, set)):
            for item in subtopics:
                value = str(item).strip()
                if value and value in allowed and value not in values:
                    values.append(value)
        if values:
            cleaned[theme_key] = values
    return cleaned


def _selected_guided_subtopics(selected_themes: Optional[List[str]] = None) -> List[str]:
    subtopic_map = _clean_guided_subtopics_by_theme()
    theme_order = [str(x) for x in (selected_themes or st.session_state.get("guided_themes_raw", []))]
    selected: List[str] = []
    for theme in theme_order:
        for subtopic in subtopic_map.get(theme, []):
            if subtopic not in selected:
                selected.append(subtopic)
    return selected


def _subtopic_search_terms(subtopic: str) -> List[str]:
    terms: List[str] = []

    def add(term: str) -> None:
        cleaned = re.sub(r"\s+", " ", str(term or "").strip())
        if cleaned and cleaned.lower() not in {x.lower() for x in terms}:
            terms.append(cleaned)

    raw = str(subtopic).strip()
    add(raw)
    base = re.sub(r"\s*\([^)]*\)", "", raw).strip(" ,-")
    if base and base != raw:
        add(base)
    for inner in re.findall(r"\(([^)]{1,30})\)", raw):
        add(inner)
    for part in re.split(r"\s*&\s*|\s*/\s*", base):
        if len(part.strip()) >= 3:
            add(part.strip())
    for alias in SUBTOPIC_TERM_OVERRIDES.get(raw, []):
        add(alias)
    return terms


def toggle_guided_theme(theme: str) -> None:
    current = [x for x in st.session_state.get("guided_themes_raw", []) if str(x).strip()]
    theme = str(theme)
    subtopic_map = _clean_guided_subtopics_by_theme()
    if theme in current:
        current = [x for x in current if x != theme]
        subtopic_map.pop(theme, None)
    else:
        current.append(theme)
    st.session_state["guided_themes_raw"] = current
    st.session_state["guided_subtopics_by_theme"] = subtopic_map
    st.session_state["guided_subtopics"] = _selected_guided_subtopics(current)


def queue_tab_navigation(
    top_target: str = "",
    actor_sub_target: str = "",
    trends_sub_target: str = "",
    advanced_sub_target: str = "",
) -> None:
    st.session_state["nav_target_top"] = str(top_target or "")
    st.session_state["nav_target_actor_sub"] = str(actor_sub_target or "")
    st.session_state["nav_target_trends_sub"] = str(trends_sub_target or "")
    st.session_state["nav_target_advanced_sub"] = str(advanced_sub_target or "")


def sync_results_table_state(scope_token: str) -> None:
    allowed_rows = [25, 50, 100, 250]
    if st.session_state.get("results_table_rows_per_page") not in allowed_rows:
        st.session_state["results_table_rows_per_page"] = 100
    if int(st.session_state.get("results_table_page", 1)) < 1:
        st.session_state["results_table_page"] = 1
    if st.session_state.get("results_table_scope_token") != str(scope_token):
        st.session_state["results_table_scope_token"] = str(scope_token)
        st.session_state["results_table_page"] = 1
        st.session_state.pop("results_table_full_export_bytes", None)
        st.session_state["results_table_full_export_query_key"] = ""
        st.session_state.pop("results_selected_project_id", None)
        st.session_state["results_selected_project_view_token"] = ""
        st.session_state.pop("results_selected_actor_id_candidate", None)
        st.session_state.pop("results_drilldown_actor_id", None)
        st.session_state.pop("results_project_table_df", None)


def build_results_scope_summary(
    relation_sql: str,
    where_sql: str,
    *,
    lang: str,
    total_budget: float,
    n_projects: int,
    n_actors: int,
) -> dict:
    fallback = {
        "headline": t(lang, "results_summary_fallback").format(
            projects=f"{int(n_projects):,}".replace(",", " "),
            budget=fmt_money(float(total_budget), lang),
            actors=f"{int(n_actors):,}".replace(",", " "),
        ),
        "detail": "",
        "primary_view": "trend",
        "n_countries": 0,
    }
    try:
        shape = fetch_df(f"""
        SELECT
          COUNT(DISTINCT country_name) AS n_countries,
          COUNT(DISTINCT theme) AS n_themes,
          COUNT(DISTINCT year) AS n_years
        FROM {relation_sql}
        WHERE {where_sql}
        """)
        if shape.empty:
            return fallback

        n_countries = int(shape["n_countries"].iloc[0] or 0)
        n_themes = int(shape["n_themes"].iloc[0] or 0)
        n_years = int(shape["n_years"].iloc[0] or 0)

        top_country = ""
        top_actor = ""
        top_theme = ""

        if n_countries > 0:
            top_country_df = fetch_df(f"""
            SELECT country_name, SUM(amount_eur) AS budget_eur
            FROM {relation_sql}
            WHERE {where_sql} AND country_name IS NOT NULL AND TRIM(country_name) <> ''
            GROUP BY country_name
            ORDER BY budget_eur DESC
            LIMIT 1
            """)
            if not top_country_df.empty:
                top_country = str(top_country_df["country_name"].iloc[0] or "").strip()

        top_actor_df = fetch_df(f"""
        SELECT COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label, SUM(amount_eur) AS budget_eur
        FROM {relation_sql}
        WHERE {where_sql} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
        GROUP BY actor_label
        ORDER BY budget_eur DESC
        LIMIT 1
        """)
        if not top_actor_df.empty:
            top_actor = str(top_actor_df["actor_label"].iloc[0] or "").strip()

        top_theme_df = fetch_df(f"""
        -- Current build stores one inferred theme label per row/project view.
        SELECT theme, SUM(amount_eur) AS budget_eur
        FROM {relation_sql}
        WHERE {where_sql} AND theme IS NOT NULL AND TRIM(theme) <> ''
        GROUP BY theme
        ORDER BY budget_eur DESC
        LIMIT 1
        """)
        if not top_theme_df.empty:
            top_theme = theme_raw_to_display(str(top_theme_df["theme"].iloc[0] or "").strip(), lang)

        if n_countries <= 1 and top_country:
            headline = t(lang, "results_summary_headline_single_country").format(
                projects=f"{int(n_projects):,}".replace(",", " "),
                budget=fmt_money(float(total_budget), lang),
                actors=f"{int(n_actors):,}".replace(",", " "),
                country=top_country,
            )
        else:
            headline = t(lang, "results_summary_headline").format(
                projects=f"{int(n_projects):,}".replace(",", " "),
                budget=fmt_money(float(total_budget), lang),
                actors=f"{int(n_actors):,}".replace(",", " "),
                countries=f"{int(n_countries):,}".replace(",", " "),
            )

        detail_parts = []
        if n_countries > 1 and top_country:
            detail_parts.append(t(lang, "results_summary_country_lead").format(country=top_country))
        if top_actor:
            detail_parts.append(t(lang, "results_summary_actor_lead").format(actor=top_actor))
        if n_themes > 1 and top_theme:
            detail_parts.append(t(lang, "results_summary_theme_lead").format(theme=top_theme))

        primary_view = "trend" if n_years > 1 else ("countries" if n_countries > 1 else "actors")
        return {
            "headline": headline,
            "detail": " ".join(detail_parts).strip(),
            "primary_view": primary_view,
            "n_countries": n_countries,
        }
    except duckdb.Error:
        return fallback


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
def load_actor_group_tables() -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Optional CSV mapping to group legal entities under one parent (PIC/group).
    Expected columns (flexible names): actor_id OR pic, plus group_id/group_name and optional is_funder.
    """
    mapping_path = ACTOR_GROUPS_PATH if ACTOR_GROUPS_PATH.exists() else (
        ACTOR_GROUPS_TEMPLATE_PATH if ACTOR_GROUPS_TEMPLATE_PATH.exists() else None
    )
    if mapping_path is None:
        return _empty_actor_map_actor(), _empty_actor_map_pic(), ""

    try:
        raw = pd.read_csv(mapping_path, dtype=str).fillna("")
    except Exception:
        return _empty_actor_map_actor(), _empty_actor_map_pic(), ""

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
        return _empty_actor_map_actor(), _empty_actor_map_pic(), mapping_path.name

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

    return by_actor, by_pic, mapping_path.name


# ============================================================
# DuckDB engine (critical: no full pandas load)
# ============================================================
@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")
    return con


def _build_domain_case_sql(theme_expr: str) -> str:
    cases = []
    for legacy_theme, domain in LEGACY_THEME_TO_DOMAIN_UI.items():
        safe_theme = str(legacy_theme).replace("'", "''")
        safe_domain = str(domain).replace("'", "''")
        cases.append(f"WHEN {theme_expr} = '{safe_theme}' THEN '{safe_domain}'")
    return "CASE " + " ".join(cases) + " ELSE '' END"


def _ensure_base_view() -> None:
    """
    Create (or replace) DuckDB compatibility views over the processed parquet.

    subsidy_base:
      - normalizes country labels / alpha3
      - exposes official CORDIS columns even on older parquet versions
      - maps `theme` to `cordis_theme_primary` for backward compatibility
      - preserves `legacy_theme` / `legacy_sub_theme`

    project_scientific_subthemes_view:
      - official exploded project x scientific_subtheme table if available
      - safe fallback from `sub_theme` otherwise
    """
    con = get_con()
    raw = f"read_parquet('{PARQUET_PATH.as_posix()}')"
    raw_cols = {
        str(c)
        for c in con.execute(f"SELECT * FROM {raw} LIMIT 0").fetchdf().columns
    }

    # Build mapping table with both full name and alpha3
    rows = []
    all_codes = set(COUNTRY_CODE_TO_NAME.keys()) | set(COUNTRY_CODE_TO_ALPHA3.keys())
    for code in sorted(all_codes):
        name = COUNTRY_CODE_TO_NAME.get(code, code)
        a3 = COUNTRY_CODE_TO_ALPHA3.get(code, code)
        safe_code = code.replace("'", "''")
        safe_name = name.replace("'", "''")
        safe_a3 = a3.replace("'", "''")
        rows.append(f"('{safe_code}', '{safe_name}', '{safe_a3}')")
    values = ", ".join(rows)

    con.execute("DROP TABLE IF EXISTS _country_map;")
    con.execute("CREATE TABLE _country_map (code VARCHAR, full_name VARCHAR, alpha3 VARCHAR);")
    con.execute(f"INSERT INTO _country_map VALUES {values};")

    def text_col(name: str) -> str:
        if name not in raw_cols:
            return "''"
        return f"TRIM(COALESCE(b.{name}, ''))"

    legacy_theme_expr = f"COALESCE(NULLIF({text_col('legacy_theme')}, ''), NULLIF({text_col('theme')}, ''), '')"
    legacy_sub_theme_expr = f"COALESCE(NULLIF({text_col('legacy_sub_theme')}, ''), NULLIF({text_col('sub_theme')}, ''), '')"
    cordis_theme_primary_expr = (
        "COALESCE("
        f"NULLIF({text_col('cordis_theme_primary')}, ''), "
        f"NULLIF({text_col('theme')}, ''), "
        f"NULLIF({text_col('section')}, ''), "
        f"NULLIF({text_col('program')}, ''), "
        "'')"
    )
    cordis_theme_primary_source_expr = (
        "COALESCE("
        f"NULLIF({text_col('cordis_theme_primary_source')}, ''), "
        f"CASE WHEN NULLIF({text_col('theme')}, '') IS NOT NULL THEN 'legacy_theme' ELSE 'fallback' END"
        ")"
    )
    sub_theme_expr = (
        "COALESCE("
        f"NULLIF({text_col('sub_theme')}, ''), "
        f"NULLIF(json_extract_string(COALESCE({text_col('scientific_subthemes')}, '[]'), '$[0]'), ''), "
        "''"
        ")"
        if "scientific_subthemes" in raw_cols
        else f"COALESCE(NULLIF({text_col('sub_theme')}, ''), '')"
    )
    scientific_subthemes_expr = (
        f"COALESCE(NULLIF({text_col('scientific_subthemes')}, ''), "
        f"CASE WHEN {sub_theme_expr} <> '' THEN '[\"' || replace({sub_theme_expr}, '\"', '\\\\\"') || '\"]' ELSE '[]' END)"
    )
    scientific_subthemes_count_expr = (
        f"COALESCE(TRY_CAST(NULLIF({text_col('scientific_subthemes_count')}, '') AS INTEGER), "
        f"CASE WHEN {sub_theme_expr} <> '' THEN 1 ELSE 0 END)"
        if "scientific_subthemes_count" in raw_cols
        else f"CASE WHEN {sub_theme_expr} <> '' THEN 1 ELSE 0 END"
    )
    cordis_domain_ui_expr = (
        "COALESCE("
        f"NULLIF({text_col('cordis_domain_ui')}, ''), "
        f"NULLIF({_build_domain_case_sql(legacy_theme_expr)}, ''), "
        "'')"
    )
    cordis_topic_primary_expr = (
        "COALESCE("
        f"NULLIF({text_col('cordis_topic_primary')}, ''), "
        f"NULLIF({text_col('topic')}, ''), "
        "''"
        ")"
    )
    cordis_topics_all_expr = (
        "COALESCE("
        f"NULLIF({text_col('cordis_topics_all')}, ''), "
        f"NULLIF({text_col('topics')}, ''), "
        f"CASE WHEN NULLIF({text_col('topic')}, '') IS NOT NULL THEN '[\"' || replace({text_col('topic')}, '\"', '\\\\\"') || '\"]' ELSE '[]' END"
        ")"
    )
    cordis_call_expr = (
        "COALESCE("
        f"NULLIF({text_col('cordis_call')}, ''), "
        f"NULLIF({text_col('call')}, ''), "
        f"NULLIF({text_col('subCall')}, ''), "
        f"NULLIF({text_col('masterCall')}, ''), "
        "'')"
    )
    cordis_framework_programme_expr = (
        "COALESCE("
        f"NULLIF({text_col('cordis_framework_programme')}, ''), "
        f"NULLIF({text_col('frameworkProgramme')}, ''), "
        f"NULLIF({text_col('program')}, ''), "
        "'')"
    )
    country_name_expr = f"COALESCE(cn.full_name, ca3.full_name, {text_col('country_name')})"
    country_alpha3_expr = f"COALESCE(cn.alpha3, ca3.alpha3, {text_col('country_alpha3')})"

    replace_exprs: List[str] = []
    extra_exprs: List[str] = []

    def upsert_expr(name: str, expr: str) -> None:
        target = replace_exprs if name in raw_cols else extra_exprs
        target.append(f"({expr}) AS {name}")

    upsert_expr("country_name", country_name_expr)
    upsert_expr("country_alpha3", country_alpha3_expr)
    upsert_expr("legacy_theme", legacy_theme_expr)
    upsert_expr("legacy_sub_theme", legacy_sub_theme_expr)
    upsert_expr("cordis_domain_ui", cordis_domain_ui_expr)
    upsert_expr("cordis_theme_primary", cordis_theme_primary_expr)
    upsert_expr("cordis_theme_primary_source", cordis_theme_primary_source_expr)
    upsert_expr("cordis_topic_primary", cordis_topic_primary_expr)
    upsert_expr("cordis_topics_all", cordis_topics_all_expr)
    upsert_expr("cordis_call", cordis_call_expr)
    upsert_expr("cordis_framework_programme", cordis_framework_programme_expr)
    upsert_expr("scientific_subthemes", scientific_subthemes_expr)
    upsert_expr("scientific_subthemes_count", scientific_subthemes_count_expr)
    upsert_expr("theme", cordis_theme_primary_expr)
    upsert_expr("sub_theme", sub_theme_expr)

    base_select = "b.*"
    if replace_exprs:
        base_select += " REPLACE(" + ", ".join(replace_exprs) + ")"

    con.execute("DROP VIEW IF EXISTS subsidy_base;")
    con.execute(f"""
        CREATE VIEW subsidy_base AS
        SELECT
            {base_select}
            {", " if extra_exprs else ""}{", ".join(extra_exprs)}
        FROM {raw} b
        LEFT JOIN _country_map cn
            ON UPPER({text_col('country_name')}) = cn.code
        LEFT JOIN _country_map ca3
            ON UPPER({text_col('country_alpha3')}) = ca3.code
        WHERE UPPER(COALESCE(b.source, '')) <> 'ADEME'
          AND UPPER(COALESCE(b.program, '')) NOT LIKE '%ADEME%'
    """)

    con.execute("DROP VIEW IF EXISTS project_scientific_subthemes_view;")
    if SCIENTIFIC_SUBTHEMES_PARQUET_PATH.exists():
        subthemes_raw = f"read_parquet('{SCIENTIFIC_SUBTHEMES_PARQUET_PATH.as_posix()}')"
        con.execute(f"""
            CREATE VIEW project_scientific_subthemes_view AS
            SELECT
              TRIM(COALESCE(projectID, '')) AS projectID,
              TRIM(COALESCE(cordis_domain_ui, '')) AS cordis_domain_ui,
              TRIM(COALESCE(cordis_theme_primary, '')) AS cordis_theme_primary,
              TRIM(COALESCE(subtheme_level_1, '')) AS subtheme_level_1,
              TRIM(COALESCE(subtheme_level_2, '')) AS subtheme_level_2,
              TRIM(COALESCE(subtheme_level_3, '')) AS subtheme_level_3,
              TRIM(COALESCE(subtheme_label, '')) AS subtheme_label,
              TRIM(COALESCE(subtheme_path, '')) AS subtheme_path,
              TRIM(COALESCE(source_method, '')) AS source_method
            FROM {subthemes_raw}
            WHERE TRIM(COALESCE(projectID, '')) <> ''
              AND TRIM(COALESCE(subtheme_label, '')) <> ''
        """)
    else:
        con.execute("""
            CREATE VIEW project_scientific_subthemes_view AS
            SELECT DISTINCT
              projectID,
              cordis_domain_ui,
              cordis_theme_primary,
              cordis_domain_ui AS subtheme_level_1,
              '' AS subtheme_level_2,
              sub_theme AS subtheme_level_3,
              sub_theme AS subtheme_label,
              CASE WHEN sub_theme <> '' THEN cordis_domain_ui || ' > ' || sub_theme ELSE '' END AS subtheme_path,
              'legacy_sub_theme' AS source_method
            FROM subsidy_base
            WHERE TRIM(COALESCE(projectID, '')) <> ''
              AND TRIM(COALESCE(sub_theme, '')) <> ''
        """)


# Track whether the view has been created this session
_BASE_VIEW_READY = False


def rel() -> str:
    global _BASE_VIEW_READY
    if not _BASE_VIEW_READY:
        _ensure_base_view()
        _BASE_VIEW_READY = True
    return "subsidy_base"


def scientific_subthemes_rel() -> str:
    global _BASE_VIEW_READY
    if not _BASE_VIEW_READY:
        _ensure_base_view()
        _BASE_VIEW_READY = True
    return "project_scientific_subthemes_view"


@st.cache_data(show_spinner=False)
def base_schema_columns(_cache_version: str = "v7_cordis_schema") -> List[str]:
    df = get_con().execute(f"SELECT * FROM {rel()} LIMIT 0").fetchdf()
    return [str(c) for c in df.columns]


def register_actor_group_tables() -> Dict[str, int]:
    by_actor, by_pic, source_name = load_actor_group_tables()

    con = get_con()
    con.register("actor_groups_by_actor", by_actor if not by_actor.empty else _empty_actor_map_actor())
    con.register("actor_groups_by_pic", by_pic if not by_pic.empty else _empty_actor_map_pic())

    return {
        "available": bool((not by_actor.empty) or (not by_pic.empty)),
        "rows_actor": int(len(by_actor)),
        "rows_pic": int(len(by_pic)),
        "source": source_name,
    }


@st.cache_data(show_spinner=False)
def actor_group_match_stats() -> Dict[str, int]:
    base = rel()
    df = fetch_df(f"""
    WITH b AS (
      SELECT DISTINCT actor_id
      FROM {base}
      WHERE actor_id IS NOT NULL AND TRIM(actor_id) <> ''
    )
    SELECT
      COUNT(*) AS total_actors,
      COUNT(*) FILTER (
        WHERE ga.actor_id IS NOT NULL
           OR gp.pic IS NOT NULL
      ) AS matched_actors
    FROM b
    LEFT JOIN actor_groups_by_actor ga ON b.actor_id = ga.actor_id
    LEFT JOIN actor_groups_by_pic gp
      ON (ga.actor_id IS NULL AND regexp_extract(b.actor_id, '([0-9]{{8,10}})$', 1) = gp.pic)
    """)
    if df.empty:
        return {"total_actors": 0, "matched_actors": 0}
    return {
        "total_actors": int(df["total_actors"].iloc[0] or 0),
        "matched_actors": int(df["matched_actors"].iloc[0] or 0),
    }


@st.cache_data(show_spinner=False)
def actor_group_key_match_stats() -> Dict[str, int]:
    base = rel()
    df = fetch_df(f"""
    WITH b AS (
      SELECT DISTINCT
        actor_id,
        regexp_extract(actor_id, '([0-9]{{8,10}})$', 1) AS actor_pic
      FROM {base}
      WHERE actor_id IS NOT NULL AND TRIM(actor_id) <> ''
    ),
    a AS (
      SELECT
        COUNT(*) AS total_keys,
        COUNT(*) FILTER (WHERE b.actor_id IS NOT NULL) AS matched_keys
      FROM actor_groups_by_actor ga
      LEFT JOIN b ON ga.actor_id = b.actor_id
      WHERE ga.actor_id IS NOT NULL AND TRIM(ga.actor_id) <> ''
    ),
    p AS (
      SELECT
        COUNT(*) AS total_keys,
        COUNT(*) FILTER (WHERE b.actor_pic IS NOT NULL AND TRIM(b.actor_pic) <> '') AS matched_keys
      FROM actor_groups_by_pic gp
      LEFT JOIN b ON gp.pic = b.actor_pic
      WHERE gp.pic IS NOT NULL AND TRIM(gp.pic) <> ''
    )
    SELECT
      COALESCE(a.total_keys, 0) + COALESCE(p.total_keys, 0) AS total_keys,
      COALESCE(a.matched_keys, 0) + COALESCE(p.matched_keys, 0) AS matched_keys
    FROM a, p
    """)
    if df.empty:
        return {"total_keys": 0, "matched_keys": 0}
    return {
        "total_keys": int(df["total_keys"].iloc[0] or 0),
        "matched_keys": int(df["matched_keys"].iloc[0] or 0),
    }


def rel_analytics(use_actor_groups: bool, exclude_funders: bool) -> str:
    base = rel()
    cols = set(base_schema_columns())
    pic_expr = "b.pic" if "pic" in cols else "regexp_extract(b.actor_id, '([0-9]{8,10})$', 1)"
    status_expr = "b.project_status" if "project_status" in cols else "'Unknown'"
    stage_blob_expr = (
        "lower(coalesce(b.title,'') || ' ' || coalesce(b.objective,'') || ' ' || "
        "coalesce(b.abstract,'') || ' ' || coalesce(b.legacy_theme,'') || ' ' || coalesce(b.section,''))"
    )
    stage_fallback_expr = (
        "CASE "
        f"WHEN regexp_matches({stage_blob_expr}, '(critical raw material|raw material|lithium|nickel|cobalt|mining|refining|feedstock|biomass|recycling|supply chain|precursor)') THEN 'Resources & feedstock' "
        f"WHEN regexp_matches({stage_blob_expr}, '(electrolyser|electrolyzer|fuel cell|reactor|stack|module|battery|turbine|membrane|electrode|catalyst|converter|inverter|component|subsystem)') THEN 'Components & core technology' "
        f"WHEN regexp_matches({stage_blob_expr}, '(grid|microgrid|pipeline|network|charging|charging station|storage system|integration|interoperability|hub|terminal|facility|plant|district heating|infrastructure|platform)') THEN 'Systems & infrastructure' "
        f"WHEN regexp_matches({stage_blob_expr}, '(pilot|demonstration|demo|deployment|operation|operations|industrialisation|industrialization|scale-up|scale up|roll-out|roll out|commissioning|field trial|validation|first-of-a-kind|foak|maintenance|trl 6|trl 7|trl 8)') THEN 'Deployment & operations' "
        f"WHEN regexp_matches({stage_blob_expr}, '(market uptake|market adoption|end-user|end user|customer|offtake|commercialisation|commercialization|procurement|go-to-market|go to market|mobility|aviation|shipping|manufacturing|trl 9)') THEN 'End-use & market' "
        "WHEN lower(coalesce(b.legacy_theme,'')) IN ('e-mobility', 'transport & aviation') THEN 'End-use & market' "
        "WHEN lower(coalesce(b.legacy_theme,'')) IN ('ai & digital', 'advanced materials', 'health & biotech') THEN 'Components & core technology' "
        "WHEN lower(coalesce(b.legacy_theme,'')) IN ('hydrogen (h2)', 'solar (pv/csp)', 'wind', 'bioenergy & saf', 'ccus', 'nuclear & smr', 'batteries & storage') THEN 'Systems & infrastructure' "
        "ELSE 'Research & concept' END"
    )
    if "value_chain_stage" in cols:
        stage_expr = (
            "CASE "
            "WHEN b.value_chain_stage IS NULL OR TRIM(b.value_chain_stage) = '' "
            "  OR lower(TRIM(b.value_chain_stage)) IN ('unknown','unspecified') "
            f"THEN {stage_fallback_expr} "
            "ELSE b.value_chain_stage END"
        )
    else:
        stage_expr = stage_fallback_expr

    actor_expr = (
        f"COALESCE(NULLIF(TRIM(ga.group_id), ''), NULLIF(TRIM(gp.group_id), ''), "
        f"CASE WHEN {pic_expr} IS NULL OR TRIM({pic_expr}) = '' THEN NULL ELSE CONCAT('PIC:', TRIM({pic_expr})) END, b.actor_id)"
        if use_actor_groups
        else "b.actor_id"
    )
    org_expr = (
        "COALESCE(NULLIF(TRIM(ga.group_name), ''), NULLIF(TRIM(gp.group_name), ''), b.org_name)"
        if use_actor_groups
        else "b.org_name"
    )
    heuristic_funder_expr = (
        "regexp_matches(lower(COALESCE(b.org_name, '')), "
        "'(\\beit\\b|\\bcinea\\b|\\beismea\\b|\\bhadea\\b|\\beuropean commission\\b|"
        "joint undertaking|executive agency|innovation fund|\\berc\\b|\\beic\\b)')"
    )
    funder_expr = f"(COALESCE(ga.is_funder, gp.is_funder, FALSE) OR {heuristic_funder_expr})"
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
        b.keywords,
        {actor_expr} AS actor_id,
        {pic_expr} AS pic,
        {org_expr} AS org_name,
        b.entity_type,
        b.country_alpha2,
        b.country_alpha3,
        b.country_name,
        b.amount_eur,
        b.cordis_domain_ui,
        b.cordis_theme_primary,
        b.cordis_theme_primary_source,
        b.cordis_topic_primary,
        b.cordis_topics_all,
        b.cordis_call,
        b.cordis_framework_programme,
        b.scientific_subthemes,
        b.scientific_subthemes_count,
        b.legacy_theme,
        b.legacy_sub_theme,
        b.theme,
        b.sub_theme,
        {stage_expr} AS value_chain_stage,
        {status_expr} AS project_status
      FROM {base} b
      LEFT JOIN actor_groups_by_actor ga ON b.actor_id = ga.actor_id
      LEFT JOIN actor_groups_by_pic gp
        ON (ga.actor_id IS NULL AND regexp_extract(b.actor_id, '([0-9]{{8,10}})$', 1) = gp.pic)
      {where_funder}
    )
    """


def _path_mtime_ns(path: Path) -> int:
    try:
        return int(path.stat().st_mtime_ns)
    except Exception:
        return 0


@st.cache_data(show_spinner=False, ttl=1800, max_entries=800)
def _fetch_df_cached(sql: str, data_token: Tuple[int, int, int, int]) -> pd.DataFrame:
    _ = data_token  # cache invalidation token
    return get_con().execute(sql).fetchdf()


def fetch_df(sql: str) -> pd.DataFrame:
    token = (
        _path_mtime_ns(PARQUET_PATH),
        _path_mtime_ns(EVENTS_PATH),
        _path_mtime_ns(ACTOR_GROUPS_PATH),
        _path_mtime_ns(CONNECTORS_MANIFEST_PATH),
    )
    return _fetch_df_cached(sql, token)


def list_str(sql: str) -> List[str]:
    df = fetch_df(sql)
    if df.empty:
        return []
    return [str(x) for x in df.iloc[:, 0].tolist() if str(x).strip()]


@st.cache_data(show_spinner=False)
def base_snapshot_stats() -> Dict[str, float]:
    b = rel()
    q = fetch_df(f"""
    SELECT
      COUNT(*) AS n_rows,
      SUM(amount_eur) AS total_budget,
      COUNT(DISTINCT projectID) AS n_projects,
      COUNT(DISTINCT actor_id) FILTER (WHERE actor_id IS NOT NULL AND TRIM(actor_id) <> '') AS n_actors,
      MIN(year) AS min_year,
      MAX(year) AS max_year
    FROM {b}
    """)
    if q.empty:
        return {"n_rows": 0, "total_budget": 0.0, "n_projects": 0, "n_actors": 0, "min_year": 0, "max_year": 0}
    r = q.iloc[0]
    return {
        "n_rows": int(r.get("n_rows") or 0),
        "total_budget": float(r.get("total_budget") or 0.0),
        "n_projects": int(r.get("n_projects") or 0),
        "n_actors": int(r.get("n_actors") or 0),
        "min_year": int(r.get("min_year") or 0),
        "max_year": int(r.get("max_year") or 0),
    }


@st.cache_data(show_spinner=False)
def events_snapshot_stats() -> Dict[str, int]:
    ev = load_events()
    if ev.empty:
        return {"n_events": 0, "n_ai": 0}
    return {
        "n_events": int(len(ev)),
        "n_ai": int((ev["tag"].astype(str) == "AI").sum()),
    }


def _to_bool_text(x: object) -> bool:
    return str(x).strip().lower() in {"1", "true", "yes", "y", "oui"}


def _extract_env_refs(text: object) -> List[str]:
    return sorted(set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", str(text or ""))))


@st.cache_data(show_spinner=False)
def connectors_snapshot_stats() -> Dict[str, object]:
    out: Dict[str, object] = {
        "manifest_total": 0,
        "manifest_ready": 0,
        "state_count": 0,
        "last_status": "—",
    }
    if CONNECTORS_MANIFEST_PATH.exists():
        try:
            m = pd.read_csv(CONNECTORS_MANIFEST_PATH, dtype=str).fillna("")
            out["manifest_total"] = int(len(m))
            ready = 0
            for _, row in m.iterrows():
                kind = str(row.get("kind", "")).strip().lower()
                url = str(row.get("url", "")).strip()
                req = [x.strip() for x in str(row.get("required_env", "")).split(",") if x.strip()]
                req += _extract_env_refs(row.get("headers_json", ""))
                req += _extract_env_refs(row.get("params_json", ""))
                req += _extract_env_refs(row.get("mcp_command", ""))
                req += _extract_env_refs(url)
                req = sorted(set(req))
                missing = [x for x in req if not str(os.getenv(x, "")).strip()]
                url_ready = (kind == "mcp") or (bool(url) and ("example." not in url.lower()) and ("localhost" not in url.lower()))
                if (len(missing) == 0) and url_ready:
                    ready += 1
            out["manifest_ready"] = int(ready)
        except Exception:
            pass

    if PIPELINE_STATE_PATH.exists():
        try:
            js = json.loads(PIPELINE_STATE_PATH.read_text(encoding="utf-8"))
            ext = js.get("external_connectors", {}) if isinstance(js, dict) else {}
            if isinstance(ext, dict):
                out["state_count"] = int(len(ext))
                if ext:
                    items = sorted(
                        ext.items(),
                        key=lambda kv: float((kv[1] or {}).get("last_run_ts", 0.0) or 0.0),
                        reverse=True,
                    )
                    cid, data = items[0]
                    reason = str((data or {}).get("last_reason", "unknown"))
                    ok = bool((data or {}).get("last_ok", False))
                    out["last_status"] = f"{cid}: {'OK' if ok else 'FAIL'} ({reason})"
        except Exception:
            pass

    return out


@st.cache_data(show_spinner=False)
def events_meta_snapshot() -> Dict[str, object]:
    default_h = 24.0
    try:
        default_h = max(0.0, float(str(os.getenv("SUBSIDY_EVENTS_MIN_REFRESH_HOURS", "24")).strip()))
    except Exception:
        default_h = 24.0
    out: Dict[str, object] = {"min_refresh_hours": default_h, "last_build_utc": "—"}
    if EVENTS_META_PATH.exists():
        try:
            js = json.loads(EVENTS_META_PATH.read_text(encoding="utf-8"))
            out["min_refresh_hours"] = float(js.get("min_refresh_hours", default_h) or default_h)
            out["last_build_utc"] = str(js.get("last_build_utc", "—") or "—")
        except Exception:
            pass
    return out


def in_list(values: List[str]) -> str:
    def _clean(v: str) -> str:
        s = str(v).replace("\x00", "").replace("\r", " ").replace("\n", " ")
        return "".join(ch for ch in s if (ord(ch) >= 32) or (ch == "\t"))

    esc = [_clean(str(v)).replace("'", "''") for v in values if v is not None and _clean(str(v)).strip()]
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
    domains: List[str],
    statuses: List[str],
    themes: List[str],
    subthemes: List[str],
    entities: List[str],
    countries: List[str],
    quick_search: str,
    table_alias: Optional[str] = None,
    quick_search_columns: Optional[List[str]] = None,
    extra_search_terms: Optional[List[str]] = None,
) -> str:
    prefix = f"{str(table_alias).strip()}." if str(table_alias or "").strip() else ""
    w = []
    if sources:
        w.append(f"{prefix}source IN {in_list(sources)}")
    if programmes:
        w.append(f"{prefix}program IN {in_list(programmes)}")
    w.append(f"{prefix}year BETWEEN {int(years[0])} AND {int(years[1])}")
    if use_section and sections:
        w.append(f"{prefix}section IN {in_list(sections)}")
    if onetech_only:
        w.append(f"{prefix}legacy_theme IN {in_list(sorted(list(ONETECH_THEMES_EN)))}")
    if domains:
        w.append(f"{prefix}cordis_domain_ui IN {in_list(domains)}")
    if statuses:
        w.append(f"{prefix}project_status IN {in_list(statuses)}")
    if themes:
        w.append(f"{prefix}cordis_theme_primary IN {in_list(themes)}")
    if subthemes:
        w.append(
            f"{prefix}projectID IN ("
            f"SELECT DISTINCT projectID FROM {scientific_subthemes_rel()} "
            f"WHERE subtheme_label IN {in_list(subthemes)})"
        )
    if entities:
        w.append(f"{prefix}entity_type IN {in_list(entities)}")
    if countries:
        w.append(f"{prefix}country_name IN {in_list(countries)}")
    if str(quick_search).strip():
        q = _normalize_quick_search(str(quick_search).strip())
        w.append(quick_search_clause(prefix, q, quick_search_columns))
    extra_terms = [
        _normalize_quick_search(str(term).strip())
        for term in (extra_search_terms or [])
        if str(term).strip()
    ]
    if extra_terms:
        w.append("(" + " OR ".join(quick_search_clause(prefix, term, quick_search_columns) for term in extra_terms) + ")")
    return " AND ".join(w) if w else "TRUE"


# ============================================================
# Events loader (small, OK in pandas)
# ============================================================
@st.cache_data(show_spinner=False)
def load_events() -> pd.DataFrame:
    cols = ["date", "theme", "tag", "title", "source", "url", "impact_direction", "notes"]
    if not EVENTS_PATH.exists():
        return pd.DataFrame(columns=cols)

    ev = pd.read_csv(EVENTS_PATH)
    for c in cols:
        if c not in ev.columns:
            ev[c] = ""
    ev = ev[cols].copy()

    ev["date"] = pd.to_datetime(ev["date"], errors="coerce")
    ev = ev.dropna(subset=["date"]).copy()
    for c in ["theme", "tag", "title", "source", "url", "impact_direction", "notes"]:
        ev[c] = ev[c].astype("string").fillna("").str.strip()

    # Backward compatibility: old files stored links only in notes.
    if "url" in ev.columns:
        link_from_notes = ev["notes"].astype(str).str.extract(r"(https?://\S+)", expand=False).fillna("")
        ev["url"] = np.where(ev["url"].astype(str).str.len() > 0, ev["url"].astype(str), link_from_notes.astype(str))

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


def _lock_age_seconds(path: Path) -> Optional[float]:
    try:
        return max(0.0, time.time() - float(path.stat().st_mtime))
    except Exception:
        return None


def refresh_with_lock() -> Tuple[bool, Dict[str, str]]:
    lock = FileLock(str(LOCK_PATH))
    acquired = False

    try:
        try:
            lock.acquire(timeout=2)
            acquired = True
        except Timeout:
            age = _lock_age_seconds(LOCK_PATH)
            # Defensive stale-lock recovery for interrupted cloud sessions.
            if age is not None and age > float(REFRESH_LOCK_STALE_SEC):
                try:
                    LOCK_PATH.unlink(missing_ok=True)
                except Exception:
                    pass
                lock = FileLock(str(LOCK_PATH))
                lock.acquire(timeout=2)
                acquired = True
            else:
                age_msg = f" Lock age ~{age/60:.0f} min." if age is not None else ""
                return False, {"lock": f"Refresh already running.{age_msg} Try again in 1–2 minutes."}

        ok, logs = rebuild_all()
        # Important: clear caches
        st.cache_data.clear()
        st.cache_resource.clear()
        return ok, logs
    except Timeout:
        age = _lock_age_seconds(LOCK_PATH)
        age_msg = f" Lock age ~{age/60:.0f} min." if age is not None else ""
        return False, {"lock": f"Refresh lock busy.{age_msg} Try again in 1–2 minutes."}
    finally:
        if acquired:
            try:
                lock.release()
            except Exception:
                pass


# ============================================================
# Header: language + hero + actions
# ============================================================
cloud_runtime = is_streamlit_cloud_runtime()
hero_col, lang_col = st.columns([6.2, 1.1])
with lang_col:
    lang = st.radio(
        "Language",
        ["FR", "EN"],
        index=0 if st.session_state.get("ui_lang", "FR") == "FR" else 1,
        horizontal=True,
        label_visibility="collapsed",
        key="ui_lang",
    )
    st.caption(t(lang, "language"))
with hero_col:
    st.markdown(
        (
            "<div class='sir-hero'>"
            f"<div class='sir-hero__eyebrow'>◈ {'Internal R&D analytics' if lang == 'EN' else 'Veille R&D interne'}</div>"
            f"<div class='sir-hero__title'>{html.escape(t(lang, 'title'))}</div>"
            f"<p class='sir-hero__subtitle'>{html.escape(t(lang, 'subtitle'))}</p>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

action_c1, action_c2, action_c3 = st.columns([1.1, 1.1, 3.8])
with action_c1:
    if st.button(t(lang, "reset"), width="stretch"):
        reset_filters()
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
with action_c2:
    refresh_clicked = st.button(
        t(lang, "refresh"),
        width="stretch",
        help=t(lang, "refresh_hint"),
        disabled=cloud_runtime,
    )
    if refresh_clicked:
        with st.spinner("Mise à jour en cours (CORDIS + events)..." if lang == "FR" else "Updating (CORDIS + events)..."):
            ok, logs = refresh_with_lock()
        st.session_state["last_rebuild_ok"] = ok
        st.session_state["last_rebuild_cloud_skip"] = False
        st.session_state["last_rebuild_logs"] = logs
        st.rerun()
with action_c3:
    st.caption(f"{t(lang,'last_update')} — {t(lang,'last_update_data')}: {_fmt_mtime(PARQUET_PATH)}")
    st.caption(f"{t(lang,'last_update')} — {t(lang,'last_update_events')}: {_fmt_mtime(EVENTS_PATH)}")

if cloud_runtime:
    st.caption(t(lang, "cloud_persistence_note"))
    st.caption(t(lang, "refresh_cloud_disabled"))
    act_url = github_actions_refresh_url()
    if act_url:
        st.caption(f"[{t(lang, 'refresh_cloud_cta')}]({act_url})")

if "last_rebuild_logs" in st.session_state:
    if st.session_state.get("last_rebuild_cloud_skip"):
        st.info(t(lang, "refresh_cloud_skip"))
    elif st.session_state.get("last_rebuild_ok"):
        st.success(t(lang, "rebuild_ok"))
    else:
        st.warning(t(lang, "rebuild_fail"))
    with st.expander(t(lang, "logs"), expanded=False):
        for k, v in st.session_state["last_rebuild_logs"].items():
            st.markdown(f"**{k}**")
            st.code(v or "", language="text")

if not PARQUET_PATH.exists():
    st.error(
        f"Base Parquet manquante : `{PARQUET_PATH}`.\n\n"
        f"➡️ Clique sur **{t(lang,'refresh')}** pour la générer (via process_build / pipeline)."
    )
    st.stop()


# ============================================================
# Metadata lists + ranges (cheap)
# ============================================================
@st.cache_data(show_spinner=False, ttl=300)
def get_meta(_cache_buster: str = "v7_cordis_meta") -> dict:
    R = rel()
    S = scientific_subthemes_rel()
    yr = fetch_df(f"SELECT MIN(year) AS miny, MAX(year) AS maxy FROM {R}")
    miny = int(yr["miny"].iloc[0])
    maxy = int(yr["maxy"].iloc[0])
    raw_countries = list_str(
        f"SELECT DISTINCT country_name FROM {R} WHERE country_name IS NOT NULL AND TRIM(country_name)<>'' ORDER BY country_name"
    )
    domains = list_str(
        f"SELECT DISTINCT cordis_domain_ui FROM {R} WHERE cordis_domain_ui IS NOT NULL AND TRIM(cordis_domain_ui)<>'' ORDER BY cordis_domain_ui"
    )
    domain_pairs_df = fetch_df(f"""
        SELECT DISTINCT
          cordis_domain_ui,
          cordis_theme_primary
        FROM {R}
        WHERE TRIM(COALESCE(cordis_domain_ui, '')) <> ''
          AND TRIM(COALESCE(cordis_theme_primary, '')) <> ''
        ORDER BY cordis_domain_ui, cordis_theme_primary
    """)
    themes_by_domain: Dict[str, List[str]] = {domain: [] for domain in domains}
    for _, row in domain_pairs_df.iterrows():
        domain = str(row.get("cordis_domain_ui") or "").strip()
        theme = str(row.get("cordis_theme_primary") or "").strip()
        if domain and theme and theme not in themes_by_domain.setdefault(domain, []):
            themes_by_domain[domain].append(theme)
    scientific_subthemes = list_str(
        f"SELECT DISTINCT subtheme_label FROM {S} WHERE subtheme_label IS NOT NULL AND TRIM(subtheme_label)<>'' ORDER BY subtheme_label"
    )
    subthemes_by_domain_df = fetch_df(f"""
        SELECT DISTINCT
          cordis_domain_ui,
          subtheme_label
        FROM {S}
        WHERE TRIM(COALESCE(cordis_domain_ui, '')) <> ''
          AND TRIM(COALESCE(subtheme_label, '')) <> ''
        ORDER BY cordis_domain_ui, subtheme_label
    """)
    scientific_subthemes_by_domain: Dict[str, List[str]] = {domain: [] for domain in domains}
    for _, row in subthemes_by_domain_df.iterrows():
        domain = str(row.get("cordis_domain_ui") or "").strip()
        subtheme = str(row.get("subtheme_label") or "").strip()
        if domain and subtheme and subtheme not in scientific_subthemes_by_domain.setdefault(domain, []):
            scientific_subthemes_by_domain[domain].append(subtheme)
    for domain in domains:
        if not scientific_subthemes_by_domain.get(domain):
            scientific_subthemes_by_domain[domain] = list(GUIDED_DOMAIN_SUBCATEGORIES.get(domain, []))

    return {
        "miny": miny,
        "maxy": maxy,
        "sources": list_str(f"SELECT DISTINCT source FROM {R} WHERE source IS NOT NULL AND TRIM(source)<>'' AND UPPER(TRIM(source)) <> 'ADEME' ORDER BY source"),
        "programmes": list_str(f"SELECT DISTINCT program FROM {R} WHERE program IS NOT NULL AND TRIM(program)<>'' AND UPPER(TRIM(program)) NOT LIKE '%ADEME%' ORDER BY program"),
        "sections": list_str(f"SELECT DISTINCT section FROM {R} WHERE section IS NOT NULL AND TRIM(section)<>'' ORDER BY section"),
        "statuses": list_str(f"SELECT DISTINCT project_status FROM {R} WHERE project_status IS NOT NULL AND TRIM(project_status)<>'' ORDER BY project_status"),
        "domains": [d for d in CORDIS_DOMAIN_UI_ORDER if d in domains] + [d for d in domains if d not in CORDIS_DOMAIN_UI_ORDER],
        "themes": list_str(f"SELECT DISTINCT cordis_theme_primary FROM {R} WHERE cordis_theme_primary IS NOT NULL AND TRIM(cordis_theme_primary)<>'' ORDER BY cordis_theme_primary"),
        "themes_by_domain": themes_by_domain,
        "scientific_subthemes": scientific_subthemes,
        "scientific_subthemes_by_domain": scientific_subthemes_by_domain,
        "entities": list_str(f"SELECT DISTINCT entity_type FROM {R} WHERE entity_type IS NOT NULL AND TRIM(entity_type)<>'' ORDER BY entity_type"),
        "countries": normalized_country_options(raw_countries),
    }

meta = get_meta()


# ============================================================
# Default filters (ensure missing keys)
# ============================================================
def _ensure_filter_state() -> None:
    eu_default = european_countries_present(meta["countries"])
    default_countries = eu_default if eu_default else meta["countries"]
    default_statuses = [s for s in ["Open", "Closed", "Unknown"] if s in meta["statuses"]]
    if not default_statuses:
        default_statuses = meta["statuses"]

    st.session_state.setdefault("f_sources", meta["sources"])
    st.session_state.setdefault("f_programmes", meta["programmes"])
    st.session_state.setdefault("f_years", (meta["miny"], meta["maxy"]))
    st.session_state.setdefault("f_use_section", False)
    st.session_state.setdefault("f_sections", [])
    st.session_state.setdefault("f_onetech_only", False)
    st.session_state.setdefault("f_domains_raw", meta["domains"])
    st.session_state.setdefault("f_statuses", default_statuses)
    st.session_state.setdefault("f_themes_raw", meta["themes"])
    st.session_state.setdefault("f_scientific_subthemes", [])
    st.session_state.setdefault("f_entity_raw", meta["entities"])
    st.session_state.setdefault("f_countries", default_countries)
    st.session_state.setdefault("f_quick_search", "")
    st.session_state.setdefault("f_use_actor_groups", False)
    st.session_state.setdefault("f_exclude_funders", True)

    # One-time migration: switch old "all countries by default" sessions to Europe default.
    if not st.session_state.get("_country_default_migrated_v6", False):
        st.session_state["f_countries"] = default_countries
        st.session_state["f_statuses"] = default_statuses
        st.session_state["f_domains_raw"] = list(meta["domains"])
        st.session_state["f_use_actor_groups"] = False
        st.session_state["f_exclude_funders"] = True
        st.session_state["_country_default_migrated_v6"] = True

    # Section filter is intentionally disabled in sidebar UX (too technical for most users).
    st.session_state["f_use_section"] = False
    st.session_state["f_sections"] = []


def _normalize_country_state(meta: dict) -> None:
    available = list(meta.get("countries", []))
    if not available:
        return
    default_countries = _default_countries_from_meta(meta)
    for key in ["f_countries", "guided_countries", "guided_countries_widget"]:
        current = st.session_state.get(key, [])
        if isinstance(current, (list, tuple, set)):
            current_values = list(current)
        elif str(current).strip():
            current_values = [str(current).strip()]
        else:
            current_values = []
        normalized = normalize_country_selection(current_values, available)
        if normalized:
            st.session_state[key] = normalized
        elif key == "f_countries" and not current_values:
            st.session_state[key] = list(default_countries)


def _themes_available_for_domains(meta: dict, domains: List[str]) -> List[str]:
    if not domains or set(domains) == set(meta.get("domains", [])):
        return list(meta.get("themes", []))
    allowed: List[str] = []
    for domain in domains:
        for theme in meta.get("themes_by_domain", {}).get(domain, []):
            if theme not in allowed:
                allowed.append(theme)
    return allowed or list(meta.get("themes", []))


def _scientific_subthemes_available_for_domains(meta: dict, domains: List[str]) -> List[str]:
    if not domains or set(domains) == set(meta.get("domains", [])):
        return list(meta.get("scientific_subthemes", []))
    allowed: List[str] = []
    for domain in domains:
        for subtheme in meta.get("scientific_subthemes_by_domain", {}).get(domain, []):
            if subtheme not in allowed:
                allowed.append(subtheme)
    return allowed or list(meta.get("scientific_subthemes", []))


_ensure_filter_state()
_normalize_country_state(meta)
st.session_state.setdefault("app_mode", "simple")
st.session_state.setdefault("sir_screen", "welcome")
st.session_state.setdefault("guided_intent", "projects")
st.session_state.setdefault("guided_intent_active", "projects")
if any(k not in st.session_state for k in ["guided_search", "guided_themes_raw", "guided_countries", "guided_years"]):
    sync_guided_entry_from_filters(meta)
_normalize_country_state(meta)
st.session_state.setdefault("guided_subtopics", [])
st.session_state.setdefault("guided_subtopics_by_theme", {})
st.session_state.setdefault("guided_countries_widget", list(st.session_state.get("guided_countries", [])))
st.session_state.setdefault("f_guided_subtopics", [])
st.session_state.setdefault("f_guided_topic_terms", [])
st.session_state.setdefault("nav_target_trends_sub", "")
st.session_state.setdefault("nav_target_advanced_sub", "")


def _current_filter_snapshot() -> Dict[str, object]:
    return {
        "f_sources": list(st.session_state.get("f_sources", [])),
        "f_programmes": list(st.session_state.get("f_programmes", [])),
        "f_years": tuple(st.session_state.get("f_years", (meta["miny"], meta["maxy"]))),
        "f_domains_raw": list(st.session_state.get("f_domains_raw", [])),
        "f_statuses": list(st.session_state.get("f_statuses", [])),
        "f_themes_raw": list(st.session_state.get("f_themes_raw", [])),
        "f_scientific_subthemes": list(st.session_state.get("f_scientific_subthemes", [])),
        "f_entity_raw": list(st.session_state.get("f_entity_raw", [])),
        "f_countries": list(st.session_state.get("f_countries", [])),
        "f_onetech_only": bool(st.session_state.get("f_onetech_only", False)),
        "f_use_actor_groups": bool(st.session_state.get("f_use_actor_groups", False)),
        "f_exclude_funders": bool(st.session_state.get("f_exclude_funders", True)),
    }


def _apply_filter_snapshot(snapshot: Dict[str, object]) -> None:
    for key, value in snapshot.items():
        if st.session_state.get(key) != value:
            st.session_state[key] = value


def _current_universal_filter_state() -> Dict[str, object]:
    current_domains = [x for x in st.session_state.get("f_domains_raw", []) if x in meta["domains"]]
    domains_ui = current_domains or list(meta["domains"])
    themes_ui = _themes_available_for_domains(meta, domains_ui)
    current_themes = [x for x in st.session_state.get("f_themes_raw", []) if x in themes_ui]
    current_countries = [x for x in st.session_state.get("f_countries", []) if x in meta["countries"]]
    return {
        "f_years": tuple(st.session_state.get("f_years", (meta["miny"], meta["maxy"]))),
        "f_domains_raw": domains_ui,
        "f_themes_raw": current_themes or themes_ui,
        "f_countries": current_countries or _default_countries_from_meta(meta),
    }


def _simple_mode_filter_snapshot() -> Dict[str, object]:
    default_statuses = [s for s in ["Open", "Closed", "Unknown"] if s in meta["statuses"]]
    if not default_statuses:
        default_statuses = meta["statuses"]
    universal_state = _current_universal_filter_state()
    return {
        "f_sources": list(meta["sources"]),
        "f_programmes": list(meta["programmes"]),
        "f_years": universal_state["f_years"],
        "f_domains_raw": list(universal_state["f_domains_raw"]),
        "f_statuses": list(default_statuses),
        "f_themes_raw": list(universal_state["f_themes_raw"]),
        "f_scientific_subthemes": [],
        "f_entity_raw": list(meta["entities"]),
        "f_countries": list(universal_state["f_countries"]),
        "f_onetech_only": False,
        "f_use_actor_groups": False,
        "f_exclude_funders": True,
    }


previous_app_mode = str(st.session_state.get("_last_app_mode", st.session_state["app_mode"]))
if st.session_state["app_mode"] == "simple":
    if previous_app_mode != "simple":
        st.session_state["_advanced_filter_snapshot"] = _current_filter_snapshot()
    _apply_filter_snapshot(_simple_mode_filter_snapshot())
elif previous_app_mode == "simple":
    saved_snapshot = st.session_state.get("_advanced_filter_snapshot")
    if isinstance(saved_snapshot, dict):
        saved_snapshot = dict(saved_snapshot)
        saved_snapshot.update(_current_universal_filter_state())
        _apply_filter_snapshot(saved_snapshot)
st.session_state["_last_app_mode"] = st.session_state["app_mode"]


if st.session_state.get("sir_screen", "welcome") == "welcome":
    render_section_header("⌕", t(lang, "guided_home_title"), t(lang, "guided_home_caption"), "")
    with st.container(border=True):
        st.markdown("**" + t(lang, "guided_home_intro_title") + "**")
        st.write(t(lang, "guided_home_intro"))

    st.markdown(f"#### {t(lang, 'guided_home_question_title')}")
    st.caption(t(lang, "guided_home_question_caption"))
    question_cols = st.columns(3)
    current_intent = str(st.session_state.get("guided_intent", "projects") or "projects")
    for idx, intent in enumerate(GUIDED_INTENT_ORDER):
        with question_cols[idx % 3]:
            selected = intent == current_intent
            with st.container(border=True):
                st.markdown(
                    "<div class='sir-guided-question-copy'>"
                    f"<p class='sir-guided-question-title'>{html.escape((('✓ ' if selected else '') + guided_intent_title(lang, intent)))}</p>"
                    f"<p class='sir-guided-question-desc'>{html.escape(guided_intent_desc(lang, intent))}</p>"
                    "</div>",
                    unsafe_allow_html=True,
                )
                if st.button(
                    t(lang, "guided_home_selected_question_button") if selected else t(lang, "guided_home_choose_question"),
                    key=f"guided_intent::{intent}",
                    width="stretch",
                    type="primary" if selected else "secondary",
                ):
                    st.session_state["guided_intent"] = intent
                    st.session_state["guided_intent_active"] = intent
                    st.rerun()
    with st.container(border=True):
        st.markdown("**" + t(lang, "guided_home_selected_question") + "**")
        st.write(guided_intent_title(lang, current_intent))
        st.caption(t(lang, "guided_home_selected_question_desc"))

    example_prompts = [
        t(lang, "guided_home_example_1"),
        t(lang, "guided_home_example_2"),
        t(lang, "guided_home_example_3"),
    ]
    st.caption(t(lang, "guided_home_examples_label"))
    st.markdown(
        "<div class='sir-guided-pill-row'>" + "".join(
            f"<span class='sir-guided-pill'>{html.escape(p)}</span>" for p in example_prompts
        ) + "</div>",
        unsafe_allow_html=True,
    )

    selected_theme_count = len([x for x in st.session_state.get("guided_themes_raw", []) if x in meta["domains"]]) or len(meta["domains"])
    visible_guided_countries = [x for x in st.session_state.get("guided_countries_widget", st.session_state.get("guided_countries", [])) if x in meta["countries"]]
    selected_country_count = len(visible_guided_countries) or len(_default_countries_from_meta(meta))
    guided_years_value = tuple(st.session_state.get("guided_years", (meta["miny"], meta["maxy"])))
    summary_c1, summary_c2, summary_c3 = st.columns(3)
    summary_c1.metric(t(lang, "guided_home_metric_themes"), f"{selected_theme_count:,}".replace(",", " "))
    summary_c2.metric(t(lang, "guided_home_metric_countries"), f"{selected_country_count:,}".replace(",", " "))
    summary_c3.metric(t(lang, "guided_home_metric_period"), f"{int(guided_years_value[0])}–{int(guided_years_value[1])}")

    gh1, gh2 = st.columns([1.7, 1.3])
    with gh1:
        with st.container(border=True):
            st.markdown(f"<div class='sir-guided-card-title'>{html.escape(t(lang, 'guided_home_topic_card'))}</div>", unsafe_allow_html=True)
            st.markdown(f"<p class='sir-guided-card-desc'>{html.escape(t(lang, 'guided_home_topic_card_desc'))}</p>", unsafe_allow_html=True)
            st.text_input(
                t(lang, "guided_home_search"),
                key="guided_search",
                help=t(lang, "guided_home_search_help"),
                placeholder=t(lang, "main_search_placeholder"),
            )
            st.caption(t(lang, "guided_home_theme_cards_help"))
            current_guided_themes = [x for x in st.session_state.get("guided_themes_raw", []) if x in meta["domains"]]
            theme_cols = st.columns(2)
            for theme in meta["domains"]:
                widget_key = f"guided_theme_selected::{theme}"
                if widget_key not in st.session_state:
                    st.session_state[widget_key] = theme in current_guided_themes
            for idx, theme in enumerate(meta["domains"]):
                label = domain_raw_to_display(str(theme), lang)
                with theme_cols[idx % 2]:
                    with st.container(border=True):
                        st.markdown(
                            "<div class='sir-guided-theme-copy'>"
                            f"<p class='sir-guided-theme-title'>{html.escape(label)}</p>"
                            "</div>",
                            unsafe_allow_html=True,
                        )
                        st.caption(t(lang, "guided_home_theme_select_note"))
                        st.checkbox(t(lang, "guided_home_theme_select_action"), key=f"guided_theme_selected::{theme}")
            guided_theme_choices = [
                theme for theme in meta["domains"]
                if bool(st.session_state.get(f"guided_theme_selected::{theme}", False))
            ]
            if guided_theme_choices != current_guided_themes:
                subtopic_map = _clean_guided_subtopics_by_theme()
                st.session_state["guided_themes_raw"] = guided_theme_choices
                st.session_state["guided_subtopics_by_theme"] = {
                    theme: subtopic_map.get(theme, [])
                    for theme in guided_theme_choices
                    if subtopic_map.get(theme)
                }
                st.session_state["guided_subtopics"] = _selected_guided_subtopics(guided_theme_choices)
            st.caption(t(lang, "guided_home_topics_help"))
            if guided_theme_choices:
                st.caption(t(lang, "guided_home_selected_themes"))
                st.markdown(
                    "<div class='sir-guided-pill-row'>" + "".join(
                        f"<span class='sir-guided-pill'>{html.escape(domain_raw_to_display(str(x), lang))}</span>"
                        for x in guided_theme_choices
                    ) + "</div>",
                    unsafe_allow_html=True,
                )
            guided_subtopic_map = _clean_guided_subtopics_by_theme()
            if guided_theme_choices:
                st.caption(t(lang, "guided_home_subtopics_help"))
                updated_subtopic_map: Dict[str, List[str]] = {}
                for theme in guided_theme_choices:
                    theme_key = str(theme)
                    available_subtopics = GUIDED_DOMAIN_SUBCATEGORIES.get(theme_key, [])
                    if not available_subtopics:
                        continue
                    current_subtopics = [x for x in guided_subtopic_map.get(theme_key, []) if x in available_subtopics]
                    with st.container(border=True):
                        st.markdown("**" + domain_raw_to_display(theme_key, lang) + "**")
                        selected_subtopics = st.multiselect(
                            t(lang, "guided_home_subtopics"),
                            available_subtopics,
                            default=current_subtopics,
                            key=f"guided_subtopics_widget::{theme_key}",
                        )
                    if selected_subtopics:
                        updated_subtopic_map[theme_key] = selected_subtopics
                st.session_state["guided_subtopics_by_theme"] = updated_subtopic_map
                st.session_state["guided_subtopics"] = _selected_guided_subtopics(guided_theme_choices)
            else:
                st.session_state["guided_subtopics_by_theme"] = {}
                st.session_state["guided_subtopics"] = []

    with gh2:
        with st.container(border=True):
            st.markdown(f"<div class='sir-guided-card-title'>{html.escape(t(lang, 'guided_home_scope_card'))}</div>", unsafe_allow_html=True)
            st.markdown(f"<p class='sir-guided-card-desc'>{html.escape(t(lang, 'guided_home_scope_card_desc'))}</p>", unsafe_allow_html=True)
            guided_eu27 = eu27_countries_present(meta["countries"])
            guided_assoc = associated_countries_present(meta["countries"])
            guided_eu_plus = list(dict.fromkeys(guided_eu27 + guided_assoc))

            def _apply_guided_country_preset(countries_list):
                st.session_state["guided_countries"] = list(countries_list)
                st.session_state["guided_countries_widget"] = list(countries_list)

            gp1, gp2, gp3 = st.columns(3)
            with gp1:
                st.button(t(lang, "country_preset_eu27"), key="guided_country_preset_eu27_btn", width="stretch",
                          on_click=_apply_guided_country_preset, args=(guided_eu27,))
            with gp2:
                st.button(t(lang, "country_preset_associated"), key="guided_country_preset_associated_btn", width="stretch",
                          on_click=_apply_guided_country_preset, args=(guided_eu_plus,))
            with gp3:
                st.button(t(lang, "country_preset_all"), key="guided_country_preset_all_btn", width="stretch",
                          on_click=_apply_guided_country_preset, args=(list(meta["countries"]),))
            if not st.session_state.get("guided_countries"):
                st.session_state["guided_countries"] = [
                    x for x in st.session_state.get("guided_countries", []) if x in meta["countries"]
                ] or _default_countries_from_meta(meta)
            st.session_state["guided_countries_widget"] = [
                x for x in st.session_state.get("guided_countries_widget", st.session_state.get("guided_countries", []))
                if x in meta["countries"]
            ] or list(st.session_state.get("guided_countries", []))
            st.multiselect(
                t(lang, "countries"),
                meta["countries"],
                key="guided_countries_widget",
                format_func=country_raw_to_display,
            )
            st.session_state["guided_countries"] = [
                x for x in st.session_state.get("guided_countries_widget", []) if x in meta["countries"]
            ]
            st.caption(t(lang, "guided_home_countries_help"))
            st.slider(
                t(lang, "period"),
                meta["miny"],
                meta["maxy"],
                tuple(st.session_state.get("guided_years", (meta["miny"], meta["maxy"]))),
                key="guided_years",
            )
            st.caption(t(lang, "guided_home_period_help"))

    st.markdown(
        "<div class='sir-guided-next'>"
        f"<div class='sir-guided-next__title'>{html.escape(t(lang, 'guided_home_next_title'))}</div>"
        "<ul>"
        f"<li>{html.escape(t(lang, 'guided_home_next_1'))}</li>"
        f"<li>{html.escape(t(lang, 'guided_home_next_2'))}</li>"
        f"<li>{html.escape(t(lang, 'guided_home_next_3'))}</li>"
        "</ul>"
        "</div>",
        unsafe_allow_html=True,
    )

    launch_c1, launch_c2 = st.columns([1.6, 4.4])
    with launch_c1:
        if st.button(t(lang, "guided_home_open"), key="guided_home_open_btn", width="stretch", type="primary"):
            apply_guided_entry_to_filters(meta)
            st.session_state["_advanced_filter_snapshot"] = _current_filter_snapshot()
            st.session_state["app_mode"] = guided_intent_mode(st.session_state.get("guided_intent", "projects"))
            st.session_state["_last_app_mode"] = st.session_state["app_mode"]
            st.session_state["sir_screen"] = "analysis"
            apply_guided_intent_navigation(lang)
            st.rerun()
    with launch_c2:
        st.caption(t(lang, "guided_home_caption"))

    st.stop()

actor_map_info = register_actor_group_tables()

# ============================================================
# App mode
# ============================================================
st.radio(
    t(lang, "app_mode_label"),
    ["simple", "advanced"],
    horizontal=True,
    key="app_mode",
    format_func=lambda mode: t(lang, "app_mode_simple") if str(mode) == "simple" else t(lang, "app_mode_advanced"),
)

nav_back_c1, nav_back_c2 = st.columns([1.4, 4.6])
with nav_back_c1:
    if st.button(t(lang, "guided_home_back"), key="guided_home_back_btn", width="stretch"):
        sync_guided_entry_from_filters(meta)
        st.session_state["sir_screen"] = "welcome"
        st.rerun()
with nav_back_c2:
    st.caption(t(lang, "guided_home_analysis_note"))
    if st.session_state.get("f_guided_subtopics"):
        st.caption(t(lang, "guided_terms_applied") + ": " + ", ".join(st.session_state.get("f_guided_subtopics", [])))


# ============================================================
# Main search entry
# ============================================================
if WIP_SECTIONS.get("free_text_search", False):
    st.markdown(f"<div class='sir-wip-badge-wrap'>{wip_badge(lang)}</div>", unsafe_allow_html=True)

search_c1, search_c2 = st.columns([6, 1])
with search_c1:
    st.text_input(
        t(lang, "main_search_label"),
        key="f_quick_search",
        help=t(lang, "main_search_help"),
        placeholder=t(lang, "main_search_placeholder"),
    )
with search_c2:
    st.write("")
    st.write("")
    st.button(t(lang, "clear_search"), key="clear_search_btn", width="stretch", on_click=clear_search)
st.caption(t(lang, "main_search_support"))
with st.expander(t(lang, "search_help_more"), expanded=False):
    st.caption(t(lang, "main_search_examples"))
    st.caption(t(lang, "main_search_exploratory"))
    st.caption(t(lang, "search_literal_note"))

src_default = [x for x in st.session_state["f_sources"] if x in meta["sources"]]
prg_default = [x for x in st.session_state["f_programmes"] if x in meta["programmes"]]
status_default = [x for x in st.session_state["f_statuses"] if x in meta["statuses"]]
ctry_default = [x for x in st.session_state["f_countries"] if x in meta["countries"]]
eu_default = european_countries_present(meta["countries"])
ctry_fallback = eu_default if eu_default else meta["countries"]
domains_default = [x for x in st.session_state.get("f_domains_raw", []) if x in meta["domains"]]
themes_ui = _themes_available_for_domains(meta, domains_default or meta["domains"])
themes_default = [x for x in st.session_state["f_themes_raw"] if x in themes_ui]
scientific_subthemes_ui = _scientific_subthemes_available_for_domains(meta, domains_default or meta["domains"])
scientific_subthemes_default = [x for x in st.session_state.get("f_scientific_subthemes", []) if x in scientific_subthemes_ui]

filters_expander_label = t(lang, "filters") if st.session_state.get("app_mode") == "advanced" else ("Affiner le cadrage" if lang == "FR" else "Refine scope")
with st.expander(filters_expander_label, expanded=False):
    st.caption(t(lang, "basic_filters"))
    basic_c1, basic_c2, basic_c3, basic_c4 = st.columns(4)
    with basic_c1:
        st.session_state["f_years"] = st.slider(
            t(lang, "period"),
            meta["miny"],
            meta["maxy"],
            st.session_state["f_years"],
        )
    with basic_c2:
        st.session_state["f_domains_raw"] = st.multiselect(
            t(lang, "domains"),
            meta["domains"],
            default=domains_default or meta["domains"],
            format_func=lambda x: domain_raw_to_display(str(x), lang),
        )
    themes_ui = _themes_available_for_domains(meta, st.session_state.get("f_domains_raw", []))
    themes_default = [x for x in st.session_state["f_themes_raw"] if x in themes_ui]
    with basic_c3:
        st.session_state["f_themes_raw"] = st.multiselect(
            t(lang, "themes"),
            themes_ui,
            default=themes_default or themes_ui,
            format_func=lambda x: theme_raw_to_display(str(x), lang),
        )
    with basic_c4:
        eu27_present = eu27_countries_present(meta["countries"])
        assoc_present = associated_countries_present(meta["countries"])
        eu_plus_associated = list(dict.fromkeys(eu27_present + assoc_present))

        def _apply_sidebar_country_preset(countries_list):
            st.session_state["f_countries"] = list(countries_list)

        preset_c1, preset_c2, preset_c3 = st.columns(3)
        with preset_c1:
            st.button(t(lang, "country_preset_eu27"), key="country_preset_eu27_btn", width="stretch",
                      on_click=_apply_sidebar_country_preset, args=(eu27_present,))
        with preset_c2:
            st.button(t(lang, "country_preset_associated"), key="country_preset_associated_btn", width="stretch",
                      on_click=_apply_sidebar_country_preset, args=(eu_plus_associated,))
        with preset_c3:
            st.button(t(lang, "country_preset_all"), key="country_preset_all_btn", width="stretch",
                      on_click=_apply_sidebar_country_preset, args=(list(meta["countries"]),))
        st.multiselect(
            t(lang, "countries"),
            meta["countries"],
            key="f_countries",
            format_func=country_raw_to_display,
        )

    if st.session_state.get("app_mode") == "advanced":
        st.divider()
        st.caption(t(lang, "advanced_filters"))
        adv_c1, adv_c2, adv_c3, adv_c4 = st.columns(4)
        with adv_c1:
            st.session_state["f_programmes"] = st.multiselect(
                t(lang, "programmes"),
                meta["programmes"],
                default=prg_default or meta["programmes"],
            )
        with adv_c2:
            st.session_state["f_sources"] = st.multiselect(
                t(lang, "sources"),
                meta["sources"],
                default=src_default or meta["sources"],
            )
        entities_default = [x for x in st.session_state["f_entity_raw"] if x in meta["entities"]]
        with adv_c3:
            st.session_state["f_entity_raw"] = st.multiselect(
                t(lang, "entity"),
                meta["entities"],
                default=entities_default or meta["entities"],
                format_func=lambda x: entity_raw_to_display(str(x), lang),
            )
        with adv_c4:
            st.session_state["f_statuses"] = st.multiselect(
                t(lang, "project_status"),
                meta["statuses"],
                default=status_default or meta["statuses"],
                format_func=lambda x: status_raw_to_display(str(x), lang),
            )
        scientific_subthemes_ui = _scientific_subthemes_available_for_domains(meta, st.session_state.get("f_domains_raw", []))
        scientific_subthemes_default = [x for x in st.session_state.get("f_scientific_subthemes", []) if x in scientific_subthemes_ui]
        st.session_state["f_scientific_subthemes"] = st.multiselect(
            t(lang, "scientific_subthemes"),
            scientific_subthemes_ui,
            default=scientific_subthemes_default,
        )

        st.divider()
        st.caption(t(lang, "analysis_options"))
        ana_c1, ana_c2, ana_c3 = st.columns(3)
        with ana_c1:
            st.session_state["f_onetech_only"] = st.checkbox(
                t(lang, "onetech_only"),
                value=st.session_state["f_onetech_only"],
            )
        with ana_c2:
            st.checkbox(t(lang, "actor_grouping"), key="f_use_actor_groups")
            st.caption(t(lang, "actor_grouping_note"))
        with ana_c3:
            st.checkbox(t(lang, "exclude_funders"), key="f_exclude_funders")
    else:
        st.caption(t(lang, "filters_advanced_hint"))

render_active_filter_chips(meta, lang)
if st.session_state.get("app_mode") == "advanced":
    render_search_interpretation(meta, lang)
else:
    with st.expander(t(lang, "search_interpretation_title"), expanded=False):
        render_search_interpretation(meta, lang, compact=True)


# ============================================================
# Main WHERE
# ============================================================
R = rel_analytics(
    use_actor_groups=bool(st.session_state.get("f_use_actor_groups", False)),
    exclude_funders=bool(st.session_state.get("f_exclude_funders", False)),
)
guided_subtheme_filters = [x for x in st.session_state.get("f_guided_subtopics", []) if str(x).strip()]
advanced_subtheme_filters = [x for x in st.session_state.get("f_scientific_subthemes", []) if str(x).strip()]
scientific_subtheme_filters: List[str] = []
for value in guided_subtheme_filters + advanced_subtheme_filters:
    if value not in scientific_subtheme_filters:
        scientific_subtheme_filters.append(value)
guided_topic_terms = list(st.session_state.get("f_guided_topic_terms", []))
if "scientific_subthemes" not in set(base_schema_columns()) and "sub_theme" not in set(base_schema_columns()):
    scientific_subtheme_filters = []
W, W_R, search_notice_key = build_safe_where_pair(
    R,
    sources=st.session_state["f_sources"],
    programmes=st.session_state["f_programmes"],
    years=st.session_state["f_years"],
    use_section=False,
    sections=[],
    onetech_only=st.session_state["f_onetech_only"],
    domains=st.session_state.get("f_domains_raw", []),
    statuses=st.session_state["f_statuses"],
    themes=st.session_state["f_themes_raw"],
    subthemes=scientific_subtheme_filters,
    entities=st.session_state["f_entity_raw"],
    countries=st.session_state["f_countries"],
    quick_search=st.session_state["f_quick_search"],
    extra_search_terms=guided_topic_terms if not scientific_subtheme_filters else [],
)
W_partners, W_R_partners, _ = build_safe_where_pair(
    R,
    sources=st.session_state["f_sources"],
    programmes=st.session_state["f_programmes"],
    years=st.session_state["f_years"],
    use_section=False,
    sections=[],
    onetech_only=st.session_state["f_onetech_only"],
    domains=st.session_state.get("f_domains_raw", []),
    statuses=st.session_state["f_statuses"],
    themes=st.session_state["f_themes_raw"],
    subthemes=scientific_subtheme_filters,
    entities=meta["entities"],
    countries=st.session_state["f_countries"],
    quick_search=st.session_state["f_quick_search"],
    extra_search_terms=guided_topic_terms if not scientific_subtheme_filters else [],
)


# ============================================================
# KPIs (DuckDB)
# ============================================================
kpi, kpi_failed = safe_fetch_df_quiet(f"""
SELECT
  SUM(amount_eur) AS total_budget,
  -- DISTINCT is required because analytics rows are not one-row-per-project.
  COUNT(DISTINCT projectID) AS n_projects,
  COUNT(DISTINCT actor_id) FILTER (WHERE actor_id IS NOT NULL AND TRIM(actor_id) <> '') AS n_actors
FROM {R}
WHERE {W}
""", columns=["total_budget", "n_projects", "n_actors"])

total_budget = float(kpi["total_budget"].iloc[0] or 0.0) if not kpi.empty else 0.0
nb_projects = int(kpi["n_projects"].iloc[0] or 0) if not kpi.empty else 0
nb_actors = int(kpi["n_actors"].iloc[0] or 0) if not kpi.empty else 0

proj_stats, proj_stats_failed = safe_fetch_df_quiet(f"""
SELECT
  AVG(proj_budget) AS avg_ticket,
  MEDIAN(proj_budget) AS median_ticket
FROM (
  SELECT projectID, SUM(amount_eur) AS proj_budget
  FROM {R}
  WHERE {W}
  GROUP BY projectID
) t
""", columns=["avg_ticket", "median_ticket"])
avg_ticket = float(proj_stats["avg_ticket"].iloc[0] or 0.0) if not proj_stats.empty else 0.0
median_ticket = float(proj_stats["median_ticket"].iloc[0] or 0.0) if not proj_stats.empty else 0.0

actor_b, actor_b_failed = safe_fetch_df_quiet(f"""
SELECT actor_id, SUM(amount_eur) AS b
FROM {R}
WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
GROUP BY actor_id
""", columns=["actor_id", "b"])
top10_share = 0.0
hhi = 0.0
if not actor_b.empty and float(actor_b["b"].sum()) > 0:
    b = actor_b["b"].astype(float)
    tot = float(b.sum())
    shares = (b / tot).to_numpy()
    hhi = float(np.sum(shares**2))
    top10_share = float(b.sort_values(ascending=False).head(10).sum() / tot)

main_scope_metrics_failed = bool(kpi_failed or proj_stats_failed or actor_b_failed)

scope_items = [
    f"{int(st.session_state['f_years'][0])}-{int(st.session_state['f_years'][1])}",
    t(lang, "scope_group_on") if st.session_state.get("f_use_actor_groups", False) else t(lang, "scope_group_off"),
    t(lang, "scope_funders_off") if st.session_state.get("f_exclude_funders", False) else t(lang, "scope_funders_on"),
]
selected_domains_scope = [x for x in st.session_state.get("f_domains_raw", []) if x in meta.get("domains", [])]
if selected_domains_scope and len(selected_domains_scope) < len(meta.get("domains", [])):
    scope_items.append(f"{t(lang, 'domains')}: {_compact_filter_values(selected_domains_scope, lambda x: domain_raw_to_display(x, lang), limit=2)}")
selected_themes_scope = [x for x in st.session_state.get("f_themes_raw", []) if x in meta.get("themes", [])]
if selected_themes_scope and len(selected_themes_scope) < len(meta.get("themes", [])):
    scope_items.append(f"{t(lang, 'themes')}: {_compact_filter_values(selected_themes_scope, lambda x: theme_raw_to_display(x, lang), limit=2)}")
selected_scientific_subthemes_scope = [x for x in st.session_state.get("f_scientific_subthemes", []) if x in meta.get("scientific_subthemes", [])]
if selected_scientific_subthemes_scope:
    scope_items.append(f"{t(lang, 'scientific_subthemes')}: {_compact_filter_values(selected_scientific_subthemes_scope, limit=2)}")
if st.session_state.get("f_statuses"):
    scope_items.append(", ".join(status_raw_to_display(x, lang) for x in st.session_state["f_statuses"]))
if str(st.session_state.get("f_quick_search", "")).strip():
    scope_items.append(f"{t(lang, 'quick_search')}: {str(st.session_state.get('f_quick_search', '')).strip()}")
if st.session_state.get("f_guided_subtopics"):
    scope_items.append(f"{t(lang, 'guided_terms')}: {_compact_filter_values(st.session_state.get('f_guided_subtopics', []), limit=2)}")



# ============================================================
# Top navigation (result-first)
# ============================================================
app_mode = str(st.session_state.get("app_mode", "simple"))
if app_mode == "simple":
    top_tab_labels = [
        t(lang, "tab_explorer"),
        t(lang, "tab_markets"),
        t(lang, "tab_trends_events"),
    ]
else:
    top_tab_labels = [
        t(lang, "tab_explorer"),
        t(lang, "tab_actors_hub"),
        t(lang, "tab_markets"),
        t(lang, "tab_trends_events"),
        t(lang, "tab_advanced"),
        t(lang, "tab_admin"),
    ]
default_top_tab = str(st.session_state.get("nav_target_top", "")).strip()
if default_top_tab not in top_tab_labels:
    default_top_tab = t(lang, "tab_explorer")
if app_mode == "simple":
    tab_explorer, tab_markets, tab_trends_events = st.tabs(
        top_tab_labels,
        default=default_top_tab,
    )
    tab_actors_hub = None
    tab_advanced = None
    tab_admin = None
    hidden_actor_placeholder = st.empty()
    hidden_comp_placeholder = st.empty()
    hidden_value_chain_placeholder = st.empty()
    hidden_collaboration_placeholder = st.empty()
    hidden_concentration_placeholder = st.empty()
    hidden_data_placeholder = st.empty()
    hidden_quality_placeholder = st.empty()
    hidden_debug_placeholder = st.empty()
    hidden_docs_placeholder = st.empty()
    tab_actor = hidden_actor_placeholder.container()
    tab_comp = hidden_comp_placeholder.container()
    tab_value_chain = hidden_value_chain_placeholder.container()
    tab_collaboration = hidden_collaboration_placeholder.container()
    tab_concentration = hidden_concentration_placeholder.container()
    tab_data = hidden_data_placeholder.container()
    tab_quality = hidden_quality_placeholder.container()
    tab_debug = hidden_debug_placeholder.container()
    tab_docs = hidden_docs_placeholder.container()
else:
    tab_explorer, tab_actors_hub, tab_markets, tab_trends_events, tab_advanced, tab_admin = st.tabs(
        top_tab_labels,
        default=default_top_tab,
    )

with tab_explorer:
    if app_mode == "advanced":
        tab_results, tab_overview = st.tabs([t(lang, "sub_results"), t(lang, "sub_overview")])
    else:
        tab_results = st.container()
        tab_overview = None

if app_mode == "advanced":
    with tab_actors_hub:
        tab_actor = st.container()

default_trends_tab = str(st.session_state.get("nav_target_trends_sub", "")).strip()
trend_tab_labels = [t(lang, "tab_trends"), t(lang, "tab_compare"), t(lang, "tab_macro")]
if default_trends_tab not in trend_tab_labels:
    default_trends_tab = t(lang, "tab_trends")
with tab_trends_events:
    if app_mode == "advanced":
        tab_trends, tab_compare, tab_macro = st.tabs(
            trend_tab_labels,
            default=default_trends_tab,
        )
    else:
        tab_trends = st.container()
        tab_compare = None
        tab_macro = None

if app_mode == "advanced":
    default_advanced_sub = str(st.session_state.get("nav_target_advanced_sub", "")).strip()
    advanced_tab_labels = [
        t(lang, "sub_benchmark"),
        t(lang, "sub_value_chain"),
        t(lang, "sub_collaboration"),
        t(lang, "sub_concentration"),
    ]
    if default_advanced_sub not in advanced_tab_labels:
        default_advanced_sub = t(lang, "sub_benchmark")
    with tab_advanced:
        st.markdown(f"### {t(lang, 'advanced_title')}")
        st.caption(t(lang, "advanced_caption"))
        with st.container(border=True):
            st.markdown("**" + t(lang, "advanced_overview_title") + "**")
            st.markdown(
                "\n".join(
                    [
                        f"- {t(lang, 'advanced_overview_1')}",
                        f"- {t(lang, 'advanced_overview_2')}",
                        f"- {t(lang, 'advanced_overview_3')}",
                        f"- {t(lang, 'advanced_overview_4')}",
                    ]
                )
            )
            st.caption(t(lang, "advanced_overview_tip"))
        tab_comp, tab_value_chain, tab_collaboration, tab_concentration = st.tabs(
            advanced_tab_labels,
            default=default_advanced_sub,
        )

    with tab_admin:
        render_section_header("⋯", t(lang, "admin_title"), t(lang, "admin_caption"), t(lang, "tab_admin"))
        with st.container(border=True):
            st.markdown("**" + t(lang, "support_overview_title") + "**")
            st.markdown(
                "\n".join(
                    [
                        f"- {t(lang, 'support_overview_1')}",
                        f"- {t(lang, 'support_overview_2')}",
                        f"- {t(lang, 'support_overview_3')}",
                    ]
                )
            )
            st.caption(t(lang, "support_overview_tip"))
        tab_data, tab_quality, tab_debug = st.tabs([t(lang, "sub_data"), t(lang, "sub_quality"), t(lang, "sub_debug")])
        tab_docs = st.container()

tab_geo = tab_markets
tab_help = tab_docs
tab_guide = tab_docs
st.session_state["nav_target_top"] = ""
st.session_state["nav_target_actor_sub"] = ""
st.session_state["nav_target_trends_sub"] = ""
st.session_state["nav_target_advanced_sub"] = ""


# ============================================================
# TAB RESULTS (result-first)
# ============================================================
with tab_results:
    render_section_header("⌕", t(lang, "results_title"), t(lang, "results_caption"), t(lang, "tab_explorer"))
    if search_notice_key:
        st.warning(t(lang, search_notice_key))
    if main_scope_metrics_failed:
        st.warning(t(lang, "results_scope_partial_warning"))
        st.caption(t(lang, "view_recover_hint"))

    scope_summary = build_results_scope_summary(
        R,
        W,
        lang=lang,
        total_budget=total_budget,
        n_projects=nb_projects,
        n_actors=nb_actors,
    )

    with st.container(border=True):
        st.markdown("**" + t(lang, "results_summary_title") + "**")
        st.write(scope_summary["headline"])
        if str(scope_summary.get("detail", "")).strip():
            st.caption(scope_summary["detail"])

    st.markdown("#### ✦ " + t(lang, "kpis"))
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric(t(lang, "budget_total"), fmt_money(total_budget, lang))
    k2.metric(t(lang, "n_projects"), f"{nb_projects:,}".replace(",", " "))
    k3.metric(t(lang, "n_actors"), f"{nb_actors:,}".replace(",", " "))
    k4.metric(t(lang, "avg_ticket"), fmt_money(avg_ticket, lang))
    k5.metric(t(lang, "median_ticket"), fmt_money(median_ticket, lang))
    k6.metric(t(lang, "top10_share"), fmt_pct(top10_share, 1))
    st.caption(f"{t(lang, 'hhi')}: {hhi:.3f}")
    st.caption(f"{t(lang, 'scope_caption')}: " + " · ".join(scope_items))
    st.divider()

    if nb_projects == 0:
        render_empty_state(lang)
    else:
        st.markdown("#### " + t(lang, "results_primary_visual"))
        primary_view = str(scope_summary.get("primary_view", "trend"))
        guided_primary_override = guided_intent_primary_view(
            st.session_state.get("guided_intent_active", st.session_state.get("guided_intent", "projects"))
        )
        if guided_primary_override:
            primary_view = guided_primary_override
        if primary_view == "trend":
            st.caption(t(lang, "results_primary_trend"))
            primary_year = safe_fetch_df(f"""
            SELECT year, SUM(amount_eur) AS budget_eur
            FROM {R}
            WHERE {W}
            GROUP BY year
            ORDER BY year
            """, columns=["year", "budget_eur"], lang=lang, warning_key="results_view_unavailable")
            if primary_year.empty:
                render_view_warning(lang, "results_view_unavailable")
            else:
                fig_primary = px.bar(
                    primary_year,
                    x="year",
                    y="budget_eur",
                    color="budget_eur",
                    color_continuous_scale=R2G,
                    height=380,
                    labels={"budget_eur": "Budget (€)", "year": "Year"},
                )
                fig_primary.update_layout(coloraxis_showscale=False)
                render_plotly_chart(fig_primary, use_container_width=True)
        elif primary_view == "countries":
            st.caption(t(lang, "results_primary_countries"))
            primary_geo = safe_fetch_df(f"""
            SELECT country_name, SUM(amount_eur) AS amount_eur
            FROM {R}
            WHERE {W} AND country_name IS NOT NULL AND TRIM(country_name) <> ''
            GROUP BY country_name
            ORDER BY amount_eur DESC
            LIMIT 12
            """, columns=["country_name", "amount_eur"], lang=lang, warning_key="results_view_unavailable")
            if primary_geo.empty:
                render_view_warning(lang, "results_view_unavailable")
            else:
                fig_primary = px.bar(
                    primary_geo.iloc[::-1],
                    x="amount_eur",
                    y="country_name",
                    orientation="h",
                    color="amount_eur",
                    color_continuous_scale=R2G,
                    height=420,
                    labels={"amount_eur": "Budget (€)", "country_name": ""},
                )
                fig_primary.update_layout(coloraxis_showscale=False, yaxis_title=None)
                render_plotly_chart(fig_primary, use_container_width=True)
        else:
            st.caption(t(lang, "results_primary_actors"))
            primary_actors = safe_fetch_df(f"""
            WITH x AS (
              SELECT
                actor_id,
                COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
                amount_eur
              FROM {R}
              WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
            )
            SELECT actor_label, SUM(amount_eur) AS budget_eur
            FROM x
            GROUP BY actor_label
            ORDER BY budget_eur DESC
            LIMIT 12
            """, columns=["actor_label", "budget_eur"], lang=lang, warning_key="results_view_unavailable")
            if primary_actors.empty:
                render_view_warning(lang, "results_view_unavailable")
            else:
                fig_primary = px.bar(
                    primary_actors.iloc[::-1],
                    x="budget_eur",
                    y="actor_label",
                    orientation="h",
                    color="budget_eur",
                    color_continuous_scale=R2G,
                    height=420,
                    labels={"budget_eur": "Budget (€)", "actor_label": ""},
                )
                fig_primary.update_layout(coloraxis_showscale=False, yaxis_title=None)
                render_plotly_chart(fig_primary, use_container_width=True)

        st.divider()

        results_scope_token = f"{R}||{W}"
        sync_results_table_state(results_scope_token)

        results_base_select_sql = f"""
        SELECT
          projectID,
          MIN(year) AS year,
          MIN(title) AS title,
          MIN(cordis_domain_ui) AS cordis_domain_ui,
          MIN(cordis_theme_primary) AS theme,
          MIN(scientific_subthemes) AS scientific_subthemes,
          MIN(project_status) AS project_status,
          COUNT(DISTINCT actor_id) AS n_actors,
          COUNT(DISTINCT country_name) AS n_countries,
          SUM(amount_eur) AS budget_eur
        FROM {R}
        WHERE {W}
        GROUP BY projectID
        """
        results_total = safe_fetch_df(f"SELECT COUNT(*) AS n_rows FROM ({results_base_select_sql}) q", columns=["n_rows"], lang=lang, warning_key="results_view_unavailable")
        total_matches = int(results_total["n_rows"].iloc[0] or 0) if not results_total.empty else 0

        if total_matches == 0:
            render_empty_state(lang)
        else:
            rc1, rc2, rc3 = st.columns([1, 1, 2])
            with rc1:
                rows_per_page = st.selectbox(
                    t(lang, "rows_per_page"),
                    [25, 50, 100, 250],
                    key="results_table_rows_per_page",
                )
            max_page = max(1, (int(total_matches) + int(rows_per_page) - 1) // int(rows_per_page))
            if int(st.session_state.get("results_table_page", 1)) > max_page:
                st.session_state["results_table_page"] = max_page
            with rc2:
                page = st.number_input(
                    t(lang, "page"),
                    min_value=1,
                    max_value=max_page,
                    step=1,
                    key="results_table_page",
                )
            with rc3:
                st.caption(f"{t(lang, 'n_projects')}: {total_matches:,}".replace(",", " "))
                st.caption(f"Page {int(page)} / {int(max_page)}")

            offset = (int(page) - 1) * int(rows_per_page)
            results_projects_raw = safe_fetch_df(f"""
            {results_base_select_sql}
            ORDER BY budget_eur DESC
            LIMIT {int(rows_per_page)} OFFSET {int(offset)}
            """, columns=["projectID", "year", "title", "cordis_domain_ui", "theme", "scientific_subthemes", "project_status", "n_actors", "n_countries", "budget_eur"], lang=lang, warning_key="results_view_unavailable")
            results_projects = results_projects_raw.copy()
            results_projects["cordis_domain_ui"] = results_projects["cordis_domain_ui"].map(lambda x: domain_raw_to_display(str(x), lang))
            results_projects["theme"] = results_projects["theme"].map(lambda x: theme_raw_to_display(str(x), lang))
            results_projects["scientific_subthemes"] = results_projects["scientific_subthemes"].map(lambda x: scientific_subthemes_compact(x, limit=2))
            results_projects["project_status"] = results_projects["project_status"].map(lambda x: status_raw_to_display(str(x), lang))
            results_projects["budget_eur"] = results_projects["budget_eur"].map(lambda x: fmt_money(float(x), lang))

            st.markdown("#### " + t(lang, "results_projects_table"))
            st.caption(t(lang, "results_projects_table_caption"))
            results_view_token = f"{results_scope_token}||page={int(page)}||rpp={int(rows_per_page)}"
            if st.session_state.get("results_selected_project_view_token") != results_view_token:
                st.session_state["results_selected_project_view_token"] = results_view_token
                st.session_state.pop("results_selected_project_id", None)
                st.session_state.pop("results_project_table_df", None)

            results_projects_display = results_projects.rename(
                columns={
                    "cordis_domain_ui": t(lang, "domains"),
                    "project_status": t(lang, "project_status"),
                    "budget_eur": t(lang, "budget_total"),
                    "n_actors": t(lang, "n_actors"),
                    "theme": t(lang, "themes"),
                    "scientific_subthemes": t(lang, "scientific_subthemes"),
                }
            )
            results_table_event = st.dataframe(
                results_projects_display,
                use_container_width=True,
                height=620,
                hide_index=True,
                key="results_project_table_df",
                on_select="rerun",
                selection_mode="single-row",
            )
            selected_rows = []
            try:
                selected_rows = list(getattr(getattr(results_table_event, "selection", None), "rows", []) or [])
            except Exception:
                selected_rows = []
            if selected_rows:
                row_idx = int(selected_rows[0])
                if 0 <= row_idx < len(results_projects_raw):
                    st.session_state["results_selected_project_id"] = str(results_projects_raw.iloc[row_idx]["projectID"])

            results_export_query_key = str(abs(hash(results_base_select_sql)))
            if st.session_state.get("results_table_full_export_query_key") != results_export_query_key:
                st.session_state.pop("results_table_full_export_bytes", None)
                st.session_state["results_table_full_export_query_key"] = results_export_query_key

            re1, re2 = st.columns(2)
            with re1:
                st.download_button(
                    t(lang, "download_page"),
                    results_projects_raw.to_csv(index=False).encode("utf-8"),
                    file_name="results_project_table_page.csv",
                    mime="text/csv",
                    key="results_table_download_page",
                )
            with re2:
                if st.button(t(lang, "prepare_full_export"), width="stretch", key="results_table_prepare_full_export"):
                    with st.spinner("Préparation de l’export..." if lang == "FR" else "Preparing export..."):
                        st.session_state["results_table_full_export_bytes"] = export_query_csv_bytes(
                            f"""
                            {results_base_select_sql}
                            ORDER BY budget_eur DESC
                            """
                        )

            if "results_table_full_export_bytes" in st.session_state:
                st.download_button(
                    t(lang, "download_full"),
                    st.session_state["results_table_full_export_bytes"],
                    file_name="results_project_table_full.csv",
                    mime="text/csv",
                    key="results_table_download_full",
                )

            selected_project_id = str(st.session_state.get("results_selected_project_id", "")).strip()
            if selected_project_id:
                detail_df = safe_fetch_df(f"""
                SELECT
                  projectID,
                  MIN(title) AS title,
                  MIN(acronym) AS acronym,
                  MIN(year) AS year,
                  MIN(program) AS program,
                  MIN(cordis_domain_ui) AS cordis_domain_ui,
                  MIN(cordis_theme_primary) AS theme,
                  MIN(scientific_subthemes) AS scientific_subthemes,
                  MIN(project_status) AS project_status,
                  SUM(amount_eur) AS budget_eur,
                  COUNT(DISTINCT actor_id) AS n_actors,
                  COUNT(DISTINCT country_name) AS n_countries
                FROM {R}
                WHERE {W} AND projectID IN {in_list([selected_project_id])}
                GROUP BY projectID
                LIMIT 1
                """, columns=["projectID", "title", "acronym", "year", "program", "cordis_domain_ui", "theme", "scientific_subthemes", "project_status", "budget_eur", "n_actors", "n_countries"], lang=lang, warning_key="results_view_unavailable")
                if detail_df.empty:
                    st.session_state.pop("results_selected_project_id", None)
                else:
                    detail = detail_df.iloc[0]
                    countries_df = safe_fetch_df(f"""
                    SELECT DISTINCT country_name
                    FROM {R}
                    WHERE {W} AND projectID IN {in_list([selected_project_id])}
                      AND country_name IS NOT NULL AND TRIM(country_name) <> ''
                    ORDER BY country_name
                    LIMIT 40
                    """, columns=["country_name"], lang=lang, warning_key="results_view_unavailable")
                    actors_df = safe_fetch_df(f"""
                    SELECT
                      actor_id,
                      COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
                      COALESCE(NULLIF(TRIM(country_name), ''), 'Unknown') AS country_name,
                      SUM(amount_eur) AS budget_eur
                    FROM {R}
                    WHERE {W} AND projectID IN {in_list([selected_project_id])}
                      AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
                    GROUP BY actor_id, actor_label, country_name
                    ORDER BY budget_eur DESC, actor_label
                    LIMIT 30
                    """, columns=["actor_id", "actor_label", "country_name", "budget_eur"], lang=lang, warning_key="results_view_unavailable")

                    countries_list = [str(x) for x in countries_df["country_name"].tolist() if str(x).strip()] if not countries_df.empty else []
                    if countries_list:
                        country_select_key = f"results_detail_country_select::{selected_project_id}"
                        st.session_state.setdefault(country_select_key, countries_list[0])
                        if st.session_state.get(country_select_key) not in countries_list:
                            st.session_state[country_select_key] = countries_list[0]

                    st.divider()
                    st.markdown("#### " + ("Détail projet" if lang == "FR" else "Project detail"))
                    title_txt = str(detail.get("title") or selected_project_id).strip() or selected_project_id
                    st.caption(("Projet sélectionné depuis la table de résultats." if lang == "FR" else "Project selected from the results table."))
                    st.markdown(f"**{title_txt}**")
                    if str(detail.get("acronym") or "").strip():
                        st.caption(f"Acronym: {str(detail.get('acronym') or '').strip()}")

                    dc1, dc2, dc3, dc4 = st.columns(4)
                    dc1.metric(t(lang, "budget_total"), fmt_money(float(detail.get("budget_eur") or 0.0), lang))
                    dc2.metric(t(lang, "n_actors"), f"{int(detail.get('n_actors') or 0):,}".replace(",", " "))
                    dc3.metric(t(lang, "countries"), f"{int(detail.get('n_countries') or 0):,}".replace(",", " "))
                    dc4.metric(("Année" if lang == "FR" else "Year"), str(int(detail.get("year") or 0)) if pd.notna(detail.get("year")) else "—")

                    meta1, meta2, meta3, meta4 = st.columns(4)
                    with meta1:
                        st.caption(f"**{t(lang, 'programmes')}**")
                        st.write(str(detail.get("program") or "—"))
                    with meta2:
                        st.caption(f"**{t(lang, 'domains')}**")
                        st.write(domain_raw_to_display(str(detail.get("cordis_domain_ui") or ""), lang) if str(detail.get("cordis_domain_ui") or "").strip() else "—")
                    with meta3:
                        st.caption(f"**{t(lang, 'themes')}**")
                        st.write(theme_raw_to_display(str(detail.get("theme") or ""), lang) if str(detail.get("theme") or "").strip() else "—")
                    with meta4:
                        st.caption(f"**{t(lang, 'project_status')}**")
                        st.write(status_raw_to_display(str(detail.get("project_status") or ""), lang) if str(detail.get("project_status") or "").strip() else "—")

                    subthemes_compact = scientific_subthemes_compact(detail.get("scientific_subthemes"), limit=4)
                    if subthemes_compact:
                        st.caption(f"**{t(lang, 'scientific_subthemes')}**")
                        st.write(subthemes_compact)

                    info1, info2 = st.columns([1.3, 1.7])
                    with info1:
                        st.caption(f"**{t(lang, 'countries')}**")
                        if countries_list:
                            st.write(", ".join(countries_list))
                        else:
                            st.write("—")
                    with info2:
                        st.caption("**Actors involved**" if lang != "FR" else "**Acteurs impliqués**")
                        if actors_df.empty:
                            st.write("—")
                        else:
                            actor_lines = []
                            for _, row in actors_df.head(10).iterrows():
                                actor_lines.append(
                                    f"- {str(row['actor_label'])} ({str(row['country_name'])}) — {fmt_money(float(row['budget_eur'] or 0.0), lang)}"
                                )
                            st.markdown("\n".join(actor_lines))
                            if len(actors_df) > 10:
                                more_n = int(len(actors_df) - 10)
                                st.caption((f"+{more_n} autres acteurs" if lang == "FR" else f"+{more_n} more actors"))

                    st.caption(("Actions rapides" if lang == "FR" else "Quick actions"))
                    qa1, qa2, qa3 = st.columns(3)
                    with qa1:
                        if countries_list:
                            st.selectbox(
                                t(lang, "countries"),
                                countries_list,
                                key=country_select_key,
                                label_visibility="collapsed",
                            )
                            if st.button(("Ouvrir dans Géographie" if lang == "FR" else "Open in Geography"), key=f"results_geo_country_btn::{selected_project_id}"):
                                selected_country = str(st.session_state.get(country_select_key, "")).strip()
                                if selected_country:
                                    st.session_state["f_countries"] = [selected_country]
                                    queue_tab_navigation(top_target=t(lang, "tab_markets"))
                                    st.rerun()
                        else:
                            st.caption("—")
                    with qa2:
                        project_theme_raw = str(detail.get("theme") or "").strip()
                        if project_theme_raw:
                            if st.button(("Filtrer ce thème" if lang == "FR" else "Filter this theme"), key=f"results_filter_theme_btn::{selected_project_id}"):
                                st.session_state["f_themes_raw"] = [project_theme_raw]
                                st.rerun()
                        else:
                            st.caption("—")
                    with qa3:
                        project_program = str(detail.get("program") or "").strip()
                        if project_program:
                            if st.button(("Filtrer ce programme" if lang == "FR" else "Filter this programme"), key=f"results_filter_program_btn::{selected_project_id}"):
                                st.session_state["f_programmes"] = [project_program]
                                st.rerun()
                        else:
                            st.caption("—")

                    if not actors_df.empty:
                        actors_hook = actors_df.copy()
                        actors_hook["actor_display"] = actors_hook["actor_label"].astype(str) + " — " + actors_hook["country_name"].astype(str)
                        actor_hook_key = f"results_actor_hook_select::{selected_project_id}"
                        actor_options = actors_hook["actor_display"].astype(str).tolist()
                        ah1, ah2 = st.columns([1.8, 1.0])
                        with ah1:
                            st.selectbox(
                                ("Acteur à explorer ensuite" if lang == "FR" else "Actor to explore next"),
                                actor_options,
                                key=actor_hook_key,
                            )
                        with ah2:
                            if app_mode == "advanced" and st.button(("Ouvrir dans Acteurs" if lang == "FR" else "Open in Actors"), key=f"results_actor_drill_btn::{selected_project_id}"):
                                chosen_display = str(st.session_state.get(actor_hook_key, actor_options[0])) if actor_options else ""
                                if chosen_display:
                                    chosen_row = actors_hook[actors_hook["actor_display"].astype(str) == chosen_display].head(1)
                                    if not chosen_row.empty:
                                        chosen_actor_id = str(chosen_row.iloc[0]["actor_id"])
                                        st.session_state["results_selected_actor_id_candidate"] = chosen_actor_id
                                        st.session_state["results_drilldown_actor_id"] = chosen_actor_id
                                        queue_tab_navigation(
                                            top_target=t(lang, "tab_actors_hub"),
                                            actor_sub_target=t(lang, "tab_actor"),
                                        )
                                        st.rerun()
                        chosen_display = str(st.session_state.get(actor_hook_key, actor_options[0])) if actor_options else ""
                        if chosen_display:
                            chosen_row = actors_hook[actors_hook["actor_display"].astype(str) == chosen_display].head(1)
                            if not chosen_row.empty:
                                st.session_state["results_selected_actor_id_candidate"] = str(chosen_row.iloc[0]["actor_id"])

            st.divider()
            st.markdown("#### " + t(lang, "results_next_steps"))
            next_cols = st.columns(3 if app_mode == "advanced" else 2)
            n1 = next_cols[0]
            n2 = next_cols[1] if app_mode == "advanced" else None
            n3 = next_cols[2] if app_mode == "advanced" else next_cols[1]
            with n1:
                if st.button(t(lang, "results_next_geo"), key="results_next_geo_btn", width="stretch"):
                    queue_tab_navigation(top_target=t(lang, "tab_markets"))
                    st.rerun()
            if app_mode == "advanced" and n2 is not None:
                with n2:
                    if st.button(t(lang, "results_next_actors"), key="results_next_actors_btn", width="stretch"):
                        queue_tab_navigation(top_target=t(lang, "tab_actors_hub"))
                        st.rerun()
            with n3:
                if st.button(t(lang, "results_next_trends"), key="results_next_trends_btn", width="stretch"):
                    queue_tab_navigation(top_target=t(lang, "tab_trends_events"))
                    st.rerun()

            st.divider()
            with st.expander(t(lang, "explore_overview_title"), expanded=False):
                st.markdown(
                    "\n".join(
                        [
                            f"- {t(lang, 'explore_overview_1')}",
                            f"- {t(lang, 'explore_overview_2')}",
                            f"- {t(lang, 'explore_overview_3')}",
                            f"- {t(lang, 'explore_overview_4')}",
                            f"- {t(lang, 'explore_overview_5')}",
                        ]
                    )
                )
                st.caption(t(lang, "explore_overview_tip"))

            st.caption(t(lang, "results_other_views_caption"))
            results_view = st.radio(
                t(lang, "results_other_views"),
                [
                    t(lang, "results_trend"),
                    t(lang, "results_map"),
                    t(lang, "results_actors"),
                ],
                horizontal=True,
                index=0,
                key="results_secondary_view_mode",
            )

            if results_view == t(lang, "results_trend"):
                res_year = safe_fetch_df(f"""
                SELECT
                  year,
                  SUM(amount_eur) AS budget_eur,
                  COUNT(DISTINCT projectID) AS n_projects
                FROM {R}
                WHERE {W}
                GROUP BY year
                ORDER BY year
                """, columns=["year", "budget_eur", "n_projects"], lang=lang, warning_key="results_view_unavailable")
                if res_year.empty:
                    render_empty_state(lang)
                else:
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("#### " + t(lang, "results_budget_year"))
                        fig_year_budget = px.bar(
                            res_year,
                            x="year",
                            y="budget_eur",
                            color="budget_eur",
                            color_continuous_scale=R2G,
                            height=360,
                            labels={"budget_eur": "Budget (€)", "year": "Year"},
                        )
                        fig_year_budget.update_layout(
                            coloraxis_showscale=False,
                            paper_bgcolor=PANEL_BG,
                            plot_bgcolor=PANEL_BG,
                            font=dict(color=TEXT_PRIMARY),
                            xaxis=dict(gridcolor=GRID_COLOR, tickfont=dict(color=TEXT_SECONDARY)),
                            yaxis=dict(gridcolor=GRID_COLOR, tickfont=dict(color=TEXT_SECONDARY)),
                        )
                        render_plotly_chart(fig_year_budget, use_container_width=True)
                    with c2:
                        st.markdown("#### " + t(lang, "results_projects_year"))
                        fig_year_projects = px.line(
                            res_year,
                            x="year",
                            y="n_projects",
                            markers=True,
                            height=360,
                            labels={"n_projects": t(lang, "n_projects"), "year": "Year"},
                        )
                        fig_year_projects.update_layout(
                            paper_bgcolor=PANEL_BG,
                            plot_bgcolor=PANEL_BG,
                            font=dict(color=TEXT_PRIMARY),
                            xaxis=dict(gridcolor=GRID_COLOR, tickfont=dict(color=TEXT_SECONDARY)),
                            yaxis=dict(gridcolor=GRID_COLOR, tickfont=dict(color=TEXT_SECONDARY)),
                        )
                        render_plotly_chart(fig_year_projects, use_container_width=True)

            elif results_view == t(lang, "results_map"):
                geo_res = safe_fetch_df(f"""
                SELECT country_alpha3, country_name, SUM(amount_eur) AS amount_eur
                FROM {R}
                WHERE {W} AND country_alpha3 IS NOT NULL AND TRIM(country_alpha3) <> ''
                GROUP BY country_alpha3, country_name
                ORDER BY amount_eur DESC
                """, columns=["country_alpha3", "country_name", "amount_eur"], lang=lang, warning_key="results_view_unavailable")
                if geo_res.empty:
                    render_empty_state(lang)
                else:
                    geo_res["population"] = geo_res["country_alpha3"].map(POPULATION_BY_ALPHA3).astype(float)
                    geo_res["amount_per_million"] = np.where(
                        geo_res["population"].notna() & (geo_res["population"] > 0),
                        geo_res["amount_eur"].astype(float) / (geo_res["population"] / 1_000_000.0),
                        np.nan,
                    )
                    metric_choice = st.radio(
                        t(lang, "geo_metric"),
                        [t(lang, "geo_metric_total"), t(lang, "geo_metric_per_million")],
                        horizontal=True,
                        index=1,
                        key="results_geo_metric",
                    )
                    color_col = "amount_per_million" if metric_choice == t(lang, "geo_metric_per_million") else "amount_eur"
                    color_title = "€ / M hab." if color_col == "amount_per_million" else "Budget (€)"
                    fig_results_map = px.choropleth(
                        geo_res,
                        locations="country_alpha3",
                        color=color_col,
                        hover_name="country_name",
                        color_continuous_scale=R2G,
                        height=520,
                        labels={color_col: color_title},
                    )
                    fig_results_map.update_traces(
                        marker_line_color="rgba(208, 216, 228, 0.22)",
                        marker_line_width=0.7,
                    )
                    fig_results_map.update_geos(
                        scope="europe",
                        projection_type="natural earth",
                        showframe=False,
                        showland=True,
                        landcolor="#18263D",
                        showcountries=True,
                        countrycolor="rgba(208, 216, 228, 0.18)",
                        showcoastlines=True,
                        coastlinecolor="rgba(208, 216, 228, 0.16)",
                        showocean=True,
                        oceancolor=APP_BG,
                        showlakes=True,
                        lakecolor=APP_BG,
                        bgcolor=PANEL_BG,
                    )
                    fig_results_map.update_layout(
                        coloraxis_colorbar=dict(
                            title=color_title,
                            len=0.7,
                            bgcolor=LEGEND_BG,
                            outlinecolor=BORDER,
                            tickcolor=TEXT_MUTED,
                            tickfont=dict(color=TEXT_SECONDARY),
                        ),
                        margin=dict(l=0, r=0, t=0, b=0),
                        paper_bgcolor=PANEL_BG,
                        plot_bgcolor=PANEL_BG,
                        font=dict(color=TEXT_PRIMARY),
                    )
                    render_plotly_chart(fig_results_map, use_container_width=True)

                    st.markdown("#### " + t(lang, "results_country_rank"))
                    rank_geo = geo_res[geo_res[color_col].notna()].sort_values(color_col, ascending=False).head(15).copy()
                    rank_geo[color_col] = rank_geo[color_col].map(
                        lambda x: fmt_money(float(x), lang) if color_col == "amount_eur" else f"{float(x):,.0f} € / M".replace(",", " ")
                    )
                    st.dataframe(
                        rank_geo.rename(columns={color_col: color_title})[["country_name", color_title]],
                        use_container_width=True,
                        height=360,
                    )

            else:
                res_actors = safe_fetch_df(f"""
                WITH x AS (
                  SELECT
                    actor_id,
                    COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
                    COALESCE(NULLIF(TRIM(country_name), ''), 'Unknown') AS country_name2,
                    amount_eur,
                    projectID
                  FROM {R}
                  WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
                )
                SELECT
                  actor_id,
                  MIN(actor_label) AS actor_label,
                  MIN(country_name2) AS main_country,
                  SUM(amount_eur) AS budget_eur,
                  COUNT(DISTINCT projectID) AS n_projects
                FROM x
                GROUP BY actor_id
                ORDER BY budget_eur DESC
                LIMIT 25
                """, columns=["actor_id", "actor_label", "main_country", "budget_eur", "n_projects"], lang=lang, warning_key="results_view_unavailable")
                if res_actors.empty:
                    render_empty_state(lang)
                else:
                    res_actors["avg_per_project"] = np.where(
                        res_actors["n_projects"].astype(float) > 0,
                        res_actors["budget_eur"].astype(float) / res_actors["n_projects"].astype(float),
                        np.nan,
                    )
                    fig_actors = px.bar(
                        res_actors.iloc[::-1],
                        x="budget_eur",
                        y="actor_label",
                        orientation="h",
                        color="budget_eur",
                        color_continuous_scale=R2G,
                        height=620,
                        labels={"budget_eur": "Budget (€)", "actor_label": ""},
                    )
                    fig_actors.update_layout(
                        coloraxis_showscale=False,
                        yaxis_title=None,
                        paper_bgcolor=PANEL_BG,
                        plot_bgcolor=PANEL_BG,
                        font=dict(color=TEXT_PRIMARY),
                        xaxis=dict(gridcolor=GRID_COLOR, tickfont=dict(color=TEXT_SECONDARY)),
                        yaxis=dict(gridcolor=GRID_COLOR, tickfont=dict(color=TEXT_SECONDARY)),
                    )
                    render_plotly_chart(fig_actors, use_container_width=True)

                    actor_tbl = res_actors.copy()
                    actor_tbl["budget_eur"] = actor_tbl["budget_eur"].map(lambda x: fmt_money(float(x), lang))
                    actor_tbl["avg_per_project"] = actor_tbl["avg_per_project"].map(lambda x: fmt_money(float(x), lang))
                    st.markdown("#### " + t(lang, "results_actor_table"))
                    st.dataframe(
                        actor_tbl.rename(
                            columns={
                                "main_country": t(lang, "countries"),
                                "budget_eur": t(lang, "budget_total"),
                                "avg_per_project": t(lang, "avg_ticket"),
                            }
                        )[["actor_label", t(lang, "countries"), t(lang, "budget_total"), "n_projects", t(lang, "avg_ticket")]],
                        use_container_width=True,
                        height=360,
                    )


# ============================================================
# TAB OVERVIEW (DuckDB)
# ============================================================
if app_mode == "advanced" and tab_overview is not None:
    with tab_overview:
        render_section_header("◌", t(lang, "sub_overview"), t(lang, "overview_caption"), t(lang, "tab_explorer"))
        st.caption(t(lang, "overview_support_note"))

        st.markdown("#### " + ("Allocation du budget par type d’entité" if lang == "FR" else "Budget allocation by entity type"))
        alloc = safe_fetch_df(f"""
        SELECT entity_type, SUM(amount_eur) AS amount_eur
        FROM {R}
        WHERE {W}
        GROUP BY entity_type
        ORDER BY amount_eur DESC
        """, columns=["entity_type", "amount_eur"], lang=lang, warning_key="results_view_unavailable")
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
            render_plotly_chart(fig_alloc, use_container_width=True)
        with st.expander(t(lang, "overview_more_context"), expanded=False):
            status_mix = fetch_df(f"""
            SELECT
              project_status,
              SUM(amount_eur) AS budget_eur,
              COUNT(DISTINCT projectID) AS n_projects
            FROM {R}
            WHERE {W}
            GROUP BY project_status
            ORDER BY budget_eur DESC
            """)
            if not status_mix.empty:
                status_mix["status_display"] = status_mix["project_status"].map(lambda x: status_raw_to_display(str(x), lang))
                s1, s2 = st.columns(2)
                with s1:
                    st.markdown("#### " + t(lang, "status_budget_title"))
                    fig_status_budget = px.bar(
                        status_mix.iloc[::-1],
                        x="budget_eur",
                        y="status_display",
                        orientation="h",
                        color="budget_eur",
                        color_continuous_scale=R2G,
                        height=260,
                        labels={"budget_eur": "Budget (€)", "status_display": ""},
                    )
                    fig_status_budget.update_layout(coloraxis_showscale=False, yaxis_title=None)
                    render_plotly_chart(fig_status_budget, use_container_width=True)
                with s2:
                    st.markdown("#### " + t(lang, "status_projects_title"))
                    fig_status_projects = px.bar(
                        status_mix.iloc[::-1],
                        x="n_projects",
                        y="status_display",
                        orientation="h",
                        color="n_projects",
                        color_continuous_scale=R2G,
                        height=260,
                        labels={"n_projects": t(lang, "n_projects"), "status_display": ""},
                    )
                    fig_status_projects.update_layout(coloraxis_showscale=False, yaxis_title=None)
                    render_plotly_chart(fig_status_projects, use_container_width=True)

            st.divider()
            st.markdown("#### " + t(lang, "insights_title"))
            insights: List[str] = []
            try:
                top_theme = fetch_df(f"""
                -- Current build stores one inferred theme label per row/project view.
                SELECT theme, SUM(amount_eur) AS b
                FROM {R}
                WHERE {W}
                GROUP BY theme
                ORDER BY b DESC
                LIMIT 1
                """)
                if not top_theme.empty:
                    th = theme_raw_to_display(str(top_theme["theme"].iloc[0]), lang)
                    insights.append(
                        (
                            f"Thème leader: **{th}** ({fmt_money(float(top_theme['b'].iloc[0]), lang)})."
                            if lang == "FR"
                            else f"Leading theme: **{th}** ({fmt_money(float(top_theme['b'].iloc[0]), lang)})."
                        )
                    )
            except Exception:
                pass
            try:
                top_actor = fetch_df(f"""
                SELECT COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label, SUM(amount_eur) AS b
                FROM {R}
                WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
                GROUP BY actor_label
                ORDER BY b DESC
                LIMIT 1
                """)
                if not top_actor.empty:
                    insights.append(
                        (
                            f"Acteur principal: **{str(top_actor['actor_label'].iloc[0])[:64]}** ({fmt_money(float(top_actor['b'].iloc[0]), lang)})."
                            if lang == "FR"
                            else f"Top actor: **{str(top_actor['actor_label'].iloc[0])[:64]}** ({fmt_money(float(top_actor['b'].iloc[0]), lang)})."
                        )
                    )
            except Exception:
                pass
            try:
                yoy = fetch_df(f"""
                WITH y AS (
                  SELECT year, SUM(amount_eur) AS b
                  FROM {R}
                  WHERE {W}
                  GROUP BY year
                ),
                z AS (
                  SELECT year, b, LAG(b) OVER (ORDER BY year) AS prev_b
                  FROM y
                )
                SELECT year, b, prev_b
                FROM z
                WHERE prev_b IS NOT NULL
                ORDER BY year DESC
                LIMIT 1
                """)
                if not yoy.empty:
                    curr = float(yoy["b"].iloc[0] or 0.0)
                    prev = float(yoy["prev_b"].iloc[0] or 0.0)
                    delta = ((curr / prev) - 1.0) * 100.0 if prev > 0 else 0.0
                    insights.append(
                        (
                            f"Variation annuelle la plus récente: **{delta:+.1f}%**."
                            if lang == "FR"
                            else f"Most recent annual change: **{delta:+.1f}%**."
                        )
                    )
            except Exception:
                pass
            if insights:
                for row in insights:
                    st.markdown(f"- {row}")
            else:
                st.caption(t(lang, "no_data"))

            st.divider()
            with st.expander(t(lang, "overview_yearly_extra"), expanded=False):
                st.markdown("#### " + t(lang, "ticket_shape_title"))
                tb = fetch_df(f"""
                SELECT year, projectID, SUM(amount_eur) AS proj_budget
                FROM {R}
                WHERE {W}
                GROUP BY year, projectID
                HAVING SUM(amount_eur) > 0
                """)
                if tb.empty:
                    st.info(t(lang, "no_data"))
                else:
                    tb["year"] = tb["year"].astype(int)
                    yearly_ticket = (
                        tb.groupby("year", as_index=False)
                        .agg(
                            total_budget=("proj_budget", "sum"),
                            median_budget=("proj_budget", "median"),
                            n_projects=("projectID", "nunique"),
                        )
                        .sort_values("year")
                    )
                    fig_budget = px.bar(
                        yearly_ticket,
                        x="year",
                        y="total_budget",
                        color="total_budget",
                        color_continuous_scale=R2G,
                        height=320,
                        labels={"year": "Year", "total_budget": "Budget (€)"},
                    )
                    fig_budget.update_traces(
                        customdata=np.stack(
                            [
                                yearly_ticket["total_budget"].astype(float).apply(lambda x: fmt_money(float(x), lang)).values,
                                yearly_ticket["n_projects"].astype(int).values,
                            ],
                            axis=-1,
                        ),
                        hovertemplate="<b>%{x}</b><br>Budget: %{customdata[0]}<br>Projects: %{customdata[1]}<extra></extra>",
                    )
                    fig_budget.update_layout(
                        showlegend=False,
                        coloraxis_showscale=False,
                        margin=dict(l=20, r=20, t=10, b=10),
                    )
                    render_plotly_chart(fig_budget, use_container_width=True)

                    fig_median = px.bar(
                        yearly_ticket,
                        x="year",
                        y="median_budget",
                        color="median_budget",
                        color_continuous_scale=R2G,
                        height=280,
                        labels={"year": "Year", "median_budget": t(lang, "ticket_shape_median") + " (€)"},
                    )
                    fig_median.update_traces(
                        customdata=np.stack(
                            [
                                yearly_ticket["median_budget"].astype(float).apply(lambda x: fmt_money(float(x), lang)).values,
                                yearly_ticket["n_projects"].astype(int).values,
                            ],
                            axis=-1,
                        ),
                        hovertemplate="<b>%{x}</b><br>Median ticket: %{customdata[0]}<br>Projects: %{customdata[1]}<extra></extra>",
                    )
                    fig_median.update_layout(
                        showlegend=False,
                        coloraxis_showscale=False,
                        margin=dict(l=20, r=20, t=10, b=10),
                    )
                    render_plotly_chart(fig_median, use_container_width=True)
                    st.caption(t(lang, "ticket_shape_caption"))


# ============================================================
# TAB GEO (DuckDB)
# ============================================================
with tab_geo:
    render_section_header("◎", t(lang, "tab_geo"), t(lang, "geo_caption"), t(lang, "tab_markets"))
    st.caption(f"{t(lang, 'scope_caption')}: " + " · ".join(scope_items))
    geo_default_scope = european_countries_present(meta["countries"])
    current_geo_scope = [str(x) for x in st.session_state.get("f_countries", []) if str(x).strip()]
    if geo_default_scope and set(current_geo_scope) == set(geo_default_scope):
        st.caption(t(lang, "geo_perimeter_default"))
    else:
        st.caption(t(lang, "geo_perimeter_custom"))
    def _apply_geo_country_preset(countries_list):
        st.session_state["f_countries"] = list(countries_list)

    _geo_eu27 = eu27_countries_present(meta["countries"])
    _geo_assoc = associated_countries_present(meta["countries"])
    _geo_eu_plus = list(dict.fromkeys(_geo_eu27 + _geo_assoc))

    geo_preset_c1, geo_preset_c2, geo_preset_c3 = st.columns(3)
    with geo_preset_c1:
        st.button(t(lang, "country_preset_eu27"), key="geo_country_eu27_btn", width="stretch",
                  on_click=_apply_geo_country_preset, args=(_geo_eu27,))
    with geo_preset_c2:
        st.button(t(lang, "country_preset_associated"), key="geo_country_associated_btn", width="stretch",
                  on_click=_apply_geo_country_preset, args=(_geo_eu_plus,))
    with geo_preset_c3:
        st.button(t(lang, "country_preset_all"), key="geo_country_all_btn", width="stretch",
                  on_click=_apply_geo_country_preset, args=(list(meta["countries"]),))
    geo = safe_fetch_df(f"""
    SELECT country_alpha3, country_name, SUM(amount_eur) AS amount_eur
    FROM {R}
    WHERE {W} AND country_alpha3 IS NOT NULL AND TRIM(country_alpha3) <> ''
    GROUP BY country_alpha3, country_name
    ORDER BY amount_eur DESC
    """, columns=["country_alpha3", "country_name", "amount_eur"], lang=lang, warning_key="geo_view_unavailable")
    if geo.empty:
        render_guided_empty_state(lang, "geo_empty_hint")
    else:
        geo["population"] = geo["country_alpha3"].map(POPULATION_BY_ALPHA3).astype(float)
        geo["amount_per_million"] = np.where(
            geo["population"].notna() & (geo["population"] > 0),
            geo["amount_eur"].astype(float) / (geo["population"] / 1_000_000.0),
            np.nan,
        )
        geo["budget_str"] = geo["amount_eur"].apply(lambda v: fmt_money(float(v), lang))
        geo["per_million_str"] = geo["amount_per_million"].apply(
            lambda v: ("—" if pd.isna(v) else (f"{v:,.0f} € / M hab.".replace(",", " ")))
        )

        zoom_opts = ["Auto", "Europe", "World", "Africa", "Asia", "North America", "South America", "Oceania"]
        c1, c2 = st.columns([1.1, 1.4])
        with c1:
            metric_mode = st.selectbox(
                t(lang, "geo_metric"),
                [t(lang, "geo_metric_total"), t(lang, "geo_metric_per_million")],
                index=1,
            )

        is_per_million = metric_mode == t(lang, "geo_metric_per_million")
        color_col = "amount_per_million" if is_per_million else "amount_eur"
        color_title = "€ / M hab." if is_per_million else "Budget (€)"
        geo_rank = geo[geo[color_col].notna()].copy() if is_per_million else geo.copy()
        geo_rank = geo_rank.sort_values(color_col, ascending=False).reset_index(drop=True)
        geo_rank["rank"] = np.arange(1, len(geo_rank) + 1)

        if not geo_rank.empty:
            top_country_name = str(geo_rank.iloc[0]["country_name"])
            second_country_name = str(geo_rank.iloc[1]["country_name"]) if len(geo_rank) > 1 else ""
            with st.container(border=True):
                st.markdown("**" + t(lang, "geo_summary_title") + "**")
                if second_country_name:
                    st.write(
                        t(lang, "geo_summary_multi").format(
                            first=top_country_name,
                            second=second_country_name,
                            count=f"{len(geo_rank):,}".replace(",", " "),
                        )
                    )
                else:
                    st.write(t(lang, "geo_summary_single").format(first=top_country_name))

        country_options = geo_rank["country_name"].astype(str).tolist()
        country_scope_token = f"{W}||{'|'.join(country_options)}"
        preferred_geo_country = country_options[0] if country_options else ""
        drilldown_country = ""
        active_country_filters = [str(x) for x in st.session_state.get("f_countries", []) if str(x).strip()]
        if len(active_country_filters) == 1 and active_country_filters[0] in country_options:
            drilldown_country = active_country_filters[0]

        if st.session_state.get("geo_scope_token") != country_scope_token:
            st.session_state["geo_scope_token"] = country_scope_token
            if drilldown_country:
                st.session_state["geo_selected_country"] = drilldown_country
            elif preferred_geo_country:
                st.session_state["geo_selected_country"] = preferred_geo_country
        else:
            current_geo_country = str(st.session_state.get("geo_selected_country", "")).strip()
            if drilldown_country and current_geo_country != drilldown_country:
                st.session_state["geo_selected_country"] = drilldown_country
            elif preferred_geo_country and current_geo_country not in country_options:
                st.session_state["geo_selected_country"] = preferred_geo_country

        with c2:
            if country_options:
                st.selectbox(
                    t(lang, "geo_country_picker"),
                    country_options,
                    key="geo_selected_country",
                )

        if not st.session_state.get("_geo_zoom_default_auto_v1", False):
            st.session_state["geo_zoom"] = "Auto"
            st.session_state["_geo_zoom_default_auto_v1"] = True

        with st.expander(t(lang, "geo_advanced_options"), expanded=False):
            a, d, e = st.columns([1.2, 1.2, 1.4])
            with a:
                zoom = st.selectbox(t(lang, "zoom_on"), zoom_opts, index=0, key="geo_zoom")
            with d:
                show_borders = st.checkbox(t(lang, "borders"), value=True, key="geo_borders")
            with e:
                show_labels = st.checkbox(t(lang, "labels"), value=False, key="geo_labels")

        selected_country = str(st.session_state.get("geo_selected_country", "")).strip() if country_options else ""
        selected_geo_row = geo[geo["country_name"].astype(str) == selected_country].head(1)
        selected_rank_row = geo_rank[geo_rank["country_name"].astype(str) == selected_country].head(1)
        selected_total_budget = float(selected_geo_row.iloc[0]["amount_eur"] or 0.0) if not selected_geo_row.empty else 0.0
        selected_per_million = (
            float(selected_geo_row.iloc[0]["amount_per_million"])
            if not selected_geo_row.empty and pd.notna(selected_geo_row.iloc[0]["amount_per_million"])
            else np.nan
        )
        selected_rank = int(selected_rank_row.iloc[0]["rank"]) if not selected_rank_row.empty else 0
        selected_scope_share = (selected_total_budget / float(geo["amount_eur"].sum())) if float(geo["amount_eur"].sum() or 0.0) > 0 else 0.0

        if selected_country:
            st.markdown(f"#### {t(lang, 'geo_selected_summary')} · {selected_country}")
            g1, g2, g3, g4 = st.columns(4)
            g1.metric(t(lang, "geo_rank"), f"{selected_rank}" if selected_rank else "—")
            g2.metric(t(lang, "budget_total"), fmt_money(selected_total_budget, lang))
            g3.metric(
                t(lang, "geo_metric_per_million"),
                "—" if pd.isna(selected_per_million) else f"{selected_per_million:,.0f} € / M".replace(",", " "),
            )
            g4.metric(t(lang, "geo_scope_share"), fmt_pct(selected_scope_share, 1))
            gq1, gq2 = st.columns(2)
            with gq1:
                if st.button(t(lang, "geo_open_results"), key=f"geo_open_results::{selected_country}", width="stretch"):
                    st.session_state["f_countries"] = [selected_country]
                    queue_tab_navigation(top_target=t(lang, "tab_explorer"))
                    st.rerun()
            with gq2:
                if st.button(t(lang, "geo_open_trends"), key=f"geo_open_trends::{selected_country}", width="stretch"):
                    st.session_state["f_countries"] = [selected_country]
                    queue_tab_navigation(top_target=t(lang, "tab_trends_events"), trends_sub_target=t(lang, "tab_trends"))
                    st.rerun()

        if is_per_million and geo["amount_per_million"].isna().any():
            st.caption(t(lang, "geo_pop_missing"))

        fig_map = px.choropleth(
            geo,
            locations="country_alpha3",
            color=color_col,
            hover_name="country_name",
            color_continuous_scale=R2G,
            height=640,
            labels={color_col: color_title},
        )
        fig_map.update_traces(
            customdata=np.stack([geo["budget_str"], geo["per_million_str"]], axis=-1),
            hovertemplate=(
                "<b>%{hovertext}</b>"
                "<br>Budget: %{customdata[0]}"
                "<br>Budget / M hab.: %{customdata[1]}"
                "<extra></extra>"
            ),
            marker_line_color="rgba(208, 216, 228, 0.22)",
            marker_line_width=0.75,
        )

        geo_kwargs = dict(
            projection_type="natural earth",
            showframe=False,
            bgcolor=PANEL_BG,
            showland=True,
            landcolor="#18263D",
            showocean=True,
            oceancolor=APP_BG,
            showlakes=True,
            lakecolor=APP_BG,
        )
        if show_borders:
            geo_kwargs.update(
                dict(
                    showcoastlines=True,
                    coastlinecolor="rgba(208, 216, 228, 0.20)",
                    coastlinewidth=0.8,
                    showcountries=True,
                    countrycolor="rgba(208, 216, 228, 0.22)",
                    countrywidth=0.75,
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
                    textfont=dict(size=12, color=MAP_LABEL_COLOR),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

        fig_map.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            coloraxis_colorbar=dict(
                title=color_title,
                len=0.7,
                bgcolor=LEGEND_BG,
                outlinecolor=BORDER,
                tickcolor=TEXT_MUTED,
                tickfont=dict(color=TEXT_SECONDARY),
            ),
            paper_bgcolor=PANEL_BG,
            plot_bgcolor=PANEL_BG,
            font=dict(color=TEXT_PRIMARY),
        )

        st.caption(t(lang, "geo_primary_reading"))
        geo_map_col, geo_rank_col = st.columns([1.6, 1.0])
        with geo_map_col:
            render_plotly_chart(fig_map, use_container_width=True, config={"scrollZoom": True})
        with geo_rank_col:
            st.markdown(f"#### {t(lang, 'geo_rank_table')}")
            top_c = geo_rank.copy()
            if top_c.empty:
                render_guided_empty_state(lang, "geo_empty_hint")
            else:
                total_geo_budget = float(geo["amount_eur"].sum() or 0.0)
                top_c["country_display"] = np.where(
                    top_c["country_name"].astype(str) == selected_country,
                    "→ " + top_c["country_name"].astype(str),
                    top_c["country_name"].astype(str),
                )
                if is_per_million:
                    top_c["metric_display"] = top_c["amount_per_million"].apply(
                        lambda v: "—" if pd.isna(v) else f"{float(v):,.0f} € / M".replace(",", " ")
                    )
                    metric_label = t(lang, "geo_metric_per_million")
                else:
                    top_c["metric_display"] = top_c["amount_eur"].apply(lambda v: fmt_money(float(v), lang))
                    metric_label = t(lang, "budget_total")
                top_c["share_display"] = top_c["amount_eur"].apply(
                    lambda v: fmt_pct(float(v) / total_geo_budget, 1) if total_geo_budget > 0 else "—"
                )
                st.dataframe(
                    top_c[["rank", "country_display", "metric_display", "share_display"]].rename(
                        columns={
                            "rank": "#",
                            "country_display": t(lang, "countries"),
                            "metric_display": metric_label,
                            "share_display": t(lang, "geo_scope_share"),
                        }
                    ),
                    use_container_width=True,
                    height=640,
                    hide_index=True,
                )

        if selected_country:
            country_sql = in_list([selected_country])
            country_actors = safe_fetch_df(f"""
            SELECT
              actor_id,
              COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
              SUM(amount_eur) AS budget_eur,
              COUNT(DISTINCT projectID) AS n_projects
            FROM {R}
            WHERE {W} AND country_name IN {country_sql}
              AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
            GROUP BY actor_id, actor_label
            ORDER BY budget_eur DESC
            LIMIT 12
            """, columns=["actor_id", "actor_label", "budget_eur", "n_projects"], lang=lang, warning_key="geo_view_unavailable")
            country_themes = safe_fetch_df(f"""
            SELECT
              -- Current build stores one inferred theme label per row/project view.
              theme,
              SUM(amount_eur) AS budget_eur,
              COUNT(DISTINCT projectID) AS n_projects
            FROM {R}
            WHERE {W} AND country_name IN {country_sql}
              AND theme IS NOT NULL AND TRIM(theme) <> ''
            GROUP BY theme
            ORDER BY budget_eur DESC
            LIMIT 12
            """, columns=["theme", "budget_eur", "n_projects"], lang=lang, warning_key="geo_view_unavailable")
            country_projects = safe_fetch_df(f"""
            SELECT
              projectID,
              MIN(title) AS title,
              MIN(year) AS year,
              MIN(program) AS program,
              MIN(theme) AS theme,
              SUM(amount_eur) AS budget_eur
            FROM {R}
            WHERE {W} AND country_name IN {country_sql}
            GROUP BY projectID
            ORDER BY budget_eur DESC
            LIMIT 12
            """, columns=["projectID", "title", "year", "program", "theme", "budget_eur"], lang=lang, warning_key="geo_view_unavailable")

            st.divider()
            st.markdown(f"#### {t(lang, 'geo_country_detail')}")
            d1, d2 = st.columns(2)
            with d1:
                st.markdown(f"##### {t(lang, 'geo_country_actors')}")
                if country_actors.empty:
                    render_guided_empty_state(lang, "geo_country_empty_hint")
                else:
                    fig_country_actors = px.bar(
                        country_actors.iloc[::-1],
                        x="budget_eur",
                        y="actor_label",
                        orientation="h",
                        color="budget_eur",
                        color_continuous_scale=R2G,
                        height=440,
                        labels={"budget_eur": "Budget (€)", "actor_label": ""},
                    )
                    fig_country_actors.update_layout(coloraxis_showscale=False, yaxis_title=None)
                    render_plotly_chart(fig_country_actors, use_container_width=True)
            with d2:
                st.markdown(f"##### {t(lang, 'geo_country_themes')}")
                if country_themes.empty:
                    render_guided_empty_state(lang, "geo_country_empty_hint")
                else:
                    country_themes["theme_display"] = country_themes["theme"].map(lambda x: theme_raw_to_display(str(x), lang))
                    fig_country_themes = px.bar(
                        country_themes.iloc[::-1],
                        x="budget_eur",
                        y="theme_display",
                        orientation="h",
                        color="budget_eur",
                        color_continuous_scale=R2G,
                        height=440,
                        labels={"budget_eur": "Budget (€)", "theme_display": ""},
                    )
                    fig_country_themes.update_layout(coloraxis_showscale=False, yaxis_title=None)
                    render_plotly_chart(fig_country_themes, use_container_width=True)

            st.markdown(f"##### {t(lang, 'geo_country_projects')}")
            if country_projects.empty:
                render_guided_empty_state(lang, "geo_country_empty_hint")
            else:
                country_projects["budget"] = country_projects["budget_eur"].apply(lambda v: fmt_money(float(v), lang))
                country_projects["theme_display"] = country_projects["theme"].map(
                    lambda x: theme_raw_to_display(str(x), lang) if str(x).strip() else "—"
                )
                st.dataframe(
                    country_projects[["year", "projectID", "title", "program", "theme_display", "budget"]].rename(
                        columns={"theme_display": t(lang, "themes")}
                    ),
                    use_container_width=True,
                    height=420,
                    hide_index=True,
                )


# ============================================================
# TAB COMP (Benchmark) — scatter, treemap, top (DuckDB)
# ============================================================
with tab_comp:
    render_section_header("↔", t(lang, "sub_benchmark"), t(lang, "bm_caption"), t(lang, "tab_advanced"))
    st.caption(t(lang, "adv_benchmark_helper"))
    bm_view = st.radio(
        t(lang, "benchmark_mode"),
        [t(lang, "bm_top"), t(lang, "bm_scatter"), t(lang, "bm_treemap")],
        index=0,
        horizontal=True,
    )
    st.caption(t(lang, "bm_default_caption") if bm_view == t(lang, "bm_top") else t(lang, "bm_expert_caption"))

    m = safe_fetch_df(f"""
    WITH x AS (
      SELECT
        actor_id,
        COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS org_name2,
        COALESCE(NULLIF(TRIM(country_name), ''), 'Unknown') AS country_name2,
        COALESCE(NULLIF(TRIM(entity_type), ''), 'Unknown') AS entity_type,
        amount_eur,
        projectID
      FROM {R}
      WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
    )
    SELECT
      actor_id,
      MIN(org_name2) AS org_name2,
      MIN(country_name2) AS country_name2,
      MIN(entity_type) AS entity_type,
      SUM(amount_eur) AS budget_eur,
      COUNT(DISTINCT projectID) AS n_projects
    FROM x
    GROUP BY actor_id
    """, columns=["actor_id", "org_name2", "country_name2", "entity_type", "budget_eur", "n_projects"], lang=lang, warning_key="benchmark_view_unavailable")

    if m.empty:
        render_guided_empty_state(lang, "benchmark_empty_hint")

    # Disambiguate label like your pandas logic (org + country)
    m["actor_label"] = np.where(
        m["org_name2"].astype(str) == m["actor_id"].astype(str),
        m["actor_id"].astype(str),
        (m["org_name2"].astype(str) + " — " + m["country_name2"].astype(str)),
    )
    m["ticket_eur"] = m["budget_eur"].astype(float) / m["n_projects"].astype(float).clip(lower=1.0)
    m["budget_str"] = m["budget_eur"].apply(lambda v: fmt_money(float(v), lang))
    m["ticket_str"] = m["ticket_eur"].apply(lambda v: fmt_money(float(v), lang))
    m["entity_display"] = m["entity_type"].map(lambda x: entity_raw_to_display(str(x), lang))

    with st.expander(t(lang, "bm_compare_scope"), expanded=(bm_view != t(lang, "bm_top"))):
        a, b = st.columns([1.2, 1.0])
        with a:
            pct = st.slider(t(lang, "pct_threshold"), 0, 99, 90)
        with b:
            topn = st.number_input(t(lang, "topn"), min_value=20, max_value=5000, value=60, step=10)
        thr = float(np.nanpercentile(m["budget_eur"].astype(float).values, pct)) if len(m) else 0.0
        st.caption(f"≥ {fmt_money(thr, lang)}")

    c1, c2 = st.columns([1.4, 1.3])
    with c1:
        query = st.text_input(t(lang, "search_actor"), value="")

    m2 = m[m["budget_eur"].astype(float) >= thr].copy()
    if query.strip():
        m2 = m2[m2["actor_label"].astype(str).str.contains(query.strip(), case=False, na=False)]
    m2 = m2.sort_values("budget_eur", ascending=False).head(int(topn))

    all_label = "Tous les acteurs" if lang == "FR" else "All actors"
    actor_options = [all_label] + m2["actor_label"].astype(str).tolist()
    with c2:
        picked_label = st.selectbox(t(lang, "actor_picker"), actor_options, index=0, help=t(lang, "actor_picker_hint"))
    if picked_label != all_label:
        m2 = m2[m2["actor_label"].astype(str) == picked_label].copy()

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
        with col:
            render_plotly_chart(fig, use_container_width=True)

    if bm_view == t(lang, "bm_scatter"):
        st.subheader(t(lang, "bm_scatter"))
        st.info(t(lang, "scatter_explain"), icon="ℹ️")

        if m2.empty:
            render_guided_empty_state(lang, "benchmark_empty_hint")
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
                marker=dict(line=dict(width=1, color="rgba(24,36,51,0.08)")),
            )
            fig1.update_layout(
                paper_bgcolor=PANEL_BG,
                plot_bgcolor=PANEL_BG,
                font=dict(color=TEXT_PRIMARY),
                legend=dict(bgcolor=LEGEND_BG, bordercolor=BORDER),
                xaxis=dict(gridcolor=GRID_COLOR, tickfont=dict(color=TEXT_SECONDARY)),
                yaxis=dict(gridcolor=GRID_COLOR, tickfont=dict(color=TEXT_SECONDARY)),
            )
            render_plotly_chart(fig1, use_container_width=True)
            st.caption(t(lang, "legend_tip"))

    elif bm_view == t(lang, "bm_treemap"):
        st.subheader(t(lang, "bm_treemap"))

        detail_mode = st.radio(
            t(lang, "bm_treemap_detail"),
            [t(lang, "bm_detail_simple"), t(lang, "bm_detail_standard"), t(lang, "bm_detail_detailed")],
            index=1,
            horizontal=True,
            key="tm_detail_mode",
        )
        if detail_mode == t(lang, "bm_detail_simple"):
            tm_top_themes, tm_top_countries, tm_top_actors = 6, 5, 6
        elif detail_mode == t(lang, "bm_detail_detailed"):
            tm_top_themes, tm_top_countries, tm_top_actors = 14, 10, 12
        else:
            tm_top_themes, tm_top_countries, tm_top_actors = 10, 8, 8
        tm_group_others = True

        with st.expander("Paramètres treemap" if lang == "FR" else "Treemap settings", expanded=False):
            use_advanced_tm = st.checkbox("Mode avancé" if lang == "FR" else "Advanced mode", value=False)
            if use_advanced_tm:
                tm_top_themes = st.slider("Nombre de thématiques affichées" if lang == "FR" else "Number of themes displayed", 3, 20, tm_top_themes)
                tm_top_countries = st.slider("Nombre de pays par thématique" if lang == "FR" else "Number of countries per theme", 2, 20, tm_top_countries)
                tm_top_actors = st.slider("Nombre d'acteurs par pays" if lang == "FR" else "Number of actors per country", 2, 25, tm_top_actors)
                tm_group_others = st.checkbox("Grouper le reste en « Autres »" if lang == "FR" else "Group the rest as Others", value=tm_group_others)
            st.caption(t(lang, "bm_treemap_settings_help"))
        st.caption(t(lang, "bm_treemap_help"))

        # Build treemap base by SQL (theme, country, actor)
        base = safe_fetch_df(f"""
        SELECT theme, country_name, 
               COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS org_name2,
               actor_id,
               SUM(amount_eur) AS amount_eur
        FROM {R}
        WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
        GROUP BY theme, country_name, org_name2, actor_id
        """, columns=["theme", "country_name", "org_name2", "actor_id", "amount_eur"], lang=lang, warning_key="benchmark_view_unavailable")
        if base.empty:
            render_guided_empty_state(lang, "benchmark_empty_hint")
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
                render_guided_empty_state(lang, "benchmark_empty_hint")
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
                    marker=dict(line=dict(width=0.8, color="rgba(52, 65, 86, 0.88)")),
                    customdata=np.stack([agg["budget_str"]], axis=-1),
                    hovertemplate="<b>%{label}</b><br>Budget: %{customdata[0]}<br>%{percentEntry:.1%} of parent<extra></extra>",
                    texttemplate="%{label}<br>%{percentEntry:.0%}",
                    textfont=dict(color=TEXT_PRIMARY, size=14),
                    insidetextfont=dict(color=TEXT_PRIMARY, size=14),
                    outsidetextfont=dict(color=TEXT_PRIMARY, size=13),
                    pathbar=dict(textfont=dict(color=TEXT_PRIMARY, size=13)),
                )
                fig_tree.update_layout(
                    margin=dict(l=0, r=0, t=0, b=0),
                    uniformtext=dict(minsize=13, mode="hide"),
                    coloraxis_showscale=False,
                    font=dict(color=TEXT_PRIMARY),
                    paper_bgcolor=PANEL_BG,
                    plot_bgcolor=PANEL_BG,
                )
                render_plotly_chart(fig_tree, use_container_width=True)

    else:
        st.subheader(t(lang, "bm_top"))
        if m2.empty:
            render_guided_empty_state(lang, "benchmark_empty_hint")
        else:
            bm_table = m2.copy().reset_index(drop=True)
            bm_table["rank"] = np.arange(1, len(bm_table) + 1)

            s1, s2, s3 = st.columns(3)
            s1.metric(t(lang, "n_actors"), f"{len(bm_table):,}".replace(",", " "))
            s2.metric(t(lang, "budget_total"), fmt_money(float(bm_table["budget_eur"].sum()), lang))
            s3.metric(t(lang, "countries"), f"{bm_table['country_name2'].astype(str).nunique():,}".replace(",", " "))

            top_overall = bm_table.head(15).copy()
            fig_overall = px.bar(
                top_overall.iloc[::-1],
                x="budget_eur",
                y="actor_label",
                orientation="h",
                color="budget_eur",
                color_continuous_scale=R2G,
                height=560,
                labels={"budget_eur": "Budget (€)", "actor_label": ""},
            )
            fig_overall.update_traces(
                customdata=np.stack([top_overall["budget_str"].iloc[::-1]], axis=-1),
                hovertemplate="<b>%{y}</b><br>Budget: %{customdata[0]}<extra></extra>",
            )
            fig_overall.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
            st.markdown(f"#### {t(lang, 'bm_overall_rank')}")
            render_plotly_chart(fig_overall, use_container_width=True)

            st.dataframe(
                bm_table[["rank", "actor_label", "entity_display", "country_name2", "n_projects", "budget_str", "ticket_str"]].rename(
                    columns={
                        "rank": "#",
                        "actor_label": t(lang, "actor_picker"),
                        "entity_display": t(lang, "actor_entity_type"),
                        "country_name2": t(lang, "countries"),
                        "n_projects": t(lang, "n_projects"),
                        "budget_str": t(lang, "budget_total"),
                        "ticket_str": t(lang, "actor_avg_ticket"),
                    }
                ),
                use_container_width=True,
                height=420,
                hide_index=True,
            )

            with st.expander(t(lang, "bm_breakdown_entity"), expanded=False):
                c1, c2, c3 = st.columns(3)
                top_bar("Private company", c1, "Top 20 industriels" if lang == "FR" else "Top 20 industrials")
                top_bar("Research & academia", c2, "Top 20 recherche" if lang == "FR" else "Top 20 research")
                top_bar("Public", c3, "Top 20 public" if lang == "FR" else "Top 20 public")

if app_mode == "simple":
    hidden_comp_placeholder.empty()


# ============================================================
# TAB TRENDS (DuckDB)
# ============================================================
with tab_trends:
    render_section_header("↗", t(lang, "trends_title"), t(lang, "trends_caption"), t(lang, "tab_trends_events"))
    st.caption(
        "Commence par la tendance annuelle, puis ouvre la comparaison de périodes ou le contexte macro seulement si tu as besoin d’un niveau de lecture supplémentaire."
        if lang == "FR"
        else "Start with the annual trend, then open period comparison or macro context only if you need a deeper read."
    )
    st.caption(t(lang, "budget_envelope_note"))
    trend_scope = safe_fetch_df(f"""
    SELECT year, SUM(amount_eur) AS budget_eur
    FROM {R}
    WHERE {W}
    GROUP BY year
    ORDER BY year
    """, columns=["year", "budget_eur"], lang=lang, warning_key="results_view_unavailable")
    if len(trend_scope) >= 2:
        first_year = int(trend_scope["year"].iloc[0])
        last_year = int(trend_scope["year"].iloc[-1])
        first_budget = float(trend_scope["budget_eur"].iloc[0] or 0.0)
        last_budget = float(trend_scope["budget_eur"].iloc[-1] or 0.0)
        if last_budget > first_budget * 1.05:
            trend_summary_text = t(lang, "trends_scope_summary_up").format(
                start_year=first_year,
                start_budget=fmt_money(first_budget, lang),
                end_year=last_year,
                end_budget=fmt_money(last_budget, lang),
            )
        elif last_budget < first_budget * 0.95:
            trend_summary_text = t(lang, "trends_scope_summary_down").format(
                start_year=first_year,
                start_budget=fmt_money(first_budget, lang),
                end_year=last_year,
                end_budget=fmt_money(last_budget, lang),
            )
        else:
            trend_summary_text = t(lang, "trends_scope_summary_flat").format(
                start_year=first_year,
                start_budget=fmt_money(first_budget, lang),
                end_year=last_year,
                end_budget=fmt_money(last_budget, lang),
            )
        with st.container(border=True):
            st.markdown("**" + t(lang, "trends_scope_summary_title") + "**")
            st.write(trend_summary_text)
    dim_choice = st.radio(
        t(lang, "dimension"),
        [t(lang, "dim_theme"), t(lang, "dim_program")],
        index=0,
        horizontal=True,
    )
    dim_col = "program" if dim_choice == t(lang, "dim_program") else "theme"

    dim_budget = fetch_df(f"""
    -- Current build stores one inferred theme label per row/project view.
    SELECT {dim_col} AS dim, SUM(amount_eur) AS amount_eur
    FROM {R}
    WHERE {W}
    GROUP BY dim
    ORDER BY amount_eur DESC
    """)
    if dim_budget.empty:
        render_guided_empty_state(lang, "trends_empty_hint")
    else:
        dims_all_raw = [str(x) for x in dim_budget["dim"].tolist() if str(x).strip()]
        dims_all_disp = [theme_raw_to_display(x, lang) if dim_col == "theme" else x for x in dims_all_raw]
        top_default = dims_all_disp[: min(8, len(dims_all_disp))]

        selected_dims = st.multiselect("Séries" if lang == "FR" else "Series", dims_all_disp, default=top_default)
        if not selected_dims:
            render_guided_empty_state(lang, "trends_empty_hint")
        else:
            # translate back to raw if needed
            if dim_col == "theme":
                disp_to_raw = {theme_raw_to_display(x, lang): x for x in dims_all_raw}
                selected_raw = [disp_to_raw.get(d, d) for d in selected_dims]
            else:
                selected_raw = selected_dims

            mode = st.radio(t(lang, "mode"), [t(lang, "mode_abs"), t(lang, "mode_share")], index=0, horizontal=True, key="tr_mode")

            tdf = fetch_df(f"""
            -- Current build stores one inferred theme label per row/project view.
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

            leading_dim = (
                tdf.groupby("dim", as_index=False)["amount_eur"]
                .sum()
                .sort_values("amount_eur", ascending=False)
                .head(1)
            )
            if not leading_dim.empty:
                lead_name = str(leading_dim["dim"].iloc[0])
                with st.container(border=True):
                    st.markdown("**" + t(lang, "trends_summary_title") + "**")
                    st.write(
                        t(lang, "trends_summary_share" if mode == t(lang, "mode_share") else "trends_summary_abs").format(
                            dim=lead_name
                        )
                    )

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
            render_plotly_chart(fig_area, use_container_width=True)
            if dim_col == "theme":
                st.caption(t(lang, "theme_counting_note"))
                st.caption(t(lang, "theme_method_note"))
                if "Other" in [str(x) for x in selected_raw]:
                    st.caption(t(lang, "theme_review_note"))

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
            render_plotly_chart(fig_drv, use_container_width=True)


# ============================================================
# TAB COMPARE (DuckDB)
# ============================================================
if app_mode == "advanced" and tab_compare is not None:
    with tab_compare:
        render_section_header("⇆", t(lang, "compare_title"), t(lang, "compare_caption"), t(lang, "tab_trends_events"))
        st.caption(t(lang, "compare_intro"))
        st.caption(t(lang, "budget_envelope_note"))

        min_year = meta["miny"]
        max_year = meta["maxy"]

        default_a = (max(min_year, 2014), min(max_year, 2021))
        if default_a[0] > default_a[1]:
            default_a = (min_year, min(max_year, min_year + 3))
        default_b = (max(min_year, 2021), min(max_year, 2027))
        if default_b[0] > default_b[1]:
            default_b = (max(min_year, max_year - 3), max_year)

        def _clip_period(period: Tuple[int, int]) -> Tuple[int, int]:
            y0, y1 = int(period[0]), int(period[1])
            y0 = max(min_year, min(y0, max_year))
            y1 = max(y0, min(y1, max_year))
            return (y0, y1)

        if not st.session_state.get("_compare_defaults_v2", False):
            st.session_state["cmp_period_a"] = default_a
            st.session_state["cmp_period_b"] = default_b
            st.session_state["_compare_defaults_v2"] = True
        else:
            st.session_state["cmp_period_a"] = _clip_period(st.session_state.get("cmp_period_a", default_a))
            st.session_state["cmp_period_b"] = _clip_period(st.session_state.get("cmp_period_b", default_b))

        a1, a2 = st.columns(2)
        with a1:
            period_a = st.slider(t(lang, "period_a"), min_year, max_year, value=st.session_state["cmp_period_a"], key="cmp_period_a")
        with a2:
            period_b = st.slider(t(lang, "period_b"), min_year, max_year, value=st.session_state["cmp_period_b"], key="cmp_period_b")

        dim_choice = st.radio(t(lang, "dimension"), [t(lang, "dim_theme"), t(lang, "dim_program")], index=0, horizontal=True, key="cmp_dim")
        dim_col = "program" if dim_choice == t(lang, "dim_program") else "theme"
        normalize_annual = st.checkbox(t(lang, "compare_normalize_annual"), value=False, key="cmp_normalize_annual")

        years_a = max(1, int(period_a[1]) - int(period_a[0]) + 1)
        years_b = max(1, int(period_b[1]) - int(period_b[0]) + 1)

        def budget_df(y0: int, y1: int, years_count: int) -> pd.DataFrame:
            amount_expr = f"SUM(amount_eur) / {float(max(1, years_count))}" if normalize_annual else "SUM(amount_eur)"
            return fetch_df(f"""
            WITH g AS (
              -- Current build stores one inferred theme label per row/project view.
              SELECT {dim_col} AS dim, {amount_expr} AS b
              FROM {R}
              WHERE {W} AND year BETWEEN {int(y0)} AND {int(y1)}
              GROUP BY dim
            )
            SELECT dim, b
            FROM g
            """)

        sA = budget_df(period_a[0], period_a[1], years_a)
        sB = budget_df(period_b[0], period_b[1], years_b)
        view = pd.merge(sA, sB, on="dim", how="outer", suffixes=("_A", "_B")).fillna(0.0)
        view["delta_budget"] = view["b_B"] - view["b_A"]
        view = view.sort_values("delta_budget", ascending=False)

        if view.empty:
            render_guided_empty_state(lang, "trends_empty_hint")
        else:
            if dim_col == "theme":
                view["dim_disp"] = view["dim"].map(lambda x: theme_raw_to_display(str(x), lang))
            else:
                view["dim_disp"] = view["dim"].astype(str)

            topk = st.slider("Plus fortes évolutions" if lang == "FR" else "Strongest shifts", 10, 60, 20)
            view2 = pd.concat([view.head(topk), view.tail(topk)]).drop_duplicates().sort_values("delta_budget")
            view2["delta_budget_fmt"] = view2["delta_budget"].apply(lambda x: fmt_money(float(x), lang))

            fig = px.bar(
                view2,
                x="delta_budget",
                y="dim_disp",
                orientation="h",
                height=680,
                labels={
                    "delta_budget": t(lang, "compare_delta_budget"),
                    "dim_disp": t(lang, "dim_program") if dim_col == "program" else t(lang, "dim_theme"),
                },
            )
            fig.update_layout(
                xaxis_title=t(lang, "compare_delta_budget"),
                yaxis_title=None,
            )
            fig.update_traces(
                customdata=np.stack([view2["delta_budget_fmt"]], axis=-1),
                hovertemplate="<b>%{y}</b><br>%{customdata[0]}<extra></extra>",
            )
            render_plotly_chart(fig, use_container_width=True)
            st.caption(t(lang, "compare_budget_reading"))
            st.caption(
                t(lang, "compare_period_years_normalized" if normalize_annual else "compare_period_years").format(
                    years_a=years_a,
                    years_b=years_b,
                )
            )

            table = view.head(60).copy()
            table[t(lang, "compare_budget_a")] = table["b_A"].map(lambda x: fmt_money(float(x), lang))
            table[t(lang, "compare_budget_b")] = table["b_B"].map(lambda x: fmt_money(float(x), lang))
            table["Δ"] = table["delta_budget"].map(lambda x: fmt_money(float(x), lang))
            table = table[["dim_disp", t(lang, "compare_budget_a"), t(lang, "compare_budget_b"), "Δ"]]
            table = table.rename(columns={"dim_disp": t(lang, "dim_program") if dim_col == "program" else t(lang, "dim_theme")})
            st.dataframe(table, use_container_width=True, height=520)


# ============================================================
# TAB MACRO & NEWS — independent (DuckDB + events.csv)
# ============================================================
if app_mode == "advanced" and tab_macro is not None:
    with tab_macro:
        render_section_header("↗", t(lang, "macro_title"), t(lang, "macro_subtitle"), t(lang, "tab_trends_events"))
        st.caption(t(lang, "macro_exploratory_note"))

        ev = load_events()
        if ev.empty:
            st.warning("events.csv est introuvable ou vide." if lang == "FR" else "events.csv is missing or empty.")
        else:
            with st.expander(t(lang, "macro_filters"), expanded=False):
                macro_use_global = st.checkbox(t(lang, "macro_use_global"), value=False, key="macro_use_global")
                macro_onetech = st.checkbox(t(lang, "onetech_only"), value=False, key="macro_onetech_only")
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
                show_event_labels = st.checkbox(t(lang, "macro_event_labels"), value=True, key="macro_event_labels")
                window = st.slider(t(lang, "macro_window"), 0, 3, 1, 1, key="macro_window")

            macro_parts = [f"year BETWEEN {int(macro_years[0])} AND {int(macro_years[1])}"]
            if macro_use_global:
                macro_parts.append(f"({W})")
            if macro_onetech:
                macro_parts.append(f"legacy_theme IN {in_list(sorted(list(ONETECH_THEMES_EN)))}")
            macro_W = " AND ".join(macro_parts)

            themes_raw_macro = list_str(f"SELECT DISTINCT theme FROM {R} WHERE {macro_W} ORDER BY theme")
            chosen_theme_raw: Optional[str] = None
            theme_scope_all = False
            theme_filter_sql = ""
            ev_sel = pd.DataFrame(columns=ev.columns)

            if match_mode == t(lang, "macro_match_tag"):
                tags = sorted([x for x in ev["tag"].astype(str).unique().tolist() if x.strip()])
                if not tags:
                    st.info(t(lang, "macro_no_events"))
                else:
                    chosen_tag = st.selectbox(t(lang, "macro_pick_tag"), tags, index=0)
                    ev_tag = ev[ev["tag"].astype(str) == str(chosen_tag)].copy()
                    candidate_themes = TAG_TO_THEMES.get(str(chosen_tag), set())
                    event_themes = sorted([x for x in ev_tag["theme"].astype(str).unique().tolist() if x.strip()])
                    if candidate_themes:
                        themes_candidates = [
                            raw
                            for raw in themes_raw_macro
                            if (raw in candidate_themes) or (theme_raw_to_display(raw, lang) in candidate_themes)
                        ]
                    else:
                        themes_candidates = [
                            raw
                            for raw in themes_raw_macro
                            if (raw in event_themes) or (theme_raw_to_display(raw, lang) in event_themes)
                        ]
                    if not themes_candidates and not themes_raw_macro:
                        st.info(t(lang, "no_data"))
                    else:
                        all_themes_label = t(lang, "macro_all_themes")
                        scope_options = [all_themes_label] + themes_candidates if themes_candidates else [all_themes_label]
                        chosen_scope = st.selectbox(
                            t(lang, "macro_theme_scope"),
                            scope_options,
                            index=0,
                            format_func=lambda x: str(x) if str(x) == all_themes_label else theme_raw_to_display(str(x), lang),
                        )
                        theme_scope_all = chosen_scope == all_themes_label
                        if theme_scope_all:
                            ev_sel = ev_tag.copy()
                            if themes_candidates:
                                theme_filter_sql = f" AND theme IN {in_list(themes_candidates)}"
                            elif themes_raw_macro:
                                theme_filter_sql = f" AND theme IN {in_list(themes_raw_macro)}"
                            else:
                                theme_filter_sql = " AND 1=0"
                        else:
                            chosen_theme_raw = str(chosen_scope)
                            th_disp = theme_raw_to_display(str(chosen_theme_raw), lang)
                            ev_sel = ev_tag[
                                (ev_tag["theme"].astype(str) == str(chosen_theme_raw))
                                | (ev_tag["theme"].astype(str) == th_disp)
                            ].copy()
                            theme_filter_sql = f" AND theme IN {in_list([str(chosen_theme_raw)])}"
                        if not candidate_themes:
                            st.caption(t(lang, "macro_theme_not_mapped"))
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
                    theme_filter_sql = f" AND theme IN {in_list([str(chosen_theme_raw)])}"

            if chosen_theme_raw or theme_scope_all:
                st.caption(t(lang, "macro_scope_caption"))
                st.caption(f"{t(lang, 'macro_event_count')}: {len(ev_sel)}")
                if len(ev_sel) < 3:
                    st.info(t(lang, "macro_low_coverage"))
                agg = fetch_df(f"""
                SELECT year, SUM(amount_eur) AS budget_total, COUNT(DISTINCT projectID) AS n_projects
                FROM {R}
                WHERE {macro_W}{theme_filter_sql}
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
                        ev_plot = ev_sel.sort_values("year", ascending=True).copy()
                        shown_years: set[int] = set()
                        valid_years = set(agg["year"].astype(int).tolist())
                        for _, r in ev_plot.iterrows():
                            yr = int(r["year"])
                            if yr not in valid_years:
                                continue
                            fig.add_vline(x=yr, line_width=1, line_dash="dot", opacity=0.40, line_color="rgba(34, 211, 238, 0.34)")
                            if show_event_labels and yr not in shown_years:
                                title_short = str(r.get("title", "")).strip()
                                if len(title_short) > 22:
                                    title_short = title_short[:22].rstrip() + "…"
                                fig.add_annotation(
                                    x=yr,
                                    y=1.03,
                                    yref="paper",
                                    text=title_short,
                                    showarrow=False,
                                    textangle=-30,
                                    font=dict(size=10, color=TEXT_SECONDARY),
                                    xanchor="left",
                                )
                                shown_years.add(yr)

                    render_plotly_chart(fig, use_container_width=True)

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
                            if e.get("url", ""):
                                st.markdown(f"**{t(lang, 'macro_source_link')}**: [{e.get('url')}]({e.get('url')})")
                            if e.get("impact_direction", ""):
                                st.write(f"**Impact**: {e.get('impact_direction')}")
                            if e.get("notes", ""):
                                st.write(e.get("notes"))

                        st.markdown("#### " + t(lang, "macro_same_year_events"))
                        same_year_events = ev_sel[ev_sel["year"].astype(int) == int(ey)].copy()
                        if same_year_events.empty:
                            st.caption(t(lang, "no_data"))
                        else:
                            same_year_events = same_year_events.sort_values(["date", "title"]).copy()
                            same_year_events["title_short"] = same_year_events["title"].astype(str).str.slice(0, 120)
                            cols = ["date", "tag", "title_short", "source"]
                            if "url" in same_year_events.columns:
                                cols.append("url")
                            if "impact_direction" in same_year_events.columns:
                                cols.append("impact_direction")
                            st.dataframe(
                                same_year_events[cols].rename(columns={"title_short": "title"}),
                                use_container_width=True,
                                height=220,
                            )

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
                        WHERE {macro_W}{theme_filter_sql} AND year BETWEEN {int(y0)} AND {int(y1)}
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
if app_mode == "advanced" and tab_actor is not None:
    with tab_actor:
        render_section_header("◈", t(lang, "actor_profile"), t(lang, "actor_profile_caption"), t(lang, "tab_actors_hub"))
        if st.session_state.get("f_use_actor_groups", False):
            st.caption(t(lang, "actor_group_mode_caption"))
        st.caption(f"{t(lang, 'scope_caption')}: " + " · ".join(scope_items))

        try:
            actors = fetch_df(f"""
            SELECT actor_id,
                   MIN(COALESCE(NULLIF(TRIM(org_name), ''), actor_id)) AS org_name2,
                   MIN(COALESCE(NULLIF(TRIM(country_name), ''), 'Unknown')) AS country_name2,
                   SUM(amount_eur) AS budget_eur
            FROM {R}
            WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
            GROUP BY actor_id
            ORDER BY budget_eur DESC
            LIMIT 5000
            """)
        except Exception:
            # Defensive fallback: keep actor profile usable even with malformed source labels.
            try:
                actors = fetch_df(f"""
                SELECT actor_id,
                       actor_id AS org_name2,
                       'Unknown' AS country_name2,
                       SUM(amount_eur) AS budget_eur
                FROM {R}
                WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
                GROUP BY actor_id
                ORDER BY budget_eur DESC
                LIMIT 5000
                """)
                st.warning(t(lang, "actor_query_fallback"))
            except Exception:
                actors = pd.DataFrame(columns=["actor_id", "org_name2", "country_name2", "budget_eur"])
                st.error(t(lang, "no_data"))
        if actors.empty:
            render_guided_empty_state(lang, "actor_empty_hint")
        else:
            actors["actor_label"] = np.where(
                actors["org_name2"].astype(str) == actors["actor_id"].astype(str),
                actors["actor_id"].astype(str),
                (actors["org_name2"].astype(str) + " — " + actors["country_name2"].astype(str)),
            )
            dup = actors["actor_label"].astype(str).duplicated(keep=False)
            actors["actor_display"] = np.where(
                dup,
                actors["actor_label"].astype(str) + " [" + actors["actor_id"].astype(str).str.slice(0, 18) + "]",
                actors["actor_label"].astype(str),
            )
            display_map = dict(zip(actors["actor_id"].astype(str), actors["actor_display"].astype(str)))
            actor_ids = actors["actor_id"].astype(str).tolist()
            pending_actor_drilldown = str(st.session_state.pop("results_drilldown_actor_id", "")).strip()
            if pending_actor_drilldown and pending_actor_drilldown in actor_ids:
                st.session_state["actor_profile_picker"] = pending_actor_drilldown
            current_actor_picker = str(st.session_state.get("actor_profile_picker", "")).strip()
            if current_actor_picker not in actor_ids:
                st.session_state["actor_profile_picker"] = actor_ids[0]
            selector_col, helper_col = st.columns([1.8, 1.2])
            with selector_col:
                picked_id = st.selectbox(
                    t(lang, "actor_picker"),
                    actor_ids,
                    index=0,
                    format_func=lambda aid: display_map.get(str(aid), str(aid)),
                    key="actor_profile_picker",
                )
            with helper_col:
                if str(st.session_state.get("results_selected_actor_id_candidate", "")).strip() == str(picked_id):
                    st.info(t(lang, "actor_opened_from_results"), icon="↗️")
            picked_sql_list = in_list([str(picked_id)])

            actor_summary = fetch_df(f"""
            SELECT
              MIN(COALESCE(NULLIF(TRIM(org_name), ''), actor_id)) AS org_name2,
              MIN(COALESCE(NULLIF(TRIM(country_name), ''), 'Unknown')) AS country_name2,
              MIN(COALESCE(NULLIF(TRIM(entity_type), ''), 'Unknown')) AS entity_type,
              SUM(amount_eur) AS budget_eur,
              COUNT(DISTINCT projectID) AS n_projects
            FROM {R}
            WHERE {W} AND actor_id IN {picked_sql_list}
            """)
            byy = fetch_df(f"""
            SELECT year, SUM(amount_eur) AS budget_eur, COUNT(DISTINCT projectID) AS n_projects
            FROM {R}
            WHERE {W} AND actor_id IN {picked_sql_list}
            GROUP BY year
            ORDER BY year
            """)
            mix_t = fetch_df(f"""
            -- Current build stores one inferred theme label per row/project view.
            SELECT theme, SUM(amount_eur) AS budget_eur
            FROM {R}
            WHERE {W} AND actor_id IN {picked_sql_list}
            GROUP BY theme
            ORDER BY budget_eur DESC
            LIMIT 15
            """)
            mix_t["theme_display"] = mix_t["theme"].map(lambda x: theme_raw_to_display(str(x), lang))
            mix_c = fetch_df(f"""
            SELECT country_name, SUM(amount_eur) AS budget_eur
            FROM {R}
            WHERE {W} AND actor_id IN {picked_sql_list}
            GROUP BY country_name
            ORDER BY budget_eur DESC
            LIMIT 15
            """)
            actor_projects = safe_fetch_df(f"""
            SELECT
              projectID,
              MIN(title) AS title,
              MIN(year) AS year,
              MIN(program) AS program,
              MIN(theme) AS theme,
              SUM(amount_eur) AS budget_eur
            FROM {R}
            WHERE {W} AND actor_id IN {picked_sql_list}
            GROUP BY projectID
            ORDER BY budget_eur DESC
            LIMIT 20
            """, columns=["projectID", "title", "year", "program", "theme", "budget_eur"], lang=lang, warning_key="results_view_unavailable")
            partners_scope = safe_fetch_df(f"""
            WITH my_projects AS (
              SELECT DISTINCT r.projectID
              FROM {R} r
              WHERE {W_R_partners} AND r.actor_id IN {picked_sql_list}
            )
            SELECT
              COALESCE(NULLIF(TRIM(r.org_name), ''), r.actor_id) AS org_name2,
              COALESCE(NULLIF(TRIM(r.country_name), ''), 'Unknown') AS country_name2,
              r.actor_id,
              COUNT(DISTINCT r.projectID) AS n_projects,
              SUM(r.amount_eur) AS budget_eur
            FROM {R} r
            JOIN my_projects p ON r.projectID = p.projectID
            WHERE {W_R_partners} AND r.actor_id IS NOT NULL AND TRIM(r.actor_id) <> '' AND r.actor_id NOT IN {picked_sql_list}
            GROUP BY org_name2, country_name2, r.actor_id
            ORDER BY n_projects DESC, budget_eur DESC
            LIMIT 25
            """, columns=["org_name2", "country_name2", "actor_id", "n_projects", "budget_eur"], lang=lang, warning_key="partnership_view_unavailable")
            partners_matched = safe_fetch_df(f"""
            WITH my_projects AS (
              SELECT DISTINCT r.projectID
              FROM {R} r
              WHERE {W_R_partners} AND r.actor_id IN {picked_sql_list}
            )
            SELECT
              COALESCE(NULLIF(TRIM(r.org_name), ''), r.actor_id) AS org_name2,
              COALESCE(NULLIF(TRIM(r.country_name), ''), 'Unknown') AS country_name2,
              r.actor_id,
              COUNT(DISTINCT r.projectID) AS n_projects,
              SUM(r.amount_eur) AS budget_eur
            FROM {R} r
            JOIN my_projects p ON r.projectID = p.projectID
            WHERE r.actor_id IS NOT NULL AND TRIM(r.actor_id) <> '' AND r.actor_id NOT IN {picked_sql_list}
            GROUP BY org_name2, country_name2, r.actor_id
            ORDER BY n_projects DESC, budget_eur DESC
            LIMIT 25
            """, columns=["org_name2", "country_name2", "actor_id", "n_projects", "budget_eur"], lang=lang, warning_key="partnership_view_unavailable")
            peer_df = fetch_df(f"""
            WITH x AS (
              SELECT
                actor_id,
                COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS org_name2,
                COALESCE(NULLIF(TRIM(country_name), ''), 'Unknown') AS country_name2,
                COALESCE(NULLIF(TRIM(entity_type), ''), 'Unknown') AS entity_type,
                amount_eur,
                projectID
              FROM {R}
              WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
            )
            SELECT
              actor_id,
              MIN(org_name2) AS org_name2,
              MIN(country_name2) AS country_name2,
              MIN(entity_type) AS entity_type,
              SUM(amount_eur) AS budget_eur,
              COUNT(DISTINCT projectID) AS n_projects
            FROM x
            GROUP BY actor_id
            ORDER BY budget_eur DESC
            LIMIT 5000
            """)

            actor_summary_row = actor_summary.iloc[0] if not actor_summary.empty else pd.Series(dtype="object")
            selected_actor_display = display_map.get(str(picked_id), str(picked_id))
            selected_actor_search = str(actor_summary_row.get("org_name2") or picked_id).strip() or str(picked_id)
            selected_budget = float(actor_summary_row.get("budget_eur") or 0.0)
            selected_projects = int(actor_summary_row.get("n_projects") or 0)
            selected_country = str(actor_summary_row.get("country_name2") or "—")
            selected_entity_type = str(actor_summary_row.get("entity_type") or "—")
            selected_theme = (
                theme_raw_to_display(str(mix_t.iloc[0]["theme"]), lang)
                if not mix_t.empty and str(mix_t.iloc[0]["theme"]).strip()
                else "—"
            )
            selected_main_country = (
                str(mix_c.iloc[0]["country_name"])
                if not mix_c.empty and str(mix_c.iloc[0]["country_name"]).strip()
                else selected_country
            )

            st.markdown(f"## {selected_actor_display}")
            actor_meta = [selected_entity_type, selected_country]
            actor_meta = [x for x in actor_meta if str(x).strip() and str(x).strip() != "—"]
            if actor_meta:
                st.caption(" · ".join(actor_meta))

            with st.container(border=True):
                st.markdown("**" + t(lang, "actor_answer_title") + "**")
                st.write(
                    (
                        f"Cet acteur capte {fmt_money(selected_budget, lang)} via {selected_projects:,} projets, surtout sur {selected_theme} et principalement en {selected_main_country}."
                        if lang == "FR"
                        else f"This actor captures {fmt_money(selected_budget, lang)} across {selected_projects:,} projects, mainly in {selected_theme} and primarily in {selected_main_country}."
                    ).replace(",", " ")
                )
            st.caption(
                "Lis d’abord l’évolution, les thèmes dominants et les projets principaux ; ouvre ensuite les partenaires et les comparables pour approfondir."
                if lang == "FR"
                else "Start with trend, leading themes, and top projects; then use partners and comparable actors to go deeper."
            )

            s1, s2, s3, s4 = st.columns(4)
            s1.metric(t(lang, "budget_total"), fmt_money(selected_budget, lang))
            s2.metric(t(lang, "n_projects"), f"{selected_projects:,}".replace(",", " "))
            s3.metric(t(lang, "actor_top_theme"), selected_theme)
            s4.metric(t(lang, "actor_main_country"), selected_main_country)
            aq1, aq2, aq3 = st.columns(3)
            with aq1:
                if st.button(t(lang, "actor_open_results"), key=f"actor_open_results::{picked_id}", width="stretch"):
                    st.session_state["f_quick_search"] = selected_actor_search
                    queue_tab_navigation(top_target=t(lang, "tab_explorer"))
                    st.rerun()
            with aq2:
                if selected_main_country and selected_main_country != "—":
                    if st.button(t(lang, "actor_open_geo"), key=f"actor_open_geo::{picked_id}", width="stretch"):
                        st.session_state["f_countries"] = [selected_main_country]
                        queue_tab_navigation(top_target=t(lang, "tab_markets"))
                        st.rerun()
                else:
                    st.caption("—")
            with aq3:
                top_theme_raw = str(mix_t.iloc[0]["theme"]) if not mix_t.empty else ""
                if top_theme_raw.strip():
                    if st.button(t(lang, "actor_open_trends"), key=f"actor_open_trends::{picked_id}", width="stretch"):
                        st.session_state["f_themes_raw"] = [top_theme_raw]
                        queue_tab_navigation(top_target=t(lang, "tab_trends_events"), trends_sub_target=t(lang, "tab_trends"))
                        st.rerun()
                else:
                    st.caption("—")

            actor_profile_tab, actor_partners_tab, actor_peers_tab = st.tabs(
                [t(lang, "actor_tab_profile"), t(lang, "actor_tab_partners"), t(lang, "actor_tab_peers")],
                default=t(lang, "actor_tab_profile"),
            )

            with actor_profile_tab:
                st.markdown(f"#### {t(lang, 'actor_trend')}")
                c1, c2 = st.columns(2)
                with c1:
                    figb = px.bar(byy, x="year", y="budget_eur", height=360, labels={"budget_eur": "Budget (€)"})
                    render_plotly_chart(figb, use_container_width=True)
                with c2:
                    fign = px.line(byy, x="year", y="n_projects", markers=True, height=360,
                                   labels={"n_projects": "Projets" if lang == "FR" else "Projects"})
                    render_plotly_chart(fign, use_container_width=True)

                st.divider()
                st.markdown(f"#### {t(lang, 'actor_mix_theme')}")
                if len(mix_t) <= 1:
                    figt = px.bar(
                        mix_t.iloc[::-1],
                        x="budget_eur",
                        y="theme_display",
                        orientation="h",
                        height=420,
                        color_discrete_sequence=["rgba(37,99,235,0.92)"],
                        labels={"budget_eur": "Budget (€)", "theme_display": ""},
                    )
                else:
                    figt = px.bar(
                        mix_t.iloc[::-1],
                        x="budget_eur",
                        y="theme_display",
                        orientation="h",
                        height=420,
                        color="budget_eur",
                        color_continuous_scale=R2G,
                        labels={"budget_eur": "Budget (€)", "theme_display": ""},
                    )
                    figt.update_layout(coloraxis_showscale=False)
                render_plotly_chart(figt, use_container_width=True)

                st.markdown(f"#### {t(lang, 'actor_mix_country')}")
                if mix_c.empty:
                    st.info(t(lang, "no_data"))
                elif len(mix_c) <= 1:
                    c_name = str(mix_c["country_name"].iloc[0])
                    c_budget = float(mix_c["budget_eur"].iloc[0] or 0.0)
                    c1g, c2g, c3g = st.columns(3)
                    c1g.metric(t(lang, "budget_total"), fmt_money(c_budget, lang))
                    c2g.metric(t(lang, "actor_countries"), "1")
                    c3g.metric(t(lang, "actor_main_country"), c_name)
                    st.caption(t(lang, "actor_geo_single_country"))
                else:
                    c_main = str(mix_c.iloc[0]["country_name"])
                    c_total = float(mix_c["budget_eur"].sum())
                    c1g, c2g, c3g = st.columns(3)
                    c1g.metric(t(lang, "budget_total"), fmt_money(c_total, lang))
                    c2g.metric(t(lang, "actor_countries"), f"{len(mix_c)}")
                    c3g.metric(t(lang, "actor_main_country"), c_main)
                    figc = px.bar(
                        mix_c.iloc[::-1],
                        x="budget_eur",
                        y="country_name",
                        orientation="h",
                        height=420,
                        color="budget_eur",
                        color_continuous_scale=R2G,
                        labels={"budget_eur": "Budget (€)", "country_name": ""},
                    )
                    figc.update_layout(coloraxis_showscale=False)
                    render_plotly_chart(figc, use_container_width=True)

                st.divider()
                st.markdown(f"#### {t(lang, 'actor_top_projects')}")
                if actor_projects.empty:
                    render_guided_empty_state(lang, "actor_empty_hint")
                else:
                    actor_projects["budget"] = actor_projects["budget_eur"].map(lambda x: fmt_money(float(x), lang))
                    actor_projects["theme_display"] = actor_projects["theme"].map(
                        lambda x: theme_raw_to_display(str(x), lang) if str(x).strip() else "—"
                    )
                    st.dataframe(
                        actor_projects[["year", "projectID", "title", "program", "theme_display", "budget"]].rename(
                            columns={"theme_display": t(lang, "themes")}
                        ),
                        use_container_width=True,
                        height=360,
                        hide_index=True,
                    )

            with actor_partners_tab:
                st.markdown(f"#### {t(lang, 'actor_partners')}")
                st.caption(t(lang, "actor_partners_caption"))
                st.caption(t(lang, "partners_entity_filter_note"))
                partner_reading = st.radio(
                    t(lang, "actor_partners_mode"),
                    [t(lang, "actor_partners_mode_scope"), t(lang, "actor_partners_mode_matched")],
                    index=0,
                    horizontal=True,
                )
                partners = partners_scope if partner_reading == t(lang, "actor_partners_mode_scope") else partners_matched
                st.caption(
                    t(lang, "actor_partners_mode_scope_caption")
                    if partner_reading == t(lang, "actor_partners_mode_scope")
                    else t(lang, "actor_partners_mode_matched_caption")
                )
                if (
                    partner_reading == t(lang, "actor_partners_mode_scope")
                    and len(partners_matched) > len(partners_scope)
                ):
                    st.info(t(lang, "actor_partners_scope_note_extra"))
                if partners.empty:
                    render_guided_message(lang, t(lang, "net_no_partners"), "partnership_empty_hint")
                else:
                    partners["actor_label"] = np.where(
                        partners["org_name2"].astype(str) == partners["actor_id"].astype(str),
                        partners["actor_id"].astype(str),
                        (partners["org_name2"].astype(str) + " — " + partners["country_name2"].astype(str)),
                    )
                    partners["budget_eur"] = partners["budget_eur"].apply(lambda v: fmt_money(float(v), lang))
                    st.dataframe(partners[["actor_label", "n_projects", "budget_eur"]], use_container_width=True, height=520)

            with actor_peers_tab:
                st.markdown(f"#### {t(lang, 'actor_peer_table')}")
                st.caption(t(lang, "actor_peer_caption"))
                if peer_df.empty:
                    st.info(t(lang, "no_data"))
                else:
                    peer_df["actor_label"] = np.where(
                        peer_df["org_name2"].astype(str) == peer_df["actor_id"].astype(str),
                        peer_df["actor_id"].astype(str),
                        (peer_df["org_name2"].astype(str) + " — " + peer_df["country_name2"].astype(str)),
                    )
                    peer_df["ticket_eur"] = peer_df["budget_eur"].astype(float) / peer_df["n_projects"].astype(float).clip(lower=1.0)
                    peer_df = peer_df.sort_values("budget_eur", ascending=False).reset_index(drop=True)
                    peer_df["overall_rank"] = np.arange(1, len(peer_df) + 1)
                    selected_peer_row = peer_df[peer_df["actor_id"].astype(str) == str(picked_id)].head(1)

                    peer_group_label = "Tous les acteurs" if lang == "FR" else "All actors"
                    peer_scope = peer_df.copy()
                    if not selected_peer_row.empty:
                        peer_entity_type = str(selected_peer_row.iloc[0]["entity_type"] or "").strip()
                        if peer_entity_type and peer_entity_type.lower() != "unknown":
                            peer_scope_candidate = peer_df[peer_df["entity_type"].astype(str) == peer_entity_type].copy()
                            if len(peer_scope_candidate) > 1:
                                peer_scope = peer_scope_candidate.copy()
                                peer_group_label = peer_entity_type

                    peer_scope = peer_scope.sort_values("budget_eur", ascending=False).reset_index(drop=True)
                    peer_scope["peer_rank"] = np.arange(1, len(peer_scope) + 1)
                    selected_peer_scope_row = peer_scope[peer_scope["actor_id"].astype(str) == str(picked_id)].head(1)
                    selected_ticket = float(selected_peer_row.iloc[0]["ticket_eur"] or 0.0) if not selected_peer_row.empty else 0.0
                    selected_rank_overall = int(selected_peer_row.iloc[0]["overall_rank"]) if not selected_peer_row.empty else 0
                    selected_rank_peer = int(selected_peer_scope_row.iloc[0]["peer_rank"]) if not selected_peer_scope_row.empty else 0

                    p1, p2, p3, p4 = st.columns(4)
                    p1.metric(t(lang, "actor_rank_overall"), f"{selected_rank_overall}" if selected_rank_overall else "—")
                    p2.metric(t(lang, "actor_rank_peer"), f"{selected_rank_peer}" if selected_rank_peer else "—")
                    p3.metric(t(lang, "actor_avg_ticket"), fmt_money(selected_ticket, lang))
                    p4.metric(t(lang, "actor_peer_group"), peer_group_label)

                    peer_table = peer_scope.head(12).copy()
                    if str(picked_id) not in peer_table["actor_id"].astype(str).tolist():
                        peer_table = pd.concat(
                            [peer_table, peer_scope[peer_scope["actor_id"].astype(str) == str(picked_id)].head(1)],
                            ignore_index=True,
                        )
                    peer_table = peer_table.drop_duplicates(subset=["actor_id"]).copy()
                    selected_prefix = "Sélectionné — " if lang == "FR" else "Selected — "
                    peer_table["actor_display"] = np.where(
                        peer_table["actor_id"].astype(str) == str(picked_id),
                        selected_prefix + peer_table["actor_label"].astype(str),
                        peer_table["actor_label"].astype(str),
                    )
                    peer_table["budget_fmt"] = peer_table["budget_eur"].apply(lambda v: fmt_money(float(v), lang))
                    peer_table["ticket_fmt"] = peer_table["ticket_eur"].apply(lambda v: fmt_money(float(v), lang))
                    st.dataframe(
                        peer_table[["peer_rank", "actor_display", "entity_type", "n_projects", "budget_fmt", "ticket_fmt"]].rename(
                            columns={
                                "peer_rank": "#",
                                "actor_display": t(lang, "actor_picker"),
                                "entity_type": t(lang, "actor_entity_type"),
                                "n_projects": t(lang, "n_projects"),
                                "budget_fmt": t(lang, "budget_total"),
                                "ticket_fmt": t(lang, "actor_avg_ticket"),
                            }
                        ),
                        use_container_width=True,
                        height=520,
                        hide_index=True,
                    )



# ============================================================
# TAB ADVANCED VALUE CHAIN (DuckDB)
# ============================================================
if app_mode == "advanced" and tab_value_chain is not None:
    with tab_value_chain:
        render_section_header("⇄", t(lang, "sub_value_chain"), t(lang, "vc_default_caption"), t(lang, "tab_advanced"))
        if WIP_SECTIONS.get("value_chain", False) and not ENABLE_SANKEY_CLICK:
            st.markdown(f"<div class='sir-wip-badge-wrap'>{wip_badge(lang)}</div>", unsafe_allow_html=True)
        st.caption(t(lang, "adv_value_chain_helper"))
        st.caption(t(lang, "value_chain_method_note"))

        st.markdown("#### " + ("Étapes et acteurs (budget -> acteurs)" if lang == "FR" else "Stages and actors (budget -> actors)"))
        vc_dim = safe_fetch_df(f"""
        -- Current build stores one inferred theme label per row/project view.
        SELECT theme, value_chain_stage, SUM(amount_eur) AS budget_eur
        FROM {R}
        WHERE {W}
        GROUP BY theme, value_chain_stage
        ORDER BY budget_eur DESC
        """, columns=["theme", "value_chain_stage", "budget_eur"], lang=lang, warning_key="value_chain_view_unavailable")

        if vc_dim.empty:
            if "value_chain_stage" not in set(base_schema_columns()):
                st.info(t(lang, "missing_stage_col"))
                st.caption(t(lang, "value_chain_empty_hint"))
            else:
                render_guided_empty_state(lang, "value_chain_empty_hint")
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
                    10,
                    1,
                )
            include_unspecified = st.checkbox(t(lang, "include_unspecified"), value=False, key="vc_include_unspecified")

            if picked_themes:
                vc = safe_fetch_df(f"""
                SELECT
                  value_chain_stage,
                  actor_id,
                  COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
                  SUM(amount_eur) AS budget_eur
                FROM {R}
                WHERE {W} AND theme IN {in_list(picked_themes)}
                GROUP BY value_chain_stage, actor_id, actor_label
                """, columns=["value_chain_stage", "actor_id", "actor_label", "budget_eur"], lang=lang, warning_key="value_chain_view_unavailable")

                if vc.empty:
                    render_guided_empty_state(lang, "value_chain_empty_hint")
                else:
                    if (not include_unspecified) and (vc["value_chain_stage"].astype(str) != "Unspecified").any():
                        vc = vc[vc["value_chain_stage"].astype(str) != "Unspecified"].copy()
                    if vc.empty:
                        render_guided_empty_state(lang, "value_chain_empty_hint")
                    else:
                        all_stages = vc["value_chain_stage"].astype(str).unique().tolist()
                        stage_options = [s for s in VALUE_CHAIN_ORDER if s in all_stages] + sorted([s for s in all_stages if s not in VALUE_CHAIN_ORDER])
                        pending_click = st.session_state.pop("vc_pending_click", None)
                        stage_mode = st.radio(
                            t(lang, "vc_stage_mode"),
                            [t(lang, "vc_stage_mode_all"), t(lang, "vc_stage_mode_custom")],
                            index=0,
                            horizontal=True,
                            key="vc_stage_mode",
                        )
                        if stage_mode == t(lang, "vc_stage_mode_all"):
                            picked_stages = list(stage_options)
                        else:
                            picked_stages = st.multiselect(
                                t(lang, "vc_stage_filter"),
                                stage_options,
                                default=stage_options,
                            )
                        if not picked_stages:
                            render_guided_empty_state(lang, "value_chain_empty_hint")
                            st.divider()
                        else:
                            vc = vc[vc["value_chain_stage"].astype(str).isin([str(x) for x in picked_stages])].copy()
                            if vc.empty:
                                render_guided_empty_state(lang, "value_chain_empty_hint")
                                st.divider()
                            else:
                                vc_base = vc.copy()
                                stage_all_label = t(lang, "vc_all_stages")
                                if (
                                    isinstance(pending_click, dict)
                                    and pending_click.get("kind") == "stage"
                                    and str(pending_click.get("value", "")) in [str(x) for x in picked_stages]
                                ):
                                    st.session_state["vc_highlight_stage_select"] = str(pending_click.get("value"))
                                    st.session_state["vc_isolate_stage"] = True
                                    st.session_state["vc_isolate_actor"] = False
                                summary_stage_order = (
                                    [s for s in VALUE_CHAIN_ORDER if s in vc_base["value_chain_stage"].astype(str).unique().tolist()]
                                    + sorted([s for s in vc_base["value_chain_stage"].astype(str).unique().tolist() if s not in VALUE_CHAIN_ORDER])
                                )
                                present_stages = vc_base["value_chain_stage"].astype(str).nunique()
                                only_research = present_stages == 1 and vc_base["value_chain_stage"].astype(str).iloc[0] == "Research & concept"
                                if only_research:
                                    st.warning(t(lang, "vc_single_stage_warn"))

                                st.markdown("#### " + t(lang, "vc_stage_summary"))
                                stage_tbl = (
                                    vc_base.groupby("value_chain_stage", as_index=False)["budget_eur"]
                                    .sum()
                                    .sort_values("budget_eur", ascending=False)
                                )
                                stage_tbl["budget"] = stage_tbl["budget_eur"].map(lambda x: fmt_money(float(x), lang))
                                stage_tbl = stage_tbl.rename(columns={"value_chain_stage": ("Étape chaîne de valeur" if lang == "FR" else "Value-chain stage")})
                                st.dataframe(
                                    stage_tbl[[("Étape chaîne de valeur" if lang == "FR" else "Value-chain stage"), "budget"]],
                                    use_container_width=True,
                                    height=260,
                                )

                                if summary_stage_order:
                                    if str(st.session_state.get("vc_stage_focus_select", "")).strip() not in [str(x) for x in summary_stage_order]:
                                        st.session_state["vc_stage_focus_select"] = str(summary_stage_order[0])
                                st.markdown("##### " + t(lang, "vc_top_actors_stage"))
                                stage_focus = st.selectbox(
                                    t(lang, "vc_stage_focus"),
                                    summary_stage_order,
                                    index=0,
                                    key="vc_stage_focus_select",
                                )
                                stage_only = vc_base[vc_base["value_chain_stage"].astype(str) == str(stage_focus)].copy()
                                if stage_only.empty:
                                    render_guided_empty_state(lang, "value_chain_empty_hint")
                                else:
                                    stage_rank = (
                                        stage_only.groupby(["actor_id", "actor_label"], as_index=False)["budget_eur"]
                                        .sum()
                                        .sort_values("budget_eur", ascending=False)
                                    )
                                    top_stage_n = st.slider(
                                        t(lang, "vc_top_actors_stage"),
                                        5,
                                        40,
                                        8,
                                        1,
                                        key="vc_top_stage_slider",
                                    )
                                    stage_rank = stage_rank.head(int(top_stage_n)).copy()
                                    stage_rank["actor_display"] = stage_rank["actor_label"].astype(str) + " — " + stage_rank["budget_eur"].map(
                                        lambda x: fmt_money(float(x), lang)
                                    )

                                    fig_stage = px.bar(
                                        stage_rank.iloc[::-1],
                                        x="budget_eur",
                                        y="actor_label",
                                        orientation="h",
                                        height=460,
                                        color="budget_eur",
                                        color_continuous_scale=R2G,
                                        labels={"budget_eur": "Budget (€)", "actor_label": ""},
                                    )
                                    fig_stage.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
                                    render_plotly_chart(fig_stage, use_container_width=True)

                                    actor_focus_options = stage_rank["actor_display"].astype(str).tolist()
                                    if actor_focus_options and str(st.session_state.get("vc_actor_focus_select", "")).strip() not in actor_focus_options:
                                        st.session_state["vc_actor_focus_select"] = str(actor_focus_options[0])
                                    actor_focus_display = st.selectbox(
                                        t(lang, "vc_actor_focus"),
                                        actor_focus_options,
                                        index=0,
                                        key="vc_actor_focus_select",
                                    )
                                    actor_focus_id_raw = str(stage_rank[stage_rank["actor_display"] == actor_focus_display]["actor_id"].iloc[0])
                                    stage_sql_list = in_list([str(stage_focus)])
                                    actor_sql_list = in_list([actor_focus_id_raw])
                                    try:
                                        proj_focus = fetch_df(f"""
                                        SELECT
                                          projectID,
                                          MIN(year) AS year,
                                          MIN(title) AS title,
                                          SUM(amount_eur) AS budget_eur
                                        FROM {R}
                                        WHERE {W}
                                          AND theme IN {in_list(picked_themes)}
                                          AND value_chain_stage IN {stage_sql_list}
                                          AND actor_id IN {actor_sql_list}
                                        GROUP BY projectID
                                        ORDER BY budget_eur DESC
                                        LIMIT 120
                                        """)
                                    except Exception:
                                        proj_focus = fetch_df(f"""
                                        SELECT
                                          projectID,
                                          MIN(year) AS year,
                                          MIN(title) AS title,
                                          SUM(amount_eur) AS budget_eur
                                        FROM {R}
                                        WHERE {W}
                                          AND value_chain_stage IN {stage_sql_list}
                                          AND actor_id IN {actor_sql_list}
                                        GROUP BY projectID
                                        ORDER BY budget_eur DESC
                                        LIMIT 120
                                        """)
                                    st.markdown("##### " + t(lang, "vc_projects_focus"))
                                    if proj_focus.empty:
                                        render_guided_empty_state(lang, "value_chain_empty_hint")
                                    else:
                                        proj_focus["budget"] = proj_focus["budget_eur"].map(lambda x: fmt_money(float(x), lang))
                                        st.dataframe(
                                            proj_focus[["year", "projectID", "title", "budget"]],
                                            use_container_width=True,
                                            height=320,
                                        )

                                with st.expander(t(lang, "vc_flow_expert"), expanded=False):
                                    st.caption(t(lang, "vc_expert_caption"))
                                    current_stage_highlight = str(st.session_state.get("vc_highlight_stage_select", "")).strip()
                                    if current_stage_highlight not in [stage_all_label] + [str(x) for x in picked_stages]:
                                        st.session_state["vc_highlight_stage_select"] = stage_all_label
                                    h1, h2 = st.columns([2, 1])
                                    with h1:
                                        stage_highlight = st.selectbox(
                                            t(lang, "vc_highlight_stage"),
                                            [stage_all_label] + [str(x) for x in picked_stages],
                                            index=0,
                                            key="vc_highlight_stage_select",
                                        )
                                    with h2:
                                        vc_isolate_stage = st.checkbox(
                                            t(lang, "vc_isolate_stage"),
                                            value=False,
                                            key="vc_isolate_stage",
                                        )
                                    st.caption(t(lang, "vc_isolation_help"))

                                    vc_view = vc_base.copy()
                                    if (stage_highlight != stage_all_label) and vc_isolate_stage:
                                        vc_view = vc_view[vc_view["value_chain_stage"].astype(str) == str(stage_highlight)].copy()

                                    if vc_view.empty:
                                        render_guided_empty_state(lang, "value_chain_empty_hint")
                                        vc_view = vc_base.copy()

                                    rank_actors = (
                                        vc_view.groupby(["actor_id", "actor_label"], as_index=False)["budget_eur"]
                                    .sum()
                                    .sort_values("budget_eur", ascending=False)
                                    .head(int(vc_top_actors))
                                    )
                                    vc_view = vc_view.merge(rank_actors[["actor_id"]], on="actor_id", how="inner")

                                    stage_order = (
                                        [s for s in VALUE_CHAIN_ORDER if s in vc_view["value_chain_stage"].astype(str).unique().tolist()]
                                        + sorted([s for s in vc_view["value_chain_stage"].astype(str).unique().tolist() if s not in VALUE_CHAIN_ORDER])
                                    )
                                    actor_order = rank_actors["actor_label"].astype(str).tolist()
                                    actor_all_label = t(lang, "vc_all_actors")
                                    if (
                                        isinstance(pending_click, dict)
                                        and pending_click.get("kind") == "actor"
                                        and str(pending_click.get("value", "")) in [str(x) for x in actor_order]
                                    ):
                                        st.session_state["vc_highlight_actor_select"] = str(pending_click.get("value"))
                                        st.session_state["vc_isolate_actor"] = True
                                        st.session_state["vc_isolate_stage"] = False
                                    current_actor_highlight = str(st.session_state.get("vc_highlight_actor_select", "")).strip()
                                    if current_actor_highlight not in [actor_all_label] + actor_order:
                                        st.session_state["vc_highlight_actor_select"] = actor_all_label
                                    a1h, a2h = st.columns([2, 1])
                                    with a1h:
                                        actor_highlight = st.selectbox(
                                            t(lang, "vc_highlight_actor"),
                                            [actor_all_label] + actor_order,
                                            index=0,
                                            key="vc_highlight_actor_select",
                                        )
                                    with a2h:
                                        vc_isolate_actor = st.checkbox(
                                            t(lang, "vc_isolate_actor"),
                                            value=False,
                                            key="vc_isolate_actor",
                                        )
                                    if (actor_highlight != actor_all_label) and vc_isolate_actor:
                                        vc_view = vc_view[vc_view["actor_label"].astype(str) == str(actor_highlight)].copy()
                                        if vc_view.empty:
                                            render_guided_empty_state(lang, "value_chain_empty_hint")
                                            vc_view = vc_base.copy()
                                        rank_actors = (
                                            vc_view.groupby(["actor_id", "actor_label"], as_index=False)["budget_eur"]
                                        .sum()
                                        .sort_values("budget_eur", ascending=False)
                                        .head(int(vc_top_actors))
                                        )
                                        vc_view = vc_view.merge(rank_actors[["actor_id"]], on="actor_id", how="inner")
                                        stage_order = (
                                            [s for s in VALUE_CHAIN_ORDER if s in vc_view["value_chain_stage"].astype(str).unique().tolist()]
                                            + sorted([s for s in vc_view["value_chain_stage"].astype(str).unique().tolist() if s not in VALUE_CHAIN_ORDER])
                                        )
                                        actor_order = rank_actors["actor_label"].astype(str).tolist()

                                    node_labels = stage_order + actor_order
                                    node_idx = {k: i for i, k in enumerate(node_labels)}

                                    links = (
                                        vc_view.groupby(["value_chain_stage", "actor_label"], as_index=False)["budget_eur"]
                                        .sum()
                                        .sort_values("budget_eur", ascending=False)
                                    )
                                    source = [node_idx[str(s)] for s in links["value_chain_stage"].astype(str)]
                                    target = [node_idx[str(a)] for a in links["actor_label"].astype(str)]
                                    value = links["budget_eur"].astype(float).tolist()

                                    connected_actors: set[str] = set()
                                    stage_focus_on = stage_highlight != stage_all_label
                                    actor_focus_on = actor_highlight != actor_all_label
                                    if stage_focus_on:
                                        connected_actors = set(
                                            links[links["value_chain_stage"].astype(str) == str(stage_highlight)]["actor_label"].astype(str).tolist()
                                        )
                                    link_colors: List[str] = []
                                    for stg, act in zip(
                                        links["value_chain_stage"].astype(str).tolist(),
                                        links["actor_label"].astype(str).tolist(),
                                    ):
                                        if not stage_focus_on and not actor_focus_on:
                                            link_colors.append(STAGE_COLORS.get(stg, "rgba(148,163,184,0.34)"))
                                        else:
                                            is_stage = stage_focus_on and (stg == str(stage_highlight))
                                            is_actor = actor_focus_on and (act == str(actor_highlight))
                                            if is_stage and is_actor:
                                                link_colors.append("rgba(34,211,238,0.94)")
                                            elif is_stage:
                                                link_colors.append("rgba(34,211,238,0.82)")
                                            elif is_actor:
                                                link_colors.append("rgba(37,99,235,0.82)")
                                            else:
                                                link_colors.append("rgba(148,163,184,0.10)")

                                    node_colors: List[str] = []
                                    for label in node_labels:
                                        if label in stage_order:
                                            if not stage_focus_on and not actor_focus_on:
                                                node_colors.append(STAGE_COLORS.get(label, "rgba(148,163,184,0.42)"))
                                            elif stage_focus_on and (label == str(stage_highlight)):
                                                node_colors.append("rgba(34,211,238,0.94)")
                                            else:
                                                node_colors.append("rgba(148,163,184,0.18)")
                                        else:
                                            if not stage_focus_on and not actor_focus_on:
                                                node_colors.append("rgba(37,99,235,0.78)")
                                            elif actor_focus_on and (str(label) == str(actor_highlight)):
                                                node_colors.append("rgba(37,99,235,0.94)")
                                            elif stage_focus_on and (str(label) in connected_actors):
                                                node_colors.append("rgba(37,99,235,0.84)")
                                            else:
                                                node_colors.append("rgba(37,99,235,0.18)")

                                    fig_sankey = go.Figure(
                                        data=[
                                            go.Sankey(
                                                node=dict(
                                                    pad=14,
                                                    thickness=14,
                                                    line=dict(color="rgba(52,65,86,0.92)", width=0.7),
                                                    label=node_labels,
                                                    color=node_colors,
                                                ),
                                                link=dict(source=source, target=target, value=value, color=link_colors),
                                            )
                                        ]
                                    )
                                    fig_sankey.update_layout(
                                        height=620,
                                        margin=dict(l=10, r=10, t=20, b=10),
                                        paper_bgcolor=PANEL_BG,
                                        plot_bgcolor=PANEL_BG,
                                        font=dict(color=TEXT_SECONDARY),
                                    )
                                    if HAS_PLOTLY_EVENTS and plotly_events is not None:
                                        clicked = plotly_events(
                                            fig_sankey,
                                            click_event=True,
                                            hover_event=False,
                                            select_event=False,
                                            override_height=620,
                                            key="vc_sankey_clicks",
                                        )
                                        st.caption(t(lang, "vc_click_hint"))
                                        if clicked:
                                            ev0 = clicked[0] if isinstance(clicked, list) and clicked else {}
                                            label_clicked = str((ev0 or {}).get("label", "")).strip()
                                            if not label_clicked:
                                                idx = (ev0 or {}).get("pointIndex", (ev0 or {}).get("pointNumber"))
                                                if isinstance(idx, int) and 0 <= int(idx) < len(node_labels):
                                                    label_clicked = str(node_labels[int(idx)])
                                            if label_clicked in stage_order:
                                                st.session_state["vc_pending_click"] = {"kind": "stage", "value": label_clicked}
                                                st.rerun()
                                            elif label_clicked in actor_order:
                                                st.session_state["vc_pending_click"] = {"kind": "actor", "value": label_clicked}
                                                st.rerun()
                                    else:
                                        render_plotly_chart(fig_sankey, use_container_width=True)
                                        st.caption(t(lang, "vc_click_unavailable"))
            else:
                render_guided_message(
                    lang,
                    "Sélectionne au moins une thématique." if lang == "FR" else "Select at least one theme.",
                    "value_chain_empty_hint",
                )


# ============================================================
# TAB ADVANCED COLLABORATION (DuckDB)
# ============================================================
if app_mode == "advanced" and tab_collaboration is not None:
    with tab_collaboration:
        render_section_header("⟡", t(lang, "sub_collaboration"), t(lang, "net_default_caption"), t(lang, "tab_advanced"))
        st.caption(t(lang, "adv_collaboration_helper"))
        st.caption(t(lang, "partnership_stage_note"))
        actor_rank = safe_fetch_df(f"""
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
        """, columns=["actor_id", "actor_label", "budget_eur", "n_projects"], lang=lang, warning_key="partnership_view_unavailable")

        if actor_rank.empty:
            render_guided_empty_state(lang, "partnership_empty_hint")
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
                    t(lang, "net_focal_actor"),
                    actor_rank["actor_display"].astype(str).tolist(),
                    index=0,
                )
            with nc2:
                net_top = st.slider(
                    t(lang, "net_top_partners"),
                    5,
                    40,
                    8,
                    1,
                )

            focal_row = actor_rank[actor_rank["actor_display"].astype(str) == focal_display].iloc[0]
            focal_id_raw = str(focal_row["actor_id"])
            focal_sql_list = in_list([focal_id_raw])
            focal_label = str(focal_row["actor_label"])

            partners = safe_fetch_df(f"""
            WITH part AS (
              SELECT
                projectID,
                actor_id,
                COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
                SUM(amount_eur) AS actor_budget
              FROM {R}
              WHERE {W_partners} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
              GROUP BY projectID, actor_id, actor_label
            ),
            focus_projects AS (
              SELECT DISTINCT projectID FROM part WHERE actor_id IN {focal_sql_list}
            )
            SELECT
              p.actor_id,
              p.actor_label,
              COUNT(DISTINCT p.projectID) AS shared_projects,
              SUM(p.actor_budget) AS partner_budget
            FROM part p
            JOIN focus_projects f ON p.projectID = f.projectID
            WHERE p.actor_id NOT IN {focal_sql_list}
            GROUP BY p.actor_id, p.actor_label
            ORDER BY shared_projects DESC, partner_budget DESC
            LIMIT {int(net_top)}
            """, columns=["actor_id", "actor_label", "shared_projects", "partner_budget"], lang=lang, warning_key="partnership_view_unavailable")

            if partners.empty:
                render_guided_message(lang, t(lang, "net_no_partners"), "partnership_empty_hint")
            else:
                dup_p = partners["actor_label"].astype(str).duplicated(keep=False)
                partners["partner_display"] = np.where(
                    dup_p,
                    partners["actor_label"].astype(str) + " [" + partners["actor_id"].astype(str).str.slice(0, 18) + "]",
                    partners["actor_label"].astype(str),
                )
                partner_all_label = t(lang, "net_all_partners")
                focus_display = st.selectbox(
                    t(lang, "net_focus_partner"),
                    [partner_all_label] + partners["partner_display"].astype(str).tolist(),
                    index=0,
                    key="net_focus_partner",
                )
                isolate_partner = st.checkbox(
                    t(lang, "net_isolate_partner"),
                    value=False,
                    key="net_isolate_partner",
                )
                st.caption(t(lang, "net_focus_help"))

                focus_partner_id: Optional[str] = None
                if focus_display != partner_all_label:
                    focus_partner_id = str(
                        partners.loc[partners["partner_display"].astype(str) == str(focus_display), "actor_id"].iloc[0]
                    )

                partners_view = partners.copy()
                if focus_partner_id and isolate_partner:
                    partners_view = partners_view[partners_view["actor_id"].astype(str) == str(focus_partner_id)].copy()
                    if partners_view.empty:
                        partners_view = partners.copy()

                st.markdown(f"## {focal_label}")
                ns1, ns2, ns3, ns4 = st.columns(4)
                ns1.metric(t(lang, "net_focal_actor"), focal_label[:32] + ("…" if len(focal_label) > 32 else ""))
                ns2.metric(t(lang, "net_partner_table"), f"{len(partners_view):,}".replace(",", " "))
                ns3.metric(
                    t(lang, "net_shared_projects_total"),
                    f"{int(partners_view['shared_projects'].sum()):,}".replace(",", " "),
                )
                ns4.metric(
                    t(lang, "net_partner_budget_total"),
                    fmt_money(float(partners_view["partner_budget"].sum()), lang),
                )

                st.markdown("#### " + t(lang, "net_partner_table"))
                ptab = partners_view.copy()
                ptab["budget"] = ptab["partner_budget"].apply(lambda x: fmt_money(float(x), lang))
                ptab = ptab.rename(
                    columns={
                        "actor_label": ("Partenaire" if lang == "FR" else "Partner"),
                        "shared_projects": ("Projets communs" if lang == "FR" else "Shared projects"),
                    }
                )
                st.dataframe(
                    ptab[[("Partenaire" if lang == "FR" else "Partner"), ("Projets communs" if lang == "FR" else "Shared projects"), "budget"]],
                    use_container_width=True,
                    height=320,
                )

                with st.expander(t(lang, "net_graph_expert"), expanded=False):
                    st.caption(t(lang, "net_expert_caption"))
                    n = len(partners_view)
                    angles = np.linspace(0, 2 * np.pi, n, endpoint=False)
                    rx, ry = np.cos(angles), np.sin(angles)

                    fig_net = go.Figure()
                    max_shared = float(max(1, partners_view["shared_projects"].max()))
                    for i, (_, r) in enumerate(partners_view.iterrows()):
                        w = 1.0 + 3.5 * (float(r["shared_projects"]) / max_shared)
                        is_focus = bool(focus_partner_id) and (str(r["actor_id"]) == str(focus_partner_id))
                        line_color = (
                            "rgba(34,211,238,0.90)"
                            if is_focus
                            else ("rgba(37,99,235,0.36)" if not focus_partner_id else "rgba(37,99,235,0.14)")
                        )
                        fig_net.add_trace(
                            go.Scatter(
                                x=[0.0, float(rx[i])],
                                y=[0.0, float(ry[i])],
                                mode="lines",
                                line=dict(width=w, color=line_color),
                                hoverinfo="skip",
                                showlegend=False,
                            )
                        )

                    partner_size = 14 + 22 * (partners_view["shared_projects"].astype(float) / max_shared)
                    partner_colors = []
                    for aid in partners_view["actor_id"].astype(str).tolist():
                        is_focus = bool(focus_partner_id) and (aid == str(focus_partner_id))
                        if is_focus:
                            partner_colors.append("rgba(34,211,238,0.94)")
                        elif focus_partner_id:
                            partner_colors.append("rgba(37,99,235,0.24)")
                        else:
                            partner_colors.append("rgba(37,99,235,0.78)")
                    fig_net.add_trace(
                        go.Scatter(
                            x=[0.0],
                            y=[0.0],
                            mode="markers+text",
                            marker=dict(size=34, color="rgba(20,184,166,0.90)", line=dict(width=1, color="rgba(52,65,86,0.95)")),
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
                            marker=dict(size=partner_size, color=partner_colors, line=dict(width=0.8, color="rgba(52,65,86,0.88)")),
                            text=[str(x)[:34] for x in partners_view["actor_label"].astype(str).tolist()],
                            textposition="top center",
                            customdata=np.stack(
                                [
                                    partners_view["shared_projects"].astype(int).values,
                                    partners_view["partner_budget"].astype(float).apply(lambda x: fmt_money(float(x), lang)).values,
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
                        paper_bgcolor=PANEL_BG,
                        plot_bgcolor=PANEL_BG,
                        font=dict(color=TEXT_SECONDARY),
                    )
                    render_plotly_chart(fig_net, use_container_width=True)
                    st.caption(t(lang, "partners_entity_filter_note"))



# ============================================================
# TAB ADVANCED CONCENTRATION (DuckDB)
# ============================================================
if app_mode == "advanced" and tab_concentration is not None:
    with tab_concentration:
        render_section_header("◔", t(lang, "concentration_title"), t(lang, "concentration_caption"), t(lang, "tab_advanced"))
        st.caption(t(lang, "adv_concentration_helper"))
        conc = safe_fetch_df(f"""
        SELECT
          COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
          actor_id,
          SUM(amount_eur) AS b
        FROM {R}
        WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
        GROUP BY actor_label, actor_id
        ORDER BY b DESC
        LIMIT 15
        """, columns=["actor_label", "actor_id", "b"], lang=lang, warning_key="concentration_view_unavailable")
        if conc.empty or float(conc["b"].sum()) <= 0:
            render_guided_empty_state(lang, "concentration_empty_hint")
        else:
            conc = conc.copy()
            conc["actor_label"] = conc["actor_label"].astype(str).str.slice(0, 32)
            conc["cum_share"] = conc["b"].astype(float).cumsum() / float(conc["b"].astype(float).sum()) * 100.0
            conc["budget_str"] = conc["b"].astype(float).apply(lambda x: fmt_money(float(x), lang))

            fig_p = go.Figure()
            fig_p.add_trace(
                go.Bar(
                    x=conc["actor_label"],
                    y=conc["b"],
                    name=t(lang, "concentration_budget"),
                    marker=dict(color="rgba(37,99,235,0.74)", line=dict(color="rgba(52,65,86,0.88)", width=0.8)),
                    customdata=np.stack([conc["budget_str"]], axis=-1),
                    hovertemplate="<b>%{x}</b><br>Budget: %{customdata[0]}<extra></extra>",
                )
            )
            fig_p.add_trace(
                go.Scatter(
                    x=conc["actor_label"],
                    y=conc["cum_share"],
                    yaxis="y2",
                    mode="lines+markers",
                    name=t(lang, "concentration_cum"),
                    line=dict(color="rgba(34,197,94,0.96)", width=2.4),
                    marker=dict(size=6, color="rgba(34,197,94,0.96)"),
                    hovertemplate="<b>%{x}</b><br>Cumulative: %{y:.1f}%<extra></extra>",
                )
            )
            fig_p.update_layout(
                height=460,
                xaxis=dict(title="", tickangle=-35),
                yaxis=dict(title="Budget (€)", showgrid=True, gridcolor=GRID_COLOR),
                yaxis2=dict(title=t(lang, "concentration_cum"), overlaying="y", side="right", range=[0, 100]),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0),
                margin=dict(l=20, r=20, t=20, b=80),
                paper_bgcolor=PANEL_BG,
                plot_bgcolor=PANEL_BG,
                font=dict(color=TEXT_PRIMARY),
            )
            render_plotly_chart(fig_p, use_container_width=True)
            st.caption(t(lang, "concentration_caption"))



# ============================================================
# TAB DATA (paginated, DuckDB) + export
# ============================================================
if app_mode == "advanced" and tab_data is not None:
    with tab_data:
        render_section_header("▣", t(lang, "sub_data"), t(lang, "data_warning"), t(lang, "tab_admin"))

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
        if "project_status" in page_df.columns:
            page_df["project_status"] = page_df["project_status"].map(lambda x: status_raw_to_display(str(x), lang))
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
# TAB DEBUG (diagnostics)
# ============================================================
if app_mode == "advanced" and tab_debug is not None:
    with tab_debug:
        render_section_header("⚙", t(lang, "debug_title"), t(lang, "debug_caption"), t(lang, "tab_admin"))

        mapping_cov = {"total_actors": 0, "matched_actors": 0}
        mapping_keys = {"total_keys": 0, "matched_keys": 0}
        mapping_key_pct = 0.0
        if actor_map_info.get("available", False):
            mapping_cov = actor_group_match_stats()
            mapping_keys = actor_group_key_match_stats()
            if mapping_keys["total_keys"] > 0:
                mapping_key_pct = 100.0 * mapping_keys["matched_keys"] / mapping_keys["total_keys"]
            if mapping_keys["total_keys"] > 0 and mapping_key_pct >= 80.0:
                st.caption(t(lang, "mapping_status_ready"))
            else:
                st.caption(t(lang, "mapping_status_partial"))
        else:
            st.caption(t(lang, "mapping_status_missing_short"))
        if not actor_map_info.get("available", False):
            st.caption(t(lang, "exclude_funders_heuristic"))

        with st.expander(t(lang, "mapping_diag_toggle"), expanded=True):
            st.caption(f"**{t(lang, 'mapping_summary')}**")
            if actor_map_info.get("available", False):
                st.caption(f"{t(lang, 'mapping_loaded_count')}: {int(actor_map_info.get('rows_actor', 0)) + int(actor_map_info.get('rows_pic', 0))}")
                st.caption(
                    f"{t(lang, 'mapping_keys_matched')}: "
                    f"{mapping_keys['matched_keys']}/{mapping_keys['total_keys']} ({mapping_key_pct:.1f}%)"
                )
                st.caption(f"{t(lang, 'mapping_global_impact')}: {mapping_cov['matched_actors']:,}")
                if actor_map_info.get("source"):
                    st.caption(f"{t(lang, 'actor_groups_source')}: `{actor_map_info.get('source')}`")
                if mapping_keys["total_keys"] > 0 and mapping_keys["matched_keys"] == 0:
                    st.warning(t(lang, "mapping_keys_issue"))
                    st.caption(t(lang, "mapping_mode_fallback"))
                elif mapping_key_pct < 80.0:
                    st.warning(t(lang, "mapping_keys_partial"))
                    st.caption(t(lang, "mapping_mode_fallback"))
                else:
                    st.caption(t(lang, "mapping_mode_explicit"))
            else:
                st.info(t(lang, "actor_groups_missing"))
                st.caption(t(lang, "mapping_mode_pic_only"))

        st.caption(f"{t(lang, 'build_sha')}: {current_git_sha()}")
        with st.expander(t(lang, "diag_snapshot"), expanded=True):
            st.caption(t(lang, "diag_snapshot_hint"))
            ds = base_snapshot_stats()
            es = events_snapshot_stats()
            cs = connectors_snapshot_stats()
            em = events_meta_snapshot()
            st.caption(f"{t(lang, 'diag_rows')}: {ds['n_rows']:,}")
            st.caption(f"{t(lang, 'diag_budget')}: {fmt_money(float(ds['total_budget']), lang)}")
            st.caption(f"{t(lang, 'diag_projects')}: {ds['n_projects']:,}")
            st.caption(f"{t(lang, 'diag_actors')}: {ds['n_actors']:,}")
            st.caption(f"{t(lang, 'diag_years')}: {ds['min_year']}–{ds['max_year']}")
            st.caption(f"{t(lang, 'diag_events')}: {es['n_events']:,}")
            st.caption(f"{t(lang, 'diag_events_ai')}: {es['n_ai']:,}")
            st.caption(f"{t(lang, 'diag_connectors')}: {int(cs.get('manifest_total', 0))}")
            st.caption(f"{t(lang, 'diag_connectors_ready')}: {int(cs.get('manifest_ready', 0))}")
            st.caption(f"{t(lang, 'diag_connectors_last')}: {cs.get('last_status', '—')}")
            st.caption(f"{t(lang, 'diag_events_policy')}: {t(lang, 'diag_events_policy_value').format(hours=float(em.get('min_refresh_hours', 24.0)))}")
            st.caption(f"{t(lang, 'last_update')} — {t(lang, 'last_update_events')}: {str(em.get('last_build_utc', '—'))}")



# ============================================================
# TAB QUALITY (DuckDB)
# ============================================================
if app_mode == "advanced" and tab_quality is not None:
    with tab_quality:
        render_section_header("✓", t(lang, "quality_title"), "", t(lang, "tab_admin"))
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
if app_mode == "advanced" and tab_help is not None:
    with tab_help:
        render_section_header("ⓘ", t(lang, "docs_title"), "", t(lang, "tab_admin"))
        st.markdown("#### " + t(lang, "help_title"))

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
if app_mode == "advanced" and tab_guide is not None:
    with tab_guide:
        st.divider()
        render_section_header("✧", t(lang, "guide_title"), "", t(lang, "tab_admin"))

        if lang == "FR":
            st.markdown("Guide pour lire les vues correctement, sans sur-interpréter. Tout dépend du **périmètre filtré** (sidebar).")
            with st.expander("1) Vue d’ensemble : KPIs, allocation, tickets, concentration", expanded=True):
                st.markdown(
                    """
    - KPIs = ordres de grandeur (budget, projets, acteurs, tickets).
    - Budget annuel + ticket médian = lecture simple des tendances.
    - Pareto + HHI = concentration / dépendance à quelques acteurs.
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
            with st.expander("5) Comparaison : écarts de budget entre périodes", expanded=False):
                st.markdown(
                    """
    - Compare A vs B en budget.
    - Barres à droite = plus de budget en période B.
    - Barres à gauche = moins de budget en période B.
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
    - Sélection des étapes + focus étape/acteur = lecture ciblée des entreprises par maillon.
    - Clic sur un nœud Sankey (si activé) = isolation automatique de l'étape/acteur.
    - Graphe étoile = collaborations autour d’un acteur focal.
    - Utiliser le mode regroupé (PIC/groupe) pour une lecture “groupe industriel”.
                    """
                )
        else:
            st.markdown("Guide to interpret the views correctly without over-claiming. Everything depends on the **filtered scope** (sidebar).")
            with st.expander("1) Overview: KPIs, allocation, tickets, concentration", expanded=True):
                st.markdown(
                    """
    - KPIs = orders of magnitude.
    - Annual budget + median ticket = simple trend reading.
    - Pareto + HHI = concentration / dependency risk.
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
            with st.expander("5) Compare: budget gaps across periods", expanded=False):
                st.markdown(
                    """
    - Compares A vs B in budget.
    - Bars to the right = more funding in period B.
    - Bars to the left = less funding in period B.
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
    - Stage selection + stage/actor focus = targeted reading of companies by chain link.
    - Click on a Sankey node (when enabled) automatically isolates stage/actor.
    - Star graph = collaborations around one focal actor.
    - Use grouped mode (PIC/group) for industrial-group level reading.
                    """
                )
