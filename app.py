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

import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

import duckdb
from filelock import FileLock, Timeout

# Interactive Sankey clicks can trigger rerun loops on Streamlit Cloud.
# Keep click mode disabled for stability; isolation is controlled via selectors.
ENABLE_SANKEY_CLICK = False
plotly_events = None
HAS_PLOTLY_EVENTS = False


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

st.set_page_config(page_title="Subsidy Intelligence Radar", layout="wide", initial_sidebar_state="expanded")

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

  section[data-testid="stSidebar"] {
    background: var(--sir-sidebar);
    border-right: 1px solid var(--sir-border);
    color-scheme: dark;
  }

  section[data-testid="stSidebar"] .block-container {
    padding-top: 1rem;
  }

  h1, h2, h3, h4, h5 {
    color: var(--sir-text);
    letter-spacing: -0.02em;
    font-weight: 700;
  }

  h1 {
    margin-bottom: 0.2rem;
  }

  h3 {
    margin-top: 0.9rem;
  }

  .stCaption,
  [data-testid="stCaptionContainer"] {
    color: var(--sir-text-muted) !important;
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

  p, li, span, div, label, button, input, textarea, select {
    color: inherit;
    font-family: var(--sir-font) !important;
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
    padding: 14px 16px;
    border-radius: 16px;
    box-shadow: var(--sir-shadow);
  }

  div[data-testid="metric-container"] [data-testid="stMetricLabel"] {
    color: var(--sir-text-secondary) !important;
    font-weight: 600;
  }

  div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: var(--sir-text) !important;
    font-weight: 700;
    letter-spacing: -0.02em;
  }

  .stTabs [data-baseweb="tab-list"] {
    gap: 0.45rem;
    margin-bottom: 0.6rem;
  }

  .stTabs [data-baseweb="tab"] {
    background: var(--sir-surface);
    border: 1px solid var(--sir-border);
    border-radius: 12px;
    padding: 10px 14px;
    color: var(--sir-text-secondary);
    font-weight: 600;
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
    background: linear-gradient(135deg, rgba(91, 192, 235, 0.11), rgba(34, 211, 238, 0.07)) !important;
    border: 1px solid rgba(91, 192, 235, 0.24) !important;
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
    font-size: 0.84rem;
    font-weight: 550;
    line-height: 1.2;
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
    margin: 14px 0 8px 0;
    color: var(--sir-text);
    font-size: 2.3rem;
    line-height: 1.06;
    letter-spacing: -0.03em;
    font-weight: 760;
  }

  .sir-hero__subtitle {
    margin: 0;
    color: var(--sir-text-secondary);
    font-size: 1rem;
    line-height: 1.6;
    max-width: 78ch;
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
    font-size: 1.22rem;
    line-height: 1.25;
    font-weight: 720;
    margin: 0;
    letter-spacing: -0.02em;
  }

  .sir-section-head__desc {
    color: var(--sir-text-secondary);
    font-size: 0.96rem;
    line-height: 1.55;
    margin: 6px 0 0 0;
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
    font-size: 0.84rem;
    font-weight: 600;
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

EUROPE_DEFAULT_COUNTRIES = [
    "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Czechia",
    "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland",
    "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland",
    "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden",
    "Norway", "Switzerland", "United Kingdom", "Iceland",
]

# World Bank/UN style rounded values (inhabitants). Used only for budget-per-population normalization in map.
POPULATION_BY_ALPHA3 = {
    "AUT": 9130000, "BEL": 11700000, "BGR": 6440000, "HRV": 3870000, "CYP": 1250000, "CZE": 10900000,
    "DNK": 5960000, "EST": 1370000, "FIN": 5600000, "FRA": 68400000, "DEU": 84500000, "GRC": 10300000,
    "HUN": 9580000, "IRL": 5320000, "ITA": 58900000, "LVA": 1880000, "LTU": 2860000, "LUX": 673000,
    "MLT": 564000, "NLD": 18000000, "POL": 37700000, "PRT": 10500000, "ROU": 19000000, "SVK": 5430000,
    "SVN": 2120000, "ESP": 48800000, "SWE": 10600000, "NOR": 5560000, "CHE": 8920000, "GBR": 68200000,
    "ISL": 394000,
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
        "title": "Subsidy Intelligence Radar",
        "subtitle": "Recherche guidée sur les subventions UE : résultats, acteurs, géographie, tendances et événements.",
        "reset": "Réinitialiser",
        "refresh": "Rafraîchir les données",
        "refresh_hint": "Met à jour CORDIS + events (offline), puis recharge l’app.",
        "filters": "Filtres",
        "basic_filters": "Filtres principaux",
        "advanced_filters": "Plus de filtres",
        "analysis_options": "Options d'analyse",
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
        "themes": "Thématiques",
        "entity": "Type d’entité",
        "countries": "Pays",
        "quick_search": "Recherche rapide",
        "quick_search_hint": "Recherche dans acteur, projet, acronyme ou titre",
        "main_search_support": "Recherche libre dans les projets et acteurs. Pour le pays, la période ou le programme, utilise les filtres.",
        "search_simplified_notice": "La recherche a été simplifiée pour éviter une erreur. Essaie un mot-clé simple puis affine avec les filtres.",
        "search_ignored_notice": "La recherche n’a pas pu être appliquée avec cette saisie. Les filtres sont conservés ; essaie un mot-clé plus simple.",
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
        "bm_default_caption": "Vue par défaut: lecture simple, table-first, du périmètre courant.",
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
        "compare_caption": "Comparaison en **% du budget total** de chaque période, Δ en **points de %**.",
        "actor_profile": "Fiche acteur",
        "actor_group_mode_caption": "Vue groupe active: les fiches et graphes peuvent agréger plusieurs entités juridiques via mapping ou PIC.",
        "actor_profile_caption": "Sélectionne un acteur puis lis rapidement son profil, ses partenaires et ses pairs dans le périmètre actif.",
        "actor_opened_from_results": "Ouvert depuis un projet sélectionné dans Résultats.",
        "actor_trend": "Évolution (budget & projets)",
        "actor_mix_theme": "Mix thématique",
        "actor_mix_country": "Mix géographique",
        "actor_partners": "Top co-participants",
        "actor_partners_caption": "Co-participants sur les mêmes projets, dans le périmètre actif.",
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
        "tab_markets": "◎ Marchés & géographie",
        "tab_trends_events": "↗ Tendances & événements",
        "tab_advanced": "◇ Analyse avancée",
        "tab_admin": "⚙ Admin & méthode",
        "sub_results": "Résultats",
        "sub_overview": "Synthèse",
        "overview_caption": "Vue secondaire de synthèse : utilise d'abord Résultats pour explorer le périmètre, puis viens ici pour une lecture plus compacte.",
        "overview_yearly_extra": "Complément annuel : budgets et ticket médian",
        "sub_benchmark": "Comparer les acteurs",
        "sub_network": "Chaîne & réseau",
        "sub_value_chain": "Étapes et acteurs",
        "sub_collaboration": "Partenariats",
        "sub_concentration": "Concentration du financement",
        "sub_data": "Données",
        "sub_quality": "Qualité",
        "sub_debug": "Debug",
        "advanced_title": "Analyse avancée",
        "advanced_caption": "Vues expertes pour benchmark, chaîne de valeur, collaborations et concentration. Les surfaces par défaut restent dans Recherche, Acteurs et Géographie.",
        "adv_benchmark_helper": "Repère rapidement quels acteurs dominent le périmètre, puis ouvre les vues expertes si besoin.",
        "adv_value_chain_helper": "Vois à quelle étape interviennent les acteurs et quels projets sont liés à chaque étape.",
        "adv_collaboration_helper": "Identifie les partenaires clés d’un acteur avant d’ouvrir la carte réseau.",
        "adv_concentration_helper": "Vois si le financement est réparti entre beaucoup d’acteurs ou concentré sur quelques-uns.",
        "debug_title": "Debug & diagnostics",
        "debug_caption": "Surfaces techniques déplacées hors de la sidebar pour garder l'exploration lisible.",
        "results_title": "Résultats du périmètre",
        "results_caption": "Une requête, plusieurs lectures du même périmètre.",
        "results_view": "Vue principale",
        "results_table": "Table projets",
        "results_trend": "Tendance",
        "results_map": "Carte",
        "results_actors": "Acteurs",
        "main_search_label": "Que veux-tu explorer ?",
        "main_search_help": "Recherche libre sur acteur, projet, acronyme ou titre",
        "main_search_placeholder": "Ex. AI Germany, hydrogen France, CNRS batteries",
        "active_filters": "Filtres actifs",
        "clear_search": "Effacer la recherche",
        "no_results_title": "Aucun résultat pour ce périmètre.",
        "no_results_hint": "Essaie d’élargir le pays, la période ou la thématique.",
        "no_results_reset": "Réinitialiser les filtres",
        "results_projects_table": "Projets trouvés",
        "results_actor_table": "Acteurs dominants",
        "results_budget_year": "Budget par année",
        "results_projects_year": "Projets par année",
        "results_country_rank": "Classement pays",
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
        "macro_title": "Macro & actualités — analyse approfondie",
        "macro_subtitle": "Onglet indépendant : filtres macro internes (ne dépend pas de la sidebar).",
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
        "diag_snapshot": "Diagnostic snapshot",
        "diag_snapshot_hint": "Ces valeurs sont globales (hors filtres sidebar).",
        "diag_rows": "Lignes dataset",
        "diag_budget": "Budget dataset",
        "diag_projects": "Projets dataset",
        "diag_actors": "Acteurs dataset",
        "diag_years": "Plage années dataset",
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
        "reset": "Reset",
        "refresh": "Refresh data",
        "refresh_hint": "Updates CORDIS + events (offline), then reloads the app.",
        "filters": "Filters",
        "basic_filters": "Main filters",
        "advanced_filters": "More filters",
        "analysis_options": "Analysis options",
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
        "themes": "Themes",
        "entity": "Entity type",
        "countries": "Countries",
        "quick_search": "Quick search",
        "quick_search_hint": "Search actor, project, acronym or title",
        "main_search_support": "Free-text search across projects and actors. Use filters for country, time period, or programme.",
        "search_simplified_notice": "Search was simplified to avoid an error. Try a simpler keyword, then refine with filters.",
        "search_ignored_notice": "Search could not be applied safely for this input. Filters are still active; try a simpler keyword.",
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
        "compare_caption": "Comparison as **% of total budget** in each period, Δ in **percentage points**.",
        "actor_profile": "Actor profile",
        "actor_group_mode_caption": "Group view is active: profiles and charts may aggregate several legal entities through mapping or PIC.",
        "actor_profile_caption": "Pick an actor, then read their profile, partners, and peers within the active scope.",
        "actor_opened_from_results": "Opened from a selected project in Results.",
        "actor_trend": "Trend (budget & projects)",
        "actor_mix_theme": "Theme mix",
        "actor_mix_country": "Geography mix",
        "actor_partners": "Top co-participants",
        "actor_partners_caption": "Co-participants on the same projects within the active scope.",
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
        "tab_markets": "◎ Markets & geography",
        "tab_trends_events": "↗ Trends & events",
        "tab_advanced": "◇ Advanced",
        "tab_admin": "⚙ Admin & method",
        "sub_results": "Results",
        "sub_overview": "Overview",
        "overview_caption": "Secondary summary view: use Results first to explore the scope, then come here for a more compact readout.",
        "overview_yearly_extra": "Yearly add-on: budgets and median ticket",
        "sub_benchmark": "Compare actors",
        "sub_network": "Value chain & network",
        "sub_value_chain": "Stages and actors",
        "sub_collaboration": "Partnerships",
        "sub_concentration": "Funding concentration",
        "sub_data": "Data",
        "sub_quality": "Quality",
        "sub_debug": "Debug",
        "advanced_title": "Advanced analysis",
        "advanced_caption": "Expert views for benchmark, value chain, collaboration, and concentration. Default user flows stay in Search, Actors, and Geography.",
        "adv_benchmark_helper": "Use this to spot the leading actors in the current scope before opening the expert charts.",
        "adv_value_chain_helper": "Use this to see which actors are active at each stage and which projects sit behind them.",
        "adv_collaboration_helper": "Use this to identify an actor’s key partners before opening the network map.",
        "adv_concentration_helper": "Use this to see whether funding is spread across many actors or concentrated in a few.",
        "debug_title": "Debug & diagnostics",
        "debug_caption": "Technical surfaces moved out of the sidebar to keep exploration readable.",
        "results_title": "Results in scope",
        "results_caption": "One query, several readings of the same scope.",
        "results_view": "Primary view",
        "results_table": "Project table",
        "results_trend": "Trend",
        "results_map": "Map",
        "results_actors": "Actors",
        "main_search_label": "What do you want to explore?",
        "main_search_help": "Free search across actor, project, acronym, or title",
        "main_search_placeholder": "E.g. AI Germany, hydrogen France, CNRS batteries",
        "active_filters": "Active filters",
        "clear_search": "Clear search",
        "no_results_title": "No results for this scope.",
        "no_results_hint": "Try widening country, time, or theme.",
        "no_results_reset": "Reset filters",
        "results_projects_table": "Matching projects",
        "results_actor_table": "Leading actors",
        "results_budget_year": "Budget by year",
        "results_projects_year": "Projects by year",
        "results_country_rank": "Country ranking",
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
        "macro_title": "Macro & news — deep dive",
        "macro_subtitle": "Independent tab: internal macro filters (does not depend on the sidebar).",
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
    cols = columns or ["projectID", "acronym", "title", "org_name", "actor_id"]
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
    statuses: List[str],
    themes: List[str],
    entities: List[str],
    countries: List[str],
    quick_search: str,
) -> Tuple[str, str, Optional[str]]:
    normalized_search = _normalize_quick_search(quick_search)
    base_kwargs = dict(
        sources=sources,
        programmes=programmes,
        years=years,
        use_section=use_section,
        sections=sections,
        onetech_only=onetech_only,
        statuses=statuses,
        themes=themes,
        entities=entities,
        countries=countries,
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
        labels.append(f"{t(lang, 'countries')}: {_compact_filter_values(countries)}")

    themes = [x for x in st.session_state.get("f_themes_raw", []) if x in meta.get("themes", [])]
    if themes and len(themes) < len(meta.get("themes", [])):
        labels.append(f"{t(lang, 'themes')}: {_compact_filter_values(themes, lambda x: theme_raw_to_display(x, lang))}")

    entities = [x for x in st.session_state.get("f_entity_raw", []) if x in meta.get("entities", [])]
    if entities and len(entities) < len(meta.get("entities", [])):
        labels.append(f"{t(lang, 'entity')}: {_compact_filter_values(entities, lambda x: entity_raw_to_display(x, lang))}")

    statuses = [x for x in st.session_state.get("f_statuses", []) if x in meta.get("statuses", [])]
    if statuses and len(statuses) < len(meta.get("statuses", [])):
        labels.append(f"{t(lang, 'project_status')}: {_compact_filter_values(statuses, lambda x: status_raw_to_display(x, lang))}")

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


def render_empty_state(lang: str) -> None:
    st.warning(t(lang, "no_results_title"))
    st.caption(t(lang, "no_results_hint"))
    st.button(t(lang, "no_results_reset"), key="empty_state_reset", on_click=reset_filters)


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


def render_plotly_chart(fig: go.Figure, **kwargs):
    kwargs.setdefault("theme", None)
    return st.plotly_chart(fig, **kwargs)


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
    existing = set(countries or [])
    ordered = [c for c in EUROPE_DEFAULT_COUNTRIES if c in existing]
    return ordered


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


def clear_search() -> None:
    st.session_state["f_quick_search"] = ""


def queue_tab_navigation(top_target: str = "", actor_sub_target: str = "") -> None:
    st.session_state["nav_target_top"] = str(top_target or "")
    st.session_state["nav_target_actor_sub"] = str(actor_sub_target or "")


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


def rel() -> str:
    return f"read_parquet('{PARQUET_PATH.as_posix()}')"


@st.cache_data(show_spinner=False)
def base_schema_columns() -> List[str]:
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
        "coalesce(b.abstract,'') || ' ' || coalesce(b.theme,'') || ' ' || coalesce(b.section,''))"
    )
    stage_fallback_expr = (
        "CASE "
        f"WHEN regexp_matches({stage_blob_expr}, '(critical raw material|raw material|lithium|nickel|cobalt|mining|refining|feedstock|biomass|recycling|supply chain|precursor)') THEN 'Resources & feedstock' "
        f"WHEN regexp_matches({stage_blob_expr}, '(electrolyser|electrolyzer|fuel cell|reactor|stack|module|battery|turbine|membrane|electrode|catalyst|converter|inverter|component|subsystem)') THEN 'Components & core technology' "
        f"WHEN regexp_matches({stage_blob_expr}, '(grid|microgrid|pipeline|network|charging|charging station|storage system|integration|interoperability|hub|terminal|facility|plant|district heating|infrastructure|platform)') THEN 'Systems & infrastructure' "
        f"WHEN regexp_matches({stage_blob_expr}, '(pilot|demonstration|demo|deployment|operation|operations|industrialisation|industrialization|scale-up|scale up|roll-out|roll out|commissioning|field trial|validation|first-of-a-kind|foak|maintenance|trl 6|trl 7|trl 8)') THEN 'Deployment & operations' "
        f"WHEN regexp_matches({stage_blob_expr}, '(market uptake|market adoption|end-user|end user|customer|offtake|commercialisation|commercialization|procurement|go-to-market|go to market|mobility|aviation|shipping|manufacturing|trl 9)') THEN 'End-use & market' "
        "WHEN lower(coalesce(b.theme,'')) IN ('e-mobility', 'transport & aviation') THEN 'End-use & market' "
        "WHEN lower(coalesce(b.theme,'')) IN ('ai & digital', 'advanced materials', 'health & biotech') THEN 'Components & core technology' "
        "WHEN lower(coalesce(b.theme,'')) IN ('hydrogen (h2)', 'solar (pv/csp)', 'wind', 'bioenergy & saf', 'ccus', 'nuclear & smr', 'batteries & storage') THEN 'Systems & infrastructure' "
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
    statuses: List[str],
    themes: List[str],
    entities: List[str],
    countries: List[str],
    quick_search: str,
    table_alias: Optional[str] = None,
    quick_search_columns: Optional[List[str]] = None,
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
        w.append(f"{prefix}theme IN {in_list(sorted(list(ONETECH_THEMES_EN)))}")
    if statuses:
        w.append(f"{prefix}project_status IN {in_list(statuses)}")
    if themes:
        w.append(f"{prefix}theme IN {in_list(themes)}")
    if entities:
        w.append(f"{prefix}entity_type IN {in_list(entities)}")
    if countries:
        w.append(f"{prefix}country_name IN {in_list(countries)}")
    if str(quick_search).strip():
        q = _normalize_quick_search(str(quick_search).strip())
        w.append(quick_search_clause(prefix, q, quick_search_columns))
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

    if cloud_runtime:
        st.caption(t(lang, "cloud_persistence_note"))
        st.caption(t(lang, "refresh_cloud_disabled"))
        act_url = github_actions_refresh_url()
        if act_url:
            st.caption(f"[{t(lang, 'refresh_cloud_cta')}]({act_url})")

    if "last_rebuild_logs" in st.session_state:
        st.divider()
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


# ============================================================
# Guard
# ============================================================
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
        "statuses": list_str(f"SELECT DISTINCT project_status FROM {R} WHERE project_status IS NOT NULL AND TRIM(project_status)<>'' ORDER BY project_status"),
        "themes": list_str(f"SELECT DISTINCT theme FROM {R} WHERE theme IS NOT NULL AND TRIM(theme)<>'' ORDER BY theme"),
        "entities": list_str(f"SELECT DISTINCT entity_type FROM {R} WHERE entity_type IS NOT NULL AND TRIM(entity_type)<>'' ORDER BY entity_type"),
        "countries": list_str(f"SELECT DISTINCT country_name FROM {R} WHERE country_name IS NOT NULL AND TRIM(country_name)<>'' ORDER BY country_name"),
    }

meta = get_meta()
actor_map_info = register_actor_group_tables()


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
    st.session_state.setdefault("f_statuses", default_statuses)
    st.session_state.setdefault("f_themes_raw", meta["themes"])
    st.session_state.setdefault("f_entity_raw", meta["entities"])
    st.session_state.setdefault("f_countries", default_countries)
    st.session_state.setdefault("f_quick_search", "")
    st.session_state.setdefault("f_use_actor_groups", False)
    st.session_state.setdefault("f_exclude_funders", True)

    # One-time migration: switch old "all countries by default" sessions to Europe default.
    if not st.session_state.get("_country_default_migrated_v6", False):
        st.session_state["f_countries"] = default_countries
        st.session_state["f_statuses"] = default_statuses
        st.session_state["f_use_actor_groups"] = False
        st.session_state["f_exclude_funders"] = True
        st.session_state["_country_default_migrated_v6"] = True

    # Section filter is intentionally disabled in sidebar UX (too technical for most users).
    st.session_state["f_use_section"] = False
    st.session_state["f_sections"] = []


_ensure_filter_state()


# ============================================================
# Main search entry
# ============================================================
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
render_active_filter_chips(meta, lang)


# ============================================================
# Sidebar filters (display mapping FR/EN, raw stored)
# ============================================================
with st.sidebar:
    st.header("◫ " + t(lang, "filters"))

    src_default = [x for x in st.session_state["f_sources"] if x in meta["sources"]]
    prg_default = [x for x in st.session_state["f_programmes"] if x in meta["programmes"]]
    status_default = [x for x in st.session_state["f_statuses"] if x in meta["statuses"]]
    ctry_default = [x for x in st.session_state["f_countries"] if x in meta["countries"]]
    eu_default = european_countries_present(meta["countries"])
    ctry_fallback = eu_default if eu_default else meta["countries"]

    with st.expander(t(lang, "basic_filters"), expanded=True):
        st.session_state["f_years"] = st.slider(t(lang, "period"), meta["miny"], meta["maxy"], st.session_state["f_years"])
        themes_ui = [x for x in meta["themes"] if (not st.session_state["f_onetech_only"]) or (x in ONETECH_THEMES_EN)]
        themes_default = [x for x in st.session_state["f_themes_raw"] if x in themes_ui]
        st.session_state["f_themes_raw"] = st.multiselect(
            t(lang, "themes"),
            themes_ui,
            default=themes_default or themes_ui,
            format_func=lambda x: theme_raw_to_display(str(x), lang),
        )
        st.session_state["f_countries"] = st.multiselect(t(lang, "countries"), meta["countries"], default=ctry_default or ctry_fallback)
        entities_default = [x for x in st.session_state["f_entity_raw"] if x in meta["entities"]]
        st.session_state["f_entity_raw"] = st.multiselect(
            t(lang, "entity"),
            meta["entities"],
            default=entities_default or meta["entities"],
            format_func=lambda x: entity_raw_to_display(str(x), lang),
        )

    with st.expander(t(lang, "advanced_filters"), expanded=False):
        st.session_state["f_sources"] = st.multiselect(t(lang, "sources"), meta["sources"], default=src_default or meta["sources"])
        st.session_state["f_programmes"] = st.multiselect(t(lang, "programmes"), meta["programmes"], default=prg_default or meta["programmes"])
        st.session_state["f_statuses"] = st.multiselect(
            t(lang, "project_status"),
            meta["statuses"],
            default=status_default or meta["statuses"],
            format_func=lambda x: status_raw_to_display(str(x), lang),
        )
        st.session_state["f_onetech_only"] = st.checkbox(t(lang, "onetech_only"), value=st.session_state["f_onetech_only"])
        st.checkbox(t(lang, "actor_grouping"), key="f_use_actor_groups")
        st.checkbox(t(lang, "exclude_funders"), key="f_exclude_funders")


# ============================================================
# Main WHERE
# ============================================================
R = rel_analytics(
    use_actor_groups=bool(st.session_state.get("f_use_actor_groups", False)),
    exclude_funders=bool(st.session_state.get("f_exclude_funders", False)),
)
W, W_R, search_notice_key = build_safe_where_pair(
    R,
    sources=st.session_state["f_sources"],
    programmes=st.session_state["f_programmes"],
    years=st.session_state["f_years"],
    use_section=False,
    sections=[],
    onetech_only=st.session_state["f_onetech_only"],
    statuses=st.session_state["f_statuses"],
    themes=st.session_state["f_themes_raw"],
    entities=st.session_state["f_entity_raw"],
    countries=st.session_state["f_countries"],
    quick_search=st.session_state["f_quick_search"],
)


# ============================================================
# KPIs (DuckDB)
# ============================================================
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

scope_items = [
    f"{int(st.session_state['f_years'][0])}-{int(st.session_state['f_years'][1])}",
    t(lang, "scope_group_on") if st.session_state.get("f_use_actor_groups", False) else t(lang, "scope_group_off"),
    t(lang, "scope_funders_off") if st.session_state.get("f_exclude_funders", False) else t(lang, "scope_funders_on"),
]
if st.session_state.get("f_statuses"):
    scope_items.append(", ".join(status_raw_to_display(x, lang) for x in st.session_state["f_statuses"]))
if str(st.session_state.get("f_quick_search", "")).strip():
    scope_items.append(f"{t(lang, 'quick_search')}: {str(st.session_state.get('f_quick_search', '')).strip()}")



# ============================================================
# Top navigation (result-first)
# ============================================================
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
tab_explorer, tab_actors_hub, tab_markets, tab_trends_events, tab_advanced, tab_admin = st.tabs(
    top_tab_labels,
    default=default_top_tab,
)

with tab_explorer:
    tab_results, tab_overview = st.tabs([t(lang, "sub_results"), t(lang, "sub_overview")])

with tab_actors_hub:
    tab_actor = st.container()

with tab_trends_events:
    tab_trends, tab_compare, tab_macro = st.tabs(
        [t(lang, "tab_trends"), t(lang, "tab_compare"), t(lang, "tab_macro")]
    )

with tab_advanced:
    st.markdown(f"### {t(lang, 'advanced_title')}")
    st.caption(t(lang, "advanced_caption"))
    tab_comp, tab_value_chain, tab_collaboration, tab_concentration = st.tabs(
        [
            t(lang, "sub_benchmark"),
            t(lang, "sub_value_chain"),
            t(lang, "sub_collaboration"),
            t(lang, "sub_concentration"),
        ]
    )

with tab_admin:
    tab_data, tab_quality, tab_debug = st.tabs([t(lang, "sub_data"), t(lang, "sub_quality"), t(lang, "sub_debug")])
    tab_docs = st.container()

tab_geo = tab_markets
tab_help = tab_docs
tab_guide = tab_docs
st.session_state["nav_target_top"] = ""
st.session_state["nav_target_actor_sub"] = ""


# ============================================================
# TAB RESULTS (result-first)
# ============================================================
with tab_results:
    render_section_header("⌕", t(lang, "results_title"), t(lang, "results_caption"), t(lang, "tab_explorer"))
    if search_notice_key:
        st.warning(t(lang, search_notice_key))
    with st.container(border=True):
        st.markdown("**" + t(lang, "explore_overview_title") + "**")
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
    st.markdown("#### ✦ " + t(lang, "kpis"))
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric(t(lang, "budget_total"), fmt_money(total_budget, lang))
    k2.metric(t(lang, "n_projects"), f"{nb_projects:,}".replace(",", " "))
    k3.metric(t(lang, "n_actors"), f"{nb_actors:,}".replace(",", " "))
    k4.metric(t(lang, "avg_ticket"), fmt_money(avg_ticket, lang))
    k5.metric(t(lang, "median_ticket"), fmt_money(median_ticket, lang))
    k6.metric(t(lang, "top10_share"), fmt_pct(top10_share, 1))
    st.caption(f"{t(lang, 'hhi')}: {hhi:.3f}")
    st.caption(
        f"{t(lang, 'scope_caption')}: " + " · ".join(scope_items)
    )
    st.divider()

    if nb_projects == 0:
        render_empty_state(lang)
    else:
        results_view = st.radio(
            t(lang, "results_view"),
            [
                t(lang, "results_table"),
                t(lang, "results_trend"),
                t(lang, "results_map"),
                t(lang, "results_actors"),
            ],
            horizontal=True,
            index=0,
            key="results_view_mode",
        )

        if results_view == t(lang, "results_table"):
            results_scope_token = f"{R}||{W}"
            sync_results_table_state(results_scope_token)

            results_base_select_sql = f"""
            SELECT
              projectID,
              MIN(year) AS year,
              MIN(title) AS title,
              MIN(theme) AS theme,
              MIN(project_status) AS project_status,
              COUNT(DISTINCT actor_id) AS n_actors,
              COUNT(DISTINCT country_name) AS n_countries,
              SUM(amount_eur) AS budget_eur
            FROM {R}
            WHERE {W}
            GROUP BY projectID
            """
            results_total = fetch_df(f"SELECT COUNT(*) AS n_rows FROM ({results_base_select_sql}) q")
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
                results_projects_raw = fetch_df(f"""
                {results_base_select_sql}
                ORDER BY budget_eur DESC
                LIMIT {int(rows_per_page)} OFFSET {int(offset)}
                """)
                results_projects = results_projects_raw.copy()
                results_projects["theme"] = results_projects["theme"].map(lambda x: theme_raw_to_display(str(x), lang))
                results_projects["project_status"] = results_projects["project_status"].map(lambda x: status_raw_to_display(str(x), lang))
                results_projects["budget_eur"] = results_projects["budget_eur"].map(lambda x: fmt_money(float(x), lang))
                st.markdown("#### " + t(lang, "results_projects_table"))
                results_view_token = f"{results_scope_token}||page={int(page)}||rpp={int(rows_per_page)}"
                if st.session_state.get("results_selected_project_view_token") != results_view_token:
                    st.session_state["results_selected_project_view_token"] = results_view_token
                    st.session_state.pop("results_selected_project_id", None)
                    st.session_state.pop("results_project_table_df", None)

                results_projects_display = results_projects.rename(
                    columns={
                        "project_status": t(lang, "project_status"),
                        "budget_eur": t(lang, "budget_total"),
                        "n_actors": t(lang, "n_actors"),
                        "theme": t(lang, "themes"),
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
                    detail_df = fetch_df(f"""
                    SELECT
                      projectID,
                      MIN(title) AS title,
                      MIN(acronym) AS acronym,
                      MIN(year) AS year,
                      MIN(program) AS program,
                      MIN(theme) AS theme,
                      MIN(project_status) AS project_status,
                      SUM(amount_eur) AS budget_eur,
                      COUNT(DISTINCT actor_id) AS n_actors,
                      COUNT(DISTINCT country_name) AS n_countries
                    FROM {R}
                    WHERE {W} AND projectID IN {in_list([selected_project_id])}
                    GROUP BY projectID
                    LIMIT 1
                    """)
                    if detail_df.empty:
                        st.session_state.pop("results_selected_project_id", None)
                    else:
                        detail = detail_df.iloc[0]
                        countries_df = fetch_df(f"""
                        SELECT DISTINCT country_name
                        FROM {R}
                        WHERE {W} AND projectID IN {in_list([selected_project_id])}
                          AND country_name IS NOT NULL AND TRIM(country_name) <> ''
                        ORDER BY country_name
                        LIMIT 40
                        """)
                        actors_df = fetch_df(f"""
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
                        """)

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

                        meta1, meta2, meta3 = st.columns(3)
                        with meta1:
                            st.caption(f"**{t(lang, 'programmes')}**")
                            st.write(str(detail.get("program") or "—"))
                        with meta2:
                            st.caption(f"**{t(lang, 'themes')}**")
                            st.write(theme_raw_to_display(str(detail.get("theme") or ""), lang) if str(detail.get("theme") or "").strip() else "—")
                        with meta3:
                            st.caption(f"**{t(lang, 'project_status')}**")
                            st.write(status_raw_to_display(str(detail.get("project_status") or ""), lang) if str(detail.get("project_status") or "").strip() else "—")

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
                                if st.button(("Ouvrir dans Acteurs" if lang == "FR" else "Open in Actors"), key=f"results_actor_drill_btn::{selected_project_id}"):
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

        elif results_view == t(lang, "results_trend"):
            res_year = fetch_df(f"""
            SELECT
              year,
              SUM(amount_eur) AS budget_eur,
              COUNT(DISTINCT projectID) AS n_projects
            FROM {R}
            WHERE {W}
            GROUP BY year
            ORDER BY year
            """)
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
            geo_res = fetch_df(f"""
            SELECT country_alpha3, country_name, SUM(amount_eur) AS amount_eur
            FROM {R}
            WHERE {W} AND country_alpha3 IS NOT NULL AND TRIM(country_alpha3) <> ''
            GROUP BY country_alpha3, country_name
            ORDER BY amount_eur DESC
            """)
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
            res_actors = fetch_df(f"""
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
            """)
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
with tab_overview:
    render_section_header("◌", t(lang, "sub_overview"), t(lang, "overview_caption"), t(lang, "tab_explorer"))

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
        render_plotly_chart(fig_alloc, use_container_width=True)

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
            st.markdown("### " + t(lang, "status_budget_title"))
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
            st.markdown("### " + t(lang, "status_projects_title"))
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
    st.markdown("### " + t(lang, "insights_title"))
    insights: List[str] = []
    try:
        top_theme = fetch_df(f"""
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
        st.markdown("### " + t(lang, "ticket_shape_title"))
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

        country_options = geo_rank["country_name"].astype(str).tolist()
        country_scope_token = f"{W}||{'|'.join(country_options)}"
        drilldown_country = ""
        active_country_filters = [str(x) for x in st.session_state.get("f_countries", []) if str(x).strip()]
        if len(active_country_filters) == 1 and active_country_filters[0] in country_options:
            drilldown_country = active_country_filters[0]

        if st.session_state.get("geo_scope_token") != country_scope_token:
            st.session_state["geo_scope_token"] = country_scope_token
            if drilldown_country:
                st.session_state["geo_selected_country"] = drilldown_country
            elif country_options:
                st.session_state["geo_selected_country"] = country_options[0]
        else:
            current_geo_country = str(st.session_state.get("geo_selected_country", "")).strip()
            if drilldown_country and current_geo_country != drilldown_country:
                st.session_state["geo_selected_country"] = drilldown_country
            elif country_options and current_geo_country not in country_options:
                st.session_state["geo_selected_country"] = country_options[0]

        with c2:
            if country_options:
                st.selectbox(
                    t(lang, "geo_country_picker"),
                    country_options,
                    key="geo_selected_country",
                )

        with st.expander(t(lang, "geo_advanced_options"), expanded=False):
            a, b, d, e = st.columns([1.2, 1.1, 1.2, 1.4])
            with a:
                zoom = st.selectbox(t(lang, "zoom_on"), zoom_opts, index=1, key="geo_zoom")
            with b:
                projection = st.selectbox(t(lang, "projection"), ["natural earth", "mercator"], index=0, key="geo_projection")
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
            st.markdown(f"## {selected_country}")
            g1, g2, g3, g4 = st.columns(4)
            g1.metric(t(lang, "geo_rank"), f"{selected_rank}" if selected_rank else "—")
            g2.metric(t(lang, "budget_total"), fmt_money(selected_total_budget, lang))
            g3.metric(
                t(lang, "geo_metric_per_million"),
                "—" if pd.isna(selected_per_million) else f"{selected_per_million:,.0f} € / M".replace(",", " "),
            )
            g4.metric(t(lang, "geo_scope_share"), fmt_pct(selected_scope_share, 1))

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
            projection_type=projection,
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
        render_plotly_chart(fig_map, use_container_width=True, config={"scrollZoom": True})

        st.markdown(f"#### {t(lang, 'top_countries')}")
        top_c = geo_rank.head(15).copy()
        if top_c.empty:
            st.info(t(lang, "no_data"))
        else:
            top_c["country_display"] = np.where(
                top_c["country_name"].astype(str) == selected_country,
                "→ " + top_c["country_name"].astype(str),
                top_c["country_name"].astype(str),
            )
            fig_bar = px.bar(
                top_c,
                x=color_col,
                y="country_display",
                orientation="h",
                color=color_col,
                color_continuous_scale=R2G,
                height=520,
                labels={color_col: color_title, "country_display": ""},
            )
            fig_bar.update_traces(
                customdata=np.stack([top_c["budget_str"], top_c["per_million_str"]], axis=-1),
                hovertemplate=(
                    "<b>%{y}</b>"
                    "<br>Budget: %{customdata[0]}"
                    "<br>Budget / M hab.: %{customdata[1]}"
                    "<extra></extra>"
                ),
            )
            fig_bar.update_layout(showlegend=False, yaxis_title=None, coloraxis_showscale=False)
            render_plotly_chart(fig_bar, use_container_width=True)

        if selected_country:
            country_sql = in_list([selected_country])
            country_actors = fetch_df(f"""
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
            """)
            country_themes = fetch_df(f"""
            SELECT
              theme,
              SUM(amount_eur) AS budget_eur,
              COUNT(DISTINCT projectID) AS n_projects
            FROM {R}
            WHERE {W} AND country_name IN {country_sql}
              AND theme IS NOT NULL AND TRIM(theme) <> ''
            GROUP BY theme
            ORDER BY budget_eur DESC
            LIMIT 12
            """)
            country_projects = fetch_df(f"""
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
            """)

            st.divider()
            st.markdown(f"#### {t(lang, 'geo_country_detail')}")
            d1, d2 = st.columns(2)
            with d1:
                st.markdown(f"##### {t(lang, 'geo_country_actors')}")
                if country_actors.empty:
                    st.info(t(lang, "no_data"))
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
                    st.info(t(lang, "no_data"))
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
                st.info(t(lang, "no_data"))
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

    m = fetch_df(f"""
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
    m["entity_display"] = m["entity_type"].map(lambda x: entity_raw_to_display(str(x), lang))

    with st.expander(t(lang, "bm_compare_scope"), expanded=(bm_view != t(lang, "bm_top"))):
        a, b = st.columns([1.2, 1.0])
        with a:
            pct = st.slider(t(lang, "pct_threshold"), 0, 99, 90)
        with b:
            topn = st.number_input(t(lang, "topn"), min_value=20, max_value=5000, value=200, step=10)
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
            st.info(t(lang, "no_data"))
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


# ============================================================
# TAB TRENDS (DuckDB)
# ============================================================
with tab_trends:
    dim_choice = st.radio(
        t(lang, "dimension"),
        [t(lang, "dim_theme"), t(lang, "dim_program")],
        index=0,
        horizontal=True,
    )
    dim_col = "program" if dim_choice == t(lang, "dim_program") else "theme"

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
            render_plotly_chart(fig_area, use_container_width=True)

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
with tab_compare:
    st.markdown(f"### {t(lang, 'compare_title')}")
    st.caption(t(lang, "compare_caption"))

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

    topk = st.slider("Plus fortes évolutions" if lang == "FR" else "Strongest shifts", 10, 60, 25)
    view2 = pd.concat([view.head(topk), view.tail(topk)]).drop_duplicates().sort_values("delta_share")

    fig = px.bar(
        view2,
        x=(view2["delta_share"] * 100.0),
        y=view2["dim_disp"],
        orientation="h",
        height=680,
        labels={"x": "Δ (points de %)" if lang == "FR" else "Δ (pp)", "y": ""},
    )
    render_plotly_chart(fig, use_container_width=True)

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
    render_section_header("↗", t(lang, "macro_title"), t(lang, "macro_subtitle"), t(lang, "tab_trends_events"))

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
            macro_parts.append(f"theme IN {in_list(sorted(list(ONETECH_THEMES_EN)))}")
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
        st.info(t(lang, "no_data"))
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
        partners = fetch_df(f"""
        WITH my_projects AS (
          SELECT DISTINCT r.projectID
          FROM {R} r
          WHERE {W_R} AND r.actor_id IN {picked_sql_list}
        )
        SELECT
          COALESCE(NULLIF(TRIM(r.org_name), ''), r.actor_id) AS org_name2,
          COALESCE(NULLIF(TRIM(r.country_name), ''), 'Unknown') AS country_name2,
          r.actor_id,
          COUNT(DISTINCT r.projectID) AS n_projects,
          SUM(r.amount_eur) AS budget_eur
        FROM {R} r
        JOIN my_projects p ON r.projectID = p.projectID
        WHERE {W_R} AND r.actor_id IS NOT NULL AND TRIM(r.actor_id) <> '' AND r.actor_id NOT IN {picked_sql_list}
        GROUP BY org_name2, country_name2, r.actor_id
        ORDER BY n_projects DESC, budget_eur DESC
        LIMIT 25
        """)
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

        s1, s2, s3, s4 = st.columns(4)
        s1.metric(t(lang, "budget_total"), fmt_money(selected_budget, lang))
        s2.metric(t(lang, "n_projects"), f"{selected_projects:,}".replace(",", " "))
        s3.metric(t(lang, "actor_top_theme"), selected_theme)
        s4.metric(t(lang, "actor_main_country"), selected_main_country)

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

        with actor_partners_tab:
            st.markdown(f"#### {t(lang, 'actor_partners')}")
            st.caption(t(lang, "actor_partners_caption"))
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
with tab_value_chain:
    render_section_header("⇄", t(lang, "sub_value_chain"), t(lang, "vc_default_caption"), t(lang, "tab_advanced"))
    st.caption(t(lang, "adv_value_chain_helper"))

    st.markdown("#### " + ("Étapes et acteurs (budget -> acteurs)" if lang == "FR" else "Stages and actors (budget -> actors)"))
    try:
        vc_dim = fetch_df(f"""
        SELECT theme, value_chain_stage, SUM(amount_eur) AS budget_eur
        FROM {R}
        WHERE {W}
        GROUP BY theme, value_chain_stage
        ORDER BY budget_eur DESC
        """)
    except Exception:
        st.error(t(lang, "vc_query_error"))
        vc_dim = pd.DataFrame(columns=["theme", "value_chain_stage", "budget_eur"])

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
        include_unspecified = st.checkbox(t(lang, "include_unspecified"), value=False, key="vc_include_unspecified")

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
                if (not include_unspecified) and (vc["value_chain_stage"].astype(str) != "Unspecified").any():
                    vc = vc[vc["value_chain_stage"].astype(str) != "Unspecified"].copy()
                if vc.empty:
                    st.info(t(lang, "no_data"))
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
                        st.info(t(lang, "no_data"))
                        st.divider()
                    else:
                        vc = vc[vc["value_chain_stage"].astype(str).isin([str(x) for x in picked_stages])].copy()
                        if vc.empty:
                            st.info(t(lang, "no_data"))
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
                                st.info(t(lang, "no_data"))
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
                                    15,
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
                                    st.info(t(lang, "no_data"))
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
                                    st.info(t(lang, "no_data"))
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
                                        st.info(t(lang, "no_data"))
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
            st.info("Sélectionne au moins une thématique." if lang == "FR" else "Select at least one theme.")

# ============================================================
# TAB ADVANCED COLLABORATION (DuckDB)
# ============================================================
with tab_collaboration:
    render_section_header("⟡", t(lang, "sub_collaboration"), t(lang, "net_default_caption"), t(lang, "tab_advanced"))
    st.caption(t(lang, "adv_collaboration_helper"))
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
                t(lang, "net_focal_actor"),
                actor_rank["actor_display"].astype(str).tolist(),
                index=0,
            )
        with nc2:
            net_top = st.slider(
                t(lang, "net_top_partners"),
                5,
                40,
                16,
                1,
            )

        focal_row = actor_rank[actor_rank["actor_display"].astype(str) == focal_display].iloc[0]
        focal_id_raw = str(focal_row["actor_id"])
        focal_sql_list = in_list([focal_id_raw])
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
        """)

        if partners.empty:
            st.info("Aucun partenaire dans le périmètre." if lang == "FR" else "No partners in scope.")
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


# ============================================================
# TAB ADVANCED CONCENTRATION (DuckDB)
# ============================================================
with tab_concentration:
    render_section_header("◔", t(lang, "concentration_title"), t(lang, "concentration_caption"), t(lang, "tab_advanced"))
    st.caption(t(lang, "adv_concentration_helper"))
    conc = fetch_df(f"""
    SELECT
      COALESCE(NULLIF(TRIM(org_name), ''), actor_id) AS actor_label,
      actor_id,
      SUM(amount_eur) AS b
    FROM {R}
    WHERE {W} AND actor_id IS NOT NULL AND TRIM(actor_id) <> ''
    GROUP BY actor_label, actor_id
    ORDER BY b DESC
    LIMIT 25
    """)
    if conc.empty or float(conc["b"].sum()) <= 0:
        st.info(t(lang, "no_data"))
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
- Stage selection + stage/actor focus = targeted reading of companies by chain link.
- Click on a Sankey node (when enabled) automatically isolates stage/actor.
- Star graph = collaborations around one focal actor.
- Use grouped mode (PIC/group) for industrial-group level reading.
                """
            )