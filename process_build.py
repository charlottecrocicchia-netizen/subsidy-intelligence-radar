#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 13:05:16 2026

@author: charlottecrocicchia
"""

# process_build.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ----------------------------
# Theme inference (yours, kept)
# ----------------------------
ONETECH = {
    "Hydrogen (H2)": ["hydrogen", "electroly", "fuel cell", "pem", "sofc", "h2"],
    "Solar (PV/CSP)": ["solar", "photovolta", "perovskite", "csp", "pv"],
    "Wind": ["wind", "offshore", "turbine", "floating", "blade"],
    "Bioenergy & SAF": ["biofuel", "biomass", "biogas", "saf", "aviation fuel", "e-fuel", "microalgae"],
    "CCUS": ["carbon capture", "ccus", "ccs", "co2 storage", "dac", "utilisation", "cement"],
    "Nuclear & SMR": ["nuclear", "smr", "fission", "fusion", "reactor"],
    "Batteries & Storage": ["battery", "lithium", "ion", "storage", "supercapacitor", "solid state"],
    "AI & Digital": ["artificial intelligence", "machine learning", "digital twin", "iot", "robotic", "big data"],
    "Advanced materials": ["graphene", "nanotech", "coating", "composite", "materials"],
    "E-mobility": ["electric vehicle", "ev", "charging", "powertrain", "battery swap"],
}

GENERIC = {
    "Climate & Environment": ["climate", "adaptation", "biodiversity", "environment", "pollution", "circular", "recycling"],
    "Industry & Manufacturing": ["manufacturing", "factory", "process", "industrial", "automation", "additive", "3d print"],
    "Transport & Aviation": ["aviation", "aircraft", "rail", "maritime", "shipping", "mobility", "logistics"],
    "Health & Biotech": ["health", "medical", "clinical", "vaccine", "biotech", "diagnostic"],
    "Space": ["space", "satellite", "launcher", "orbit", "earth observation"],
    "Agriculture & Food": ["agri", "crop", "soil", "food", "farming", "aquaculture"],
    "Security & Resilience": ["security", "cyber", "defence", "defense", "crisis", "resilience"],
}

def infer_theme(*parts: str) -> str:
    txt = " ".join([p or "" for p in parts]).lower()
    for theme, keys in ONETECH.items():
        if any(k in txt for k in keys):
            return theme
    for theme, keys in GENERIC.items():
        if any(k in txt for k in keys):
            return theme
    return "Other"


# ----------------------------
# Country helpers (minimal, robust)
# ----------------------------
_FALLBACK_NAME = {
    "FR": "France", "DE": "Germany", "NL": "Netherlands", "ES": "Spain", "IT": "Italy", "BE": "Belgium",
    "SE": "Sweden", "DK": "Denmark", "FI": "Finland", "NO": "Norway", "IE": "Ireland", "PT": "Portugal",
    "PL": "Poland", "AT": "Austria", "GR": "Greece", "CZ": "Czechia", "HU": "Hungary", "RO": "Romania",
    "BG": "Bulgaria", "SK": "Slovakia", "SI": "Slovenia", "HR": "Croatia", "LT": "Lithuania", "LV": "Latvia",
    "EE": "Estonia", "LU": "Luxembourg", "CY": "Cyprus", "MT": "Malta", "CH": "Switzerland", "UK": "United Kingdom",
    "US": "United States",
}
_FALLBACK_A3 = {
    "FR": "FRA", "DE": "DEU", "NL": "NLD", "ES": "ESP", "IT": "ITA", "BE": "BEL",
    "SE": "SWE", "DK": "DNK", "FI": "FIN", "NO": "NOR", "IE": "IRL", "PT": "PRT",
    "PL": "POL", "AT": "AUT", "GR": "GRC", "CZ": "CZE", "HU": "HUN", "RO": "ROU",
    "BG": "BGR", "SK": "SVK", "SI": "SVN", "HR": "HRV", "LT": "LTU", "LV": "LVA",
    "EE": "EST", "LU": "LUX", "CY": "CYP", "MT": "MLT", "CH": "CHE", "UK": "GBR",
    "US": "USA",
}

def country_name(alpha2: str) -> str:
    a2 = str(alpha2).strip().upper()
    if not a2 or a2 in {"NAN", "NONE"}:
        return ""
    try:
        import pycountry
        c = pycountry.countries.get(alpha_2=a2)
        return c.name if c else _FALLBACK_NAME.get(a2, a2)
    except Exception:
        return _FALLBACK_NAME.get(a2, a2)

def country_alpha3(alpha2: str) -> str:
    a2 = str(alpha2).strip().upper()
    if not a2 or a2 in {"NAN", "NONE"}:
        return ""
    try:
        import pycountry
        c = pycountry.countries.get(alpha_2=a2)
        return c.alpha_3 if c else _FALLBACK_A3.get(a2, a2)
    except Exception:
        return _FALLBACK_A3.get(a2, a2)


# ----------------------------
# Entity type (CORDIS activityType)
# ----------------------------
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


# ----------------------------
# Normalisation helpers (for actor_id)
# ----------------------------
_WS = re.compile(r"\s+")
_NONALNUM = re.compile(r"[^A-Z0-9]+")

def norm_name(x: Any) -> str:
    s = str(x) if x is not None else ""
    s = s.strip().upper()
    s = _WS.sub(" ", s)
    s = s.replace("&", " AND ")
    s = _NONALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s

def pick_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ----------------------------
# CORDIS loader
# ----------------------------
def _read_cordis_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", on_bad_lines="skip", low_memory=False)

def load_cordis_program(label: str, folder: Path) -> pd.DataFrame:
    proj = _read_cordis_csv(folder / "project.csv")
    org = _read_cordis_csv(folder / "organization.csv")

    # Contribution numeric
    if "ecContribution" in org.columns:
        org["ecContribution"] = pd.to_numeric(
            org["ecContribution"].astype(str).str.replace(",", ".", regex=False),
            errors="coerce"
        )
    else:
        org["ecContribution"] = np.nan

    # org name
    name_col = pick_col(org, "name", "organisationName", "organizationName")
    if not name_col:
        org["org_name"] = ""
    else:
        org["org_name"] = org[name_col].astype("string").fillna("").str.strip()

    # stable org id if present
    org_id_col = pick_col(org, "id", "organisationID", "organizationID", "orgID")
    if org_id_col:
        org["_org_id"] = org[org_id_col].astype("string").fillna("").astype(str)
    else:
        org["_org_id"] = ""

    # country alpha2
    ccol = pick_col(org, "country", "countryCode")
    org["country_alpha2"] = (org[ccol].astype("string") if ccol else "").fillna("").astype(str).str.upper().str.strip()

    # drop rows without org name (avoid “Unknown actor” pollution)
    org = org[org["org_name"].astype(str).str.len() > 0].copy()

    # actor_id (CORDIS)
    # priority: explicit org id -> else normalized name + country
    org["org_name_norm"] = org["org_name"].apply(norm_name)
    org["actor_id"] = np.where(
        org["_org_id"].astype(str).str.len() > 0,
        "CORDIS:" + org["country_alpha2"].astype(str) + ":" + org["_org_id"].astype(str),
        "CORDIS:" + org["country_alpha2"].astype(str) + ":" + org["org_name_norm"].astype(str),
    )

    # project fields (include abstract if present)
    keep_proj = [c for c in [
        "id", "acronym", "title",
        "objective", "abstract", "summary", "content",
        "startDate", "endDate",
        "frameworkProgramme", "programmeDivisionTitle", "programmeDivision",
        "topic", "topics", "call"
    ] if c in proj.columns]
    proj2 = proj[keep_proj].copy()

    # abstract col name resolution
    abs_col = pick_col(proj2, "abstract", "summary", "content")
    if abs_col:
        proj2["abstract"] = proj2[abs_col].astype("string").fillna("").str.strip()
    else:
        proj2["abstract"] = ""

    if "objective" in proj2.columns:
        proj2["objective"] = proj2["objective"].astype("string").fillna("").str.strip()
    else:
        proj2["objective"] = ""

    # merge org x project
    df = org.merge(proj2, left_on="projectID", right_on="id", how="left", suffixes=("", "_p"))

    # year
    df["year"] = pd.to_datetime(df.get("startDate"), errors="coerce").dt.year

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

    # theme uses title+acronym+objective+abstract
    df["theme"] = [
        infer_theme(
            str(t) if t is not None else "",
            str(a) if a is not None else "",
            str(o) if o is not None else "",
            str(ab) if ab is not None else ""
        )
        for t, a, o, ab in zip(
            df.get("title", pd.Series([""] * len(df))).fillna(""),
            df.get("acronym", pd.Series([""] * len(df))).fillna(""),
            df.get("objective", pd.Series([""] * len(df))).fillna(""),
            df.get("abstract", pd.Series([""] * len(df))).fillna(""),
        )
    ]

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
        "org_name": df["org_name"].astype(str),
        "entity_type": df["entity_type"].astype(str),
        "country_alpha2": df["country_alpha2"].astype(str),
        "country_alpha3": df["country_alpha3"].astype(str),
        "country_name": df["country_name"].astype(str),
        "amount_eur": df["ecContribution"],
        "theme": df["theme"].astype(str),
    })

    return out


# ----------------------------
# ADEME loader (robust + no “Unknown actor mega”)
# ----------------------------
def load_ademe(folder: Path) -> pd.DataFrame:
    f = folder / "ademe_aides_full.csv"
    if not f.exists():
        return pd.DataFrame(columns=[
            "source","program","section","year","projectID","acronym","title","objective","abstract",
            "actor_id","org_name","entity_type","country_alpha2","country_alpha3","country_name","amount_eur","theme"
        ])

    # detect separator
    head = f.read_text(errors="ignore").splitlines()[0]
    sep = ";" if head.count(";") > head.count(",") else ","
    df = pd.read_csv(f, sep=sep, low_memory=False)

    # helpers
    def pick(*cols):
        for c in cols:
            if c in df.columns:
                return df[c]
        return pd.Series([np.nan] * len(df))

    # name / title / amount / date
    org = pick("beneficiaire", "Bénéficiaire", "beneficiaires", "nom", "Nom", "raison_sociale", "Raison sociale")
    title = pick("objet", "Objet", "description", "Description", "intitule", "Intitulé")
    amount = pick("montant", "Montant", "montant_eur", "montant (€)", "montant_accorde", "Montant accordé")
    date = pick("date", "Date", "date_versement", "dateConvention", "date_signature", "Date signature")

    # section/dispositif
    section = pick("dispositif", "Dispositif", "nature", "Nature", "programme", "Programme").fillna("ADEME")

    # parse
    org_s = org.astype("string").fillna("").astype(str).str.strip()
    title_s = title.astype("string").fillna("").astype(str).str.strip()
    amount_num = pd.to_numeric(amount.astype(str).str.replace(",", ".", regex=False), errors="coerce")
    year = pd.to_datetime(date, errors="coerce").dt.year

    # build actor_id: prefer SIRET/SIREN if present
    siret_col = pick_col(df, "siret", "SIRET")
    siren_col = pick_col(df, "siren", "SIREN")
    commune_col = pick_col(df, "commune", "Commune", "ville", "Ville")
    dept_col = pick_col(df, "departement", "Département", "dept", "DEP")

    loc_hint = ""
    if commune_col:
        loc_hint = df[commune_col].astype("string").fillna("").astype(str).str.strip()
    elif dept_col:
        loc_hint = df[dept_col].astype("string").fillna("").astype(str).str.strip()

    org_norm = org_s.apply(norm_name)
    loc_norm = pd.Series(loc_hint).apply(norm_name) if isinstance(loc_hint, pd.Series) else pd.Series([""] * len(df))

    actor_id = pd.Series([""] * len(df), dtype="string")

    if siret_col:
        siret = df[siret_col].astype("string").fillna("").astype(str).str.replace(r"\D+", "", regex=True)
        actor_id = np.where(siret.str.len() >= 10, "FR:SIRET:" + siret, actor_id)
    if siren_col:
        siren = df[siren_col].astype("string").fillna("").astype(str).str.replace(r"\D+", "", regex=True)
        actor_id = np.where((pd.Series(actor_id).astype(str).str.len() == 0) & (siren.str.len() >= 9), "FR:SIREN:" + siren, actor_id)

    # fallback: name + location (stable-ish)
    actor_id = np.where(
        pd.Series(actor_id).astype(str).str.len() == 0,
        "FR:ADEME:" + org_norm.astype(str) + ":" + loc_norm.astype(str),
        actor_id
    )

    # DROP rows where beneficiary is empty (critical to avoid giant “Unknown actor”)
    mask_ok = org_s.str.len() > 0
    df_out = pd.DataFrame({
        "source": "FR",
        "program": "ADEME (France)",
        "section": section.astype("string").fillna("ADEME").astype(str),
        "year": year,
        "projectID": pick("id", "ID", "reference", "Référence", "numero_dossier", "Numéro dossier").astype("string").fillna("").astype(str),
        "acronym": "",
        "title": title_s,
        "objective": "",
        "abstract": "",
        "actor_id": pd.Series(actor_id).astype("string").fillna("").astype(str),
        "org_name": org_s,
        "entity_type": "Unknown",
        "country_alpha2": "FR",
        "country_alpha3": "FRA",
        "country_name": "France",
        "amount_eur": amount_num,
        "theme": [infer_theme(str(t)) for t in title_s.fillna("").astype(str)],
    })

    df_out = df_out[mask_ok].copy()

    return df_out


# ----------------------------
# Main build
# ----------------------------
def build_processed_dataset(raw_dir: Path, out_csv: Path) -> None:
    cordis_root = raw_dir / "cordis"
    ademe_root = raw_dir / "france" / "ademe"

    dfs: List[pd.DataFrame] = []

    he = cordis_root / "horizon_europe"
    h2 = cordis_root / "h2020"

    if he.exists():
        dfs.append(load_cordis_program("Horizon Europe", he))
    if h2.exists():
        dfs.append(load_cordis_program("Horizon 2020", h2))

    # ADEME (optional)
    dfs.append(load_ademe(ademe_root))

    out = pd.concat(dfs, ignore_index=True)

    # Clean / enforce schema
    for c in ["source","program","section","projectID","acronym","title","objective","abstract","actor_id","org_name","entity_type","country_alpha2","country_alpha3","country_name","theme"]:
        if c not in out.columns:
            out[c] = ""

    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    out["amount_eur"] = pd.to_numeric(out["amount_eur"], errors="coerce").fillna(0.0)

    # keep only valid rows (avoid empty ids/names)
    out = out[out["projectID"].astype(str).str.len() > 0].copy()
    out = out[out["actor_id"].astype(str).str.len() > 0].copy()
    out = out[out["org_name"].astype(str).str.len() > 0].copy()
    out = out[out["country_alpha3"].astype(str).str.len() > 0].copy()
    out = out.dropna(subset=["year"]).copy()

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)


def main():
    base = Path(__file__).resolve().parent
    raw_dir = base / "data" / "raw"
    out_csv = base / "data" / "processed" / "subsidy_base.csv"
    build_processed_dataset(raw_dir=raw_dir, out_csv=out_csv)
    print(f"[OK] Wrote: {out_csv}")


if __name__ == "__main__":
    main()
