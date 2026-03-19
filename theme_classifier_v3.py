#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
theme_classifier_v3.py — Enrichissement des sous-thèmes scientifiques multi-label
==============================================================================

Ce module n’attribue plus le thème principal produit. Celui-ci est désormais
piloté par les métadonnées CORDIS officielles dans process_build.py.

Rôle de ce module :
- déduire des sous-thèmes scientifiques multi-label au niveau projet,
- rester déterministe et léger,
- produire une table exploitable projet x sous-thème.
"""

from __future__ import annotations

import json
import time
import warnings
from typing import Dict, Iterable, List, Sequence

import pandas as pd

from cordis_taxonomy import (
    infer_scientific_subtheme_records,
    first_scientific_subtheme,
    scientific_subtheme_labels,
)


DEFAULT_TEXT_COLUMNS: Sequence[str] = (
    "title",
    "acronym",
    "objective",
    "abstract",
    "keywords",
    "topics",
    "topic",
    "call",
    "masterCall",
    "subCall",
    "fundingScheme",
)


def _project_level_frame(df: pd.DataFrame, project_id_col: str) -> pd.DataFrame:
    if project_id_col not in df.columns:
        raise KeyError(f"Missing project id column: {project_id_col}")

    def first_non_empty(series: pd.Series) -> str:
        for value in series.astype("string").fillna("").astype(str):
            clean = value.strip()
            if clean:
                return clean
        return ""

    cols = [
        c
        for c in [
            project_id_col,
            "title",
            "acronym",
            "objective",
            "abstract",
            "keywords",
            "topic",
            "topics",
            "call",
            "masterCall",
            "subCall",
            "fundingScheme",
            "programmeDivisionTitle",
            "programmeDivision",
            "frameworkProgramme",
            "cordis_domain_ui",
            "cordis_theme_primary",
            "cordis_topic_primary",
            "cordis_topics_all",
            "cordis_call",
            "cordis_framework_programme",
        ]
        if c in df.columns
    ]
    grouped = df[cols].groupby(project_id_col, as_index=False).agg(first_non_empty)
    grouped = grouped.rename(columns={project_id_col: "projectID"})
    return grouped


def classify_scientific_subthemes(
    df: pd.DataFrame,
    project_id_col: str = "projectID",
    verbose: bool = True,
) -> pd.DataFrame:
    t0 = time.time()
    out = df.copy()
    if project_id_col not in out.columns:
        out["scientific_subthemes"] = "[]"
        out["scientific_subthemes_count"] = 0
        out["sub_theme"] = out.get("sub_theme", "")
        return out

    project_df = _project_level_frame(out, project_id_col)
    if verbose:
        print(f"[classifier] {len(out):,} rows, {len(project_df):,} unique projects")
        print("[classifier] Using deterministic scientific multi-label rules")

    records_by_project: Dict[str, List[Dict[str, str]]] = {}
    for _, row in project_df.iterrows():
        pid = str(row.get("projectID") or "").strip()
        if not pid:
            continue
        records_by_project[pid] = infer_scientific_subtheme_records(row.to_dict())

    project_df["_scientific_records"] = project_df["projectID"].map(records_by_project).apply(lambda x: x if isinstance(x, list) else [])
    project_df["scientific_subthemes"] = project_df["_scientific_records"].apply(lambda recs: json.dumps(scientific_subtheme_labels(recs), ensure_ascii=False))
    project_df["scientific_subthemes_count"] = project_df["_scientific_records"].apply(lambda recs: len(scientific_subtheme_labels(recs)))
    project_df["sub_theme"] = project_df["_scientific_records"].apply(first_scientific_subtheme)

    mapping = project_df.set_index("projectID")[["scientific_subthemes", "scientific_subthemes_count", "sub_theme"]]
    out["scientific_subthemes"] = out[project_id_col].map(mapping["scientific_subthemes"]).fillna("[]")
    out["scientific_subthemes_count"] = out[project_id_col].map(mapping["scientific_subthemes_count"]).fillna(0).astype(int)
    out["sub_theme"] = out[project_id_col].map(mapping["sub_theme"]).fillna("")

    elapsed = time.time() - t0
    if verbose:
        n_with = int((project_df["scientific_subthemes_count"] > 0).sum())
        pct = (100.0 * n_with / len(project_df)) if len(project_df) else 0.0
        print(f"[classifier] Scientific sub-themes on {n_with:,} projects ({pct:.1f}%)")
        print(f"[classifier] Done in {elapsed:.1f}s")
    return out



def build_project_scientific_subthemes_table(
    df: pd.DataFrame,
    project_id_col: str = "projectID",
) -> pd.DataFrame:
    project_df = _project_level_frame(df, project_id_col)
    rows: List[Dict[str, str]] = []
    for _, row in project_df.iterrows():
        pid = str(row.get("projectID") or "").strip()
        if not pid:
            continue
        recs = infer_scientific_subtheme_records(row.to_dict())
        domain = str(row.get("cordis_domain_ui") or "").strip()
        primary = str(row.get("cordis_theme_primary") or "").strip()
        for rec in recs:
            rows.append(
                {
                    "projectID": pid,
                    "cordis_domain_ui": domain,
                    "cordis_theme_primary": primary,
                    **rec,
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=[
                "projectID",
                "cordis_domain_ui",
                "cordis_theme_primary",
                "subtheme_level_1",
                "subtheme_level_2",
                "subtheme_level_3",
                "subtheme_label",
                "subtheme_path",
                "source_method",
            ]
        )
    table = pd.DataFrame(rows).drop_duplicates().sort_values([
        "projectID",
        "subtheme_level_1",
        "subtheme_level_2",
        "subtheme_label",
    ]).reset_index(drop=True)
    return table



def classify_projects(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    warnings.warn(
        "classify_projects() is deprecated for primary-theme assignment and now only enriches scientific multi-label sub-themes.",
        DeprecationWarning,
        stacklevel=2,
    )
    return classify_scientific_subthemes(df, *args, **kwargs)


if __name__ == "__main__":
    sample = pd.DataFrame(
        {
            "projectID": ["P1", "P2", "P3"],
            "title": [
                "Hydrogen production by electrolysis for industrial decarbonisation",
                "Digital twin platform for predictive maintenance in manufacturing",
                "ERC frontier research on topological phases in quantum materials",
            ],
            "objective": ["", "", ""],
            "abstract": ["", "", ""],
            "topics": ["HORIZON-JU-CLEANH2-2025-01-02", "HORIZON-CL4-2025-02", "ERC-2025-STG"],
            "keywords": ["electrolyser, hydrogen", "digital twin, predictive maintenance", "frontier research, quantum materials"],
            "cordis_domain_ui": ["Energy", "Industrial Technologies", "Fundamental Research"],
            "cordis_theme_primary": ["HORIZON-JU-CLEANH2-2025-01-02", "HORIZON-CL4-2025-02", "ERC-2025-STG"],
        }
    )
    enriched = classify_scientific_subthemes(sample, verbose=True)
    print(enriched[["projectID", "cordis_domain_ui", "cordis_theme_primary", "sub_theme", "scientific_subthemes"]].to_string(index=False))
