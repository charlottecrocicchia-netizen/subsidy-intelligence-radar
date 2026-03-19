#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
theme_classifier_v3.py — Classifieur thématique par embeddings IA
=================================================================

Architecture en 2 passes :
  1. Embeddings (sentence-transformers) : encode chaque projet, compare aux
     descriptions de référence par thème/sous-thème via cosine similarity.
  2. Fallback regex : pour les cas sous le seuil de confiance.

Usage dans process_build.py :
    from theme_classifier_v3 import classify_projects
    df = classify_projects(df)  # ajoute colonnes 'theme' + 'sub_theme'

Performances attendues sur 55 000 lignes (~20k projets uniques) :
  - Mac M1/M2 : ~30-60s
  - Mac Intel  : ~60-120s
  - Linux CI   : ~45-90s
  - Streamlit Cloud : JAMAIS exécuté (lit le parquet pré-calculé)

Dépendance : pip install sentence-transformers
  (installé uniquement en local pour le build)
"""

from __future__ import annotations

import re
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ============================================================
# 1. RÉFÉRENTIEL THÉMATIQUE
#    Chaque thème a une description riche + des sous-thèmes.
#    Le modèle encode ces textes et compare par cosine similarity.
# ============================================================

THEME_REFERENCES: Dict[str, Dict] = {
    "Hydrogen (H2)": {
        "description": (
            "Hydrogen energy production, storage, transport and end-use. "
            "Electrolysis, electrolysers (PEM, alkaline, solid oxide), fuel cells, "
            "hydrogen refuelling stations, power-to-gas, green hydrogen, "
            "hydrogen pipeline infrastructure, hydrogen mobility applications, "
            "catalysts and membranes for hydrogen systems."
        ),
        "sub_themes": {
            "Hydrogen production": "Hydrogen production methods including electrolysis, steam methane reforming, water splitting",
            "Electrolysis": "PEM electrolyser, alkaline electrolysis, solid oxide electrolyser, proton exchange membrane",
            "Hydrogen storage": "Hydrogen storage technologies, compressed gas, liquid hydrogen, metal hydrides, underground storage",
            "Hydrogen transport & distribution": "Hydrogen pipeline, hydrogen transport, distribution network, hydrogen refuelling",
            "Fuel cells": "Fuel cell technology, PEM fuel cell, SOFC, hydrogen fuel cell for vehicles and stationary power",
            "Industrial uses": "Hydrogen in industrial processes, steel making with hydrogen, ammonia production, chemical feedstock",
            "Mobility applications": "Hydrogen vehicles, hydrogen buses, hydrogen trucks, hydrogen ships, hydrogen aviation",
            "Catalysts, membranes & safety": "Catalysts for hydrogen, membrane technology, hydrogen safety, hydrogen sensors",
        },
    },
    "Solar (PV/CSP)": {
        "description": (
            "Solar energy including photovoltaics (PV), concentrated solar power (CSP), "
            "solar cells, solar panels, perovskite solar cells, tandem cells, "
            "thin-film technologies, solar thermal, agrivoltaics, building-integrated PV, "
            "solar module manufacturing and recycling."
        ),
        "sub_themes": {
            "Photovoltaic materials": "Perovskite, silicon, thin-film, tandem, multijunction solar cell materials",
            "PV cells & architectures": "Solar cell design, heterojunction, back-contact, bifacial cells",
            "PV modules & balance of system": "Solar panels, modules, inverters, mounting systems, balance of system",
            "Concentrated solar power (CSP)": "CSP tower, parabolic trough, solar thermal electricity, molten salt",
            "Solar thermal": "Solar water heating, solar thermal collectors, solar cooling",
            "Grid integration & flexibility": "Solar grid integration, intermittency management, solar forecasting",
            "Recycling & sustainability": "PV module recycling, end-of-life solar panels, circular economy for PV",
        },
    },
    "Wind": {
        "description": (
            "Wind energy generation including onshore and offshore wind farms, "
            "floating wind platforms, wind turbines, rotor blades, "
            "wind tower design, aerodynamics, wake effects, "
            "operations and maintenance of wind installations, grid integration."
        ),
        "sub_themes": {
            "Onshore wind": "Onshore wind farms, land-based wind turbines, community wind projects",
            "Offshore wind": "Offshore wind farms, fixed-bottom offshore, North Sea wind energy",
            "Floating wind": "Floating offshore wind platforms, deep-water wind energy, spar buoy, tension leg platform",
            "Turbines, blades & materials": "Wind turbine design, rotor blades, blade materials, drivetrain, gearbox",
            "Aerodynamics & wake effects": "Wind farm aerodynamics, wake modelling, turbulence, yaw control",
            "Operations & maintenance": "Wind farm O&M, predictive maintenance for wind, condition monitoring",
            "Grid integration": "Wind power grid integration, power electronics for wind, frequency response",
        },
    },
    "Bioenergy & SAF": {
        "description": (
            "Bioenergy, biofuels, biogas, biomass conversion, biomethane, "
            "sustainable aviation fuels (SAF), e-fuels, biorefinery, "
            "lignocellulosic biomass processing, anaerobic digestion, "
            "microalgae biofuel, pyrolysis, gasification, waste-to-energy."
        ),
        "sub_themes": {
            "Biomass & feedstocks": "Biomass resources, lignocellulosic feedstock, agricultural residues, forestry waste",
            "Biofuels": "Bioethanol, biodiesel, first and second generation biofuels",
            "Advanced biofuels": "Advanced biofuels from waste, cellulosic ethanol, drop-in biofuels",
            "Sustainable aviation fuel (SAF)": "SAF production, HEFA, Fischer-Tropsch SAF, alcohol-to-jet, power-to-liquid",
            "Biochemical conversion": "Enzymatic hydrolysis, fermentation, anaerobic digestion, biogas upgrading",
            "Thermochemical conversion": "Pyrolysis, gasification, hydrothermal liquefaction, torrefaction",
            "Lifecycle assessment & sustainability": "Bioenergy LCA, sustainability certification, land use, carbon neutrality",
        },
    },
    "CCUS": {
        "description": (
            "Carbon capture, utilisation and storage (CCUS/CCS). "
            "CO2 capture from industrial point sources, direct air capture (DAC), "
            "CO2 geological storage, CO2 transport by pipeline or ship, "
            "CO2 utilisation for fuels or chemicals, carbon mineralization, "
            "monitoring, reporting and verification of stored CO2."
        ),
        "sub_themes": {
            "CO2 capture": "Post-combustion capture, pre-combustion, oxyfuel, CO2 separation, sorbents, membranes for CO2",
            "Direct air capture (DAC)": "Direct air capture technology, solid sorbent DAC, liquid solvent DAC",
            "CO2 transport": "CO2 pipeline, CO2 shipping, CO2 transport infrastructure",
            "CO2 storage": "Geological CO2 storage, saline aquifer, depleted reservoir, storage monitoring",
            "CO2 utilization": "CO2 to fuels, CO2 to chemicals, carbon utilization, e-methanol, e-kerosene from CO2",
            "Carbon mineralization": "Mineral carbonation, accelerated weathering, CO2 in concrete",
            "Monitoring, reporting & verification": "CO2 storage MRV, seismic monitoring, leakage detection",
        },
    },
    "Nuclear & SMR": {
        "description": (
            "Nuclear energy including fission and fusion reactors, "
            "small modular reactors (SMR), Generation IV reactors, "
            "nuclear fuel cycle, nuclear waste management, decommissioning, "
            "tokamak fusion, ITER, nuclear safety and licensing, Euratom research."
        ),
        "sub_themes": {
            "SMR technologies": "Small modular reactor, SMR design, factory-built nuclear, microreactor",
            "Reactor systems": "Nuclear reactor design, Gen IV, molten salt reactor, fast reactor, lead-cooled",
            "Fuel cycle": "Nuclear fuel, uranium enrichment, fuel fabrication, MOX fuel, thorium cycle",
            "Nuclear materials": "Radiation-resistant materials, nuclear-grade steel, cladding materials",
            "Safety & licensing": "Nuclear safety, probabilistic safety assessment, licensing framework",
            "Waste management": "Nuclear waste, spent fuel management, vitrification, deep geological repository",
            "Decommissioning": "Nuclear decommissioning, site remediation, dismantling",
            "Fusion": "Nuclear fusion, tokamak, stellarator, ITER, DEMO, plasma physics, tritium breeding",
        },
    },
    "Batteries & Storage": {
        "description": (
            "Battery technologies and energy storage systems. "
            "Lithium-ion batteries, solid-state batteries, sodium-ion, "
            "battery cells, modules, packs, battery management systems (BMS), "
            "stationary energy storage, battery recycling, second life, "
            "supercapacitors, redox flow batteries, grid-scale storage."
        ),
        "sub_themes": {
            "Battery chemistries": "Lithium-ion, sodium-ion, lithium-sulfur, zinc-air, battery chemistry research",
            "Electrode & electrolyte materials": "Cathode, anode, electrolyte, solid electrolyte, separator materials",
            "Cells, modules & packs": "Battery cell manufacturing, module assembly, pack design, gigafactory",
            "Battery management systems": "BMS, state of charge, state of health, battery diagnostics, thermal management",
            "Stationary storage": "Grid-scale storage, behind-the-meter, peak shaving, frequency regulation",
            "Aging, diagnostics & safety": "Battery aging, degradation, thermal runaway, battery safety testing",
            "Recycling & second life": "Battery recycling, hydrometallurgy, pyrometallurgy, second life applications",
        },
    },
    "AI & Digital": {
        "description": (
            "Artificial intelligence, machine learning, deep learning, "
            "digital twins, computer vision, natural language processing, "
            "IoT platforms, big data analytics, cloud and edge computing, "
            "cybersecurity, quantum computing, robotics, data-driven optimization."
        ),
        "sub_themes": {
            "Artificial intelligence & machine learning": "AI, ML, deep learning, neural networks, reinforcement learning",
            "Computer vision & sensing": "Image recognition, object detection, LiDAR, remote sensing with AI",
            "Digital twins": "Digital twin simulation, virtual replica, real-time digital model",
            "Modeling & simulation": "Computational modeling, simulation tools, multi-physics simulation",
            "Optimization & decision support": "AI optimization, scheduling, planning, decision support systems",
            "Data platforms & software": "Big data platform, data pipeline, software architecture, cloud computing",
            "Cybersecurity": "Cyber threat detection, network security, encryption, privacy-preserving AI",
        },
    },
    "Advanced materials": {
        "description": (
            "Advanced and functional materials research including graphene, "
            "nanomaterials, nanocomposites, metamaterials, carbon nanotubes, "
            "coatings, surface engineering, composites, ceramics, "
            "critical raw materials, rare earth elements, membrane technology."
        ),
        "sub_themes": {
            "Catalysts": "Catalytic materials, electrocatalysts, photocatalysts, heterogeneous catalysis",
            "Membranes & separators": "Membrane technology, ion exchange membranes, gas separation membranes",
            "Coatings & surface engineering": "Protective coatings, anti-corrosion, self-healing coatings, surface treatment",
            "Composites & structural materials": "Carbon fibre composites, polymer composites, lightweight structural materials",
            "Nanomaterials": "Graphene, carbon nanotubes, nanoparticles, quantum dots, 2D materials",
            "Functional materials": "Smart materials, piezoelectric, thermoelectric, shape memory alloys",
            "Recyclable & circular materials": "Recyclable materials, bio-based materials, circular economy materials",
        },
    },
    "E-mobility": {
        "description": (
            "Electric mobility including electric vehicles (cars, buses, trucks), "
            "EV charging infrastructure, fast charging, vehicle-to-grid (V2G), "
            "electric drivetrains, power electronics, smart charging, "
            "battery electric vehicles, plug-in hybrids, electric maritime and aviation."
        ),
        "sub_themes": {
            "Electric vehicles": "Electric car, electric bus, electric truck, BEV design and development",
            "Charging infrastructure": "EV charging station, fast charger, ultra-fast charging, charging network",
            "Power electronics": "Inverter, converter, wide-bandgap semiconductor, SiC, GaN for EV",
            "Electric drivetrains": "Electric motor, e-axle, transmission, powertrain electrification",
            "Smart charging": "Smart charging management, demand response, load balancing for EV",
            "Vehicle-to-grid (V2G)": "V2G, bidirectional charging, vehicle-grid integration, grid services from EVs",
            "Heavy-duty & maritime electrification": "Electric ship, electric ferry, electric heavy-duty truck, mining vehicle electrification",
        },
    },
    "Climate & Environment": {
        "description": (
            "Climate change, environmental protection, biodiversity, "
            "ecosystem restoration, pollution monitoring and remediation, "
            "circular economy, waste management, water treatment, "
            "air quality, greenhouse gas emissions, carbon footprint, "
            "nature-based solutions, ocean health."
        ),
        "sub_themes": {
            "Climate adaptation": "Climate change adaptation, resilience, urban heat island, flood protection",
            "Climate mitigation": "GHG emission reduction, decarbonization pathways, net-zero strategies",
            "Carbon footprint & LCA": "Life cycle assessment, carbon footprint, environmental footprint, scope 1 2 3",
            "Air quality": "Air pollution monitoring, particulate matter, NOx, urban air quality",
            "Water resources & treatment": "Water treatment, wastewater, desalination, water reuse, water purification",
            "Pollution monitoring & remediation": "Soil remediation, contaminated site, micropollutants, environmental sensors",
            "Biodiversity & ecosystems": "Biodiversity conservation, ecosystem services, habitat restoration, rewilding",
            "Circular economy & waste": "Circular economy, waste recycling, waste reduction, resource efficiency",
        },
    },
    "Industry & Manufacturing": {
        "description": (
            "Industrial production, manufacturing processes, smart factories, "
            "Industry 4.0, industrial automation, robotics, additive manufacturing, "
            "semiconductor fabrication, pilot lines, process intensification, "
            "industrial decarbonisation, predictive maintenance, quality control."
        ),
        "sub_themes": {
            "Advanced manufacturing": "Advanced manufacturing processes, precision manufacturing, micro-manufacturing",
            "Process intensification": "Process intensification, reactor design, continuous flow, catalytic process",
            "Industrial decarbonization": "Industrial decarbonisation, electrification of heat, green steel, cement decarbonisation",
            "Automation & robotics": "Industrial robots, collaborative robots, automated assembly, AGV",
            "Sensors & industrial monitoring": "Industrial sensors, condition monitoring, vibration analysis",
            "Predictive maintenance": "Predictive maintenance, fault detection, remaining useful life prediction",
            "Supply chain & logistics": "Supply chain optimization, logistics, inventory management, digital supply chain",
            "Semiconductor & electronics": "Semiconductor fabrication, chip design, photonics, integrated circuits, pilot line",
        },
    },
    "Transport & Aviation": {
        "description": (
            "Transportation systems including aviation, aircraft design, "
            "sustainable aviation, rail transport, maritime shipping, "
            "urban mobility, autonomous vehicles, unmanned aerial vehicles, "
            "air traffic management, freight logistics, rolling stock."
        ),
        "sub_themes": {
            "Sustainable aviation": "Green aviation, hybrid-electric aircraft, hydrogen aircraft, low-emission flight",
            "Aeronautics systems": "Aircraft design, airframe, propulsion, avionics, flight control",
            "Rail transport": "Railway, high-speed rail, rolling stock, rail signalling, freight rail",
            "Maritime transport": "Shipping, maritime vessels, port logistics, autonomous shipping, green shipping",
            "Logistics & freight": "Freight transport, intermodal logistics, last-mile delivery, supply chain",
            "Low-emission mobility systems": "Urban mobility, multimodal transport, MaaS, low-carbon transport",
            "Transport infrastructure": "Road infrastructure, bridge, tunnel, transport network planning",
            "Traffic optimization & operations": "Air traffic management, traffic flow optimization, congestion management",
        },
    },
    "Health & Biotech": {
        "description": (
            "Health research, medicine, biotechnology, drug discovery, "
            "diagnostics, medical devices, vaccines, genomics, "
            "personalised medicine, clinical trials, digital health, "
            "cancer research, infectious disease, gene therapy, CRISPR."
        ),
        "sub_themes": {
            "Drug discovery": "Drug target identification, high-throughput screening, lead optimization, pharmacology",
            "Bioprocessing & biomanufacturing": "Bioreactor, cell culture, fermentation, biopharmaceutical manufacturing",
            "Diagnostics": "Point-of-care diagnostic, biosensor, lateral flow, PCR-based diagnostic",
            "Medical devices": "Medical device development, implant, surgical robot, wearable medical device",
            "Genomics & omics": "Genomics, transcriptomics, proteomics, metabolomics, multi-omics integration",
            "Vaccines & therapeutics": "Vaccine development, mRNA vaccine, monoclonal antibody, immunotherapy, CAR-T",
            "Digital health": "Telemedicine, remote patient monitoring, health data platform, AI in healthcare",
            "Gene & cell therapy": "Gene therapy, CRISPR gene editing, cell therapy, stem cell therapy, regenerative medicine",
        },
    },
    "Space": {
        "description": (
            "Space technology, satellite systems, earth observation, "
            "space launch and propulsion, planetary exploration, "
            "satellite navigation (Galileo), space debris, "
            "in-orbit manufacturing, LEO constellations, Copernicus."
        ),
        "sub_themes": {
            "Earth observation": "Earth observation satellite, remote sensing, Copernicus, environmental monitoring from space",
            "Satellite systems": "Communication satellite, LEO constellation, satellite platform, antenna, payload",
            "Launch systems": "Rocket, launch vehicle, microlauncher, reusable launch system",
            "Space propulsion": "Electric propulsion, ion engine, solar sail, green propellant",
            "Space robotics": "In-orbit servicing, space manipulator, autonomous docking, debris removal",
            "Planetary exploration": "Mars mission, lunar exploration, asteroid mining, deep space probe",
            "Telecommunications & navigation": "Satellite communication, Galileo navigation, GNSS, broadband from space",
            "Space situational awareness": "Space debris tracking, collision avoidance, space weather, SSA",
        },
    },
    "Agriculture & Food": {
        "description": (
            "Agriculture, food production and food systems, "
            "precision agriculture, crop improvement, soil health, "
            "aquaculture, livestock, sustainable farming, "
            "food safety, alternative proteins, vertical farming, agroecology."
        ),
        "sub_themes": {
            "Precision agriculture": "Precision agriculture, variable rate, GPS-guided, drone-based crop monitoring",
            "Agri-biotech": "Agricultural biotechnology, transgenic crops, genome editing in agriculture, biopesticides",
            "Soil health": "Soil microbiome, soil carbon sequestration, soil fertility, erosion prevention",
            "Crop improvement": "Plant breeding, genomic selection, drought-tolerant crop, disease-resistant variety",
            "Food processing": "Food technology, preservation, packaging, novel food processing, cold chain",
            "Alternative proteins": "Insect protein, cultured meat, plant-based protein, fermentation-derived protein",
            "Water efficiency in agriculture": "Irrigation efficiency, drip irrigation, water-smart agriculture",
            "Food systems sustainability": "Sustainable food system, food waste reduction, short supply chain, organic farming",
        },
    },
    "Security & Resilience": {
        "description": (
            "Security, defence, cyber resilience, critical infrastructure protection, "
            "border security, counter-terrorism, CBRN, civil security, "
            "disaster risk reduction, emergency response, surveillance and detection."
        ),
        "sub_themes": {
            "Cybersecurity": "Cyber attack detection, network security, encryption, threat intelligence",
            "Critical infrastructure resilience": "Protection of energy grids, water systems, transport from disruption",
            "Energy security": "Security of energy supply, strategic reserves, diversification of energy sources",
            "Disaster risk reduction": "Natural disaster preparedness, early warning system, flood risk, earthquake",
            "Civil security": "Public safety, law enforcement technology, crowd management, CCTV analytics",
            "Defense technologies": "Defence research, military technology, dual-use technology",
            "Surveillance & detection": "Sensor network, radar, UAV surveillance, explosive detection",
            "Emergency response systems": "First responder technology, search and rescue, crisis communication",
        },
    },
}

# Label pour les projets non classés
FALLBACK_LABEL = "Other"
FALLBACK_SUBLABEL = ""

# Seuils de confiance
EMBEDDING_THRESHOLD = 0.32      # cosine sim min pour accepter un thème
SUB_THEME_THRESHOLD = 0.30      # cosine sim min pour accepter un sous-thème
FALLBACK_REGEX_THRESHOLD = 2    # score regex min (identique à v2)


# ============================================================
# 2. REGEX FALLBACK (récupéré de la v2, pour les cas limites)
# ============================================================

REGEX_KEYWORDS: Dict[str, List[Tuple[str, int]]] = {
    "Hydrogen (H2)": [
        ("hydrogen", 2), ("electrolyser", 2), ("electrolyzer", 2),
        ("electrolysis", 2), ("fuel cell", 2), ("fuel cells", 2),
    ],
    "Solar (PV/CSP)": [
        ("solar", 2), ("photovoltaic", 2), ("photovoltaics", 2),
    ],
    "Wind": [
        ("wind turbine", 2), ("wind energy", 2), ("wind power", 2),
        ("wind farm", 2), ("offshore wind", 2), ("onshore wind", 2),
        ("floating wind", 2),
    ],
    "Bioenergy & SAF": [
        ("bioenergy", 2), ("biofuel", 2), ("biogas", 2),
        ("biomass", 2), ("biomethane", 2), ("sustainable aviation fuel", 2),
    ],
    "CCUS": [
        ("CCUS", 2), ("CCS", 2), ("carbon capture", 2),
        ("CO2 capture", 2), ("CO2 storage", 2), ("direct air capture", 2),
    ],
    "Nuclear & SMR": [
        ("nuclear", 2), ("fission", 2), ("fusion", 2),
        ("tokamak", 2), ("Euratom", 2),
    ],
    "Batteries & Storage": [
        ("battery", 2), ("batteries", 2), ("lithium", 2),
        ("energy storage", 2), ("supercapacitor", 2),
    ],
    "AI & Digital": [
        ("artificial intelligence", 2), ("machine learning", 2),
        ("deep learning", 2), ("digital twin", 2),
    ],
    "Advanced materials": [
        ("graphene", 2), ("nanomaterial", 2), ("nanocomposite", 2),
        ("advanced material", 2), ("advanced materials", 2),
        ("coating", 2), ("coatings", 2), ("critical raw material", 2),
    ],
    "E-mobility": [
        ("electric vehicle", 2), ("e-mobility", 2),
        ("EV charging", 2), ("charging station", 2), ("vehicle-to-grid", 2),
    ],
    "Climate & Environment": [
        ("climate", 2), ("biodiversity", 2), ("ecosystem", 2),
        ("pollution", 2), ("circular economy", 2), ("greenhouse gas", 2),
    ],
    "Industry & Manufacturing": [
        ("manufacturing", 2), ("industry 4.0", 2),
        ("semiconductor", 2), ("additive manufacturing", 2),
    ],
    "Transport & Aviation": [
        ("aviation", 2), ("aircraft", 2), ("aeronautic", 2),
        ("rail transport", 2), ("maritime transport", 2),
    ],
    "Health & Biotech": [
        ("health", 2), ("medical", 2), ("clinical", 2),
        ("vaccine", 2), ("cancer", 2), ("CRISPR", 2), ("gene therapy", 2),
    ],
    "Space": [
        ("satellite", 2), ("space", 2), ("earth observation", 2),
    ],
    "Agriculture & Food": [
        ("agriculture", 2), ("farming", 2), ("crop", 2), ("food", 2),
        ("aquaculture", 2),
    ],
    "Security & Resilience": [
        ("cybersecurity", 2), ("border security", 2),
        ("counter-terrorism", 2), ("CBRN", 2), ("critical infrastructure", 2),
    ],
}

_REGEX_COMPILED: Dict[str, re.Pattern] = {}


def _ensure_regex():
    if _REGEX_COMPILED:
        return
    for theme, kws in REGEX_KEYWORDS.items():
        w2 = [kw for kw, w in kws if w >= 2]
        if w2:
            pat = "|".join(r"\b" + re.escape(k) + r"\b" for k in w2)
            _REGEX_COMPILED[theme] = re.compile(pat, re.IGNORECASE)


def _regex_classify(text: str) -> str:
    """Simple regex fallback classifier."""
    _ensure_regex()
    best_theme = FALLBACK_LABEL
    best_count = 0
    t = text.lower()
    for theme, pat in _REGEX_COMPILED.items():
        matches = pat.findall(t)
        if len(matches) > best_count:
            best_count = len(matches)
            best_theme = theme
    return best_theme if best_count > 0 else FALLBACK_LABEL


def _regex_classify_series(texts: pd.Series) -> pd.Series:
    """Vectorized regex fallback."""
    _ensure_regex()
    s = texts.fillna("").astype(str).str.lower()
    n = len(s)
    themes = list(_REGEX_COMPILED.keys())
    scores = np.zeros((n, len(themes)), dtype=np.int16)
    for j, theme in enumerate(themes):
        scores[:, j] = s.str.findall(_REGEX_COMPILED[theme]).str.len().astype(np.int16).values
    best_idx = np.argmax(scores, axis=1)
    best_score = scores[np.arange(n), best_idx]
    result = np.where(best_score > 0, np.array(themes)[best_idx], FALLBACK_LABEL)
    return pd.Series(result, index=texts.index)


# ============================================================
# 3. EMBEDDINGS CLASSIFIER
# ============================================================

def _try_load_model():
    """Try to load sentence-transformers model. Returns None if not available."""
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("all-MiniLM-L6-v2")
        return model
    except ImportError:
        warnings.warn(
            "[theme_classifier_v3] sentence-transformers not installed. "
            "Using regex fallback. Install with: pip install sentence-transformers"
        )
        return None
    except Exception as e:
        warnings.warn(f"[theme_classifier_v3] Failed to load model: {e}. Using regex fallback.")
        return None


def _build_reference_embeddings(model) -> Tuple[np.ndarray, List[str], np.ndarray, List[str], List[str]]:
    """
    Build reference embeddings for themes and sub-themes.

    Returns:
        theme_embeddings: (n_themes, dim)
        theme_names: list of theme names
        sub_embeddings: (n_sub_themes, dim)
        sub_names: list of sub-theme names
        sub_parents: list of parent theme for each sub-theme
    """
    theme_texts = []
    theme_names = []
    sub_texts = []
    sub_names = []
    sub_parents = []

    for theme, ref in THEME_REFERENCES.items():
        theme_texts.append(ref["description"])
        theme_names.append(theme)
        for sub_name, sub_desc in ref.get("sub_themes", {}).items():
            sub_texts.append(sub_desc)
            sub_names.append(sub_name)
            sub_parents.append(theme)

    # Encode all references in one batch
    all_texts = theme_texts + sub_texts
    all_emb = model.encode(all_texts, batch_size=64, show_progress_bar=False, normalize_embeddings=True)

    n_themes = len(theme_texts)
    theme_emb = all_emb[:n_themes]
    sub_emb = all_emb[n_themes:]

    return (
        np.array(theme_emb),
        theme_names,
        np.array(sub_emb),
        sub_names,
        sub_parents,
    )


def _cosine_similarity_matrix(A: np.ndarray, B: np.ndarray) -> np.ndarray:
    """Compute cosine similarity between rows of A and rows of B.
    Both A and B should be L2-normalized. Returns (len(A), len(B))."""
    return A @ B.T


def classify_projects(
    df: pd.DataFrame,
    text_columns: List[str] = ("title", "acronym", "objective", "abstract"),
    project_id_col: str = "projectID",
    batch_size: int = 256,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Classify projects by theme and sub_theme using embeddings (preferred)
    or regex fallback.

    Deduplicates by projectID to encode only unique projects (~20k instead of 55k).
    Then broadcasts results back to all rows.

    Adds columns: 'theme', 'sub_theme'

    Args:
        df: DataFrame with project data
        text_columns: columns to concatenate for classification text
        project_id_col: column to group by for deduplication
        batch_size: encoding batch size
        verbose: print progress
    """
    import time

    t0 = time.time()
    df = df.copy()

    # Build combined text per row
    parts = []
    for col in text_columns:
        if col in df.columns:
            parts.append(df[col].fillna("").astype(str))
    if not parts:
        df["theme"] = FALLBACK_LABEL
        df["sub_theme"] = FALLBACK_SUBLABEL
        return df

    combined = parts[0]
    for p in parts[1:]:
        combined = combined + " " + p

    # Deduplicate by project to avoid encoding the same text 10x
    # (CORDIS has one row per participant, ~10 participants per project)
    proj_text = pd.DataFrame({
        "pid": df[project_id_col].fillna("").astype(str),
        "text": combined,
    })
    unique_projects = proj_text.groupby("pid")["text"].first().reset_index()
    n_unique = len(unique_projects)

    if verbose:
        print(f"[classifier] {len(df):,} rows, {n_unique:,} unique projects")

    # Try embeddings model
    model = _try_load_model()

    if model is not None:
        if verbose:
            print("[classifier] Using sentence-transformers embeddings")

        # Build reference embeddings
        theme_emb, theme_names, sub_emb, sub_names, sub_parents = _build_reference_embeddings(model)

        # Encode project texts in batches
        project_texts = unique_projects["text"].tolist()

        # Truncate long texts (model max is 256 tokens, ~1200 chars is safe)
        project_texts_trunc = [t[:1500] if len(t) > 1500 else t for t in project_texts]

        if verbose:
            print(f"[classifier] Encoding {n_unique:,} project texts...")

        project_emb = model.encode(
            project_texts_trunc,
            batch_size=batch_size,
            show_progress_bar=verbose,
            normalize_embeddings=True,
        )
        project_emb = np.array(project_emb)

        # Compute similarities
        theme_sims = _cosine_similarity_matrix(project_emb, theme_emb)  # (n_proj, n_themes)
        sub_sims = _cosine_similarity_matrix(project_emb, sub_emb)      # (n_proj, n_sub)

        # Best theme per project
        best_theme_idx = np.argmax(theme_sims, axis=1)
        best_theme_sim = theme_sims[np.arange(n_unique), best_theme_idx]
        themes = np.array(theme_names)[best_theme_idx]

        # Apply threshold — below threshold, use regex fallback
        low_conf_mask = best_theme_sim < EMBEDDING_THRESHOLD
        if low_conf_mask.any():
            low_texts = unique_projects.loc[low_conf_mask, "text"]
            regex_themes = _regex_classify_series(low_texts)
            themes[low_conf_mask] = regex_themes.values
            if verbose:
                print(f"[classifier] {low_conf_mask.sum():,} projects below embedding threshold, regex fallback used")

        # Best sub-theme per project (constrained to assigned theme)
        sub_theme_result = np.full(n_unique, "", dtype=object)
        for i in range(n_unique):
            assigned_theme = themes[i]
            if assigned_theme == FALLBACK_LABEL:
                continue
            # Find sub-theme indices belonging to this theme
            valid_sub_idx = [j for j, parent in enumerate(sub_parents) if parent == assigned_theme]
            if not valid_sub_idx:
                continue
            sub_scores = sub_sims[i, valid_sub_idx]
            best_local = np.argmax(sub_scores)
            if sub_scores[best_local] >= SUB_THEME_THRESHOLD:
                sub_theme_result[i] = sub_names[valid_sub_idx[best_local]]

        unique_projects["theme"] = themes
        unique_projects["sub_theme"] = sub_theme_result

    else:
        # Regex fallback (no model available)
        if verbose:
            print("[classifier] sentence-transformers not available, using regex fallback")
        unique_projects["theme"] = _regex_classify_series(unique_projects["text"]).values
        unique_projects["sub_theme"] = ""

    # Broadcast back to all rows via projectID
    theme_map = unique_projects.set_index("pid")[["theme", "sub_theme"]]
    df["theme"] = df[project_id_col].map(theme_map["theme"]).fillna(FALLBACK_LABEL)
    df["sub_theme"] = df[project_id_col].map(theme_map["sub_theme"]).fillna("")

    elapsed = time.time() - t0
    if verbose:
        vc = unique_projects["theme"].value_counts()
        other_n = int((unique_projects["theme"] == FALLBACK_LABEL).sum())
        other_pct = other_n / n_unique * 100
        print(f"[classifier] Done in {elapsed:.1f}s")
        print(f"[classifier] Other/Multithématique: {other_n:,} ({other_pct:.1f}%)")
        for t, c in vc.head(10).items():
            print(f"  {t:<28} {c:>6} ({c/n_unique*100:4.1f}%)")

    return df


# ============================================================
# 4. STANDALONE TEST
# ============================================================

if __name__ == "__main__":
    test_titles = [
        "Green hydrogen production through anion exchange membrane electrolysis",
        "Floating offshore wind energy platform design and demonstration",
        "Solid-state lithium battery for electric mobility",
        "Carbon capture and geological storage demonstration",
        "CRISPR-based gene therapy for rare genetic disorders",
        "Precision agriculture platform using drone-based multispectral imaging",
        "Digital transformation of European manufacturing SMEs via Industry 4.0",
        "Earth observation satellite constellation for environmental monitoring",
        "ERC Starting Grant: Topological phases in quantum materials",
        "Marie Sklodowska-Curie Innovative Training Network: Complex Systems",
        "European pilot line for beyond 2nm leading edge System-on-Chip leadership",
        "COST: Strengthening science through research networking",
    ]

    test_df = pd.DataFrame({
        "projectID": [f"TEST_{i}" for i in range(len(test_titles))],
        "title": test_titles,
        "objective": [""] * len(test_titles),
        "abstract": [""] * len(test_titles),
        "acronym": [""] * len(test_titles),
    })

    result = classify_projects(test_df, verbose=True)

    print("\n" + "=" * 100)
    print(f"{'Theme':<28} {'Sub-theme':<35} Title")
    print("=" * 100)
    for _, row in result.iterrows():
        print(f"{row['theme']:<28} {row['sub_theme']:<35} {row['title'][:50]}")
