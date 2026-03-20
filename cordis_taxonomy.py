from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

CORDIS_DOMAIN_UI_ORDER: List[str] = [
    "Climate Change and Environment",
    "Digital Economy",
    "Energy",
    "Food and Natural Resources",
    "Fundamental Research",
    "Health",
    "Industrial Technologies",
    "Security",
    "Society",
    "Space",
    "Transport and Mobility",
]

CORDIS_DOMAIN_UI_FR: Dict[str, str] = {
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
}

LEGACY_THEME_TO_DOMAIN_UI: Dict[str, str] = {
    "Hydrogen (H2)": "Energy",
    "Solar (PV/CSP)": "Energy",
    "Wind": "Energy",
    "Bioenergy & SAF": "Energy",
    "CCUS": "Energy",
    "Nuclear & SMR": "Energy",
    "Batteries & Storage": "Energy",
    "AI & Digital": "Digital Economy",
    "Advanced materials": "Industrial Technologies",
    "E-mobility": "Transport and Mobility",
    "Climate & Environment": "Climate Change and Environment",
    "Industry & Manufacturing": "Industrial Technologies",
    "Transport & Aviation": "Transport and Mobility",
    "Health & Biotech": "Health",
    "Space": "Space",
    "Agriculture & Food": "Food and Natural Resources",
    "Security & Resilience": "Security",
    "Other": "Fundamental Research",
}

_INVALID_VALUES = {
    "",
    "-",
    "--",
    "n/a",
    "na",
    "none",
    "null",
    "nan",
    "unspecified",
    "unknown",
    "not available",
}


def normalize_spaces(value: Any) -> str:
    txt = str(value or "")
    txt = txt.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return re.sub(r"\s+", " ", txt).strip()


def clean_official_value(value: Any) -> str:
    txt = normalize_spaces(value)
    if txt.lower() in _INVALID_VALUES:
        return ""
    return txt


def split_multi_values(*values: Any) -> List[str]:
    out: List[str] = []
    for value in values:
        txt = clean_official_value(value)
        if not txt:
            continue
        parts = [txt]
        if any(sep in txt for sep in [";", "|", "\n"]):
            parts = re.split(r"\s*(?:;|\||\n)\s*", txt)
        elif "," in txt and len(re.findall(r"[A-Z0-9][A-Z0-9\-_/]+", txt)) >= 2:
            parts = re.split(r"\s*,\s*", txt)
        for part in parts:
            clean = clean_official_value(part)
            if clean and clean not in out:
                out.append(clean)
    return out


def json_list(values: Sequence[str]) -> str:
    return json.dumps([str(v) for v in values if str(v).strip()], ensure_ascii=False)


def first_non_empty(*values: Any) -> str:
    for value in values:
        clean = clean_official_value(value)
        if clean:
            return clean
    return ""


def derive_cordis_call(record: Mapping[str, Any]) -> str:
    return first_non_empty(
        record.get("cordis_call"),
        record.get("call"),
        record.get("subCall"),
        record.get("masterCall"),
    )


def derive_cordis_topics(record: Mapping[str, Any]) -> List[str]:
    topics = split_multi_values(record.get("topic"), record.get("topics"))
    if topics:
        return topics
    fallback = derive_cordis_call(record)
    return [fallback] if fallback else []


def derive_cordis_theme_primary(record: Mapping[str, Any]) -> Tuple[str, str]:
    topics = derive_cordis_topics(record)
    topic_primary = topics[0] if topics else ""
    candidates = [
        ("programmeDivisionTitle", record.get("programmeDivisionTitle")),
        ("programmeDivision", record.get("programmeDivision")),
        ("topic", topic_primary),
        ("call", derive_cordis_call(record)),
        ("frameworkProgramme", first_non_empty(record.get("cordis_framework_programme"), record.get("frameworkProgramme"), record.get("program"))),
    ]
    for source, raw in candidates:
        clean = clean_official_value(raw)
        if clean:
            return clean, source
    fallback = first_non_empty(record.get("section"), record.get("program"), "Unspecified CORDIS theme")
    return fallback, "fallback"


_DOMAIN_RULES: Dict[str, Dict[str, Sequence[str]]] = {
    "Fundamental Research": {
        "official": [
            r"\berc\b", r"\bmsca\b", r"marie sk", r"postdoctoral fellow", r"doctoral network",
            r"\bitn\b", r"\bif-20", r"\bpf-0", r"\bfet\b", r"research infrastructure",
            r"teaming", r"twinning", r"era[- ]chairs", r"widening",
        ],
        "text": [r"frontier research", r"basic research", r"fundamental research"],
    },
    "Health": {
        "official": [
            r"\bcl1\b", r"\bsc1\b", r"\bbhc\b", r"\bphc\b", r"\beu4health\b", r"\bimi\b", r"\bih[-_]?ju\b",
            r"cancer", r"health", r"medical", r"clinical", r"vaccine", r"therap",
        ],
        "text": [r"\bhealth\b", r"\bmedical\b", r"\bclinical\b", r"\bpatient\b", r"\bdisease\b"],
    },
    "Security": {
        "official": [
            r"\bcl3\b", r"\bsu[-_]", r"\bdrs\b", r"\bbes\b", r"\bfct\b", r"\binfra\b",
            r"security", r"secure", r"border", r"disaster", r"resilience",
        ],
        "text": [r"\bsecurity\b", r"\bcybersecurity\b", r"critical infrastructure", r"emergency response"],
    },
    "Society": {
        "official": [
            r"\bcl2\b", r"culture", r"creative", r"inclusive society", r"democracy", r"governance",
            r"migration", r"social", r"humanities", r"swafs", r"researchers'? night",
        ],
        "text": [r"policy", r"governance", r"inequality", r"social innovation", r"education"],
    },
    "Space": {
        "official": [
            r"\bspace\b", r"\bcopernicus\b", r"\bgalileo\b", r"\bcassini\b", r"satellite", r"earth observation",
            r"launcher", r"space situational awareness",
        ],
        "text": [r"\bsatellite\b", r"earth observation", r"space debris", r"orbital"],
    },
    "Climate Change and Environment": {
        "official": [
            r"-cl5-.*-d1", r"-cl5-.*-d2", r"\bsc5\b", r"\bce[-_]?sc5\b", r"\benv\b", r"climate", r"environment",
            r"biodiversity", r"water", r"circular", r"pollution",
        ],
        "text": [r"climate", r"environment", r"biodiversity", r"water treatment", r"circular economy"],
    },
    "Energy": {
        "official": [
            r"-cl5-.*-d3", r"-cl5-.*-d4", r"\bsc3\b", r"\blc[-_]?sc3\b", r"\bcleanh2\b", r"\bfch\b",
            r"\beuratom\b", r"energy", r"battery", r"hydrogen", r"clean energy", r"fusion",
        ],
        "text": [r"hydrogen", r"solar", r"wind", r"battery", r"energy storage", r"ccus", r"nuclear"],
    },
    "Transport and Mobility": {
        "official": [
            r"-cl5-.*-d5", r"-cl5-.*-d6", r"\bmg[-_]", r"\bgv[-_]", r"shift2rail", r"sesar", r"clean[- ]aviation",
            r"cleansky", r"transport", r"mobility", r"aviation", r"rail", r"maritime",
        ],
        "text": [r"transport", r"mobility", r"aviation", r"rail", r"shipping", r"electric vehicle"],
    },
    "Food and Natural Resources": {
        "official": [
            r"\bcl6\b", r"\bsc2\b", r"\bsfs\b", r"\bbbi\b", r"\bcbe\b", r"food", r"agri", r"agriculture",
            r"bioeconomy", r"natural resources", r"forestry", r"fisheries",
        ],
        "text": [r"agriculture", r"food", r"crop", r"soil", r"aquaculture", r"bioeconomy"],
    },
    "Digital Economy": {
        "official": [
            r"\bict\b", r"digital", r"data", r"cyber", r"ai", r"artificial intelligence", r"quantum", r"cloud",
            r"hpc", r"software", r"platform", r"internet of things",
        ],
        "text": [r"artificial intelligence", r"machine learning", r"digital twin", r"data platform", r"software"],
    },
    "Industrial Technologies": {
        "official": [
            r"\bnmbp\b", r"\bspire\b", r"\bfof\b", r"\becsel\b", r"\bkdt\b", r"\bchips\b", r"photonics",
            r"semiconductor", r"manufacturing", r"industry", r"materials", r"pilot line", r"process intensification",
            r"raw materials", r"advanced materials",
        ],
        "text": [r"manufacturing", r"factory", r"semiconductor", r"photonics", r"materials", r"robotics"],
    },
}

_DOMAIN_RULES_COMPILED = {
    domain: {
        kind: [re.compile(pat, re.IGNORECASE) for pat in patterns]
        for kind, patterns in rule.items()
    }
    for domain, rule in _DOMAIN_RULES.items()
}


def _score_patterns(patterns: Sequence[re.Pattern], text: str) -> int:
    if not text:
        return 0
    return sum(1 for pat in patterns if pat.search(text))


def infer_cordis_domain_ui(record: Mapping[str, Any]) -> str:
    official_bits = [
        record.get("cordis_theme_primary"),
        record.get("cordis_topic_primary"),
        record.get("cordis_topics_all"),
        record.get("cordis_call"),
        record.get("cordis_framework_programme"),
        record.get("fundingScheme"),
        record.get("keywords"),
        record.get("program"),
    ]
    text_bits = [record.get("title"), record.get("objective"), record.get("abstract"), record.get("keywords")]
    official_blob = normalize_spaces(" ".join([str(x) for x in official_bits if str(x or "").strip()])).lower()
    text_blob = normalize_spaces(" ".join([str(x) for x in text_bits if str(x or "").strip()])).lower()

    best_domain = ""
    best_score = -1
    for domain in CORDIS_DOMAIN_UI_ORDER:
        rule = _DOMAIN_RULES_COMPILED.get(domain, {})
        score = 3 * _score_patterns(rule.get("official", []), official_blob) + _score_patterns(rule.get("text", []), text_blob)
        if score > best_score:
            best_domain = domain
            best_score = score

    if best_score > 0:
        return best_domain

    legacy_theme = clean_official_value(record.get("legacy_theme") or record.get("theme"))
    if legacy_theme and legacy_theme in LEGACY_THEME_TO_DOMAIN_UI:
        return LEGACY_THEME_TO_DOMAIN_UI[legacy_theme]

    official_lower = official_blob.lower()
    if any(token in official_lower for token in ["erc", "msca", "itn", "doctoral network", "postdoctoral"]):
        return "Fundamental Research"
    if any(token in official_lower for token in ["eic", "smeinst", "accelerator", "pathfinder", "transition"]):
        return "Industrial Technologies"
    return "Fundamental Research"


SCIENTIFIC_SUBTHEME_RULES: List[Dict[str, Any]] = [
    {"domain": "Energy", "family": "Hydrogen", "label": "Hydrogen production", "patterns": [r"hydrogen production", r"green hydrogen", r"renewable hydrogen", r"hydrogen value chain"]},
    {"domain": "Energy", "family": "Hydrogen", "label": "Electrolysis & fuel cells", "patterns": [r"electrolys", r"fuel cell", r"pem", r"soec", r"sofc"]},
    {"domain": "Energy", "family": "Solar", "label": "Solar photovoltaics", "patterns": [r"photovolta", r"solar cell", r"solar panel", r"perovskite"]},
    {"domain": "Energy", "family": "Solar", "label": "Solar thermal and CSP", "patterns": [r"concentrated solar", r"\bcsp\b", r"solar thermal"]},
    {"domain": "Energy", "family": "Wind", "label": "Wind systems", "patterns": [r"offshore wind", r"onshore wind", r"floating wind", r"wind turbine", r"wind farm"]},
    {"domain": "Energy", "family": "Energy storage", "label": "Batteries and stationary storage", "patterns": [r"battery", r"stationary storage", r"energy storage", r"supercapacitor", r"redox flow"]},
    {"domain": "Energy", "family": "Carbon management", "label": "CCUS and carbon removal", "patterns": [r"carbon capture", r"co2 capture", r"ccus", r"\bccs\b", r"direct air capture", r"co2 storage"]},
    {"domain": "Energy", "family": "Nuclear", "label": "Nuclear and fusion systems", "patterns": [r"nuclear", r"fusion", r"tokamak", r"smr", r"euratom"]},
    {"domain": "Energy", "family": "Energy systems", "label": "Smart grids and flexibility", "patterns": [r"smart grid", r"grid integration", r"flexibility", r"microgrid", r"demand response"]},
    {"domain": "Energy", "family": "Bioenergy", "label": "Bioenergy and sustainable fuels", "patterns": [r"biofuel", r"bioenergy", r"biogas", r"sustainable aviation fuel", r"saf", r"biomass"]},

    {"domain": "Digital Economy", "family": "AI and data", "label": "Artificial intelligence and machine learning", "patterns": [r"artificial intelligence", r"machine learning", r"deep learning", r"neural network"]},
    {"domain": "Digital Economy", "family": "AI and data", "label": "Data platforms and software", "patterns": [r"data platform", r"data space", r"software", r"cloud computing", r"data pipeline"]},
    {"domain": "Digital Economy", "family": "AI and data", "label": "Digital twins and simulation", "patterns": [r"digital twin", r"modelling", r"modeling", r"simulation"]},
    {"domain": "Digital Economy", "family": "Cyber and trust", "label": "Cybersecurity", "patterns": [r"cybersecurity", r"cyber resilience", r"encryption", r"privacy-preserving"]},
    {"domain": "Digital Economy", "family": "Compute", "label": "High-performance and quantum computing", "patterns": [r"high-performance computing", r"\bhpc\b", r"quantum computing", r"quantum software"]},
    {"domain": "Digital Economy", "family": "Connected systems", "label": "IoT and sensing systems", "patterns": [r"internet of things", r"\biot\b", r"sensor network", r"connected device"]},

    {"domain": "Climate Change and Environment", "family": "Climate", "label": "Climate adaptation", "patterns": [r"climate adaptation", r"resilience to climate", r"flood risk", r"heat island"]},
    {"domain": "Climate Change and Environment", "family": "Climate", "label": "Climate mitigation", "patterns": [r"climate mitigation", r"greenhouse gas", r"ghg", r"decarboni[sz]ation pathway", r"net zero"]},
    {"domain": "Climate Change and Environment", "family": "Footprint", "label": "Carbon footprint and life-cycle assessment", "patterns": [r"carbon footprint", r"life cycle assessment", r"\blca\b", r"environmental footprint"]},
    {"domain": "Climate Change and Environment", "family": "Water", "label": "Water resources and treatment", "patterns": [r"water treatment", r"wastewater", r"desalination", r"water reuse", r"water purification"]},
    {"domain": "Climate Change and Environment", "family": "Biodiversity", "label": "Biodiversity and ecosystems", "patterns": [r"biodiversity", r"ecosystem", r"habitat restoration", r"rewilding"]},
    {"domain": "Climate Change and Environment", "family": "Circular economy", "label": "Circular economy and waste", "patterns": [r"circular economy", r"waste management", r"waste recycling", r"resource efficiency"]},
    {"domain": "Climate Change and Environment", "family": "Pollution", "label": "Pollution monitoring and remediation", "patterns": [r"pollution", r"soil remediation", r"air quality", r"micropollutant"]},

    {"domain": "Food and Natural Resources", "family": "Agriculture", "label": "Precision agriculture", "patterns": [r"precision agriculture", r"smart farming", r"drone-based crop", r"agri-tech"]},
    {"domain": "Food and Natural Resources", "family": "Agriculture", "label": "Soil health", "patterns": [r"soil health", r"soil microbiome", r"soil fertility", r"soil carbon"]},
    {"domain": "Food and Natural Resources", "family": "Agriculture", "label": "Crop improvement", "patterns": [r"crop improvement", r"plant breeding", r"drought-tolerant crop", r"disease-resistant variety"]},
    {"domain": "Food and Natural Resources", "family": "Food systems", "label": "Food processing and food systems", "patterns": [r"food processing", r"food system", r"food safety", r"food waste"]},
    {"domain": "Food and Natural Resources", "family": "Bioeconomy", "label": "Bioeconomy and forestry", "patterns": [r"bioeconomy", r"forestry", r"forest management", r"wood-based"]},
    {"domain": "Food and Natural Resources", "family": "Proteins", "label": "Alternative proteins", "patterns": [r"alternative protein", r"cultured meat", r"plant-based protein", r"insect protein"]},
    {"domain": "Food and Natural Resources", "family": "Aquaculture", "label": "Aquaculture and fisheries", "patterns": [r"aquaculture", r"fisheries", r"fish farming", r"marine food system"]},

    {"domain": "Fundamental Research", "family": "Programmes", "label": "European Research Council", "patterns": [r"\berc\b", r"european research council"]},
    {"domain": "Fundamental Research", "family": "Programmes", "label": "Marie Skłodowska-Curie Actions", "patterns": [r"\bmsca\b", r"marie sk", r"postdoctoral fellowship", r"doctoral network"]},
    {"domain": "Fundamental Research", "family": "Programmes", "label": "Research infrastructures", "patterns": [r"research infrastructure", r"\beinfra\b", r"large-scale facility"]},
    {"domain": "Fundamental Research", "family": "Science", "label": "Fundamental physics and mathematics", "patterns": [r"quantum field", r"string theory", r"topological", r"astrophysics", r"fundamental physics"]},
    {"domain": "Fundamental Research", "family": "Science", "label": "Frontier life sciences", "patterns": [r"frontier biology", r"cell biology", r"molecular mechanism", r"evolutionary biology"]},

    {"domain": "Health", "family": "Biomedicine", "label": "Drug discovery", "patterns": [r"drug discovery", r"lead optimisation", r"lead optimization", r"pharmacology"]},
    {"domain": "Health", "family": "Biomedicine", "label": "Diagnostics", "patterns": [r"diagnostic", r"biosensor", r"point-of-care", r"pcr-based"]},
    {"domain": "Health", "family": "Biomedicine", "label": "Medical devices", "patterns": [r"medical device", r"implant", r"wearable medical", r"surgical robot"]},
    {"domain": "Health", "family": "Omics", "label": "Genomics and omics", "patterns": [r"genomic", r"genomics", r"transcriptomics", r"proteomics", r"metabolomics"]},
    {"domain": "Health", "family": "Therapeutics", "label": "Vaccines and therapeutics", "patterns": [r"vaccine", r"therapeutic", r"immunotherapy", r"antibody"]},
    {"domain": "Health", "family": "Therapeutics", "label": "Gene and cell therapy", "patterns": [r"gene therapy", r"cell therapy", r"crispr", r"stem cell"]},
    {"domain": "Health", "family": "Health systems", "label": "Digital health", "patterns": [r"digital health", r"telemedicine", r"remote patient monitoring", r"health data platform"]},
    {"domain": "Health", "family": "Bioprocessing", "label": "Bioprocessing and biomanufacturing", "patterns": [r"bioprocess", r"biomanufacturing", r"bioreactor", r"cell culture", r"fermentation"]},

    {"domain": "Industrial Technologies", "family": "Manufacturing", "label": "Advanced manufacturing", "patterns": [r"advanced manufacturing", r"precision manufacturing", r"micro-manufacturing", r"factory"]},
    {"domain": "Industrial Technologies", "family": "Manufacturing", "label": "Process intensification", "patterns": [r"process intensification", r"continuous flow", r"reactor design"]},
    {"domain": "Industrial Technologies", "family": "Industry", "label": "Industrial decarbonization", "patterns": [r"industrial decarboni[sz]ation", r"green steel", r"cement decarboni[sz]ation", r"electrification of heat"]},
    {"domain": "Industrial Technologies", "family": "Automation", "label": "Robotics and automation", "patterns": [r"robotics", r"industrial robot", r"automated assembly", r"cobot"]},
    {"domain": "Industrial Technologies", "family": "Electronics", "label": "Semiconductors and electronics", "patterns": [r"semiconductor", r"integrated circuit", r"chip", r"system-on-chip", r"pilot line"]},
    {"domain": "Industrial Technologies", "family": "Electronics", "label": "Photonics and optics", "patterns": [r"photonics", r"optics", r"laser system", r"optoelectronic"]},
    {"domain": "Industrial Technologies", "family": "Materials", "label": "Advanced materials", "patterns": [r"advanced material", r"nanomaterial", r"graphene", r"composite material", r"coating"]},
    {"domain": "Industrial Technologies", "family": "Monitoring", "label": "Sensors and industrial monitoring", "patterns": [r"condition monitoring", r"industrial sensor", r"predictive maintenance", r"quality control"]},

    {"domain": "Security", "family": "Cyber", "label": "Cybersecurity", "patterns": [r"cybersecurity", r"cyber attack", r"network security", r"threat intelligence"]},
    {"domain": "Security", "family": "Infrastructure", "label": "Critical infrastructure resilience", "patterns": [r"critical infrastructure", r"grid resilience", r"infrastructure protection"]},
    {"domain": "Security", "family": "Civil security", "label": "Civil security and border management", "patterns": [r"civil security", r"border security", r"law enforcement"]},
    {"domain": "Security", "family": "Defence", "label": "Defence technologies", "patterns": [r"defen[sc]e", r"military", r"dual-use"]},
    {"domain": "Security", "family": "Risk", "label": "Disaster risk reduction", "patterns": [r"disaster risk", r"early warning", r"crisis management", r"earthquake", r"wildfire"]},
    {"domain": "Security", "family": "Response", "label": "Emergency response systems", "patterns": [r"emergency response", r"search and rescue", r"first responder"]},
    {"domain": "Security", "family": "Detection", "label": "Surveillance and detection", "patterns": [r"surveillance", r"radar", r"uav surveillance", r"explosive detection"]},

    {"domain": "Society", "family": "Governance", "label": "Governance and democracy", "patterns": [r"governance", r"democracy", r"public policy", r"institutional trust"]},
    {"domain": "Society", "family": "Culture", "label": "Culture and heritage", "patterns": [r"cultural heritage", r"museum", r"archive", r"creative industr"]},
    {"domain": "Society", "family": "Education", "label": "Education and skills", "patterns": [r"education", r"skills", r"training", r"lifelong learning"]},
    {"domain": "Society", "family": "Inclusion", "label": "Social inclusion and inequality", "patterns": [r"social inclusion", r"inequality", r"poverty", r"gender equality"]},
    {"domain": "Society", "family": "Demography", "label": "Migration and demographics", "patterns": [r"migration", r"demographic", r"ageing", r"population"]},
    {"domain": "Society", "family": "Engagement", "label": "Science communication and public engagement", "patterns": [r"science communication", r"public engagement", r"researchers'? night", r"citizen science"]},

    {"domain": "Space", "family": "Earth observation", "label": "Earth observation", "patterns": [r"earth observation", r"copernicus", r"remote sensing"]},
    {"domain": "Space", "family": "Satellites", "label": "Satellite systems", "patterns": [r"satellite", r"leo constellation", r"payload", r"antenna"]},
    {"domain": "Space", "family": "Navigation", "label": "Telecommunications and navigation", "patterns": [r"galileo", r"gnss", r"satellite communication", r"telecommunication from space"]},
    {"domain": "Space", "family": "Launch", "label": "Launch systems", "patterns": [r"launch vehicle", r"rocket", r"launcher", r"microlauncher"]},
    {"domain": "Space", "family": "Propulsion", "label": "Space propulsion", "patterns": [r"space propulsion", r"ion engine", r"electric propulsion"]},
    {"domain": "Space", "family": "Operations", "label": "Space robotics and in-orbit services", "patterns": [r"space robotics", r"in-orbit servicing", r"autonomous docking", r"debris removal"]},
    {"domain": "Space", "family": "Safety", "label": "Space situational awareness", "patterns": [r"space situational awareness", r"space debris", r"collision avoidance"]},

    {"domain": "Transport and Mobility", "family": "Aviation", "label": "Sustainable aviation", "patterns": [r"sustainable aviation", r"clean aviation", r"hydrogen aircraft", r"hybrid-electric aircraft"]},
    {"domain": "Transport and Mobility", "family": "Aeronautics", "label": "Aeronautics systems", "patterns": [r"aircraft design", r"airframe", r"avionics", r"flight control"]},
    {"domain": "Transport and Mobility", "family": "Rail", "label": "Rail transport", "patterns": [r"rail", r"railway", r"rolling stock", r"shift2rail"]},
    {"domain": "Transport and Mobility", "family": "Maritime", "label": "Maritime transport", "patterns": [r"maritime", r"shipping", r"green shipping", r"port logistics"]},
    {"domain": "Transport and Mobility", "family": "Logistics", "label": "Logistics and freight", "patterns": [r"logistics", r"freight", r"intermodal", r"supply chain"]},
    {"domain": "Transport and Mobility", "family": "Urban mobility", "label": "Urban mobility systems", "patterns": [r"urban mobility", r"maas", r"multimodal transport", r"traffic management"]},
    {"domain": "Transport and Mobility", "family": "Electrification", "label": "Electric mobility systems", "patterns": [r"electric vehicle", r"e-mobility", r"charging infrastructure", r"vehicle-to-grid", r"v2g"]},
    {"domain": "Transport and Mobility", "family": "Operations", "label": "Traffic optimization and operations", "patterns": [r"air traffic", r"traffic optimization", r"congestion management"]},
]

for rule in SCIENTIFIC_SUBTHEME_RULES:
    rule["compiled_patterns"] = [re.compile(pat, re.IGNORECASE) for pat in rule["patterns"]]

SCIENTIFIC_SUBTHEMES_BY_DOMAIN: Dict[str, List[str]] = {}
for domain in CORDIS_DOMAIN_UI_ORDER:
    SCIENTIFIC_SUBTHEMES_BY_DOMAIN[domain] = [
        rule["label"]
        for rule in SCIENTIFIC_SUBTHEME_RULES
        if rule["domain"] == domain
    ]


def infer_scientific_subtheme_records(record: Mapping[str, Any]) -> List[Dict[str, str]]:
    official_blob = normalize_spaces(
        " ".join(
            [
                str(record.get("cordis_theme_primary") or ""),
                str(record.get("cordis_topic_primary") or ""),
                str(record.get("cordis_topics_all") or ""),
                str(record.get("cordis_call") or ""),
                str(record.get("cordis_framework_programme") or ""),
                str(record.get("fundingScheme") or ""),
                str(record.get("keywords") or ""),
            ]
        )
    ).lower()
    text_blob = normalize_spaces(
        " ".join(
            [
                str(record.get("title") or ""),
                str(record.get("acronym") or ""),
                str(record.get("objective") or ""),
                str(record.get("abstract") or ""),
                str(record.get("keywords") or ""),
            ]
        )
    ).lower()
    domain_hint = clean_official_value(record.get("cordis_domain_ui"))

    matches: List[Dict[str, str]] = []
    seen: set = set()
    for rule in SCIENTIFIC_SUBTHEME_RULES:
        official_hits = sum(1 for pat in rule["compiled_patterns"] if pat.search(official_blob))
        text_hits = sum(1 for pat in rule["compiled_patterns"] if pat.search(text_blob))
        if official_hits <= 0 and text_hits <= 0:
            continue
        label = rule["label"]
        if label in seen:
            continue
        seen.add(label)
        if official_hits > 0 and text_hits > 0:
            source_method = "official_metadata+text_rule"
        elif official_hits > 0:
            source_method = "official_metadata_rule"
        else:
            source_method = "text_rule"
        matches.append(
            {
                "subtheme_level_1": rule["domain"],
                "subtheme_level_2": rule["family"],
                "subtheme_level_3": label,
                "subtheme_label": label,
                "subtheme_path": f"{rule['domain']} > {rule['family']} > {label}",
                "source_method": source_method,
            }
        )

    if domain_hint and matches:
        matches.sort(key=lambda item: (0 if item["subtheme_level_1"] == domain_hint else 1, item["subtheme_level_2"], item["subtheme_label"]))
    else:
        matches.sort(key=lambda item: (CORDIS_DOMAIN_UI_ORDER.index(item["subtheme_level_1"]) if item["subtheme_level_1"] in CORDIS_DOMAIN_UI_ORDER else 999, item["subtheme_level_2"], item["subtheme_label"]))
    return matches


def first_scientific_subtheme(records: Sequence[Mapping[str, str]]) -> str:
    for record in records:
        label = clean_official_value(record.get("subtheme_label"))
        if label:
            return label
    return ""


def scientific_subtheme_labels(records: Sequence[Mapping[str, str]]) -> List[str]:
    labels: List[str] = []
    for record in records:
        label = clean_official_value(record.get("subtheme_label"))
        if label and label not in labels:
            labels.append(label)
    return labels