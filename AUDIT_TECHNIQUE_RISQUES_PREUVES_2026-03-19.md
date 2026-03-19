# Audit technique — Risques, preuves, recommandations

Date : 19 mars 2026  
Périmètre : application Streamlit, pipeline de build CORDIS, dataset traité courant.

## 1. Objet

Ce document complète l'audit détaillé en donnant une lecture plus opérationnelle pour une équipe produit / data / engineering :
- quels points sont solides ;
- quels risques restent ouverts ;
- quelles preuves ont été observées ;
- quelles actions sont recommandées.

## 2. Synthèse technique

| Sujet | Niveau | Conclusion |
|---|---|---|
| Modèle de données CORDIS-first | Faible risque | Bien en place |
| Unicité du thème principal | Faible risque | Conforme |
| Unicité du domaine UI | Faible risque | Conforme |
| Cohérence multi-label des sous-thèmes | Faible risque | Très bonne |
| Règles de comptage global | Faible risque | Solides |
| Qualité structurelle du parquet | Faible risque | Très bonne |
| Lisibilité UX métier | Faible risque | Forte amélioration confirmée |
| Finesse scientifique des sous-thèmes | Risque moyen | Bonne base, calibration encore utile |
| Doublons résiduels | Risque faible à moyen | Limités mais réels |
| Performance du rebuild complet | Risque moyen | Non bloquant, mais à optimiser |
| Dépendance aux métadonnées `topic` | Risque moyen | Acceptable, à expliciter |

## 3. Matrice preuves / risques / recommandations

### 3.1 Architecture de données

| Point | Constat | Preuve | Risque | Recommandation |
|---|---|---|---|---|
| Séparation domaine / thème / sous-thèmes | Bonne | Colonnes dédiées dans le parquet et le build | Faible | Conserver cette architecture |
| Compatibilité legacy | Bonne | `legacy_theme`, `legacy_sub_theme`, alias `theme` | Faible | Garder pendant la transition |
| Table annexe sous-thèmes | Bonne | `project_scientific_subthemes.parquet` présent | Faible | En faire la base des analyses fines |

### 3.2 Qualité structurelle des données

| Point | Constat | Preuve | Risque | Recommandation |
|---|---|---|---|---|
| Complétude des champs critiques | Excellente | 0 valeur manquante sur les champs critiques audités | Faible | Ajouter un test automatique de régression |
| Budget négatif | Aucun | 0 ligne à budget négatif | Faible | Maintenir le contrôle build |
| Doublons exacts | Faibles mais présents | 291 doublons exacts | Faible à moyen | Ajouter alerte ou check build |
| Zéros budgétaires | Présents au grain participant | 53 387 lignes à 0 € | Moyen interprétatif | Documenter explicitement le grain participant |

### 3.3 Classification principale

| Point | Constat | Preuve | Risque | Recommandation |
|---|---|---|---|---|
| Unicité du thème principal | Conforme | 0 violation projet -> thème | Faible | Maintenir test automatique |
| Unicité du domaine UI | Conforme | 0 violation projet -> domaine | Faible | Maintenir test automatique |
| Source du thème principal | Très uniforme | 100 % `topic` | Moyen | L'expliquer clairement dans la doc et les présentations |
| Couverture des 11 domaines CORDIS | Complète | 11 domaines présents | Faible | Conserver `cordis_domain_ui` comme niveau macro par défaut |

### 3.4 Sous-thèmes scientifiques

| Point | Constat | Preuve | Risque | Recommandation |
|---|---|---|---|---|
| Couverture sous-thèmes | Très bonne | 95,2 % des projets couverts | Faible | Conserver la logique multi-label |
| Multi-label réel | Oui | 75,0 % des projets ont >1 sous-thème | Faible | Continuer à séparer exploration fine et total principal |
| Cohérence colonne / table annexe | Excellente | 0 mismatch | Faible | Ajouter un check automatique au build |
| Qualité sémantique fine | Bonne mais perfectible | Structure robuste, calibration encore ouverte | Moyen | Auditer un échantillon ciblé |

### 3.5 Comptage et agrégations

| Point | Constat | Preuve | Risque | Recommandation |
|---|---|---|---|---|
| Total projets | Sain | `COUNT(DISTINCT projectID)` dans les vues principales | Faible | Ne pas changer cette règle |
| Budget global | Sain sous réserve d'interprétation | Somme au grain participant puis agrégation | Faible à moyen | Expliquer le modèle de budget dans la méthode |
| Sous-thèmes et totaux | Bonne séparation | table annexe utilisée pour filtre fin, pas pour total global | Faible | Garder cette séparation explicite dans l'UI |

### 3.6 UX / lisibilité métier

| Point | Constat | Preuve | Risque | Recommandation |
|---|---|---|---|---|
| Graphes macro lisibles | Oui | Domaines CORDIS par défaut | Faible | Conserver ce default |
| Humanisation des codes CORDIS | Oui | module `cordis_labels.py` | Faible | Continuer à centraliser ici |
| Cohérence terminologique | Bonne | `Domaines CORDIS`, `Thème principal CORDIS`, `Sous-thèmes scientifiques` | Faible | Maintenir cette discipline |
| UX non technique | Forte amélioration | plus de codes bruts comme labels principaux | Faible | Garder les codes en tooltip/détail uniquement |

## 4. Chiffres clés pour discussion équipe

### Dataset courant
- 296 940 lignes
- 54 413 projets distincts
- 58 314 acteurs distincts
- 194 pays
- 42 colonnes
- 2014–2027

### Qualité structurelle
- 0 valeur manquante sur les champs critiques audités
- 0 budget négatif
- 291 doublons exacts
- 53 387 lignes participant à 0 €

### Classification
- 0 incohérence projet -> thème principal
- 0 incohérence projet -> domaine UI
- 11 domaines CORDIS couverts
- 100 % des thèmes principaux dérivés depuis `topic` sur ce dump

### Sous-thèmes
- 51 794 projets avec sous-thèmes
- 2 619 sans sous-thèmes
- 40 802 projets multi-label
- 138 087 lignes dans la table annexe
- 78 sous-thèmes distincts
- 0 mismatch colonne / table annexe

## 5. Lecture critique honnête

### Ce qu'on peut affirmer sans surpromettre
- le système est bien structuré et stable ;
- les totaux principaux sont cohérents ;
- les sous-thèmes sont réellement multi-label ;
- l'UX est désormais adaptée à un public métier.

### Ce qu'il faut éviter d'affirmer trop vite
- que la finesse scientifique des sous-thèmes est parfaite ;
- que la source du thème principal reflète déjà toute la profondeur CORDIS disponible ;
- que le rebuild est déjà industrialisé au niveau optimal.

## 6. Recommandations priorisées

### Priorité A — sécurisation qualité
- ajouter des tests automatiques sur :
  - unicité `projectID -> cordis_theme_primary`
  - unicité `projectID -> cordis_domain_ui`
  - cohérence `scientific_subthemes_count`
  - non-nullité des champs critiques

### Priorité B — explicabilité
- documenter noir sur blanc :
  - le grain participant de `amount_eur`
  - la règle de comptage `COUNT(DISTINCT projectID)`
  - la séparation entre total principal et exploration sous-thèmes

### Priorité C — amélioration ciblée
- lancer un audit métier sur un échantillon de sous-thèmes ;
- surveiller les doublons exacts ;
- optimiser le temps de rebuild si le projet entre en usage régulier.

## 7. Utilisation recommandée de ce document

Ce document peut servir :
- en réunion d'équipe pour cadrer l'état réel du produit ;
- en support de validation interne ;
- en base pour décider les prochains chantiers qualité ;
- en preuve qu'il existe déjà un socle robuste, sans masquer les sujets restants.

## 8. Références
- [Audit détaillé](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/AUDIT_QUALITE_CORDIS_RADAR_2026-03-19.md)
- [Executive summary](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/AUDIT_EXECUTIVE_SUMMARY_CORDIS_RADAR_2026-03-19.md)
- [app.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/app.py)
- [process_build.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/process_build.py)
- [subsidy_base.parquet](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/data/processed/subsidy_base.parquet)
