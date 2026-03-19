# Audit qualité — Subsidy Intelligence Radar

Date d'audit : 19 mars 2026  
Périmètre audité : code applicatif Streamlit, pipeline de build CORDIS, dataset traité courant, cohérence des classifications et des règles de comptage.

## 1. Conclusion exécutive

Le projet présente aujourd'hui un niveau de qualité globalement solide et défendable pour une application d'analyse CORDIS orientée métier.

Les points les plus forts sont les suivants :
- le modèle de données principal est désormais aligné sur CORDIS et non plus sur une taxonomie métier interne ;
- le thème principal et le domaine UI sont stables par projet ;
- les sous-thèmes scientifiques sont bien multi-label et cohérents avec leur table annexe ;
- les champs critiques sont remplis à 100 % dans le dataset traité courant ;
- les règles de comptage global restent saines, avec comptage des projets en `COUNT(DISTINCT projectID)` et sans recomposition trompeuse des totaux à partir des sous-thèmes ;
- l'UX a été récemment clarifiée pour éviter l'affichage brut des codes CORDIS dans les vues synthétiques.

En synthèse, l'application est crédible techniquement, lisible côté métier, et sa structure de données est nettement plus robuste qu'avant la refonte CORDIS-first.

## 2. Verdict synthétique

| Axe audité | Verdict | Commentaire |
|---|---|---|
| Architecture du modèle de données | Fort | Modèle CORDIS-first explicite, colonnes dédiées, compatibilité legacy maintenue |
| Qualité structurelle du dataset traité | Fort | 42 colonnes, aucune valeur manquante sur les champs critiques audités |
| Intégrité du thème principal | Fort | 0 violation d'unicité du thème principal par projet |
| Intégrité du domaine UI | Fort | 0 violation d'unicité du domaine UI par projet |
| Sous-thèmes scientifiques multi-label | Fort | Très bonne couverture et cohérence parfaite avec la table annexe |
| Règles de comptage principales | Fort | Comptage projet distinct, séparation claire entre total principal et exploration sous-thèmes |
| Lisibilité UX métier | Fort | Domaines CORDIS par défaut, humanisation des libellés CORDIS, microcopy réalignée |
| Précision scientifique fine des sous-thèmes | Moyen à fort | Structure très bonne, mais une calibration continue reste souhaitable |
| Performance opérationnelle du rebuild complet | Moyen | Build complet encore lourd sur volumétrie réelle |
| Couverture des métadonnées officielles les plus fines | Moyen | Dans ce dump courant, le thème principal provient en pratique du `topic` pour 100 % des projets |

## 3. Méthode d'audit

L'audit repose sur quatre niveaux de vérification :
- lecture du code de [process_build.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/process_build.py), [app.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/app.py), [pipeline.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/pipeline.py), [theme_classifier_v3.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/theme_classifier_v3.py), [cordis_taxonomy.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/cordis_taxonomy.py) et [cordis_labels.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/cordis_labels.py) ;
- compilation Python des modules principaux ;
- audit quantitatif du parquet traité [subsidy_base.parquet](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/data/processed/subsidy_base.parquet) ;
- audit de cohérence de la table annexe [project_scientific_subthemes.parquet](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/data/processed/project_scientific_subthemes.parquet).

## 4. État du code audité

### 4.1 Qualité de l'état du repo
- Worktree Git propre au moment de l'audit : aucun changement non commit détecté.
- Compilation valide sur les fichiers principaux :
  - [app.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/app.py)
  - [process_build.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/process_build.py)
  - [pipeline.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/pipeline.py)
  - [theme_classifier_v3.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/theme_classifier_v3.py)
  - [cordis_taxonomy.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/cordis_taxonomy.py)
  - [cordis_labels.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/cordis_labels.py)

### 4.2 Qualité de l'architecture
Le repo repose désormais sur une séparation saine entre :
- domaine de navigation : `cordis_domain_ui` ;
- thème principal officiel : `cordis_theme_primary` ;
- source d'attribution : `cordis_theme_primary_source` ;
- granularité scientifique fine : `scientific_subthemes` multi-label ;
- compatibilité transitoire : `legacy_theme`, `legacy_sub_theme`, `theme`, `sub_theme`.

Cette architecture est bien plus robuste qu'une logique unique de "thème maison" et elle est cohérente avec une exploitation CORDIS-first.

## 5. Audit du dataset traité courant

### 5.1 Volumétrie et structure
Le dataset courant contient :
- 296 940 lignes ;
- 54 413 projets distincts ;
- 58 314 acteurs distincts ;
- 194 pays ;
- 2 programmes (`Horizon 2020`, `Horizon Europe`) ;
- une couverture temporelle allant de 2014 à 2027.

Le parquet courant expose 42 colonnes, incluant toutes les colonnes CORDIS-first attendues :
- `cordis_domain_ui`
- `cordis_theme_primary`
- `cordis_theme_primary_source`
- `cordis_topic_primary`
- `cordis_topics_all`
- `cordis_call`
- `cordis_framework_programme`
- `scientific_subthemes`
- `scientific_subthemes_count`
- `legacy_theme`
- `legacy_sub_theme`

### 5.2 Complétude des champs critiques
Sur le dataset courant, les champs critiques audités sont remplis à 100 % :
- `projectID`
- `title`
- `actor_id`
- `org_name`
- `country_name`
- `amount_eur`
- `cordis_domain_ui`
- `cordis_theme_primary`
- `cordis_theme_primary_source`
- `scientific_subthemes`
- `scientific_subthemes_count`

Verdict : très bon niveau de complétude structurelle.

### 5.3 Qualité budgétaire
- 0 ligne avec budget négatif.
- 53 387 lignes à budget nul au niveau participant, soit environ 18,0 % des lignes.
- 1 seul projet avec budget total nul au niveau projet.

Lecture recommandée :
- la présence de lignes participant à 0 € n'est pas, en soi, une anomalie critique dans CORDIS ;
- en revanche, elle doit être comprise comme une caractéristique du grain participant, pas comme un défaut de l'agrégation projet.

### 5.4 Dédoublonnage
- 291 doublons exacts détectés sur 296 940 lignes, soit environ 0,10 % des lignes.

Verdict : faible niveau de duplication, à surveiller mais pas alarmant.

## 6. Audit des règles de classification

### 6.1 Thème principal CORDIS
Le build calcule le thème principal par projet dans [process_build.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/process_build.py) via une dérivation officielle :
- `programmeDivisionTitle`
- sinon `programmeDivision`
- sinon `topic`
- sinon `call`
- sinon `frameworkProgramme`
- sinon fallback contrôlé

Constat sur les données courantes :
- 0 projet avec plusieurs `cordis_theme_primary` différents ;
- 0 projet avec plusieurs `cordis_domain_ui` différents ;
- `cordis_theme_primary_source = topic` pour 100 % des projets du dump courant.

Interprétation :
- la logique de cascade est bien codée ;
- sur ce dataset concret, le `topic` est systématiquement le champ officiel le plus utile ;
- ce n'est pas un défaut du code, mais une caractéristique des données disponibles dans ce dump.

### 6.2 Domaine CORDIS UI
Répartition projet des 11 domaines CORDIS :
- Fundamental Research : 27 269 projets
- Digital Economy : 7 096
- Climate Change and Environment : 4 422
- Health : 4 359
- Industrial Technologies : 2 672
- Energy : 2 377
- Food and Natural Resources : 1 798
- Transport and Mobility : 1 524
- Society : 1 516
- Security : 859
- Space : 521

Verdict : couverture complète des 11 domaines, sans trou de classification.

Point d'attention :
- la distribution est fortement concentrée sur `Fundamental Research` ;
- cela peut être cohérent avec la structure des topics CORDIS, mais mérite d'être gardé en tête dans l'interprétation métier.

### 6.3 Sous-thèmes scientifiques multi-label
Le dispositif multi-label est l'un des points les plus forts de la version actuelle.

Constats mesurés :
- 51 794 projets avec au moins un sous-thème scientifique, soit 95,2 % des projets ;
- 2 619 projets sans sous-thème, soit 4,8 % ;
- 40 802 projets avec plusieurs sous-thèmes, soit 75,0 % des projets ;
- 138 087 lignes dans la table annexe projet x sous-thème ;
- 78 sous-thèmes distincts dans cette table annexe ;
- 0 mismatch entre `scientific_subthemes_count` et le nombre réellement parsé ;
- 0 mismatch entre la colonne `scientific_subthemes` et la table annexe `project_scientific_subthemes`.

Verdict : excellente cohérence structurelle du multi-label.

### 6.4 Compatibilité legacy
Le code conserve une compatibilité utile :
- `theme` reste exposé comme alias de `cordis_theme_primary` côté vue de compatibilité ;
- `sub_theme` retombe sur le premier sous-thème quand nécessaire ;
- `legacy_theme` et `legacy_sub_theme` restent présents pour les zones anciennes et pour des usages de transition.

Verdict : bonne stratégie de migration, prudente et maintenable.

## 7. Audit des règles de comptage

### 7.1 Comptage des projets
Le total projet est protégé par une logique de `COUNT(DISTINCT projectID)` dans [app.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/app.py).

Constat de cohérence :
- aucun projet n'a plusieurs thèmes principaux ou domaines UI ;
- le risque de double comptage principal par explosion de sous-thèmes est correctement évité.

### 7.2 Budget principal
Le budget reste calculé à partir des contributions participant (`ecContribution`) chargées depuis CORDIS dans [process_build.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/process_build.py).

Lecture correcte à communiquer :
- la donnée brute de base est au grain participant ;
- le budget projet résulte donc de la somme des contributions participants ;
- c'est cohérent avec le modèle CORDIS et adapté aux agrégations projet / acteur / pays.

### 7.3 Sous-thèmes et total global
Le code sépare correctement :
- total principal : projet distinct / thème principal ;
- exploration fine : projet x sous-thème via table annexe.

C'est la bonne architecture analytique. Il ne faut pas sommer les sous-thèmes pour reconstruire un total global, et le code actuel va dans ce sens.

## 8. Audit UX et lisibilité métier

### 8.1 Point fort majeur
L'application est désormais beaucoup plus lisible côté métier qu'avant.

Les éléments les plus positifs :
- la home et les filtres simples sont structurés autour des `Domaines CORDIS` ;
- les vues synthétiques utilisent désormais les domaines comme lecture par défaut ;
- les codes CORDIS bruts ne sont plus montrés comme libellés principaux dans les graphes ;
- un module dédié [cordis_labels.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/cordis_labels.py) centralise l'humanisation des libellés ;
- les tooltips gardent l'accès à la valeur brute quand cela reste utile.

### 8.2 Exemples concrets de lisibilité gagnée
Exemples observés sur le formateur de labels :
- `HORIZON-JU-EUROHPC-2025-AI-01-IBA-01` devient `EuroHPC JU · 2025 · IA`
- `HORIZON-JTI-CLEANH2-2022-06-02` devient `Clean Hydrogen JU · 2022`
- `ERC-2021-ADG` devient `Conseil européen de la recherche · Advanced Grant · 2021`
- `NMBP-26-2016` devient `Matériaux et production · 2016`

### 8.3 Microcopy
Les textes d'aide et de méthode sont beaucoup plus cohérents avec le nouveau modèle :
- `Domaines CORDIS`
- `Thème principal CORDIS`
- `Sous-thèmes scientifiques`

L'ancien vocabulaire centré sur les thèmes inférés par mots-clés n'est plus structurant dans les vues principales.

## 9. Forces objectivement démontrables

Tu peux défendre les points suivants auprès de l'équipe :
- le code compile proprement ;
- le worktree audité est propre ;
- le modèle de données est désormais aligné sur CORDIS ;
- les champs critiques sont remplis à 100 % ;
- le thème principal et le domaine sont déterministes par projet ;
- les sous-thèmes scientifiques sont réellement multi-label ;
- la table annexe projet x sous-thème est cohérente à 100 % avec la colonne sérialisée ;
- les vues principales n'utilisent plus une taxonomie interne comme pilier produit ;
- les graphes synthétiques sont désormais compréhensibles par un utilisateur métier non technique.

## 10. Limites et points de vigilance

Cet audit est positif, mais il y a quelques limites réelles à signaler honnêtement.

### 10.1 Source du thème principal trop uniforme
Le `cordis_theme_primary_source` vaut `topic` pour 100 % des projets du dump courant.

Cela veut dire :
- la logique de cascade est bien codée ;
- mais les champs plus fins `programmeDivisionTitle` / `programmeDivision` ne structurent pas concrètement ce dump.

C'est une limite de données disponibles plus qu'un défaut de code.

### 10.2 Doublons exacts résiduels
291 lignes exactes sont encore dupliquées.

Le niveau reste faible, mais il serait utile d'ajouter à terme un contrôle automatique ou une alerte build si ce nombre augmente.

### 10.3 Lignes à 0 € au grain participant
53 387 lignes ont un `amount_eur = 0`.

Ce n'est pas bloquant pour l'usage, mais ce point doit être compris dans la documentation métier :
- toutes les lignes participant n'ont pas nécessairement une contribution utile pour une lecture budgétaire fine ;
- au niveau projet, l'impact est faible car 1 seul projet aboutit à un total nul.

### 10.4 Performance du rebuild
Le rebuild complet reste coûteux sur la volumétrie réelle. Ce n'est pas un problème de qualité analytique, mais c'est un sujet d'industrialisation opérationnelle.

## 11. Recommandations prioritaires

### Priorité 1 — à faire si l'objectif est de rassurer davantage l'équipe
- ajouter un petit bloc de tests automatiques de non-régression sur :
  - unicité `projectID -> cordis_theme_primary`
  - unicité `projectID -> cordis_domain_ui`
  - cohérence `scientific_subthemes_count`
  - absence de champs critiques vides

### Priorité 2 — à faire si l'objectif est d'aller vers une version encore plus industrialisée
- documenter explicitement dans le README et/ou la documentation technique :
  - le grain participant de `amount_eur`
  - la règle `COUNT(DISTINCT projectID)` pour les KPI globaux
  - le fait qu'on ne recompose jamais le total global depuis les sous-thèmes

### Priorité 3 — à faire si l'objectif est d'améliorer encore la finesse scientifique
- auditer un échantillon ciblé de sous-thèmes pour repérer les quelques rattachements encore bruités ;
- ajuster progressivement la logique d'enrichissement sans casser la stabilité globale.

## 12. Conclusion finale

Le code et les données audités sont robustes sur les dimensions qui comptent le plus pour la crédibilité du produit :
- structure CORDIS-first ;
- cohérence du thème principal ;
- séparation saine entre lecture globale et exploration fine ;
- bonne qualité structurelle du parquet ;
- UX devenue lisible pour des utilisateurs non techniques.

En l'état, il est raisonnable de présenter cette version comme :
- techniquement sérieuse ;
- analytiquement cohérente ;
- suffisamment robuste pour une démonstration ou un partage équipe ;
- avec quelques limites connues, clairement identifiées et non bloquantes.

## 13. Références auditées
- [app.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/app.py)
- [process_build.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/process_build.py)
- [pipeline.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/pipeline.py)
- [theme_classifier_v3.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/theme_classifier_v3.py)
- [cordis_taxonomy.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/cordis_taxonomy.py)
- [cordis_labels.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/cordis_labels.py)
- [subsidy_base.parquet](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/data/processed/subsidy_base.parquet)
- [project_scientific_subthemes.parquet](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/data/processed/project_scientific_subthemes.parquet)
