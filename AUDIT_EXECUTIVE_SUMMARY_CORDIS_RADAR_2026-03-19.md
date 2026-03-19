# Executive Summary — Subsidy Intelligence Radar

Date : 19 mars 2026  
Périmètre : application Streamlit, pipeline de build CORDIS, dataset traité courant.

## Message clé

Le Subsidy Intelligence Radar a atteint un niveau de maturité solide et crédible pour un usage métier.

La refonte récente a apporté trois gains structurants :
- un modèle de données désormais aligné sur CORDIS ;
- des règles de comptage robustes ;
- une interface devenue lisible pour des utilisateurs non techniques.

## Ce qui est fort aujourd'hui

### 1. Le modèle est sain
L'application n'est plus structurée autour d'une taxonomie interne opaque.

Le socle principal repose maintenant sur :
- `cordis_domain_ui` pour la navigation métier ;
- `cordis_theme_primary` pour le thème principal officiel CORDIS ;
- `scientific_subthemes` pour l'exploration fine multi-label.

### 2. Les données sont propres au niveau structurel
Sur le parquet traité courant :
- 296 940 lignes
- 54 413 projets distincts
- 58 314 acteurs distincts
- 42 colonnes
- 100 % de complétude sur les champs critiques audités
- 0 budget négatif
- 0 incohérence d'unicité du thème principal par projet
- 0 incohérence d'unicité du domaine par projet

### 3. Les sous-thèmes scientifiques sont bien industrialisés
- 95,2 % des projets ont au moins un sous-thème scientifique
- 75,0 % des projets ont des sous-thèmes multi-label
- 0 écart entre la colonne sérialisée et la table annexe projet x sous-thème

### 4. Les KPI globaux restent défendables
Les règles de comptage sont cohérentes :
- total projets en `COUNT(DISTINCT projectID)`
- thème principal unique par projet
- pas de reconstruction trompeuse des totaux globaux via les sous-thèmes

### 5. L'UX est devenue beaucoup plus lisible
Les vues principales ne montrent plus brutalement des codes CORDIS incompréhensibles.

Les lectures par défaut passent maintenant par les `Domaines CORDIS`, et les codes techniques sont relégués aux tooltips, détails ou exports.

## Ce que cela prouve

En l'état, le produit peut être présenté comme :
- techniquement sérieux ;
- analytiquement cohérent ;
- lisible pour des utilisateurs métier ;
- suffisamment robuste pour des démonstrations, pilotes ou échanges d'équipe.

## Limites à signaler honnêtement

Le projet est solide, mais pas “figé” :
- le thème principal provient en pratique du `topic` pour 100 % du dump courant ;
- il subsiste 291 doublons exacts dans les lignes traitées ;
- 18,0 % des lignes participant portent un budget à 0 €, ce qui doit être compris comme une caractéristique du grain participant ;
- le rebuild complet reste coûteux en temps.

Aucune de ces limites ne remet en cause la crédibilité globale du système, mais elles doivent être connues.

## Recommandation de positionnement équipe

La bonne manière de présenter cette version est :
- un produit déjà robuste sur le fond ;
- bien restructuré autour de CORDIS ;
- avec encore quelques chantiers d'industrialisation et de calibration fine.

## Trois messages simples à porter en réunion

1. Le modèle n'est plus “maison d'abord”, il est désormais CORDIS-first.
2. Les totaux principaux sont cohérents et défendables.
3. L'expérience utilisateur est maintenant lisible pour un public métier.

## Références
- [Audit détaillé](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/AUDIT_QUALITE_CORDIS_RADAR_2026-03-19.md)
- [app.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/app.py)
- [process_build.py](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/process_build.py)
- [subsidy_base.parquet](/Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar/data/processed/subsidy_base.parquet)
