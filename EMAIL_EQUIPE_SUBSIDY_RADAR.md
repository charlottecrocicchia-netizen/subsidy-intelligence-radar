Objet: Subsidy Intelligence Radar - fonctionnement, onglets, APIs et automatisation

Bonjour a tous,

Voici un recapitulatif clair du fonctionnement de l'outil Subsidy Intelligence Radar et de son mode d'exploitation.

1) Finalite de l'outil
- Analyser les projets lauréats Horizon Europe / H2020 (source CORDIS), avec vues macro, benchmark acteurs, geographie, tendances, comparaison de periodes, et chaine/reseau de collaboration.
- Offrir une base reproductible pour les analyses R&D internes.

2) Donnees et APIs utilisees
Sources actives dans le pipeline:
- CORDIS bulk CSV ZIP (Horizon Europe + H2020)
- ADEME via API data.gouv.fr
- EC Newsroom RSS (evenements)
- EUR-Lex / Cellar SPARQL (evenements)

Sources optionnelles (framework deja pret, a configurer dans `data/external/connectors_manifest.csv`):
- CINEA API
- Qlik API
- EU Funding API
- MCP/Kaila via commande

Important:
- Les APIs ne sont pas appelees en continu au chargement des pages.
- Elles sont appelees uniquement pendant les refresh (manuel ou GitHub Actions).
- L'app lit ensuite des fichiers preprocesses (parquet/csv), ce qui stabilise la performance.

3) Ce qui est automatique vs manuel
Automatique:
- Refresh via bouton UI (pipeline + events + clear cache)
- Refresh durable via GitHub Actions (`.github/workflows/refresh-data.yml`)

Manuel (parametrage initial):
- Completer `connectors_manifest.csv` avec endpoints/headers/commandes reels
- Fournir tokens via variables d'environnement / GitHub Secrets
- Maintenir le mapping groupe (`data/external/actor_groups.csv`)

4) Explication onglet par onglet

Vue d'ensemble:
- KPIs principaux, structure des tickets, concentration.
- Sert a valider le perimetre filtre avant interpretation.

Geographie:
- Carte + top pays.
- Metrique standard recommandee: budget par million d'habitants.

Benchmark acteurs:
- Comparaison relative des acteurs (scatter, treemap, rankings).
- Peut consolider les entites legales via le mode groupe/PIC.

Tendances:
- Evolution des themes/sections dans le temps (absolu ou part).

Comparaison:
- Delta entre deux periodes A/B (part et budget).

Macro & actualites:
- Superpose des evenements a la serie budget d'une thematique.
- Contexte analytique (hypotheses), pas preuve de causalite.
- Le budget de cet onglet est local a la thematique selectionnee (pas le budget global).

Fiche acteur:
- Trajectoire de l'acteur, repartitions theme/geo, partenaires.

Chaine & reseau:
- Chaine de valeur (Sankey) + reseau de collaboration autour d'un acteur focal.
- Drilldown jusqu'aux projets sur un couple etape+acteur.

Donnees:
- Vue paginee des lignes (anti MessageSizeError) + exports.

Qualite:
- Controle completude/coherence des champs et montants.

Aide / Guide:
- Support utilisateur et bonnes pratiques d'interpretation.

5) Points d'attention metier
- "Exclure financeurs/agences" est active pour eviter de melanger financeurs et entreprises.
- Le mode regroupement groupe/PIC depend de la qualite du mapping (coverage visible en sidebar).
- Macro/actualites depend de la couverture du `events.csv`: certains tags peuvent avoir peu d'evenements.

6) Gouvernance et perennite
- Source de verite code: GitHub.
- Source de verite data calculee: artefacts versionnes (`subsidy_base.parquet`, `events.csv`).
- Deploiement Streamlit relie a la branche GitHub.
- Verification rapide en production: comparer `Version code` (SHA) dans la sidebar avec le dernier commit GitHub.

7) References techniques
- Guide rapide: `README.md`
- Documentation complete: `DOCUMENTATION_TECHNIQUE_COMPLETE.md`

Cordialement,

[Votre nom]
