# Subsidy Intelligence Radar — quick start (local + Streamlit Cloud)

Documentation technique complete:
- `DOCUMENTATION_TECHNIQUE_COMPLETE.md`
- Email equipe pret a envoyer:
  - `EMAIL_EQUIPE_SUBSIDY_RADAR.md`

## Run now (end-to-end)
```bash
cd /Users/charlottecrocicchia/Desktop/TotalEnergies/subsidy-intelligence-radar

# Option A: venv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Option B: conda (if you prefer your existing env)
# conda activate pyshtools_env
# python -m pip install -r requirements.txt

streamlit run app.py
```

Then in the app:
1. Click **Refresh** once (it rebuilds data + events + master actor/group tables).
2. Check tabs:
`Overview`, `Macro & news`, `Actor profile`, `Value chain & network`.

## Local run
```bash
cd /path/to/subsidy_radar_fixed_v2
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

In the app, click **Refresh** to (re)build:
- `pipeline.py` (download + build `data/processed/subsidy_base.csv`)
- `build_events.py` (build `data/external/events.csv`)

If `build_events.py` fails with `No module named feedparser`, run:
```bash
python -m pip install -r requirements.txt
```
with the same Python interpreter used to launch Streamlit.

## Actor grouping (PIC / group)
`data/external/actor_groups.csv` is now versioned so Streamlit Cloud can use the same mapping as local runs.

Expected columns:
- `actor_id` (preferred join key)
- `pic` (optional fallback join key)
- `group_id`
- `group_name`
- `is_funder` (`true/false`, optional)

A template is provided in:
- `data/external/actor_groups.template.csv`
- App fallback: if `actor_groups.csv` is missing, the app reads `actor_groups.template.csv`.
- The sidebar shows mapping coverage (`matched_actors / total_actors`) to verify whether mapping rows actually match your dataset IDs.

## Optional incremental connectors (API / MCP)
`data/external/connectors_manifest.csv` is versioned and can be used by local refresh and GitHub Actions refresh:
- CINEA / Qlik / EU Funding APIs (`kind=api_json` or `api_csv`)
- MCP command connectors (`kind=mcp`)

Template:
- `data/external/connectors_manifest.template.csv`
- Use environment variables for credentials (`${...}`), never plain tokens in git.

On each pipeline refresh:
- connectors are checked incrementally
- each connector keeps its own stamp in `data/processed/_state.json`
- failures are isolated (one connector can fail without stopping others)

Environment-variable placeholders are supported in the manifest:
- `${CINEA_API_TOKEN}`
- `${QLIK_API_TOKEN}`
- `${EU_FUNDING_API_TOKEN}`
- `${KAILA_API_TOKEN}`

Example:
```bash
export CINEA_API_TOKEN="xxx"
export QLIK_API_TOKEN="xxx"
export EU_FUNDING_API_TOKEN="xxx"
export KAILA_API_TOKEN="xxx"
```

## New derived outputs
`process_build.py` now also writes:
- `data/processed/actor_master.{csv,parquet}`
- `data/processed/group_master.{csv,parquet}`
- `data/processed/project_actor_links.{csv,parquet}`

The base dataset includes additional fields:
- `pic`
- `value_chain_stage`
- `project_status` (`Open` / `Closed` / `Unknown`)

## Streamlit Community Cloud
Push this repo to GitHub, then deploy with main file `app.py`.

Important:
- A Streamlit Cloud app can recompute files at runtime, but those file writes are not guaranteed to persist forever across restarts/redeploys.
- Durable automation is handled by GitHub Actions (`.github/workflows/refresh-data.yml`), which updates tracked data in the repo.

### Enable full automation (recommended)
1. Push this branch to GitHub.
2. In GitHub, open **Actions** and verify workflow **Refresh Data** appears.
3. Run it once with **Run workflow**.
4. Optionally add repository secrets for connectors:
`CINEA_API_TOKEN`, `QLIK_API_TOKEN`, `EU_FUNDING_API_TOKEN`, `KAILA_API_TOKEN`.
5. Keep Streamlit linked to the same branch (or `main`) to pick up automated commits.

### Deployment checks (important)
1. In app sidebar, compare `Version code` (git SHA) with latest GitHub commit.
2. In `Geography`, default map metric should be `Budget / million inhabitants (€)`.
3. In sidebar mapping block:
`Mapping groups loaded` + `Mapping coverage` must appear if mapping file is detected.
4. `Exclude funders / agencies` remains available even without mapping file (heuristic mode).
5. Country filter default should be Europe-first (you can then add non-European countries).
6. In `Value chain & network`, use `Themes` + `Value-chain stages to display` + `Stage to explore` for actor/project drilldown.

## Troubleshooting

### `Failed to push` + `Could not resolve host: github.com`
This is a local network/DNS issue, not a repository/code issue.
- Reconnect internet / VPN.
- Retry push when DNS is back.

### URL Streamlit not updated after push
- Confirm commit exists on `origin/main`.
- Confirm Streamlit app points to the same repository and branch.
- Force a Streamlit redeploy.
- Verify sidebar `Version code`.

### Grouping toggle has no visible effect
- Check mapping coverage in sidebar.
- If coverage is near `0%`, your `actor_id`/`pic` values in `actor_groups.csv` do not match current dataset identifiers.
- Update mapping file, then click `Refresh`.

## GitHub push (recommended)
Yes, you should push to GitHub for versioning + deployment.

```bash
git add .
git commit -m "Refactor app + incremental connectors + actor/group master model"
git push origin <your-branch>
```

Notes:
- `data/raw/` stays ignored.
- `data/processed/subsidy_base.parquet`, `data/external/events.csv`, `data/external/actor_groups.csv`, `data/external/connectors_manifest.csv` are tracked for cloud runs.
- If the parquet becomes too large for GitHub limits, use Git LFS or a smaller processed export.
