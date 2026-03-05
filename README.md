# Subsidy Intelligence Radar — quick start (local + Streamlit Cloud)

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

## Optional actor grouping (PIC / group)
You can optionally add `data/external/actor_groups.csv` to merge legal entities under one parent group.

Expected columns:
- `actor_id` (preferred join key)
- `pic` (optional fallback join key)
- `group_id`
- `group_name`
- `is_funder` (`true/false`, optional)

A template is provided in:
- `data/external/actor_groups.template.csv`
- Keep your real `data/external/actor_groups.csv` local (this file is git-ignored).

## Optional incremental connectors (API / MCP)
You can optionally add `data/external/connectors_manifest.csv` (from template) to pull external data incrementally:
- CINEA / Qlik / EU Funding APIs (`kind=api_json` or `api_csv`)
- MCP command connectors (`kind=mcp`)

Template:
- `data/external/connectors_manifest.template.csv`
- Keep your real `data/external/connectors_manifest.csv` local (this file is git-ignored).

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
Push this repo to GitHub (keep `data/` ignored via `.gitignore`). Then deploy with main file `app.py`.

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

## GitHub push (recommended)
Yes, you should push to GitHub for versioning + deployment.

```bash
git add .
git commit -m "Refactor app + incremental connectors + actor/group master model"
git push origin <your-branch>
```

Notes:
- `data/raw/` stays ignored.
- `data/processed/subsidy_base.parquet` and `data/external/events.csv` are tracked for Cloud startup.
- If the parquet becomes too large for GitHub limits, use Git LFS or a smaller processed export.
