# Subsidy Intelligence Radar — quick start (local + Streamlit Cloud)

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

## Streamlit Community Cloud
Push this repo to GitHub (keep `data/` ignored via `.gitignore`). Then deploy with main file `app.py`.
