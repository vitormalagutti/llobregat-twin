# Llobregat Watershed Monitoring Dashboard

A live monitoring dashboard for the Llobregat River watershed (Catalonia, Spain) —
the primary water supply for the Barcelona metropolitan area.

> ⚠️ **This is a monitoring dashboard, not a digital twin.** A rainfall-runoff simulation
> model will be integrated in Phase 3. Until then, this tool displays observed data only.

## What it shows

- Interactive map of the watershed with gauge stations, reservoirs, and meteo stations
- 7-day river flow and stage time-series for 6 key gauging stations
- 30-day reservoir storage trends for La Baells, Sant Ponç, and La Llosa del Cavall
- Precipitation, temperature, and wind data from AEMET weather stations
- Alert indicators when values cross configurable thresholds

## Data sources

| Source | Data | Update frequency |
|--------|------|-----------------|
| [ACA Sentilo](https://aplicacions.aca.gencat.cat/sdim2/apirest) | River gauges, reservoir levels | 30 min |
| [AEMET OpenData](https://opendata.aemet.es) | Precipitation, temperature, wind | Daily summaries |

## Running locally

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_ORG/llobregat-twin.git
cd llobregat-twin

# 2. Install dependencies
pip install -r requirements.txt

# 3. Fix NumPy 2.x incompatibility (Anaconda users only)
pip install --upgrade xarray

# 4. Set up secrets
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit .streamlit/secrets.toml and add your AEMET API key
# Get a free key at: https://opendata.aemet.es/centrodedescargas/inicio

# 5. Fetch initial data
python -m data.fetchers.refresh_all

# 6. Run the app
streamlit run app/app.py
```

## Deployment

The app is designed for Streamlit Cloud. Data is refreshed every 30 minutes via a
GitHub Actions cron job. Before deploying:

1. Add `AEMET_API_KEY` as a GitHub Actions secret (repo Settings → Secrets)
2. Connect the repo to Streamlit Cloud, entry point: `app/app.py`
3. Add `AEMET_API_KEY` as a Streamlit Cloud secret in the app settings

## Project structure

```
llobregat-twin/
├── .github/workflows/   # Scheduled data refresh (every 30 min)
├── app/                 # Streamlit dashboard
│   ├── app.py           # Entry point and landing page
│   └── pages/           # 01_overview, 02_rivers, 03_reservoirs, 04_meteorology, 05_aquifers
├── config/
│   ├── station_metadata.yaml   # Verified ACA + AEMET station IDs
│   └── thresholds.yaml         # Alert thresholds
├── data/
│   ├── fetchers/        # ACA Sentilo + AEMET API clients
│   │   ├── aca.py
│   │   ├── aemet.py
│   │   ├── refresh_all.py
│   │   └── discover_stations.py
│   ├── cache/           # Parquet data cache (gitignored)
│   └── static/          # Discovered station CSVs, GeoJSON (future)
├── docs/                # INSTRUCTIONS, PROJECT_MEMORY, DATA_SCHEMA, DECISIONS
└── tests/               # Pytest test suite
```

## Watershed overview

- **Main stem**: Llobregat rises in the Pre-Pyrenees (Cadí massif) at ~1,200 m,
  flows ~170 km to the Mediterranean south of Barcelona
- **Key tributaries**: Cardener (joins at Manresa), Anoia (joins near Martorell)
- **Key reservoirs**: La Baells (109 hm³), Sant Ponç (24 hm³), La Llosa del Cavall (79 hm³)
- **Mean annual flow** at Martorell: ~17 m³/s (highly variable — Mediterranean regime)

## Phase roadmap

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Data layer: API fetchers, Parquet cache, GitHub Actions refresh | ✅ Complete |
| 2 | Dashboard improvements: GeoJSON maps, multi-station charts, date selector | 🔄 Next |
| 3 | Twin layer: GR4J rainfall-runoff model + data assimilation + forecasts | ⏳ Future |
