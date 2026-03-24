# Llobregat Watershed Monitoring Dashboard

A live monitoring dashboard for the Llobregat River watershed (Catalonia, Spain) —
the primary water supply for the Barcelona metropolitan area.

> ⚠️ **This is a monitoring dashboard, not a digital twin.** A rainfall-runoff simulation
> model will be integrated in Phase 3. Until then, this tool displays observed data only.

## What it shows

- Interactive map of the watershed with gauge stations, reservoirs, meteo stations, and piezometers
- Real-time and recent time-series data for river flows, reservoir levels, precipitation, and aquifer levels
- Alert indicators when values cross configurable thresholds

## Data sources

| Source | Data | Update frequency |
|--------|------|-----------------|
| [ACA SDIM](https://aca.gencat.cat) | River gauges, reservoir levels, piezometric levels | 30 min |
| [AEMET OpenData](https://opendata.aemet.es) | Precipitation, temperature, wind | 30 min |

## Running locally

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_ORG/llobregat-twin.git
cd llobregat-twin

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up secrets
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit .streamlit/secrets.toml and add your AEMET API key

# 4. Fetch initial data
python -m data.fetchers.refresh_all

# 5. Run the app
streamlit run app/app.py
```

## Deployment

The app is deployed on Streamlit Cloud. Data is refreshed every 30 minutes via a
GitHub Actions cron job that commits updated Parquet cache files to the repository.

## Project structure

```
llobregat-twin/
├── .github/workflows/   # Scheduled data refresh
├── app/                 # Streamlit dashboard (pages)
├── config/              # Thresholds and station metadata (YAML)
├── data/
│   ├── fetchers/        # ACA and AEMET API clients
│   ├── cache/           # Parquet data cache (gitignored except .gitkeep)
│   └── static/          # GeoJSON watershed boundary, river network
├── docs/                # INSTRUCTIONS, PROJECT_MEMORY, DATA_SCHEMA, DECISIONS
└── tests/               # Pytest test suite
```

## Watershed overview

- **Main stem**: Llobregat rises in the Pre-Pyrenees (Cadí massif) at ~1,200 m,
  flows ~170 km to the Mediterranean south of Barcelona
- **Key tributaries**: Cardener (joins at Manresa), Anoia (joins near Martorell)
- **Key reservoirs**: La Baells (109 hm³), Sant Ponç (24 hm³)
- **Mean annual flow** at Martorell: ~17 m³/s (highly variable — Mediterranean regime)

## Phase roadmap

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Data layer: API fetchers, cache, GitHub Actions refresh | 🔄 In progress |
| 2 | Dashboard: Streamlit multi-page app, maps, charts, alerts | ⏳ Planned |
| 3 | Twin layer: GR4J rainfall-runoff model + data assimilation | ⏳ Future |
