# Project Memory — Llobregat Digital Twin

_This file is updated at the end of every agent session._

## Current status

- Phase: 1 — Data layer (scaffold complete, API clients written, not yet tested against live endpoints)
- Last updated: 2026-03-24
- Last session summary: Full Phase 1 scaffold created from scratch. All directories, docs, config, fetchers, app skeleton, and tests written.

## What exists

### Repository structure
All directories and files per spec are in place at `~/Documents/Llobregat_Viz/llobregat-twin/`.

### Docs layer (`docs/`)
- `INSTRUCTIONS.md` ✅ — operating constitution for future agents
- `PROJECT_MEMORY.md` ✅ — this file
- `DATA_SCHEMA.md` ✅ — data contract between fetching and presentation layers
- `DECISIONS.md` ✅ — 7 architectural decisions logged (D001–D007)

### Config layer (`config/`)
- `thresholds.yaml` ✅ — provisional alert thresholds (flow, reservoir, piezo)
- `station_metadata.yaml` ✅ — gauge, reservoir, meteo station definitions

### Data layer (`data/`)
- `fetchers/utils.py` ✅ — retry decorator, rate limiter, HTTP client factory, logging setup
- `fetchers/aca.py` ✅ — ACA SDIM client: gauge, reservoir, piezometric data
- `fetchers/aemet.py` ✅ — AEMET OpenData client: two-step response pattern, meteo obs
- `fetchers/refresh_all.py` ✅ — master refresh script, called by GitHub Actions
- `static/stations_aca.csv` ✅ — placeholder station catalogue (VERIFY against real ACA API)
- `static/stations_aemet.csv` ✅ — placeholder AEMET stations (VERIFY against AEMET inventory)
- `cache/.gitkeep` ✅

### App layer (`app/`)
- `app.py` ✅ — Streamlit entry point, sidebar, landing page
- `pages/01_overview.py` ✅ — interactive Folium map + system status table
- `pages/02_rivers.py` ✅ — flow hydrograph + stage, threshold overlays
- `pages/03_reservoirs.py` ✅ — storage %, volume trend, alert badges
- `pages/04_meteorology.py` ✅ — precip bar, temp, wind charts
- `pages/05_aquifers.py` ✅ — piezometric levels (graceful missing-data handling)

### Tests (`tests/`)
- `test_aca_fetcher.py` ✅ — schema compliance, HTTP 204 handling, data parsing, NaN convention, no hardcoded secrets
- `test_aemet_fetcher.py` ✅ — schema compliance, two-step response, null field handling, coordinate conversion
- `test_data_schema.py` ✅ — reusable schema validators, synthetic DataFrame tests

### CI/CD
- `.github/workflows/refresh_data.yml` ✅ — 30-min cron + manual trigger, commits updated cache

## Task backlog (ordered by priority)

### Immediate — must do before app is useful
- [ ] **Verify ACA station IDs**: Run `fetch_aca_station_catalogue()` against live ACA SDIM API.
  Update `config/station_metadata.yaml` and `data/static/stations_aca.csv` with real codes.
  Current IDs (E003, E010, E020, E030, BAELLS, SPONC) are PLACEHOLDERS.
- [ ] **Verify ACA API endpoint + authentication**: The SDIM base URL (`https://sdim.aca.gencat.cat/sdim2`) and variable codes (1,2,5,6,9) need live verification. Check https://aca.gencat.cat developer portal.
- [ ] **Verify AEMET station IDs**: Run `fetch_aemet_station_catalogue()` with the user's AEMET key.
  Update `config/station_metadata.yaml` with real idema codes.
- [ ] **Run test suite**: `pytest tests/ -v` in an environment with all deps installed (httpx, pytest, pyarrow).
- [ ] **First live data fetch**: `python -m data.fetchers.refresh_all` with real API keys.
- [ ] **Deploy to Streamlit Cloud**: Push repo to GitHub, connect to Streamlit Cloud, set AEMET_API_KEY secret.

### Phase 2 — Dashboard improvements (after live data confirmed)
- [ ] Add watershed GeoJSON overlays to the overview map (boundary, river network, sub-basins)
- [ ] Multi-station flow comparison chart on rivers page
- [ ] Last-N-days selector on all time-series pages
- [ ] Alert state reflected in page tab colours / emojis
- [ ] Populate piezometric station list once ACA piezo catalogue explored
- [ ] Calibrate piezometric alert thresholds per station

### Phase 3 — Twin layer (future)
- [ ] Integrate GR4J or HBV rainfall-runoff model
- [ ] Data assimilation: nudge model state with live observations
- [ ] 48-hour flow forecast display
- [ ] Rename product from "monitoring dashboard" to "digital twin"

## Known issues and caveats

1. **Station IDs are PLACEHOLDERS**: `config/station_metadata.yaml` uses invented IDs (E003, E010, E020, E030, BAELLS, SPONC). These MUST be replaced with real ACA SDIM codes before any data can be fetched.

2. **ACA API endpoint unverified**: The base URL `https://sdim.aca.gencat.cat/sdim2` and all path patterns (`/registry/estacions`, `/series`) are based on best available knowledge. The actual ACA SDIM developer portal may have different paths. Priority task for next session.

3. **ACA variable codes unverified**: Variable codes 1=flow, 2=stage, 5=reservoir volume, 6=reservoir level, 9=piezo are provisional. Verify against the ACA SDIM variable catalogue.

4. **Git init blocked on mount**: The `git init` completed but the commit step failed due to a macOS/Linux filesystem permission issue with the lock file on the mounted volume. User must run `bash init_git.sh` from their Mac terminal to create the initial commit.

5. **Tests not yet run against live deps**: `pytest` and `httpx` could not be installed in the session sandbox (network blocked). All 15 .py files passed AST syntax check. Tests should be run locally after `pip install -r requirements.txt`.

6. **No GeoJSON files yet**: `data/static/watershed_boundary.geojson`, `river_network.geojson`, and `sub_basins.geojson` do not exist. The overview map will render without these overlays until they are downloaded from ICGC / ACA cartography portal.

7. **AEMET coord conversion**: The `_aemet_coord_to_decimal()` function handles the most common AEMET formats. If edge-case formats appear in the station inventory, update the function and add tests.

## Decisions log

See `docs/DECISIONS.md` for full rationale.

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-24 | Parquet over SQLite for cache | Columnar, no server, pandas-native |
| 2026-03-24 | DuckDB as fallback | For complex cross-parquet queries |
| 2026-03-24 | Streamlit Cloud deployment | Free tier, GitHub-integrated |
| 2026-03-24 | GitHub Actions cron for refresh | Serverless, version-controlled |
| 2026-03-24 | Folium for maps | Full GeoJSON support |
| 2026-03-24 | httpx for API calls | Modern async-capable HTTP client |
| 2026-03-24 | No "digital twin" label until Phase 3 | Honest product labelling |
