# Project Memory — Llobregat Digital Twin

_This file is updated at the end of every agent session._

## Current status

- Phase: 1 — Data layer complete and live ✅
- Last updated: 2026-03-25
- Last session summary: All ACA and AEMET API clients verified against live endpoints. Real station IDs discovered and committed. Parser bugs fixed. Full data pipeline running. Streamlit app rendering with live data.

## What works right now

- `python -m data.fetchers.refresh_all` runs cleanly end-to-end
- All 6 ACA gauge stations fetching ~200 rows/week each ✅
- All 3 ACA reservoirs fetching ~200 rows/month each ✅
- AEMET Berga (0076) and Manresa (0149X) fetching meteo obs ✅
- `streamlit run app/app.py` loads and all 5 pages render ✅
- Git repo initialised, 3 commits on `main` ✅

## Live station inventory

### ACA gauge stations (provider: AFORAMENT-EST)

| id | name | flow_sensor | level_sensor |
|----|------|------------|--------------|
| 081445-001 | Berga (Llobregat) | CALC001304 | 081445-001-ANA002 |
| 080180-005 | Balsareny (Llobregat) | CALC001229 | 080180-005-ANA001 |
| 080538-005 | Castellbell i el Vilar | CALC001244 | 080538-005-ANA001 |
| 081141-003 | Martorell (Llobregat+Anoia) | CALC001975 | — |
| 080478-006 | Cardona (Cardener) | CALC001241 | 080478-006-ANA001 |
| 082172-002 | Sant Joan Despí | CALC001261 | 082172-002-ANA010 |

### ACA reservoirs (provider: EMBASSAMENT-EST)

| id | name | volume_sensor | level_sensor | capacity_hm3 |
|----|------|--------------|--------------|-------------|
| 082687-001 | La Baells | CALC000697 | 082687-001-ANA015 | 109 |
| 250753-004 | Sant Ponç | CALC000699 | 250753-004-ANA010 | 24 |
| 081419-003 | La Llosa del Cavall | CALC000698 | 081419-003-ANA005 | 79 |

### AEMET meteorological stations

| id | name | status |
|----|------|--------|
| 0076 | Berga | ✅ working |
| 0149X | Manresa | ✅ working |
| 0200E | Martorell | ❌ AEMET 404 — wrong ID, needs replacing |
| 0229I | Molins de Rei | ❌ AEMET 404 — wrong ID, needs replacing |

Run `python -m data.fetchers.discover_stations` to get a full AEMET catalogue
for the watershed and pick replacements for the two broken station IDs.

## What exists

### Repository structure
All directories and files per spec are at `~/Documents/Llobregat_Viz/llobregat-twin/`.

### Docs layer (`docs/`)
- `INSTRUCTIONS.md` ✅ — operating constitution for future agents
- `PROJECT_MEMORY.md` ✅ — this file
- `DATA_SCHEMA.md` ✅ — data contract between fetching and presentation layers
- `DECISIONS.md` ✅ — 10 architectural decisions logged (D001–D010)

### Config layer (`config/`)
- `thresholds.yaml` ✅ — provisional alert thresholds (flow, reservoir, piezo)
- `station_metadata.yaml` ✅ — real verified station IDs (ACA confirmed live)

### Data layer (`data/`)
- `fetchers/utils.py` ✅ — retry, rate limiter, HTTP client (60s timeout), logging
- `fetchers/aca.py` ✅ — ACA Sentilo client; parser fixed for `timestamp` key
- `fetchers/aemet.py` ✅ — AEMET OpenData client; two-step response, key as query param
- `fetchers/refresh_all.py` ✅ — reads AEMET key from env or `.streamlit/secrets.toml`
- `fetchers/discover_stations.py` ✅ — live catalogue discovery (ACA + AEMET)
- `static/discovered_aca_gauges.csv` ✅ — real ACA gauge component IDs
- `static/discovered_aca_reservoirs.csv` ✅ — real ACA reservoir component IDs
- `cache/` — populated with parquet files after each refresh (gitignored)

### App layer (`app/`)
- `app.py` ✅ — Streamlit entry point, sidebar, landing page
- `pages/01_overview.py` ✅ — Folium map + system status table
- `pages/02_rivers.py` ✅ — flow hydrograph + stage, threshold overlays
- `pages/03_reservoirs.py` ✅ — storage %, volume trend, alert badges
- `pages/04_meteorology.py` ✅ — precip bar, temp, wind charts
- `pages/05_aquifers.py` ✅ — piezometric levels (graceful missing-data handling)

### Tests (`tests/`)
- `test_aca_fetcher.py` ✅ — schema compliance, HTTP 204, parsing, NaN convention
- `test_aemet_fetcher.py` ✅ — schema compliance, two-step response, coord conversion
- `test_data_schema.py` ✅ — reusable schema validators, synthetic DataFrame tests

### CI/CD
- `.github/workflows/refresh_data.yml` ✅ — 30-min cron + manual trigger
  ⚠️ Needs `AEMET_API_KEY` secret added in GitHub repo settings before it can run.

## Task backlog (ordered by priority)

### Immediate
- [ ] **Fix AEMET station IDs 0200E and 0229I**: Run `python -m data.fetchers.discover_stations`
  and find real AEMET codes for lower Llobregat / Martorell area.
- [ ] **Add AEMET_API_KEY secret to GitHub Actions**: Repo settings → Secrets and variables →
  Actions → New repository secret. Name: `AEMET_API_KEY`.
- [ ] **Run pytest tests**: `cd llobregat-twin && pip install -r requirements.txt && pytest tests/ -v`
- [ ] **Deploy to Streamlit Cloud**: Connect GitHub repo, point at `app/app.py`, add secret.

### Phase 2 — Dashboard improvements
- [ ] Add watershed GeoJSON overlays to the overview map (boundary, river network, sub-basins)
  Download from: https://www.icgc.cat/ca/Descarregues
- [ ] Multi-station flow comparison chart on rivers page
- [ ] Last-N-days selector on all time-series pages
- [ ] Alert state reflected in page tab colours / emojis
- [ ] Populate piezometric station list (run discover_stations with `componentType=piezometre`)
- [ ] Calibrate alert thresholds in `config/thresholds.yaml` from historical ACA data

### Phase 3 — Twin layer (future)
- [ ] Integrate GR4J or HBV rainfall-runoff model
- [ ] Data assimilation: nudge model state with live observations
- [ ] 48-hour flow forecast display
- [ ] Rename product from "monitoring dashboard" to "digital twin"

## Known issues and caveats

1. **AEMET station IDs 0200E and 0229I return 404**: These were placeholder guesses. The
   Berga (0076) and Manresa (0149X) stations work. Need to run AEMET catalogue discovery
   to find real codes for Martorell and Molins de Rei area.

2. **NumPy 2.x / Anaconda package incompatibility**: Anaconda ships `numexpr`, `bottleneck`,
   and `xarray` compiled against NumPy 1.x. With NumPy 2.4.3, these show `AttributeError`
   on import. Workaround: `pip install --upgrade xarray` fixes the Streamlit page crash.
   Full fix: `conda install "numpy<2"` (may be blocked by environment inconsistency).

3. **500 row cap per ACA request**: Sentilo `limit=500` is the max. Gauges at 1-hour
   resolution give 168 obs/week — well under the cap. Reservoirs at 6-hour resolution
   give 28 obs/week. If higher resolution or longer windows are needed, implement
   pagination by sliding the `from`/`to` window.

4. **No GeoJSON files yet**: `data/static/watershed_boundary.geojson`, `river_network.geojson`,
   and `sub_basins.geojson` do not exist. The overview map renders without these overlays.
   Download from ICGC portal: https://www.icgc.cat/ca/Descarregues

5. **Git commits require running from Mac terminal**: The git identity on the Linux
   sandbox differs from the macOS host. All git operations (add/commit/push) must
   be done by the user in their Mac Terminal, or via a provided shell script.

6. **AEMET returns daily summaries (13 obs / week)**: The `/observacion/convencional/`
   endpoint returns daily climatological summaries, not hourly. For sub-daily precip,
   consider the `/observacion/convencional/datos/estacion/{id}/` endpoint with tighter
   date windows, or add the XEMA (Metecat) API for Catalan hourly data.

## Decisions log

See `docs/DECISIONS.md` for full rationale.

| Date | ID | Decision |
|------|----|----------|
| 2026-03-24 | D001 | Parquet over SQLite for cache |
| 2026-03-24 | D002 | DuckDB as escape hatch for complex queries |
| 2026-03-24 | D003 | Streamlit Cloud for deployment |
| 2026-03-24 | D004 | GitHub Actions cron for data refresh |
| 2026-03-24 | D005 | Folium via streamlit-folium for maps |
| 2026-03-24 | D006 | httpx for API calls |
| 2026-03-24 | D007 | No "digital twin" label until Phase 3 |
| 2026-03-25 | D008 | ACA uses Sentilo platform, not custom SDIM API |
| 2026-03-25 | D009 | AEMET API key sent as query param (not just header) |
| 2026-03-25 | D010 | secrets.toml fallback in refresh_all.py for local runs |
