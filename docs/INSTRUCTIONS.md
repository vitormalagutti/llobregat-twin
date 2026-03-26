# Instructions for the Llobregat Digital Twin Agent

## Project mission
Build a live watershed monitoring dashboard for the Llobregat River (Catalonia, Spain),
pulling data from ACA (Agència Catalana de l'Aigua) and AEMET (Agencia Estatal de
Meteorología) APIs, displayed in a Streamlit app deployed on Streamlit Cloud.

## Operating rules

1. Read `PROJECT_MEMORY.md` and `DATA_SCHEMA.md` before every session.
2. Update `PROJECT_MEMORY.md` at the end of every session.
3. Commit to git after every meaningful milestone (working fetcher, new page, bug fix).
4. Never hardcode API keys. Use environment variables or Streamlit secrets.
5. Never overwrite working code without first reading it and understanding it.
6. Handle missing data gracefully. ACA piezometric data is patchy — always degrade
   gracefully with a visible "data unavailable" state rather than crashing.
7. Write tests for every fetcher function before considering it done.
8. Flag to the human before: changing the data schema, adding new dependencies,
   changing deployment configuration, or making any architectural decision not
   already covered by these instructions.
9. **All git operations must be run from the user's Mac terminal** — the agent's
   sandbox cannot commit to the mounted volume due to filesystem permissions.
   Provide `git add` + `git commit` commands for the user to paste.

## Scope per layer

- **Data layer** (fetchers, cache, scheduler): owns API clients, parquet cache, GitHub Actions.
  Does NOT write Streamlit UI code.
- **Presentation layer** (Streamlit pages): reads from parquet cache only.
  Does NOT call APIs directly. Does NOT modify the data schema.
- **Config layer** (thresholds, station metadata): human-editable YAML.
  Agents may read and suggest changes but must flag before modifying.

## Coding conventions

- Python: follow PEP 8, use type hints on all function signatures
- Function names: snake_case, descriptive (`fetch_aca_gauge_data`, not `get_data`)
- All timestamps: UTC internally, convert to `Europe/Madrid` for display only
- Units: SI throughout internally (m³/s for flow, m for levels, mm for precip).
  Display with unit labels. Never mix unit systems silently.
- Missing data: represent as `np.nan` in DataFrames, never as `0` or `-9999` unless
  the source API uses that convention (document it in `DATA_SCHEMA.md` if so)
- Logging: use Python `logging` module, not `print` statements, in fetchers

## Verified API behaviours (as of 2026-03-25)

### ACA Sentilo platform
- **Base URL**: `https://aplicacions.aca.gencat.cat/sdim2/apirest`
- **Catalogue**: `GET /catalog?componentType={aforament|embassament|piezometre}`
- **Data**: `GET /data/{provider}/{sensorId}?from=DD/MM/YYYYTHH:MM:SS&to=...&limit=500`
- **Auth**: No auth required for public read. `IDENTITY_KEY` header is optional.
- **Timestamp key in response**: `"timestamp"` (string `DD/MM/YYYYTHH:MM:SS`, local Madrid time).
  Note: `"time"` key also exists but is Unix epoch milliseconds (int) — do not use for string parsing.
- **Providers**: `AFORAMENT-EST` (gauges), `EMBASSAMENT-EST` (reservoirs)
- **Sensor naming**: `ANA` sensors = raw measurements; `CALC` sensors = derived values
  (flow from rating curve, volume from bathymetry).
- **HTTP 204**: Normal for sensors with no recent data. Treat as missing, not error.
- **Max limit**: 500 observations per request. Implement pagination for long windows.

### AEMET OpenData
- **Base URL**: `https://opendata.aemet.es/opendata/api` (NOT `openapi`)
- **Auth**: API key required. Send as BOTH query param (`?api_key=KEY`) AND header (`api_key: KEY`).
  Sending only as header causes silent server disconnect.
- **Two-step response**: Step 1 returns `{"datos": "<url>", "estado": 200}`.
  Step 2 GETs the `datos` URL (pre-signed CDN, no auth needed, use 60s timeout).
- **Rate limit**: ~50 req/min on free tier. `RateLimiter(calls_per_minute=45)` is in place.
- **Station inventory**: `GET /valores/climatologicos/inventarioestaciones/todasestaciones`
- **Observation endpoint**: `GET /observacion/convencional/datos/estacion/{idema}`
  with `fechaIniStr` / `fechaFinStr` params (format: `YYYY-MM-DDTHH:MM:SSUTC`).
- **Response**: Daily summaries. Key field `fint` or `fecha` for timestamp. Spanish field names
  mapped in `AEMET_FIELD_MAP` in `aemet.py`.

## Local development setup

```bash
# 1. Clone
git clone https://github.com/YOUR_ORG/llobregat-twin.git
cd llobregat-twin

# 2. Install dependencies
pip install -r requirements.txt

# 3. Fix NumPy 2.x incompatibility (Anaconda users)
pip install --upgrade xarray        # fixes plotly.express import error
# or: conda install "numpy<2"       # pins all packages to NumPy 1.x

# 4. Add AEMET key
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit .streamlit/secrets.toml → set AEMET_API_KEY

# 5. Fetch data
python -m data.fetchers.refresh_all

# 6. Run app
streamlit run app/app.py
```

## Discovering station IDs

If station IDs need to be re-verified or new stations added:

```bash
# Discovers all ACA gauges, reservoirs in watershed bbox + all AEMET stations
# Reads AEMET key from .streamlit/secrets.toml automatically
python -m data.fetchers.discover_stations
# Outputs: data/static/discovered_aca_gauges.csv
#          data/static/discovered_aca_reservoirs.csv
#          data/static/discovered_aemet_stations.csv
```

Then update `config/station_metadata.yaml` with real IDs.

## Deployment checklist

- [ ] GitHub repo has `AEMET_API_KEY` Actions secret set
- [ ] Streamlit Cloud app connected to repo, `app/app.py` as entry point
- [ ] Streamlit Cloud secret `AEMET_API_KEY` set in app settings
- [ ] First manual refresh triggered to populate cache before go-live
- [ ] `.github/workflows/refresh_data.yml` cron is active (requires Actions secret)
