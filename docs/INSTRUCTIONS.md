# Instructions for the Llobregat Digital Twin Agent

## Project mission
Build a live watershed monitoring dashboard for the Llobregat River (Catalonia, Spain),
pulling data from ACA (Agència Catalana de l'Aigua) and AEMET (Agencia Estatal de
Meteorología) APIs, displayed in a Streamlit app deployed on Streamlit Cloud.

## Operating rules

1. Read PROJECT_MEMORY.md and DATA_SCHEMA.md before every session.
2. Update PROJECT_MEMORY.md at the end of every session.
3. Commit to git after every meaningful milestone (working fetcher, new page, bug fix).
4. Never hardcode API keys. Use environment variables or Streamlit secrets.
5. Never overwrite working code without first reading it and understanding it.
6. Handle missing data gracefully. ACA piezometric data is patchy — always degrade
   gracefully with a visible "data unavailable" state rather than crashing.
7. Write tests for every fetcher function before considering it done.
8. Flag to the human before: changing the data schema, adding new dependencies,
   changing deployment configuration, or making any architectural decision not
   already covered by these instructions.

## Scope per layer

- Data layer (fetchers, cache, scheduler): owns API clients, parquet cache, GitHub Actions.
  Does NOT write Streamlit UI code.
- Presentation layer (Streamlit pages): reads from parquet cache only.
  Does NOT call APIs directly. Does NOT modify the data schema.
- Config layer (thresholds, station metadata): human-editable YAML.
  Agents may read and suggest changes but must flag before modifying.

## Coding conventions

- Python: follow PEP 8, use type hints on all function signatures
- Function names: snake_case, descriptive (fetch_aca_gauge_data, not get_data)
- All timestamps: UTC internally, convert to Europe/Madrid for display only
- Units: SI throughout internally (m3/s for flow, m for levels, mm for precip)
  Display with unit labels. Never mix unit systems silently.
- Missing data: represent as np.nan in DataFrames, never as 0 or -9999 unless
  the source API uses that convention (document it in DATA_SCHEMA.md if so)
- Logging: use Python logging module, not print statements, in fetchers

## Known API behaviours

- ACA SDIM base URL: https://aca.gencat.cat/ca/laaca/xarxes-de-control/
  (verify current endpoint — may require checking their developer portal)
- AEMET OpenData base URL: https://opendata.aemet.es/openapi/api
  Requires free API key registered at: https://opendata.aemet.es/centrodedescargas/inicio
- ACA piezometric endpoint sometimes returns HTTP 204 (no content) for stations
  with no recent readings. Treat as missing data, not an error.
- AEMET rate limits: ~50 requests/minute on free tier. Implement backoff.
