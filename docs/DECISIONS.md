# Architectural Decisions — Llobregat Digital Twin

This file records every significant technical decision made during development,
with rationale. Update this file before implementing any architectural change.

## Decision log

| Date | ID | Decision | Rationale | Status |
|------|----|----------|-----------|--------|
| 2026-03-24 | D001 | Parquet over SQLite for cache | Columnar format, zero-config, excellent pandas integration, git-friendly (binary but diffable via schema), no server process required | Accepted |
| 2026-03-24 | D002 | DuckDB as fallback if queries grow complex | DuckDB can query multiple parquet files with SQL without loading into memory — good escape hatch if cross-station aggregations become complex | Accepted |
| 2026-03-24 | D003 | Streamlit Cloud for deployment | Free tier, native GitHub integration, automatic redeployment on push, no infra to manage for Phase 1-2 | Accepted |
| 2026-03-24 | D004 | GitHub Actions cron for data refresh | Serverless, free, version-controlled, co-located with source — avoids need for a separate scheduler service | Accepted |
| 2026-03-24 | D005 | Folium via streamlit-folium for maps | Full GeoJSON overlay support, Leaflet.js underneath, mature Python bindings, better spatial control than st.map() | Accepted |
| 2026-03-24 | D006 | httpx for API calls | Modern HTTP client, async-native, better timeout/retry ergonomics than requests | Accepted |
| 2026-03-24 | D007 | Do not label product as "digital twin" until Phase 3 | A monitoring dashboard without a rainfall-runoff simulation model is not a digital twin. Honest labelling maintains credibility. | Accepted |
| 2026-03-25 | D008 | ACA uses Sentilo platform at `aplicacions.aca.gencat.cat/sdim2/apirest` | Discovered via live API exploration. The original guessed URL (`sdim.aca.gencat.cat/sdim2/series`) was wrong. Sentilo is an open-source IoT platform used by multiple Catalan agencies. Sensor IDs verified via `/catalog?componentType=aforament` and `/catalog?componentType=embassament`. | Accepted |
| 2026-03-25 | D009 | AEMET API key sent as query param `?api_key=KEY` in addition to header | AEMET's OpenData API silently dropped requests with the key only in the header, returning `RemoteProtocolError: Server disconnected`. Adding the key as a query param resolved the issue. Both delivery methods are now used for maximum compatibility. Base URL confirmed as `opendata.aemet.es` (not `openapi`). | Accepted |
| 2026-03-25 | D010 | `refresh_all.py` falls back to reading AEMET key from `.streamlit/secrets.toml` | When running locally, the `AEMET_API_KEY` env var is not set. Rather than requiring the user to export it manually, the script reads from `secrets.toml` (which is gitignored). GitHub Actions continues to use the env var injected from the Actions secret. | Accepted |

## Template for new decisions

```
| YYYY-MM-DD | DXXX | [Short title] | [1-3 sentence rationale including alternatives considered] | Proposed / Accepted / Superseded |
```

## Superseded decisions

| Date | ID | Original decision | Superseded by | Reason |
|------|----|-------------------|---------------|--------|
| 2026-03-25 | — | ACA SDIM base URL: `sdim.aca.gencat.cat/sdim2` | D008 | Wrong endpoint. ACA uses Sentilo platform at `aplicacions.aca.gencat.cat/sdim2/apirest`. |
| 2026-03-25 | — | ACA sensor path: `{component_id}.cabal` / `{component_id}.nivell` | D008 | Sensor IDs are not derived from component IDs. Real IDs (e.g. `CALC001304`, `081445-001-ANA002`) discovered from live catalogue. |
| 2026-03-25 | — | ACA obs key: `obs.get("time")` for timestamp string | D008 | `"time"` is epoch milliseconds (int). The string timestamp is under key `"timestamp"` (format: `DD/MM/YYYYTHH:MM:SS`, local Madrid time). |
