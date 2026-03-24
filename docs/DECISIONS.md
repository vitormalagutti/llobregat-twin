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
| 2026-03-24 | D006 | httpx for async API calls | Modern HTTP client, async-native, better than requests for concurrent fetches in the refresh script | Accepted |
| 2026-03-24 | D007 | Do not label product as "digital twin" until Phase 3 | A monitoring dashboard without a rainfall-runoff simulation model is not a digital twin. Honest labelling maintains credibility. | Accepted |

## Template for new decisions

```
| YYYY-MM-DD | DXXX | [Short title] | [1-3 sentence rationale including alternatives considered] | Proposed / Accepted / Superseded |
```

## Superseded decisions

_None yet._
