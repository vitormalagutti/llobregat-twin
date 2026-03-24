# Data Schema — Llobregat Digital Twin

All data passed between the fetching layer and the presentation layer must conform
to these schemas. Fetchers write parquet files. Streamlit pages read parquet files.
Neither layer modifies the other's files.

## General conventions

- All timestamps: UTC, stored as `datetime64[ns, UTC]` in pandas
- All numeric values: float64 (allows NaN for missing)
- Missing data: np.nan — never 0, never -9999 (document exceptions below)
- File naming: `{dataset_type}_{station_id}_{YYYYMMDD}.parquet`
  e.g. `flow_E003_20250324.parquet`

## Schema: gauge flows (ACA)

File prefix: `flow_`

| Column | Type | Unit | Notes |
|--------|------|------|-------|
| timestamp | datetime64[ns, UTC] | — | Observation time |
| station_id | str | — | ACA station code |
| station_name | str | — | Human-readable name |
| flow_m3s | float64 | m3/s | Instantaneous flow |
| level_m | float64 | m | Water surface elevation |
| quality_flag | str | — | ACA quality code if available, else 'unknown' |

## Schema: reservoir levels (ACA)

File prefix: `reservoir_`

| Column | Type | Unit | Notes |
|--------|------|------|-------|
| timestamp | datetime64[ns, UTC] | — | |
| reservoir_id | str | — | ACA reservoir code |
| reservoir_name | str | — | |
| volume_hm3 | float64 | hm3 | Stored volume |
| level_m | float64 | m | Water surface elevation (m a.s.l.) |
| capacity_hm3 | float64 | hm3 | Maximum capacity (static, from metadata) |
| pct_capacity | float64 | % | volume_hm3 / capacity_hm3 × 100 |

## Schema: meteorological observations (AEMET)

File prefix: `meteo_`

| Column | Type | Unit | Notes |
|--------|------|------|-------|
| timestamp | datetime64[ns, UTC] | — | |
| station_id | str | — | AEMET idema code |
| station_name | str | — | |
| precip_mm | float64 | mm | Accumulated precipitation (period in metadata) |
| temp_c | float64 | °C | Air temperature |
| temp_max_c | float64 | °C | Max temp in period |
| temp_min_c | float64 | °C | Min temp in period |
| wind_speed_ms | float64 | m/s | Mean wind speed |
| wind_dir_deg | float64 | degrees | Wind direction (0=N, 90=E) |
| humidity_pct | float64 | % | Relative humidity |

## Schema: piezometric levels (ACA)

File prefix: `piezo_`

| Column | Type | Unit | Notes |
|--------|------|------|-------|
| timestamp | datetime64[ns, UTC] | — | |
| station_id | str | — | ACA piezometer code |
| station_name | str | — | |
| depth_m | float64 | m | Depth to water table from surface |
| level_masl | float64 | m a.s.l. | Absolute piezometric level |
| aquifer_unit | str | — | Hydrogeological unit name |

## Known exceptions to NaN convention

_None documented yet. Add here if a source API uses sentinel values._
