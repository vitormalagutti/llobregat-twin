"""
data/fetchers/aca.py

ACA (Agència Catalana de l'Aigua) — Sentilo platform API client.

The ACA publishes real-time hydrological data via a public Sentilo instance:
    Base URL:  https://aplicacions.aca.gencat.cat/sdim2/apirest
    Auth:      IDENTITY_KEY header (public read access — no key needed for open data)

Sentilo REST patterns used here:
    Catalog:   GET /catalog?componentType={type}
    Latest:    GET /data/{provider}/{sensor}
    History:   GET /data/{provider}/{sensor}?from={ts}&to={ts}&limit={n}
               Timestamps: DD/MM/YYYYTHH:MM:SS  (local time, Europe/Madrid)

Component types relevant to us:
    aforament   — river gauge stations (flow + stage)
    embassament — reservoirs (level + volume)
    piezometre  — piezometric stations (may exist)

Sensor naming convention (observed, verify via discover_stations.py):
    Within a gauge component the sensors are typically named:
        cabal    — flow (m3/s)
        nivell   — water level (m)
    Within a reservoir component:
        volum    — stored volume (hm3)
        cota     — water surface elevation (m a.s.l.)

IMPORTANT: Component and sensor IDs must be verified via discover_stations.py
before first use. The IDs in config/station_metadata.yaml start as placeholders.

All returned DataFrames conform to docs/DATA_SCHEMA.md.
"""
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
import numpy as np
import pandas as pd

from data.fetchers.utils import with_retry, RateLimiter, make_client, utc_now

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

CACHE_DIR = Path(__file__).parent.parent / "cache"
ACA_BASE_URL = "https://aplicacions.aca.gencat.cat/sdim2/apirest"
TZ_MADRID = ZoneInfo("Europe/Madrid")

# Sentilo timestamp format (local time, not UTC)
SENTILO_TS_FMT = "%d/%m/%YT%H:%M:%S"

# Default observation window for gauge/reservoir data
DEFAULT_DAYS_GAUGE = 7
DEFAULT_DAYS_RESERVOIR = 30

# Max observations per request (Sentilo default limit is 500)
SENTILO_MAX_LIMIT = 500

# Watershed bounding box
WATERSHED_BBOX = {
    "lon_min": 1.5, "lon_max": 2.3,
    "lat_min": 41.2, "lat_max": 42.2,
}

_rate_limiter = RateLimiter(calls_per_minute=30)


# ── Sentilo timestamp helpers ──────────────────────────────────────────────────

def _to_sentilo_ts(dt: datetime) -> str:
    """Convert UTC datetime to Sentilo timestamp string (Europe/Madrid local time)."""
    local_dt = dt.astimezone(TZ_MADRID)
    return local_dt.strftime(SENTILO_TS_FMT)


def _from_sentilo_ts(ts_str: str) -> pd.Timestamp:
    """Parse Sentilo timestamp string (local Madrid time) → UTC Timestamp."""
    local_dt = datetime.strptime(ts_str, SENTILO_TS_FMT).replace(tzinfo=TZ_MADRID)
    return pd.Timestamp(local_dt).tz_convert("UTC")


# ── Empty DataFrame factories ─────────────────────────────────────────────────

def _empty_gauge_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "timestamp", "station_id", "station_name",
        "flow_m3s", "level_m", "quality_flag",
    ]).astype({"flow_m3s": "float64", "level_m": "float64"})


def _empty_reservoir_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "timestamp", "reservoir_id", "reservoir_name",
        "volume_hm3", "level_m", "capacity_hm3", "pct_capacity",
    ]).astype({
        "volume_hm3": "float64", "level_m": "float64",
        "capacity_hm3": "float64", "pct_capacity": "float64",
    })


def _empty_piezo_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "timestamp", "station_id", "station_name",
        "depth_m", "level_masl", "aquifer_unit",
    ]).astype({"depth_m": "float64", "level_masl": "float64"})


# ── Cache helpers ──────────────────────────────────────────────────────────────

def _cache_path(prefix: str, entity_id: str) -> Path:
    CACHE_DIR.mkdir(exist_ok=True)
    date_str = utc_now().strftime("%Y%m%d")
    # Sanitise entity_id for filename (dots → underscores)
    safe_id = entity_id.replace(".", "_").replace("/", "_")
    return CACHE_DIR / f"{prefix}_{safe_id}_{date_str}.parquet"


def cache_to_parquet(df: pd.DataFrame, prefix: str, entity_id: str) -> Path:
    """Write DataFrame to Parquet cache. Returns path written."""
    path = _cache_path(prefix, entity_id)
    df.to_parquet(path, index=False)
    logger.info(f"Cached {len(df)} rows → {path.name}")
    return path


# ── Low-level Sentilo request helper ─────────────────────────────────────────

@with_retry(max_attempts=3, backoff_base=2.0)
def _sentilo_get(
    client: httpx.Client,
    path: str,
    params: Optional[dict] = None,
    identity_key: Optional[str] = None,
) -> Optional[dict]:
    """
    GET a Sentilo endpoint.
    Returns parsed JSON dict, or None if HTTP 204 / empty body.
    """
    _rate_limiter.wait()
    headers = {}
    if identity_key:
        headers["IDENTITY_KEY"] = identity_key

    url = f"{ACA_BASE_URL}/{path.lstrip('/')}"
    logger.debug(f"GET {url} params={params}")
    r = client.get(url, params=params, headers=headers)

    if r.status_code == 204 or not r.content:
        logger.info(f"HTTP 204 / empty for {url}")
        return None

    r.raise_for_status()
    return r.json()


def _parse_sentilo_observations(data: dict) -> list[dict]:
    """
    Extract observation records from a Sentilo GET /data/{provider}/{sensor} response.

    Sentilo may return observations in two formats:
      Format A (nested): {"sensors": [{"sensor": "...", "observations": [...]}]}
      Format B (flat):   {"observations": [...]}

    Each observation contains:
        "timestamp" — string "DD/MM/YYYYTHH:MM:SS" (Madrid local time)  ← preferred
        "time"      — Unix epoch milliseconds (int)                       ← fallback
        "value"     — numeric string e.g. "9.243"
    """
    def _parse_one(obs: dict) -> Optional[dict]:
        # ACA Sentilo uses "timestamp" (string) as primary key; "time" is epoch ms
        ts_raw = obs.get("timestamp") or obs.get("time")
        val_raw = obs.get("value")
        if ts_raw is None:
            return None
        try:
            if isinstance(ts_raw, (int, float)):
                # Epoch milliseconds → UTC Timestamp
                ts = pd.Timestamp(int(ts_raw), unit="ms", tz="UTC")
            else:
                ts = _from_sentilo_ts(str(ts_raw))
            val = float(val_raw) if val_raw not in (None, "", "null") else np.nan
        except (ValueError, TypeError):
            logger.debug(f"Could not parse obs: ts={ts_raw!r} value={val_raw!r}")
            return None
        return {"timestamp": ts, "value": val}

    rows = []

    # Format A: nested under sensors list
    for sensor_block in data.get("sensors", []):
        for obs in sensor_block.get("observations", []):
            row = _parse_one(obs)
            if row:
                rows.append(row)

    # Format B: flat observations list (actual ACA response format)
    if not rows:
        for obs in data.get("observations", []):
            row = _parse_one(obs)
            if row:
                rows.append(row)

    return rows


# ── Public: Station Catalogue ─────────────────────────────────────────────────

def fetch_aca_station_catalogue(
    component_type: str = "aforament",
    identity_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch ACA Sentilo component catalogue filtered by component type.

    Args:
        component_type: 'aforament' (gauges), 'embassament' (reservoirs),
                        'piezometre' (piezometers)
        identity_key:   Optional Sentilo token (not required for public read)

    Returns DataFrame with columns:
        provider, component_id, component_name, component_type,
        lat, lon, sensors (list of sensor names)
    """
    with make_client() as client:
        try:
            data = _sentilo_get(
                client, "/catalog",
                params={"componentType": component_type},
                identity_key=identity_key,
            )
        except httpx.HTTPStatusError as e:
            logger.error(f"Catalogue fetch failed: HTTP {e.response.status_code}")
            return pd.DataFrame()

    if data is None:
        return pd.DataFrame()

    rows = []
    # Sentilo catalog structure: {"providers": [{"provider": "...", "sensors": [...]}]}
    for prov in data.get("providers", []):
        provider_id = prov.get("provider", "")
        for sensor in prov.get("sensors", []):
            comp = sensor.get("component", "")
            comp_desc = sensor.get("componentDesc", "") or sensor.get("description", "")
            comp_type = sensor.get("componentType", "")
            loc = sensor.get("location", "")
            sensor_id = sensor.get("sensor", "")

            # Parse location "lat lon"
            lat, lon = np.nan, np.nan
            if loc:
                try:
                    parts = str(loc).split()
                    lat, lon = float(parts[0]), float(parts[1])
                except (IndexError, ValueError):
                    pass

            # Filter to watershed bbox
            if not np.isnan(lat) and not np.isnan(lon):
                if not (
                    WATERSHED_BBOX["lat_min"] <= lat <= WATERSHED_BBOX["lat_max"]
                    and WATERSHED_BBOX["lon_min"] <= lon <= WATERSHED_BBOX["lon_max"]
                ):
                    continue

            rows.append({
                "provider": provider_id,
                "component_id": comp,
                "component_name": comp_desc,
                "component_type": comp_type,
                "lat": lat,
                "lon": lon,
                "sensor_id": sensor_id,
            })

    df = pd.DataFrame(rows)
    logger.info(
        f"Found {len(df)} sensors in {len(df['component_id'].unique()) if len(df) else 0} "
        f"components of type '{component_type}' within watershed bbox."
    )
    return df


# ── Public: River Gauge Data ──────────────────────────────────────────────────

def fetch_aca_gauge_data(
    component_id: str,
    provider: str,
    station_name: str = "",
    flow_sensor: Optional[str] = None,
    level_sensor: Optional[str] = None,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    identity_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch river gauge data (flow + stage) for one component.

    Args:
        component_id:  Sentilo component ID (e.g. '081445-001')
        provider:      Sentilo provider ID (e.g. 'AFORAMENT-EST')
        station_name:  Human-readable name for output DataFrame
        flow_sensor:   Full Sentilo sensor ID for flow, e.g. 'CALC001304'
        level_sensor:  Full Sentilo sensor ID for stage, e.g. '081445-001-ANA002'
                       Pass None to skip fetching that variable.
        start_dt / end_dt: UTC datetime window (default: last 7 days)
        identity_key:  Sentilo IDENTITY_KEY header (not required for public read)

    Returns DataFrame conforming to DATA_SCHEMA.md gauge flows schema.
    """
    now = utc_now()
    start_dt = start_dt or (now - timedelta(days=DEFAULT_DAYS_GAUGE))
    end_dt = end_dt or now

    params = {
        "from": _to_sentilo_ts(start_dt),
        "to": _to_sentilo_ts(end_dt),
        "limit": SENTILO_MAX_LIMIT,
    }

    flow_rows: list[dict] = []
    level_rows: list[dict] = []

    sensors_to_fetch = []
    if flow_sensor:
        sensors_to_fetch.append((flow_sensor, flow_rows))
    if level_sensor:
        sensors_to_fetch.append((level_sensor, level_rows))

    if not sensors_to_fetch:
        logger.warning(f"No sensor IDs provided for gauge {component_id} — skipping.")
        return _empty_gauge_df()

    with make_client() as client:
        for sensor_id, target in sensors_to_fetch:
            # Sentilo data path: /data/{provider}/{sensorId}
            path = f"/data/{provider}/{sensor_id}"
            try:
                data = _sentilo_get(client, path, params=params, identity_key=identity_key)
            except httpx.HTTPStatusError as e:
                logger.warning(
                    f"Gauge sensor {sensor_id}: HTTP {e.response.status_code} — skipping."
                )
                continue

            if data is None:
                logger.info(f"No data for sensor {sensor_id}")
                continue

            parsed = _parse_sentilo_observations(data)
            target.extend(parsed)

    if not flow_rows and not level_rows:
        logger.warning(f"No gauge data for component {component_id}.")
        return _empty_gauge_df()

    df_flow = pd.DataFrame(flow_rows).rename(columns={"value": "flow_m3s"}) if flow_rows else pd.DataFrame()
    df_lev = pd.DataFrame(level_rows).rename(columns={"value": "level_m"}) if level_rows else pd.DataFrame()

    if df_flow.empty:
        merged = df_lev.copy()
        merged["flow_m3s"] = np.nan
    elif df_lev.empty:
        merged = df_flow.copy()
        merged["level_m"] = np.nan
    else:
        merged = pd.merge(
            df_flow[["timestamp", "flow_m3s"]],
            df_lev[["timestamp", "level_m"]],
            on="timestamp", how="outer",
        )

    merged["station_id"] = component_id
    merged["station_name"] = station_name or component_id
    merged["quality_flag"] = "unknown"
    merged = merged[["timestamp", "station_id", "station_name", "flow_m3s", "level_m", "quality_flag"]]
    merged = merged.sort_values("timestamp").reset_index(drop=True)

    logger.info(f"Fetched {len(merged)} gauge rows for {component_id}.")
    return merged


# ── Public: Reservoir Data ────────────────────────────────────────────────────

def fetch_aca_reservoir_data(
    component_id: str,
    provider: str,
    reservoir_name: str = "",
    capacity_hm3: float = np.nan,
    volume_sensor: Optional[str] = None,
    level_sensor: Optional[str] = None,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    identity_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch reservoir storage data for one component.

    Args:
        volume_sensor: Full Sentilo sensor ID for volume (hm3), e.g. 'CALC000697'
        level_sensor:  Full Sentilo sensor ID for water level, e.g. '082687-001-ANA015'

    Returns DataFrame conforming to DATA_SCHEMA.md reservoir schema.
    """
    now = utc_now()
    start_dt = start_dt or (now - timedelta(days=DEFAULT_DAYS_RESERVOIR))
    end_dt = end_dt or now

    params = {
        "from": _to_sentilo_ts(start_dt),
        "to": _to_sentilo_ts(end_dt),
        "limit": SENTILO_MAX_LIMIT,
    }

    vol_rows: list[dict] = []
    lev_rows: list[dict] = []

    sensors_to_fetch = []
    if volume_sensor:
        sensors_to_fetch.append((volume_sensor, vol_rows))
    if level_sensor:
        sensors_to_fetch.append((level_sensor, lev_rows))

    if not sensors_to_fetch:
        logger.warning(f"No sensor IDs for reservoir {component_id} — skipping.")
        return _empty_reservoir_df()

    with make_client() as client:
        for sensor_id, target in sensors_to_fetch:
            path = f"/data/{provider}/{sensor_id}"
            try:
                data = _sentilo_get(client, path, params=params, identity_key=identity_key)
            except httpx.HTTPStatusError as e:
                logger.warning(
                    f"Reservoir sensor {sensor_id}: HTTP {e.response.status_code} — skipping."
                )
                continue

            if data is None:
                continue

            target.extend(_parse_sentilo_observations(data))

    if not vol_rows and not lev_rows:
        logger.warning(f"No reservoir data for component {component_id}.")
        return _empty_reservoir_df()

    df_vol = pd.DataFrame(vol_rows).rename(columns={"value": "volume_hm3"}) if vol_rows else pd.DataFrame()
    df_lev = pd.DataFrame(lev_rows).rename(columns={"value": "level_m"}) if lev_rows else pd.DataFrame()

    if df_vol.empty:
        merged = df_lev.copy()
        merged["volume_hm3"] = np.nan
    elif df_lev.empty:
        merged = df_vol.copy()
        merged["level_m"] = np.nan
    else:
        merged = pd.merge(df_vol, df_lev, on="timestamp", how="outer")

    merged["reservoir_id"] = component_id
    merged["reservoir_name"] = reservoir_name or component_id
    merged["capacity_hm3"] = capacity_hm3

    if not np.isnan(capacity_hm3) and "volume_hm3" in merged.columns:
        merged["pct_capacity"] = (merged["volume_hm3"] / capacity_hm3 * 100).where(
            pd.notna(merged["volume_hm3"]), other=np.nan
        )
    else:
        merged["pct_capacity"] = np.nan

    merged = merged[
        ["timestamp", "reservoir_id", "reservoir_name",
         "volume_hm3", "level_m", "capacity_hm3", "pct_capacity"]
    ].sort_values("timestamp").reset_index(drop=True)

    logger.info(f"Fetched {len(merged)} reservoir rows for {component_id}.")
    return merged


# ── Public: Piezometric Data ──────────────────────────────────────────────────

def fetch_aca_piezo_data(
    component_id: str,
    provider: str,
    station_name: str = "",
    aquifer_unit: str = "Baix Llobregat",
    level_sensor: str = "nivell_piezometric",
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    identity_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch piezometric level data. HTTP 204 / empty response is normal — returns empty DataFrame.
    Returns DataFrame conforming to DATA_SCHEMA.md piezometric schema.
    """
    now = utc_now()
    start_dt = start_dt or (now - timedelta(days=90))
    end_dt = end_dt or now

    params = {
        "from": _to_sentilo_ts(start_dt),
        "to": _to_sentilo_ts(end_dt),
        "limit": SENTILO_MAX_LIMIT,
    }

    with make_client() as client:
        path = f"/data/{provider}/{component_id}.{level_sensor}"
        try:
            data = _sentilo_get(client, path, params=params, identity_key=identity_key)
        except httpx.HTTPStatusError as e:
            logger.error(f"Piezo fetch failed for {component_id}: HTTP {e.response.status_code}")
            return _empty_piezo_df()

    if data is None:
        logger.info(f"No piezometric data for {component_id} (HTTP 204 or empty — expected).")
        return _empty_piezo_df()

    rows = []
    for obs in (data.get("observations") or []):
        ts_raw = obs.get("time")
        val_raw = obs.get("value")
        if not ts_raw:
            continue
        try:
            ts = _from_sentilo_ts(ts_raw)
            val = float(val_raw) if val_raw not in (None, "", "null") else np.nan
            rows.append({"timestamp": ts, "level_masl": val})
        except (ValueError, TypeError):
            continue

    if not rows:
        return _empty_piezo_df()

    df = pd.DataFrame(rows)
    df["station_id"] = component_id
    df["station_name"] = station_name or component_id
    df["depth_m"] = np.nan
    df["aquifer_unit"] = aquifer_unit
    df = df[["timestamp", "station_id", "station_name", "depth_m", "level_masl", "aquifer_unit"]]
    df = df.sort_values("timestamp").reset_index(drop=True)
    logger.info(f"Fetched {len(df)} piezometric rows for {component_id}.")
    return df
