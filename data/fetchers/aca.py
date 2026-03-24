"""
data/fetchers/aca.py

ACA (Agència Catalana de l'Aigua) SDIM API client.

Fetches:
  - River gauge data (flow + stage) for the Llobregat watershed
  - Reservoir storage levels (La Baells, Sant Ponç)
  - Piezometric levels (Baix Llobregat aquifer)

All returned DataFrames conform to the schemas in docs/DATA_SCHEMA.md.
Missing / unavailable data is represented as np.nan, never 0 or -9999.

ACA API notes:
  - The SDIM REST API is at https://sdim.aca.gencat.cat/sdim2/
  - Station catalogue: GET /sdim2/registry/estacions
  - Time series: GET /sdim2/series?codiEstacio=E003&codiVariable=1&dataInici=...&dataFi=...
  - Variable codes (provisional — verify against catalogue):
      1 = river flow (m3/s)
      2 = water level / stage (m)
      5 = reservoir volume (hm3)
      6 = reservoir level (m)
      9 = piezometric level (m a.s.l.)
  - HTTP 204 = no data for this station/period (not an error)
  - Authentication: some endpoints may require a token; check developer portal.
    Pass via Authorization header if needed.

IMPORTANT: Station IDs in config/station_metadata.yaml are PLACEHOLDERS.
On first run, call fetch_aca_station_catalogue() to get real codes.
"""
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import httpx
import numpy as np
import pandas as pd

from data.fetchers.utils import with_retry, RateLimiter, make_client, utc_now

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

CACHE_DIR = Path(__file__).parent.parent / "cache"
ACA_BASE_URL = "https://sdim.aca.gencat.cat/sdim2"

# ACA variable codes — verify these against the actual SDIM catalogue
ACA_VAR_FLOW = "1"        # River flow (m3/s)
ACA_VAR_STAGE = "2"       # Water level / stage (m)
ACA_VAR_RES_VOLUME = "5"  # Reservoir volume (hm3)
ACA_VAR_RES_LEVEL = "6"   # Reservoir level (m a.s.l.)
ACA_VAR_PIEZO = "9"       # Piezometric level (m a.s.l.)

# Watershed bounding box for filtering the station catalogue
WATERSHED_BBOX = {
    "lon_min": 1.5, "lon_max": 2.3,
    "lat_min": 41.2, "lat_max": 42.2,
}

_rate_limiter = RateLimiter(calls_per_minute=30)


# ── Empty DataFrame factories (schema enforcement) ────────────────────────────

def _empty_gauge_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "timestamp", "station_id", "station_name",
        "flow_m3s", "level_m", "quality_flag",
    ]).astype({
        "timestamp": "object",
        "station_id": "str",
        "station_name": "str",
        "flow_m3s": "float64",
        "level_m": "float64",
        "quality_flag": "str",
    })


def _empty_reservoir_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "timestamp", "reservoir_id", "reservoir_name",
        "volume_hm3", "level_m", "capacity_hm3", "pct_capacity",
    ]).astype({
        "timestamp": "object",
        "reservoir_id": "str",
        "reservoir_name": "str",
        "volume_hm3": "float64",
        "level_m": "float64",
        "capacity_hm3": "float64",
        "pct_capacity": "float64",
    })


def _empty_piezo_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "timestamp", "station_id", "station_name",
        "depth_m", "level_masl", "aquifer_unit",
    ]).astype({
        "timestamp": "object",
        "station_id": "str",
        "station_name": "str",
        "depth_m": "float64",
        "level_masl": "float64",
        "aquifer_unit": "str",
    })


# ── Cache helpers ──────────────────────────────────────────────────────────────

def _cache_path(prefix: str, entity_id: str) -> Path:
    CACHE_DIR.mkdir(exist_ok=True)
    date_str = utc_now().strftime("%Y%m%d")
    return CACHE_DIR / f"{prefix}_{entity_id}_{date_str}.parquet"


def cache_to_parquet(df: pd.DataFrame, prefix: str, entity_id: str) -> Path:
    """Write DataFrame to cache as Parquet. Returns the path written."""
    path = _cache_path(prefix, entity_id)
    df.to_parquet(path, index=False)
    logger.info(f"Cached {len(df)} rows → {path.name}")
    return path


# ── Raw API helpers ────────────────────────────────────────────────────────────

@with_retry(max_attempts=3, backoff_base=2.0)
def _get_json(
    client: httpx.Client,
    path: str,
    params: Optional[dict] = None,
    api_key: Optional[str] = None,
) -> Optional[list | dict]:
    """
    Make a GET request to the ACA SDIM API.
    Returns parsed JSON or None if HTTP 204 (no content).
    Raises httpx.HTTPStatusError for other non-2xx responses.
    """
    _rate_limiter.wait()
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    url = f"{ACA_BASE_URL}/{path.lstrip('/')}"
    logger.debug(f"GET {url} params={params}")
    response = client.get(url, params=params, headers=headers)

    if response.status_code == 204:
        logger.info(f"HTTP 204 (no content) for {url} — treating as missing data.")
        return None

    response.raise_for_status()
    return response.json()


# ── Public API functions ───────────────────────────────────────────────────────

def fetch_aca_station_catalogue(
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch the complete ACA station catalogue and filter to watershed bbox.

    Returns a DataFrame with columns:
        station_id, station_name, station_type, lat, lon, variables
    """
    with make_client() as client:
        data = _get_json(client, "/registry/estacions", api_key=api_key)

    if data is None:
        logger.warning("ACA station catalogue returned no content.")
        return pd.DataFrame()

    rows = []
    for s in data if isinstance(data, list) else data.get("estacions", []):
        try:
            lat = float(s.get("latitud") or s.get("lat") or 0)
            lon = float(s.get("longitud") or s.get("lon") or 0)
        except (TypeError, ValueError):
            continue
        if not (
            WATERSHED_BBOX["lat_min"] <= lat <= WATERSHED_BBOX["lat_max"]
            and WATERSHED_BBOX["lon_min"] <= lon <= WATERSHED_BBOX["lon_max"]
        ):
            continue
        rows.append({
            "station_id": str(s.get("codi") or s.get("codiEstacio") or ""),
            "station_name": str(s.get("nom") or s.get("nomEstacio") or ""),
            "station_type": str(s.get("tipus") or ""),
            "lat": lat,
            "lon": lon,
        })

    df = pd.DataFrame(rows)
    logger.info(f"Found {len(df)} ACA stations within watershed bbox.")
    return df


def fetch_aca_gauge_data(
    station_id: str,
    station_name: str = "",
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch river gauge data (flow + stage) from ACA SDIM.

    Args:
        station_id:   ACA station code (e.g. 'E003')
        station_name: Human-readable name (used in output DataFrame)
        start_dt:     UTC datetime for start of window (default: 7 days ago)
        end_dt:       UTC datetime for end of window (default: now)
        api_key:      ACA API token (optional; from env or Streamlit secrets)

    Returns:
        DataFrame conforming to DATA_SCHEMA.md gauge flows schema,
        or empty DataFrame with correct columns if data unavailable.
    """
    now = utc_now()
    start_dt = start_dt or (now - timedelta(days=7))
    end_dt = end_dt or now

    date_fmt = "%Y-%m-%dT%H:%M:%S"
    params_base = {
        "codiEstacio": station_id,
        "dataInici": start_dt.strftime(date_fmt),
        "dataFi": end_dt.strftime(date_fmt),
    }

    flow_rows: list[dict] = []
    level_rows: list[dict] = []

    with make_client() as client:
        # Fetch flow
        for var_code, target in [(ACA_VAR_FLOW, flow_rows), (ACA_VAR_STAGE, level_rows)]:
            try:
                data = _get_json(
                    client, "/series",
                    params={**params_base, "codiVariable": var_code},
                    api_key=api_key,
                )
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"ACA gauge fetch failed for station {station_id} "
                    f"variable {var_code}: HTTP {e.response.status_code}"
                )
                data = None

            if data is None:
                continue

            # Parse response — structure may vary; handle both list and dict
            records = data if isinstance(data, list) else data.get("serie", data.get("registres", []))
            for rec in records:
                ts_raw = rec.get("data") or rec.get("timestamp") or rec.get("DataLectura")
                val_raw = rec.get("valor") or rec.get("value") or rec.get("Valor")
                if ts_raw is None:
                    continue
                try:
                    ts = pd.to_datetime(ts_raw, utc=True)
                    val = float(val_raw) if val_raw is not None else np.nan
                except (ValueError, TypeError):
                    continue
                target.append({"timestamp": ts, "value": val, "quality_flag": str(rec.get("codiQualitat") or "unknown")})

    # Merge flow and level into a single DataFrame
    df_flow = pd.DataFrame(flow_rows).rename(columns={"value": "flow_m3s"}) if flow_rows else pd.DataFrame()
    df_level = pd.DataFrame(level_rows).rename(columns={"value": "level_m"}) if level_rows else pd.DataFrame()

    if df_flow.empty and df_level.empty:
        logger.warning(f"No gauge data returned for station {station_id}.")
        return _empty_gauge_df()

    if df_flow.empty:
        merged = df_level.copy()
        merged["flow_m3s"] = np.nan
    elif df_level.empty:
        merged = df_flow.copy()
        merged["level_m"] = np.nan
    else:
        # Merge on timestamp; keep quality_flag from flow series
        merged = pd.merge(
            df_flow[["timestamp", "flow_m3s", "quality_flag"]],
            df_level[["timestamp", "level_m"]],
            on="timestamp", how="outer",
        )

    merged["station_id"] = station_id
    merged["station_name"] = station_name or station_id
    merged = merged[["timestamp", "station_id", "station_name", "flow_m3s", "level_m", "quality_flag"]]
    merged = merged.sort_values("timestamp").reset_index(drop=True)

    logger.info(
        f"Fetched {len(merged)} rows for gauge {station_id} "
        f"({start_dt.date()} → {end_dt.date()})"
    )
    return merged


def fetch_aca_reservoir_data(
    reservoir_id: str,
    reservoir_name: str = "",
    capacity_hm3: float = np.nan,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch reservoir storage data from ACA SDIM.

    Returns DataFrame conforming to DATA_SCHEMA.md reservoir levels schema.
    """
    now = utc_now()
    start_dt = start_dt or (now - timedelta(days=30))
    end_dt = end_dt or now

    date_fmt = "%Y-%m-%dT%H:%M:%S"
    params_base = {
        "codiEstacio": reservoir_id,
        "dataInici": start_dt.strftime(date_fmt),
        "dataFi": end_dt.strftime(date_fmt),
    }

    vol_rows: list[dict] = []
    level_rows: list[dict] = []

    with make_client() as client:
        for var_code, target in [(ACA_VAR_RES_VOLUME, vol_rows), (ACA_VAR_RES_LEVEL, level_rows)]:
            try:
                data = _get_json(
                    client, "/series",
                    params={**params_base, "codiVariable": var_code},
                    api_key=api_key,
                )
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"ACA reservoir fetch failed for {reservoir_id} variable {var_code}: "
                    f"HTTP {e.response.status_code}"
                )
                data = None

            if data is None:
                continue

            records = data if isinstance(data, list) else data.get("serie", data.get("registres", []))
            for rec in records:
                ts_raw = rec.get("data") or rec.get("timestamp") or rec.get("DataLectura")
                val_raw = rec.get("valor") or rec.get("value") or rec.get("Valor")
                if ts_raw is None:
                    continue
                try:
                    ts = pd.to_datetime(ts_raw, utc=True)
                    val = float(val_raw) if val_raw is not None else np.nan
                except (ValueError, TypeError):
                    continue
                target.append({"timestamp": ts, "value": val})

    df_vol = pd.DataFrame(vol_rows).rename(columns={"value": "volume_hm3"}) if vol_rows else pd.DataFrame()
    df_lev = pd.DataFrame(level_rows).rename(columns={"value": "level_m"}) if level_rows else pd.DataFrame()

    if df_vol.empty and df_lev.empty:
        logger.warning(f"No reservoir data returned for {reservoir_id}.")
        return _empty_reservoir_df()

    if df_vol.empty:
        merged = df_lev.copy()
        merged["volume_hm3"] = np.nan
    elif df_lev.empty:
        merged = df_vol.copy()
        merged["level_m"] = np.nan
    else:
        merged = pd.merge(df_vol, df_lev, on="timestamp", how="outer")

    merged["reservoir_id"] = reservoir_id
    merged["reservoir_name"] = reservoir_name or reservoir_id
    merged["capacity_hm3"] = capacity_hm3
    merged["pct_capacity"] = (merged["volume_hm3"] / capacity_hm3 * 100).where(
        pd.notna(merged["volume_hm3"]) & pd.notna(merged.get("capacity_hm3")),
        other=np.nan,
    ) if not np.isnan(capacity_hm3) else np.nan

    merged = merged[
        ["timestamp", "reservoir_id", "reservoir_name",
         "volume_hm3", "level_m", "capacity_hm3", "pct_capacity"]
    ].sort_values("timestamp").reset_index(drop=True)

    logger.info(f"Fetched {len(merged)} rows for reservoir {reservoir_id}.")
    return merged


def fetch_aca_piezo_data(
    station_id: str,
    station_name: str = "",
    aquifer_unit: str = "Baix Llobregat",
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch piezometric level data from ACA SDIM.

    Note: HTTP 204 (no content) is common for piezometric stations.
    Returns empty DataFrame with correct schema — never raises on missing data.

    Returns DataFrame conforming to DATA_SCHEMA.md piezometric schema.
    """
    now = utc_now()
    start_dt = start_dt or (now - timedelta(days=90))  # wider window for patchy data
    end_dt = end_dt or now

    date_fmt = "%Y-%m-%dT%H:%M:%S"
    params = {
        "codiEstacio": station_id,
        "codiVariable": ACA_VAR_PIEZO,
        "dataInici": start_dt.strftime(date_fmt),
        "dataFi": end_dt.strftime(date_fmt),
    }

    with make_client() as client:
        try:
            data = _get_json(client, "/series", params=params, api_key=api_key)
        except httpx.HTTPStatusError as e:
            logger.error(
                f"ACA piezo fetch failed for {station_id}: HTTP {e.response.status_code}"
            )
            return _empty_piezo_df()

    if data is None:
        logger.info(f"No piezometric data for station {station_id} (HTTP 204 or empty).")
        return _empty_piezo_df()

    rows = []
    records = data if isinstance(data, list) else data.get("serie", data.get("registres", []))
    for rec in records:
        ts_raw = rec.get("data") or rec.get("timestamp") or rec.get("DataLectura")
        val_raw = rec.get("valor") or rec.get("value") or rec.get("Valor")
        if ts_raw is None:
            continue
        try:
            ts = pd.to_datetime(ts_raw, utc=True)
            level = float(val_raw) if val_raw is not None else np.nan
        except (ValueError, TypeError):
            continue
        rows.append({"timestamp": ts, "level_masl": level})

    if not rows:
        return _empty_piezo_df()

    df = pd.DataFrame(rows)
    df["station_id"] = station_id
    df["station_name"] = station_name or station_id
    df["depth_m"] = np.nan  # Depth requires ground elevation; populate if available
    df["aquifer_unit"] = aquifer_unit
    df = df[["timestamp", "station_id", "station_name", "depth_m", "level_masl", "aquifer_unit"]]
    df = df.sort_values("timestamp").reset_index(drop=True)

    logger.info(f"Fetched {len(df)} piezometric rows for station {station_id}.")
    return df
