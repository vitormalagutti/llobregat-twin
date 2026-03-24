"""
data/fetchers/aemet.py

AEMET (Agencia Estatal de Meteorología) OpenData API client.

Fetches meteorological observations for stations within the Llobregat watershed.

API notes:
  - All requests require a free API key: https://opendata.aemet.es/centrodedescargas/inicio
  - AEMET uses a two-step response pattern:
      Step 1: GET endpoint → returns {"datos": "<url>", "estado": 200, ...}
      Step 2: GET the datos URL → returns the actual JSON data array
  - Rate limit: ~50 requests/minute on free tier
  - Station inventory: GET /api/valores/climatologicos/inventarioestaciones/todasestaciones
  - Hourly obs: GET /api/observacion/convencional/datos/estacion/{idema}
  - Date range obs: use fechaIniStr / fechaFinStr query params (format: YYYY-MM-DDTHH:MM:SSUTC)

All returned DataFrames conform to DATA_SCHEMA.md meteorological schema.
"""
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import httpx
import numpy as np
import pandas as pd

from data.fetchers.utils import (
    with_retry, RateLimiter, make_client, utc_now, format_aemet_datetime
)

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

CACHE_DIR = Path(__file__).parent.parent / "cache"
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"

# Watershed bounding box for filtering the station catalogue
WATERSHED_BBOX = {
    "lon_min": 1.5, "lon_max": 2.3,
    "lat_min": 41.2, "lat_max": 42.2,
}

_rate_limiter = RateLimiter(calls_per_minute=45)  # conservative: limit is ~50


# ── AEMET field mappings ───────────────────────────────────────────────────────
# AEMET returns fields with Spanish abbreviations. Map to our schema column names.
AEMET_FIELD_MAP = {
    "prec": "precip_mm",
    "ta": "temp_c",
    "tamax": "temp_max_c",
    "tamin": "temp_min_c",
    "vv": "wind_speed_ms",
    "dv": "wind_dir_deg",
    "hr": "humidity_pct",
    "fhta": None,       # time of max temp — ignore
    "fhTamax": None,
    "fhTamin": None,
}


# ── Empty DataFrame factory ────────────────────────────────────────────────────

def _empty_meteo_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "timestamp", "station_id", "station_name",
        "precip_mm", "temp_c", "temp_max_c", "temp_min_c",
        "wind_speed_ms", "wind_dir_deg", "humidity_pct",
    ]).astype({col: "float64" for col in [
        "precip_mm", "temp_c", "temp_max_c", "temp_min_c",
        "wind_speed_ms", "wind_dir_deg", "humidity_pct",
    ]})


# ── Cache helper ───────────────────────────────────────────────────────────────

def cache_to_parquet(df: pd.DataFrame, station_id: str) -> Path:
    """Write meteo DataFrame to cache as Parquet. Returns path written."""
    CACHE_DIR.mkdir(exist_ok=True)
    date_str = utc_now().strftime("%Y%m%d")
    path = CACHE_DIR / f"meteo_{station_id}_{date_str}.parquet"
    df.to_parquet(path, index=False)
    logger.info(f"Cached {len(df)} meteo rows → {path.name}")
    return path


# ── AEMET two-step request helper ─────────────────────────────────────────────

@with_retry(max_attempts=3, backoff_base=2.0)
def _aemet_get(
    client: httpx.Client,
    path: str,
    api_key: str,
    params: Optional[dict] = None,
) -> Optional[list]:
    """
    Execute a two-step AEMET API request.

    Step 1: GET the endpoint → response contains {"datos": "<url>"}
    Step 2: GET the datos URL → actual data list

    Returns list of observation dicts, or None if no data.
    """
    _rate_limiter.wait()
    # AEMET requires the key as a query parameter AND accepts it as a header.
    # Sending both maximises compatibility across API versions.
    all_params = {**(params or {}), "api_key": api_key}
    headers = {"api_key": api_key}
    url = f"{AEMET_BASE_URL}/{path.lstrip('/')}"
    logger.debug(f"AEMET step-1 GET {url}")

    r1 = client.get(url, params=all_params, headers=headers)
    r1.raise_for_status()
    meta = r1.json()

    estado = meta.get("estado")
    if estado == 404:
        logger.info(f"AEMET: no data for {url} (estado 404).")
        return None
    if estado not in (200, None):
        logger.warning(f"AEMET: unexpected estado={estado} for {url}.")
        return None

    datos_url = meta.get("datos")
    if not datos_url:
        logger.warning(f"AEMET: step-1 response missing 'datos' key for {url}.")
        return None

    _rate_limiter.wait()
    logger.debug(f"AEMET step-2 GET {datos_url}")
    # Step-2 URL is a pre-signed CDN URL — no auth header needed, but longer timeout
    r2 = client.get(datos_url, timeout=60.0)
    r2.raise_for_status()
    return r2.json()


# ── Public API functions ───────────────────────────────────────────────────────

def fetch_aemet_station_catalogue(api_key: str) -> pd.DataFrame:
    """
    Fetch the complete AEMET station inventory and filter to watershed bbox.

    Returns DataFrame with columns: station_id, station_name, lat, lon, elevation_m
    """
    with make_client() as client:
        try:
            data = _aemet_get(
                client,
                "/valores/climatologicos/inventarioestaciones/todasestaciones",
                api_key=api_key,
            )
        except httpx.HTTPStatusError as e:
            logger.error(f"AEMET catalogue fetch failed: HTTP {e.response.status_code}")
            return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    rows = []
    for s in data:
        # AEMET lat/lon use format "DDMMSS(N/S)" — convert to decimal degrees
        try:
            lat = _aemet_coord_to_decimal(str(s.get("latitud", "")))
            lon = _aemet_coord_to_decimal(str(s.get("longitud", "")))
        except (ValueError, TypeError):
            continue
        if not (
            WATERSHED_BBOX["lat_min"] <= lat <= WATERSHED_BBOX["lat_max"]
            and WATERSHED_BBOX["lon_min"] <= lon <= WATERSHED_BBOX["lon_max"]
        ):
            continue
        rows.append({
            "station_id": str(s.get("indicativo", "")),
            "station_name": str(s.get("nombre", "")),
            "lat": lat,
            "lon": lon,
            "elevation_m": float(s.get("altitud") or np.nan),
            "province": str(s.get("provincia", "")),
        })

    df = pd.DataFrame(rows)
    logger.info(f"Found {len(df)} AEMET stations within watershed bbox.")
    return df


def fetch_aemet_observations(
    station_id: str,
    station_name: str = "",
    api_key: Optional[str] = None,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Fetch meteorological observations from AEMET for a single station.

    Args:
        station_id:   AEMET idema station code (e.g. '0149X')
        station_name: Human-readable name
        api_key:      AEMET OpenData API key (from env AEMET_API_KEY or Streamlit secrets)
        start_dt:     UTC datetime (default: 7 days ago)
        end_dt:       UTC datetime (default: now)

    Returns:
        DataFrame conforming to DATA_SCHEMA.md meteorological schema,
        or empty DataFrame with correct columns if data unavailable.
    """
    if api_key is None:
        api_key = os.environ.get("AEMET_API_KEY", "")
    if not api_key:
        logger.error("AEMET_API_KEY not set. Cannot fetch meteorological data.")
        return _empty_meteo_df()

    now = utc_now()
    start_dt = start_dt or (now - timedelta(days=7))
    end_dt = end_dt or now

    path = f"/observacion/convencional/datos/estacion/{station_id}"
    params = {
        "fechaIniStr": format_aemet_datetime(start_dt),
        "fechaFinStr": format_aemet_datetime(end_dt),
    }

    with make_client() as client:
        try:
            data = _aemet_get(client, path, api_key=api_key, params=params)
        except httpx.HTTPStatusError as e:
            logger.error(
                f"AEMET obs fetch failed for station {station_id}: "
                f"HTTP {e.response.status_code}"
            )
            return _empty_meteo_df()

    if not data:
        logger.warning(f"No AEMET data returned for station {station_id}.")
        return _empty_meteo_df()

    rows = []
    for rec in data:
        # AEMET timestamp field: 'fint' (ISO-like) or 'fecha'
        ts_raw = rec.get("fint") or rec.get("fecha")
        if ts_raw is None:
            continue
        try:
            ts = pd.to_datetime(ts_raw, utc=True)
        except (ValueError, TypeError):
            continue

        row: dict = {"timestamp": ts}
        for aemet_field, schema_col in AEMET_FIELD_MAP.items():
            if schema_col is None:
                continue
            raw = rec.get(aemet_field)
            try:
                row[schema_col] = float(str(raw).replace(",", ".")) if raw not in (None, "", "Ip") else np.nan
            except (ValueError, TypeError):
                row[schema_col] = np.nan
        rows.append(row)

    if not rows:
        return _empty_meteo_df()

    df = pd.DataFrame(rows)
    df["station_id"] = station_id
    df["station_name"] = station_name or station_id

    # Ensure all schema columns exist
    for col in ["precip_mm", "temp_c", "temp_max_c", "temp_min_c",
                "wind_speed_ms", "wind_dir_deg", "humidity_pct"]:
        if col not in df.columns:
            df[col] = np.nan

    df = df[[
        "timestamp", "station_id", "station_name",
        "precip_mm", "temp_c", "temp_max_c", "temp_min_c",
        "wind_speed_ms", "wind_dir_deg", "humidity_pct",
    ]].sort_values("timestamp").reset_index(drop=True)

    logger.info(
        f"Fetched {len(df)} obs rows for AEMET station {station_id} "
        f"({start_dt.date()} → {end_dt.date()})"
    )
    return df


# ── Coordinate conversion ──────────────────────────────────────────────────────

def _aemet_coord_to_decimal(coord_str: str) -> float:
    """
    Convert AEMET coordinate string to decimal degrees.

    AEMET format: 'DDMMSS(N|S|E|W)' e.g. '413324N' → 41.556667
    Handles both DMS and already-decimal formats gracefully.
    """
    coord_str = coord_str.strip()
    if not coord_str:
        raise ValueError("Empty coordinate string")

    # Check for hemisphere indicator
    hemisphere = coord_str[-1].upper()
    if hemisphere not in ("N", "S", "E", "W"):
        # Assume already decimal
        return float(coord_str)

    numeric = coord_str[:-1]
    # Pad to at least 6 characters: DDMMSS
    if len(numeric) < 5:
        raise ValueError(f"Unexpected short coordinate: {coord_str!r}")

    # Last 2 digits = seconds, next 2 = minutes, rest = degrees
    seconds = int(numeric[-2:])
    minutes = int(numeric[-4:-2])
    degrees = int(numeric[:-4]) if len(numeric) > 4 else 0

    decimal = degrees + minutes / 60.0 + seconds / 3600.0
    if hemisphere in ("S", "W"):
        decimal = -decimal
    return decimal
