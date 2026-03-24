"""
data/fetchers/refresh_all.py

Master refresh script — fetches all data sources and writes to cache.

Run via:
    python -m data.fetchers.refresh_all

Or from GitHub Actions (see .github/workflows/refresh_data.yml).

Environment variables required:
    AEMET_API_KEY  — AEMET OpenData API key
    ACA_API_KEY    — ACA SDIM API key (optional; omit if not required)
"""
import logging
import os
import sys
from pathlib import Path

import yaml

# Ensure project root is on sys.path when run as __main__
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data.fetchers.utils import configure_logging
from data.fetchers import aca, aemet

configure_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_station_metadata() -> dict:
    meta_path = PROJECT_ROOT / "config" / "station_metadata.yaml"
    with open(meta_path) as f:
        return yaml.safe_load(f)


def refresh_gauges(meta: dict, api_key: str | None) -> None:
    """Fetch and cache river gauge data for all configured stations."""
    stations = meta.get("gauge_stations", [])
    if not stations:
        logger.warning("No gauge stations configured.")
        return

    for stn in stations:
        logger.info(f"Fetching gauge: {stn['name']} ({stn['id']})")
        df = aca.fetch_aca_gauge_data(
            station_id=stn["id"],
            station_name=stn["name"],
            api_key=api_key,
        )
        if df.empty:
            logger.warning(f"  → No data for {stn['id']} — skipping cache write.")
            continue
        aca.cache_to_parquet(df, prefix="flow", entity_id=stn["id"])
        logger.info(f"  → Cached {len(df)} rows.")


def refresh_reservoirs(meta: dict, api_key: str | None) -> None:
    """Fetch and cache reservoir data for all configured reservoirs."""
    reservoirs = meta.get("reservoirs", [])
    if not reservoirs:
        logger.warning("No reservoirs configured.")
        return

    for res in reservoirs:
        logger.info(f"Fetching reservoir: {res['name']} ({res['id']})")
        df = aca.fetch_aca_reservoir_data(
            reservoir_id=res["id"],
            reservoir_name=res["name"],
            capacity_hm3=float(res.get("capacity_hm3") or float("nan")),
            api_key=api_key,
        )
        if df.empty:
            logger.warning(f"  → No data for {res['id']} — skipping cache write.")
            continue
        aca.cache_to_parquet(df, prefix="reservoir", entity_id=res["id"])
        logger.info(f"  → Cached {len(df)} rows.")


def refresh_meteo(meta: dict, aemet_key: str) -> None:
    """Fetch and cache meteorological data for all configured AEMET stations."""
    stations = meta.get("meteo_stations", [])
    if not stations:
        logger.warning("No AEMET meteo stations configured.")
        return

    for stn in stations:
        logger.info(f"Fetching meteo: {stn['name']} ({stn['id']})")
        df = aemet.fetch_aemet_observations(
            station_id=stn["id"],
            station_name=stn["name"],
            api_key=aemet_key,
        )
        if df.empty:
            logger.warning(f"  → No data for AEMET {stn['id']} — skipping cache write.")
            continue
        aemet.cache_to_parquet(df, station_id=stn["id"])
        logger.info(f"  → Cached {len(df)} rows.")


def refresh_piezo(meta: dict, api_key: str | None) -> None:
    """Fetch and cache piezometric data (graceful: many stations return no data)."""
    stations = meta.get("piezo_stations") or []
    if not stations:
        logger.info("No piezometric stations configured — skipping.")
        return

    for stn in stations:
        logger.info(f"Fetching piezo: {stn['name']} ({stn['id']})")
        df = aca.fetch_aca_piezo_data(
            station_id=stn["id"],
            station_name=stn["name"],
            aquifer_unit=stn.get("aquifer_unit", "Baix Llobregat"),
            api_key=api_key,
        )
        if df.empty:
            logger.info(f"  → No piezometric data for {stn['id']} (expected for some stations).")
            continue
        aca.cache_to_parquet(df, prefix="piezo", entity_id=stn["id"])
        logger.info(f"  → Cached {len(df)} rows.")


def main() -> None:
    aemet_key = os.environ.get("AEMET_API_KEY", "")
    aca_key = os.environ.get("ACA_API_KEY", None)

    if not aemet_key:
        logger.warning(
            "AEMET_API_KEY not set. Meteorological data will not be fetched. "
            "Set the environment variable or add to .streamlit/secrets.toml."
        )

    logger.info("=== Llobregat data refresh starting ===")
    meta = load_station_metadata()

    errors: list[str] = []

    try:
        refresh_gauges(meta, aca_key)
    except Exception as e:
        logger.error(f"Gauge refresh failed: {e}", exc_info=True)
        errors.append(f"gauges: {e}")

    try:
        refresh_reservoirs(meta, aca_key)
    except Exception as e:
        logger.error(f"Reservoir refresh failed: {e}", exc_info=True)
        errors.append(f"reservoirs: {e}")

    if aemet_key:
        try:
            refresh_meteo(meta, aemet_key)
        except Exception as e:
            logger.error(f"Meteo refresh failed: {e}", exc_info=True)
            errors.append(f"meteo: {e}")

    try:
        refresh_piezo(meta, aca_key)
    except Exception as e:
        logger.error(f"Piezo refresh failed: {e}", exc_info=True)
        errors.append(f"piezo: {e}")

    if errors:
        logger.error(f"=== Refresh completed WITH ERRORS: {errors} ===")
        sys.exit(1)
    else:
        logger.info("=== Refresh completed successfully ===")


if __name__ == "__main__":
    main()
