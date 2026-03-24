"""
data/fetchers/refresh_all.py

Master refresh script — fetches all data sources and writes Parquet cache.

Run via:
    python -m data.fetchers.refresh_all

Or automatically via GitHub Actions every 30 minutes.

Environment variables:
    AEMET_API_KEY  — AEMET OpenData API key (required for meteorology)
    ACA_API_KEY    — ACA Sentilo IDENTITY_KEY (optional; public read doesn't need it)
"""
import logging
import os
import sys
from pathlib import Path

import yaml

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


def refresh_gauges(meta: dict, aca_key: str | None) -> None:
    stations = meta.get("gauge_stations", [])
    if not stations:
        logger.warning("No gauge stations configured.")
        return

    for stn in stations:
        # After discover_stations.py is run, config will have real 'component_id' and 'provider'
        # Fall back to 'id' field for backward compatibility during migration
        component_id = stn.get("component_id") or stn.get("id", "")
        provider = stn.get("provider", "")
        flow_sensor = stn.get("flow_sensor", "cabal")
        level_sensor = stn.get("level_sensor", "nivell")

        if not component_id or not provider:
            logger.warning(
                f"Gauge station '{stn.get('name', '?')}' missing component_id or provider "
                "— skipping. Run discover_stations.py first."
            )
            continue

        logger.info(f"Fetching gauge: {stn['name']} ({component_id})")
        df = aca.fetch_aca_gauge_data(
            component_id=component_id,
            provider=provider,
            station_name=stn["name"],
            flow_sensor=stn.get("flow_sensor") or None,
            level_sensor=stn.get("level_sensor") or None,
            identity_key=aca_key,
        )
        if df.empty:
            logger.warning(f"  → No data for {component_id} — skipping cache write.")
            continue
        aca.cache_to_parquet(df, prefix="flow", entity_id=component_id)
        logger.info(f"  → Cached {len(df)} rows.")


def refresh_reservoirs(meta: dict, aca_key: str | None) -> None:
    reservoirs = meta.get("reservoirs", [])
    if not reservoirs:
        logger.warning("No reservoirs configured.")
        return

    for res in reservoirs:
        component_id = res.get("component_id") or res.get("id", "")
        provider = res.get("provider", "")
        volume_sensor = res.get("volume_sensor", "volum")
        level_sensor = res.get("level_sensor", "cota")

        if not component_id or not provider:
            logger.warning(
                f"Reservoir '{res.get('name', '?')}' missing component_id or provider "
                "— skipping. Run discover_stations.py first."
            )
            continue

        logger.info(f"Fetching reservoir: {res['name']} ({component_id})")
        df = aca.fetch_aca_reservoir_data(
            component_id=component_id,
            provider=provider,
            reservoir_name=res["name"],
            capacity_hm3=float(res.get("capacity_hm3") or float("nan")),
            volume_sensor=res.get("volume_sensor") or None,
            level_sensor=res.get("level_sensor") or None,
            identity_key=aca_key,
        )
        if df.empty:
            logger.warning(f"  → No data for {component_id} — skipping cache write.")
            continue
        aca.cache_to_parquet(df, prefix="reservoir", entity_id=component_id)
        logger.info(f"  → Cached {len(df)} rows.")


def refresh_meteo(meta: dict, aemet_key: str) -> None:
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


def refresh_piezo(meta: dict, aca_key: str | None) -> None:
    stations = meta.get("piezo_stations") or []
    if not stations:
        logger.info("No piezometric stations configured — skipping.")
        return

    for stn in stations:
        component_id = stn.get("component_id") or stn.get("id", "")
        provider = stn.get("provider", "")

        if not component_id or not provider:
            logger.warning(f"Piezo station '{stn.get('name', '?')}' missing component_id/provider.")
            continue

        logger.info(f"Fetching piezo: {stn['name']} ({component_id})")
        df = aca.fetch_aca_piezo_data(
            component_id=component_id,
            provider=provider,
            station_name=stn["name"],
            aquifer_unit=stn.get("aquifer_unit", "Baix Llobregat"),
            identity_key=aca_key,
        )
        if df.empty:
            logger.info(f"  → No piezometric data for {component_id} (expected for some stations).")
            continue
        aca.cache_to_parquet(df, prefix="piezo", entity_id=component_id)
        logger.info(f"  → Cached {len(df)} rows.")


def _load_aemet_key_from_secrets() -> str:
    """
    Try to load AEMET_API_KEY from .streamlit/secrets.toml as a fallback
    when the environment variable is not set.
    Works for both manual runs and the GitHub Actions cron job (which uses
    the env var injected by the Actions secret).
    """
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            return ""

    secrets_path = PROJECT_ROOT / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return ""
    try:
        with open(secrets_path, "rb") as f:
            secrets = tomllib.load(f)
        return secrets.get("AEMET_API_KEY", "")
    except Exception:
        return ""


def main() -> None:
    aemet_key = os.environ.get("AEMET_API_KEY", "") or _load_aemet_key_from_secrets()
    aca_key = os.environ.get("ACA_API_KEY", None)

    if not aemet_key:
        logger.warning(
            "AEMET_API_KEY not set. Meteorological data will not be fetched."
        )

    logger.info("=== Llobregat data refresh starting ===")
    meta = load_station_metadata()

    errors: list[str] = []

    for label, fn, kwargs in [
        ("gauges",     refresh_gauges,     {"meta": meta, "aca_key": aca_key}),
        ("reservoirs", refresh_reservoirs, {"meta": meta, "aca_key": aca_key}),
        ("piezo",      refresh_piezo,      {"meta": meta, "aca_key": aca_key}),
    ]:
        try:
            fn(**kwargs)
        except Exception as e:
            logger.error(f"{label} refresh failed: {e}", exc_info=True)
            errors.append(f"{label}: {e}")

    if aemet_key:
        try:
            refresh_meteo(meta, aemet_key)
        except Exception as e:
            logger.error(f"meteo refresh failed: {e}", exc_info=True)
            errors.append(f"meteo: {e}")

    if errors:
        logger.error(f"=== Refresh completed WITH ERRORS: {errors} ===")
        sys.exit(1)
    else:
        logger.info("=== Refresh completed successfully ===")


if __name__ == "__main__":
    main()
