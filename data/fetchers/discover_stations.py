"""
data/fetchers/discover_stations.py

Run this ONCE locally to discover real ACA and AEMET station IDs.

Usage:
    cd ~/Documents/Llobregat_Viz/llobregat-twin
    pip install -r requirements.txt
    AEMET_API_KEY=<your_key> python -m data.fetchers.discover_stations

Outputs:
    data/static/discovered_aca_gauges.csv      — real ACA gauge component IDs
    data/static/discovered_aca_reservoirs.csv  — real ACA reservoir component IDs
    data/static/discovered_aemet_stations.csv  — real AEMET station IDs

After running, inspect these files and update:
    config/station_metadata.yaml
    data/static/stations_aca.csv
    data/static/stations_aemet.csv
"""
import json
import os
import sys
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from data.fetchers.utils import configure_logging, make_client
from data.fetchers.aca import fetch_aca_station_catalogue, ACA_BASE_URL, _sentilo_get
from data.fetchers.aemet import fetch_aemet_station_catalogue

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = PROJECT_ROOT / "data" / "static"
STATIC_DIR.mkdir(exist_ok=True)

# Watershed bounding box
BBOX = {"lon_min": 1.5, "lon_max": 2.3, "lat_min": 41.2, "lat_max": 42.2}


def discover_aca_components(component_type: str) -> pd.DataFrame:
    logger.info(f"=== Discovering ACA components: {component_type} ===")
    df = fetch_aca_station_catalogue(component_type=component_type)
    if df.empty:
        logger.warning(f"No {component_type} components found in watershed bbox.")
    else:
        logger.info(f"Found {len(df['component_id'].unique())} components, {len(df)} sensors.")
        logger.info("\n" + df.to_string(index=False))
    return df


def discover_aca_sensor_names(component_id: str, provider: str) -> list[str]:
    """Fetch the sensors available for a specific component to verify sensor names."""
    with make_client() as client:
        try:
            data = _sentilo_get(client, f"/catalog/{provider}/{component_id}")
            if data:
                sensors = [s.get("sensor") for s in data.get("sensors", [])]
                logger.info(f"  Sensors for {component_id}: {sensors}")
                return sensors
        except Exception as e:
            logger.warning(f"  Could not fetch sensors for {component_id}: {e}")
    return []


def discover_aemet_stations() -> pd.DataFrame:
    aemet_key = os.environ.get("AEMET_API_KEY", "")
    if not aemet_key:
        logger.warning("AEMET_API_KEY not set — skipping AEMET discovery.")
        return pd.DataFrame()

    logger.info("=== Discovering AEMET stations in watershed bbox ===")
    df = fetch_aemet_station_catalogue(api_key=aemet_key)
    if df.empty:
        logger.warning("No AEMET stations found in watershed bbox.")
    else:
        logger.info(f"Found {len(df)} AEMET stations:")
        logger.info("\n" + df.to_string(index=False))
    return df


def main() -> None:
    print("\n" + "="*60)
    print("  ACA + AEMET Station Discovery")
    print("="*60 + "\n")

    # ── ACA gauges ────────────────────────────────────────────────
    gauges_df = discover_aca_components("aforament")
    if not gauges_df.empty:
        out = STATIC_DIR / "discovered_aca_gauges.csv"
        gauges_df.to_csv(out, index=False)
        print(f"\n✅ Saved ACA gauges → {out.relative_to(PROJECT_ROOT)}")

        # Verify sensor names on first 3 components
        for cid, prov in gauges_df[["component_id", "provider"]].drop_duplicates().head(3).values:
            discover_aca_sensor_names(cid, prov)

    # ── ACA reservoirs ────────────────────────────────────────────
    reservoirs_df = discover_aca_components("embassament")
    if not reservoirs_df.empty:
        out = STATIC_DIR / "discovered_aca_reservoirs.csv"
        reservoirs_df.to_csv(out, index=False)
        print(f"✅ Saved ACA reservoirs → {out.relative_to(PROJECT_ROOT)}")

    # ── AEMET stations ────────────────────────────────────────────
    aemet_df = discover_aemet_stations()
    if not aemet_df.empty:
        out = STATIC_DIR / "discovered_aemet_stations.csv"
        aemet_df.to_csv(out, index=False)
        print(f"✅ Saved AEMET stations → {out.relative_to(PROJECT_ROOT)}")

    print("\n" + "="*60)
    print("  NEXT STEPS")
    print("="*60)
    print("""
1. Open data/static/discovered_aca_gauges.csv
   → Pick 4 priority gauges (Berga, Manresa, Cardener, Martorell)
   → Note their component_id, provider, and sensor names

2. Open data/static/discovered_aca_reservoirs.csv
   → Pick La Baells and Sant Ponç
   → Note their component_id, provider, and sensor names

3. Open data/static/discovered_aemet_stations.csv
   → Pick ~4 stations near the reservoirs and in the lower valley

4. Update config/station_metadata.yaml with real IDs
   (replace placeholder IDs like E003, BAELLS, etc.)

5. Run the data refresh:
   python -m data.fetchers.refresh_all
""")


if __name__ == "__main__":
    main()
