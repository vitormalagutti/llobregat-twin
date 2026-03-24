"""
Page 1 — Overview: Watershed Map + System Status

Displays:
- Interactive Folium map with all instrumentation points
- System status badges (alert / normal / no data) per station
- Cache freshness indicator

Data source: reads from data/cache/ parquet files only.
"""
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from pathlib import Path
import yaml
import numpy as np

st.set_page_config(page_title="Overview — Llobregat", layout="wide")
st.title("Watershed Overview")

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
STATIC_DIR = Path(__file__).parent.parent.parent / "data" / "static"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

# ── Load station metadata ─────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_station_metadata() -> dict:
    meta_path = CONFIG_DIR / "station_metadata.yaml"
    if not meta_path.exists():
        return {}
    with open(meta_path) as f:
        return yaml.safe_load(f)

# ── Load thresholds ────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_thresholds() -> dict:
    thresh_path = CONFIG_DIR / "thresholds.yaml"
    if not thresh_path.exists():
        return {}
    with open(thresh_path) as f:
        return yaml.safe_load(f)

# ── Load most recent cached data for a station ────────────────────────────────
@st.cache_data(ttl=1800)
def load_latest_cached(prefix: str, station_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"{prefix}_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

# ── Build map ─────────────────────────────────────────────────────────────────
meta = load_station_metadata()
thresholds = load_thresholds()

# Centre map on watershed midpoint
m = folium.Map(location=[41.75, 1.90], zoom_start=9, tiles="CartoDB positron")

# Add gauge stations
for stn in meta.get("gauge_stations", []):
    df = load_latest_cached("flow", stn["id"])
    if df.empty:
        color = "gray"
        popup_text = f"<b>{stn['name']}</b><br>No data available"
    else:
        latest = df.sort_values("timestamp").iloc[-1]
        flow = latest.get("flow_m3s", np.nan)
        flood_warn = thresholds.get("flow_alert_m3s", {}).get("defaults", {}).get("flood_warning", 300)
        flood_watch = thresholds.get("flow_alert_m3s", {}).get("defaults", {}).get("flood_watch", 100)
        low_flow = thresholds.get("flow_alert_m3s", {}).get("defaults", {}).get("low_flow_warning", 1)
        if pd.isna(flow):
            color = "gray"
        elif flow >= flood_warn:
            color = "red"
        elif flow >= flood_watch:
            color = "orange"
        elif flow <= low_flow:
            color = "purple"
        else:
            color = "blue"
        popup_text = f"<b>{stn['name']}</b><br>Flow: {flow:.2f} m³/s"

    folium.CircleMarker(
        location=[stn["lat"], stn["lon"]],
        radius=8,
        color=color,
        fill=True,
        fill_opacity=0.8,
        popup=folium.Popup(popup_text, max_width=200),
        tooltip=stn["name"],
    ).add_to(m)

# Add reservoirs
for res in meta.get("reservoirs", []):
    df = load_latest_cached("reservoir", res["id"])
    if df.empty:
        color = "gray"
        popup_text = f"<b>{res['name']}</b><br>No data available"
    else:
        latest = df.sort_values("timestamp").iloc[-1]
        pct = latest.get("pct_capacity", np.nan)
        low = thresholds.get("reservoir_alert_pct", {}).get("low", 40)
        crit = thresholds.get("reservoir_alert_pct", {}).get("critically_low", 20)
        if pd.isna(pct):
            color = "gray"
        elif pct <= crit:
            color = "red"
        elif pct <= low:
            color = "orange"
        else:
            color = "darkblue"
        popup_text = f"<b>{res['name']}</b><br>Storage: {pct:.1f}% of capacity"

    folium.Marker(
        location=[res["lat"], res["lon"]],
        icon=folium.Icon(color=color, icon="tint", prefix="fa"),
        popup=folium.Popup(popup_text, max_width=200),
        tooltip=res["name"],
    ).add_to(m)

# Add meteo stations
for stn in meta.get("meteo_stations", []):
    folium.CircleMarker(
        location=[stn["lat"], stn["lon"]],
        radius=5,
        color="green",
        fill=True,
        fill_opacity=0.6,
        popup=folium.Popup(f"<b>{stn['name']}</b><br>Meteorological station", max_width=200),
        tooltip=stn["name"],
    ).add_to(m)

# ── Legend ─────────────────────────────────────────────────────────────────────
legend_html = """
<div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
     background-color: white; padding: 10px; border-radius: 5px;
     border: 1px solid #ccc; font-size: 12px;">
  <b>Gauge stations</b><br>
  🔵 Normal &nbsp; 🟠 Flood watch &nbsp; 🔴 Flood warning<br>
  🟣 Low flow &nbsp; ⚫ No data<br><br>
  <b>Reservoirs</b> (▲) &nbsp; <b>Meteo stations</b> (🟢)
</div>
"""
m.get_root().html.add_child(folium.Element(legend_html))

st.subheader("Instrumentation map")
st_folium(m, width="100%", height=550)

# ── System status table ────────────────────────────────────────────────────────
st.subheader("System status")
status_rows = []
for stn in meta.get("gauge_stations", []):
    df = load_latest_cached("flow", stn["id"])
    if df.empty:
        status_rows.append({"Station": stn["name"], "Type": "Gauge", "Latest value": "—", "Status": "⚫ No data"})
    else:
        latest = df.sort_values("timestamp").iloc[-1]
        flow = latest.get("flow_m3s", np.nan)
        ts = latest.get("timestamp", "—")
        status_rows.append({
            "Station": stn["name"],
            "Type": "Gauge",
            "Latest value": f"{flow:.2f} m³/s" if not pd.isna(flow) else "—",
            "Status": "🟢 OK",
        })

for res in meta.get("reservoirs", []):
    df = load_latest_cached("reservoir", res["id"])
    if df.empty:
        status_rows.append({"Station": res["name"], "Type": "Reservoir", "Latest value": "—", "Status": "⚫ No data"})
    else:
        latest = df.sort_values("timestamp").iloc[-1]
        pct = latest.get("pct_capacity", np.nan)
        status_rows.append({
            "Station": res["name"],
            "Type": "Reservoir",
            "Latest value": f"{pct:.1f}%" if not pd.isna(pct) else "—",
            "Status": "🟢 OK",
        })

if status_rows:
    st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)
else:
    st.info("No station metadata loaded. Check config/station_metadata.yaml.")
