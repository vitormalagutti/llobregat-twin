"""
Page 1 — Overview: Watershed Map + System Status

Three views via tabs:
  🛰️ Satellite Map  — Folium with satellite/topo/clean tile switcher
  📊 3D Live View   — pydeck ColumnLayer extruded by current flow / storage
  📋 Status Table   — system status for all stations
"""
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from pathlib import Path
import yaml
import numpy as np

st.set_page_config(page_title="Overview — Llobregat", layout="wide")

# ── Hero banner ────────────────────────────────────────────────────────────────
st.markdown("""
<div style="background:linear-gradient(135deg,#023e8a 0%,#0096c7 60%,#48cae4 100%);
            padding:1.4rem 2rem;border-radius:12px;margin-bottom:1rem">
  <h2 style="color:white;margin:0;font-size:1.8rem">💧 Llobregat Watershed</h2>
  <p style="color:#caf0f8;margin:0.2rem 0 0;font-size:0.95rem">
    Live hydrological monitoring · Catalonia, Spain
  </p>
</div>
""", unsafe_allow_html=True)

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


@st.cache_data(ttl=3600)
def load_meta() -> dict:
    p = CONFIG_DIR / "station_metadata.yaml"
    with open(p) as f:
        return yaml.safe_load(f)


@st.cache_data(ttl=3600)
def load_thresholds() -> dict:
    p = CONFIG_DIR / "thresholds.yaml"
    if not p.exists():
        return {}
    with open(p) as f:
        return yaml.safe_load(f)


@st.cache_data(ttl=1800)
def load_latest(prefix: str, station_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"{prefix}_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])


def alert_badge(level: str) -> str:
    cfg = {
        "critical": ("#c0392b", "🔴 Critical"),
        "watch":    ("#e67e22", "🟠 Watch"),
        "normal":   ("#27ae60", "🟢 Normal"),
        "low_flow": ("#8e44ad", "🟣 Low flow"),
        "no_data":  ("#7f8c8d", "⚫ No data"),
    }
    color, label = cfg.get(level, ("#7f8c8d", level))
    return (f'<span style="background:{color};color:white;'
            f'padding:3px 10px;border-radius:12px;font-size:0.8rem;'
            f'font-weight:600">{label}</span>')


def gauge_alert(flow, thresholds):
    if pd.isna(flow):
        return "no_data"
    d = thresholds.get("flow_alert_m3s", {}).get("defaults", {})
    if flow >= d.get("flood_warning", 300):
        return "critical"
    if flow >= d.get("flood_watch", 100):
        return "watch"
    if flow <= d.get("low_flow_warning", 1):
        return "low_flow"
    return "normal"


def reservoir_alert(pct, thresholds):
    if pd.isna(pct):
        return "no_data"
    d = thresholds.get("reservoir_alert_pct", {})
    if pct <= d.get("critically_low", 20):
        return "critical"
    if pct <= d.get("low", 40):
        return "watch"
    return "normal"


meta = load_meta()
thresholds = load_thresholds()

# ── Top KPI strip ─────────────────────────────────────────────────────────────
gauges     = meta.get("gauge_stations", [])
reservoirs = meta.get("reservoirs", [])

total_vol = 0.0
total_cap = 0.0
live_gauges = 0
for res in reservoirs:
    df = load_latest("reservoir", res["id"])
    if not df.empty:
        latest = df.sort_values("timestamp").iloc[-1]
        v = latest.get("volume_hm3", np.nan)
        c = latest.get("capacity_hm3", res.get("capacity_hm3", np.nan))
        if not pd.isna(v) and not pd.isna(c):
            total_vol += v
            total_cap += c

for stn in gauges:
    df = load_latest("flow", stn["id"])
    if not df.empty:
        live_gauges += 1

total_pct = (total_vol / total_cap * 100) if total_cap > 0 else np.nan

# Martorell as main stem reference
martorell_flow = np.nan
for stn in gauges:
    if "081141-003" in stn["id"]:
        df = load_latest("flow", stn["id"])
        if not df.empty:
            martorell_flow = df.sort_values("timestamp").iloc[-1].get("flow_m3s", np.nan)

st.markdown("""<style>
[data-testid="stMetric"]{background:#f0f4f8;border-radius:10px;
  padding:12px 16px;border-left:4px solid #0096c7}
[data-testid="stMetricLabel"]{font-size:0.82rem;color:#555}
[data-testid="stMetricValue"]{font-size:1.5rem;font-weight:700}
</style>""", unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
c1.metric("🏞️ Reservoir storage",
          f"{total_pct:.1f}%" if not np.isnan(total_pct) else "—",
          f"{total_vol:.0f} / {total_cap:.0f} hm³")
c2.metric("🌊 Main stem (Martorell)",
          f"{martorell_flow:.1f} m³/s" if not np.isnan(martorell_flow) else "—")
c3.metric("📡 Live gauge stations", f"{live_gauges} / {len(gauges)}")
c4.metric("💧 Reservoirs tracked", str(len(reservoirs)))

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_sat, tab_3d, tab_table = st.tabs(["🛰️ Satellite Map", "📊 3D Live View", "📋 Status"])

# ─────────────────────────────────────────────────────────────────────────────
# TAB 1: Satellite Folium map
# ─────────────────────────────────────────────────────────────────────────────
with tab_sat:
    m = folium.Map(location=[41.75, 1.90], zoom_start=9, tiles=None)

    # Satellite (Esri — no API key)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri World Imagery",
        name="🛰️ Satellite",
        overlay=False, control=True,
    ).add_to(m)

    # Topographic
    folium.TileLayer(
        tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        attr="© OpenTopoMap",
        name="🗻 Topographic",
        overlay=False, control=True,
    ).add_to(m)

    # Clean
    folium.TileLayer("CartoDB positron", name="🗺️ Clean", overlay=False, control=True).add_to(m)

    alert_colors = {
        "critical": "red", "watch": "orange",
        "normal": "blue", "low_flow": "purple", "no_data": "gray",
    }

    # Gauge stations
    for stn in gauges:
        df = load_latest("flow", stn["id"])
        if df.empty:
            level = "no_data"
            flow_str = "No data"
            level_str = "—"
        else:
            latest = df.sort_values("timestamp").iloc[-1]
            flow = latest.get("flow_m3s", np.nan)
            lvl  = latest.get("level_m", np.nan)
            level = gauge_alert(flow, thresholds)
            flow_str  = f"{flow:.2f} m³/s" if not pd.isna(flow) else "—"
            level_str = f"{lvl:.2f} m"    if not pd.isna(lvl)  else "—"

        popup_html = f"""
        <div style='font-family:sans-serif;min-width:160px'>
          <b style='font-size:1rem'>🌊 {stn['name']}</b><br>
          <hr style='margin:4px 0'>
          <table style='font-size:0.85rem;width:100%'>
            <tr><td>Flow</td><td><b>{flow_str}</b></td></tr>
            <tr><td>Stage</td><td><b>{level_str}</b></td></tr>
            <tr><td>River</td><td>{stn.get('river','—')}</td></tr>
          </table>
        </div>"""

        folium.CircleMarker(
            location=[stn["lat"], stn["lon"]],
            radius=9, color=alert_colors[level],
            fill=True, fill_opacity=0.85, weight=2,
            popup=folium.Popup(popup_html, max_width=220),
            tooltip=f"🌊 {stn['name']}",
        ).add_to(m)

    # Reservoirs — diamond icon via DivIcon
    for res in reservoirs:
        df = load_latest("reservoir", res["id"])
        if df.empty:
            level = "no_data"
            pct_str = "No data"
            vol_str = "—"
        else:
            latest = df.sort_values("timestamp").iloc[-1]
            pct = latest.get("pct_capacity", np.nan)
            vol = latest.get("volume_hm3",  np.nan)
            level = reservoir_alert(pct, thresholds)
            pct_str = f"{pct:.1f}%" if not pd.isna(pct) else "—"
            vol_str = f"{vol:.1f} hm³" if not pd.isna(vol) else "—"

        col = alert_colors[level]
        icon_html = (
            f'<div style="width:20px;height:20px;background:{col};'
            f'transform:rotate(45deg);border:2px solid white;'
            f'box-shadow:0 0 4px rgba(0,0,0,0.5)"></div>'
        )
        popup_html = f"""
        <div style='font-family:sans-serif;min-width:160px'>
          <b style='font-size:1rem'>🏞️ {res['name']}</b><br>
          <hr style='margin:4px 0'>
          <table style='font-size:0.85rem;width:100%'>
            <tr><td>Storage</td><td><b>{pct_str}</b></td></tr>
            <tr><td>Volume</td><td><b>{vol_str}</b></td></tr>
            <tr><td>Capacity</td><td>{res.get('capacity_hm3','—')} hm³</td></tr>
          </table>
        </div>"""

        folium.Marker(
            location=[res["lat"], res["lon"]],
            icon=folium.DivIcon(html=icon_html, icon_size=(20, 20), icon_anchor=(10, 10)),
            popup=folium.Popup(popup_html, max_width=220),
            tooltip=f"🏞️ {res['name']} — {pct_str}",
        ).add_to(m)

    # Meteo stations
    for stn in meta.get("meteo_stations", []):
        folium.CircleMarker(
            location=[stn["lat"], stn["lon"]],
            radius=6, color="#2ca02c",
            fill=True, fill_opacity=0.7, weight=2,
            popup=folium.Popup(f"<b>☁️ {stn['name']}</b><br>AEMET meteo station", max_width=180),
            tooltip=f"☁️ {stn['name']}",
        ).add_to(m)

    # Legend
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
         background:rgba(255,255,255,0.95);padding:12px 16px;border-radius:10px;
         border:1px solid #ccc;font-size:12px;font-family:sans-serif;
         box-shadow:0 2px 8px rgba(0,0,0,0.15)">
      <b>🌊 Gauge stations</b><br>
      <span style="color:#1f77b4">●</span> Normal &nbsp;
      <span style="color:orange">●</span> Watch &nbsp;
      <span style="color:red">●</span> Warning &nbsp;
      <span style="color:purple">●</span> Low &nbsp;
      <span style="color:gray">●</span> No data<br><br>
      <b>🏞️ Reservoirs</b> (◆) &nbsp;&nbsp; <b>☁️ Meteo</b> (🟢)
    </div>"""
    m.get_root().html.add_child(folium.Element(legend_html))
    folium.LayerControl(position="topright", collapsed=False).add_to(m)

    st_folium(m, width="100%", height=580)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 2: pydeck 3D
# ─────────────────────────────────────────────────────────────────────────────
with tab_3d:
    try:
        import pydeck as pdk

        gauge_records = []
        max_flow = 1.0
        for stn in gauges:
            df = load_latest("flow", stn["id"])
            flow = np.nan
            if not df.empty:
                flow = df.sort_values("timestamp").iloc[-1].get("flow_m3s", np.nan)
            if not pd.isna(flow):
                max_flow = max(max_flow, float(flow))
            gauge_records.append({
                "lat": stn["lat"], "lon": stn["lon"],
                "name": stn["name"], "river": stn.get("river", ""),
                "flow": float(flow) if not pd.isna(flow) else 0.0,
            })

        for r in gauge_records:
            norm = r["flow"] / max_flow
            r["color"] = [
                int(0   + norm * 192),   # R: 0 → 192
                int(150 - norm * 100),   # G: 150 → 50
                int(200 - norm * 200),   # B: 200 → 0
                220
            ]
            r["elevation"] = r["flow"]

        res_records = []
        for res in reservoirs:
            df = load_latest("reservoir", res["id"])
            pct = np.nan
            vol = np.nan
            if not df.empty:
                latest = df.sort_values("timestamp").iloc[-1]
                pct = latest.get("pct_capacity", np.nan)
                vol = latest.get("volume_hm3", np.nan)
            pct_val = float(pct) if not pd.isna(pct) else 0.0
            res_records.append({
                "lat": res["lat"], "lon": res["lon"],
                "name": res["name"],
                "pct": pct_val,
                "vol": float(vol) if not pd.isna(vol) else 0.0,
                "cap": float(res.get("capacity_hm3", 100)),
                "elevation": pct_val * 800,
                "color": [
                    int(192 * (1 - pct_val / 100)),
                    int(39 + 100 * (pct_val / 100)),
                    int(200 * (pct_val / 100)),
                    220
                ],
            })

        gauge_layer = pdk.Layer(
            "ColumnLayer",
            data=gauge_records,
            get_position=["lon", "lat"],
            get_elevation="elevation",
            elevation_scale=600,
            radius=600,
            get_fill_color="color",
            pickable=True,
            auto_highlight=True,
            extruded=True,
        )

        res_layer = pdk.Layer(
            "ColumnLayer",
            data=res_records,
            get_position=["lon", "lat"],
            get_elevation="elevation",
            elevation_scale=5,
            radius=1200,
            get_fill_color="color",
            pickable=True,
            auto_highlight=True,
            extruded=True,
        )

        view = pdk.ViewState(
            latitude=41.80, longitude=1.85,
            zoom=8.5, pitch=52, bearing=-8,
        )

        deck = pdk.Deck(
            layers=[gauge_layer, res_layer],
            initial_view_state=view,
            map_style="mapbox://styles/mapbox/satellite-streets-v11",
            tooltip={
                "html": "<b>{name}</b><br>Flow: {flow:.1f} m³/s<br>Storage: {pct:.0f}%",
                "style": {"backgroundColor": "rgba(0,0,0,0.8)", "color": "white",
                          "padding": "8px", "borderRadius": "6px", "fontSize": "13px"},
            },
        )

        st.markdown("""
        <div style='background:#0d1b2a;color:#90e0ef;padding:10px 16px;
             border-radius:8px;margin-bottom:8px;font-size:0.85rem'>
          🌊 <b>Blue columns</b> = river gauges (height = flow m³/s) &nbsp;|&nbsp;
          🔵 <b>Teal cylinders</b> = reservoirs (height = % storage) &nbsp;|&nbsp;
          Drag to rotate · Scroll to zoom
        </div>""", unsafe_allow_html=True)

        st.pydeck_chart(deck, use_container_width=True, height=580)

    except ImportError:
        st.warning("pydeck not installed. Run `pip install pydeck` then restart the app.")
        st.info("Showing 2D fallback map instead.")

# ─────────────────────────────────────────────────────────────────────────────
# TAB 3: Status table
# ─────────────────────────────────────────────────────────────────────────────
with tab_table:
    rows = []
    for stn in gauges:
        df = load_latest("flow", stn["id"])
        if df.empty:
            rows.append({"Station": stn["name"], "Type": "🌊 Gauge",
                         "River": stn.get("river","—"), "Latest value": "—",
                         "Status": "⚫ No data", "Sub-basin": stn.get("sub_basin","—")})
        else:
            latest = df.sort_values("timestamp").iloc[-1]
            flow   = latest.get("flow_m3s", np.nan)
            ts     = pd.Timestamp(latest["timestamp"]).tz_convert("Europe/Madrid").strftime("%Y-%m-%d %H:%M") \
                     if hasattr(latest.get("timestamp"), "tz_convert") else "—"
            level  = gauge_alert(flow, thresholds)
            labels = {"critical":"🔴 Critical","watch":"🟠 Watch",
                      "normal":"🟢 Normal","low_flow":"🟣 Low flow","no_data":"⚫ No data"}
            rows.append({
                "Station": stn["name"], "Type": "🌊 Gauge",
                "River": stn.get("river","—"),
                "Latest value": f"{flow:.2f} m³/s" if not pd.isna(flow) else "—",
                "Status": labels[level], "Sub-basin": stn.get("sub_basin","—"),
            })

    for res in reservoirs:
        df = load_latest("reservoir", res["id"])
        if df.empty:
            rows.append({"Station": res["name"], "Type": "🏞️ Reservoir",
                         "River": res.get("river","—"), "Latest value": "—",
                         "Status": "⚫ No data", "Sub-basin": res.get("sub_basin","—")})
        else:
            latest = df.sort_values("timestamp").iloc[-1]
            pct   = latest.get("pct_capacity", np.nan)
            level = reservoir_alert(pct, thresholds)
            labels = {"critical":"🔴 Critical","watch":"🟠 Watch",
                      "normal":"🟢 Normal","no_data":"⚫ No data"}
            rows.append({
                "Station": res["name"], "Type": "🏞️ Reservoir",
                "River": res.get("river","—"),
                "Latest value": f"{pct:.1f}%" if not pd.isna(pct) else "—",
                "Status": labels[level], "Sub-basin": res.get("sub_basin","—"),
            })

    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Status": st.column_config.TextColumn("Status", width="medium"),
            "Latest value": st.column_config.TextColumn("Latest value", width="medium"),
        },
    )
    st.caption(f"Showing {len(rows)} monitored stations. Cache refreshed every 30 min.")
