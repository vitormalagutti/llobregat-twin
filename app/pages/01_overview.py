"""
Page 1 — Overview: Watershed Map + System Status
IBM Carbon Design System — Gray 100 dark theme
"""
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from pathlib import Path
import yaml
import numpy as np
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from app.carbon import (
    inject, hero, kpi_card, map_kpi, badge, section_label,
    status_color, status_label,
    BG, LAYER_01, LAYER_02, BORDER,
    TEXT_PRIMARY, TEXT_SECONDARY, TEXT_DISABLED,
    FONT_SANS, FONT_MONO,
    BLUE_40, C_CRITICAL, C_WATCH, C_NORMAL, C_LOW_FLOW, C_NODATA,
)

st.set_page_config(page_title="Overview — Llobregat", layout="wide")
inject()

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

# ── Watershed & river geometry (approximate, hardcoded) ────────────────────────
WATERSHED_GEOJSON = {
    "type": "FeatureCollection",
    "features": [{
        "type": "Feature",
        "properties": {"name": "Llobregat Watershed"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [1.73, 42.22], [1.62, 42.08], [1.52, 41.98], [1.50, 41.80],
                [1.55, 41.62], [1.65, 41.48], [1.78, 41.37], [1.95, 41.28],
                [2.10, 41.30], [2.18, 41.40], [2.14, 41.55], [2.03, 41.62],
                [1.97, 41.78], [1.92, 41.97], [2.00, 42.12], [1.98, 42.23],
                [1.73, 42.22]
            ]]
        }
    }]
}

RIVERS_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"name": "Llobregat", "type": "main"},
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [1.97, 42.21], [1.88, 42.12], [1.878, 42.08], [1.880, 41.86],
                    [1.858, 41.65], [1.858, 41.55], [1.948, 41.47], [1.933, 41.48],
                    [2.023, 41.39], [2.047, 41.35], [2.070, 41.30]
                ]
            }
        },
        {
            "type": "Feature",
            "properties": {"name": "Cardener", "type": "tributary"},
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [1.583, 42.10], [1.606, 41.96], [1.696, 41.92],
                    [1.763, 41.81], [1.830, 41.73], [1.858, 41.65]
                ]
            }
        },
        {
            "type": "Feature",
            "properties": {"name": "Anoia", "type": "tributary"},
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [1.788, 41.44], [1.850, 41.46], [1.929, 41.48], [1.948, 41.47]
                ]
            }
        }
    ]
}

# ── Data helpers ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_meta() -> dict:
    with open(CONFIG_DIR / "station_metadata.yaml") as f:
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

meta       = load_meta()
thresholds = load_thresholds()

gauge_stations  = meta.get("gauge_stations", [])
reservoirs      = meta.get("reservoirs", [])
meteo_stations  = meta.get("meteo_stations", [])

flood_warn  = thresholds.get("flow_alert_m3s", {}).get("defaults", {}).get("flood_warning")
flood_watch = thresholds.get("flow_alert_m3s", {}).get("defaults", {}).get("flood_watch")
low_flow    = thresholds.get("flow_alert_m3s", {}).get("defaults", {}).get("low_flow_warning")
res_crit    = thresholds.get("reservoir_alert_pct", {}).get("critically_low", 20)
res_low     = thresholds.get("reservoir_alert_pct", {}).get("low", 40)

# ── Alert helpers ──────────────────────────────────────────────────────────────
def gauge_alert(flow):
    if pd.isna(flow):                              return "no_data",  C_NODATA
    if flood_warn  and flow >= flood_warn:         return "critical", C_CRITICAL
    if flood_watch and flow >= flood_watch:        return "watch",    C_WATCH
    if low_flow    and flow <= low_flow:           return "low_flow", C_LOW_FLOW
    return "normal", C_NORMAL

def res_alert(pct):
    if np.isnan(pct):           return "no_data",  C_NODATA
    if pct <= res_crit:         return "critical", C_CRITICAL
    if pct <= res_low:          return "watch",    C_WATCH
    if pct >= 80:               return "full",     C_NORMAL
    return "normal", BLUE_40

# ── Collect latest values (BCN-first: sort lower → upper) ─────────────────────
BASIN_ORDER = {"lower_llobregat": 0, "anoia": 1, "middle_llobregat": 2,
               "cardener": 3, "upper_llobregat": 4, "other": 5}
sorted_gauges = sorted(gauge_stations,
                       key=lambda s: (BASIN_ORDER.get(s.get("sub_basin","other"), 5),
                                      s.get("priority", 9)))

gauge_latest = {}
for s in gauge_stations:
    df = load_latest("flow", s["id"])
    if df.empty:
        gauge_latest[s["id"]] = {"flow": np.nan, "level": np.nan, "trend": "→"}
        continue
    df2 = df.sort_values("timestamp")
    flow  = float(df2["flow_m3s"].iloc[-1]) if "flow_m3s" in df2.columns else np.nan
    level = float(df2["level_m"].iloc[-1])  if "level_m"  in df2.columns else np.nan
    prev  = df2["flow_m3s"].iloc[-min(len(df2), 12)] if len(df2) >= 2 else flow
    trend = "↑" if flow > prev * 1.05 else ("↓" if flow < prev * 0.95 else "→")
    gauge_latest[s["id"]] = {"flow": flow, "level": level, "trend": trend}

res_latest = {}
for r in reservoirs:
    df = load_latest("reservoir", r["id"])
    if df.empty:
        res_latest[r["id"]] = {"pct": np.nan, "vol": np.nan}
        continue
    row = df.sort_values("timestamp").iloc[-1]
    pct = float(row.get("pct_capacity", np.nan)) if not pd.isna(row.get("pct_capacity")) else np.nan
    vol = float(row.get("volume_hm3", np.nan))   if not pd.isna(row.get("volume_hm3"))   else np.nan
    res_latest[r["id"]] = {"pct": pct, "vol": vol}

meteo_latest = {}
for m in meteo_stations:
    df = load_latest("meteo", m["id"])
    if df.empty:
        meteo_latest[m["id"]] = {"temp": np.nan, "precip": np.nan}
        continue
    row = df.sort_values("timestamp").iloc[-1]
    meteo_latest[m["id"]] = {
        "temp":   float(row.get("temp_c",    np.nan)) if not pd.isna(row.get("temp_c"))    else np.nan,
        "precip": float(row.get("precip_mm", np.nan)) if not pd.isna(row.get("precip_mm")) else np.nan,
    }

# System totals
total_cap = sum(float(r.get("capacity_hm3", 0) or 0) for r in reservoirs)
total_vol = sum(v["vol"] for v in res_latest.values() if not np.isnan(v["vol"]))
sys_pct   = (total_vol / total_cap * 100) if total_cap > 0 else np.nan

# Main channel gauge (Sant Joan Despí)
sjd = next((s for s in gauge_stations if "Sant Joan Desp" in s["name"]), None)
sjd_flow  = gauge_latest[sjd["id"]]["flow"]  if sjd else np.nan
sjd_trend = gauge_latest[sjd["id"]]["trend"] if sjd else "→"

n_active = sum(1 for v in gauge_latest.values() if not np.isnan(v["flow"]))

# ── Hero banner (Carbon) ───────────────────────────────────────────────────────
sjd_flow_str = f"{sjd_flow:.1f} m³/s" if not np.isnan(sjd_flow) else "—"
sys_pct_str  = f"{sys_pct:.1f}%" if not np.isnan(sys_pct) else "—"
_, sjd_col   = gauge_alert(sjd_flow)

st.markdown(hero(
    title="Llobregat Watershed",
    subtitle=f"Live hydrological monitoring · {n_active} of {len(gauge_stations)} gauges active",
    right_label="Sant Joan Despí",
    right_value=f"{sjd_flow_str} {sjd_trend}",
    right_label2="Reservoir system",
    right_value2=sys_pct_str,
), unsafe_allow_html=True)

# ── KPI strip (BCN-first) ──────────────────────────────────────────────────────
top_stations = sorted_gauges[:6]
kpi_cols = st.columns(6)
for i, s in enumerate(top_stations):
    info = gauge_latest[s["id"]]
    flow = info["flow"]
    _, color = gauge_alert(flow)
    flow_str   = f"{flow:.1f} m³/s" if not np.isnan(flow) else "—"
    short_name = s["name"].split(" (")[0][:16]
    with kpi_cols[i]:
        st.markdown(kpi_card(
            label=short_name,
            value=flow_str,
            trend=info["trend"],
            color=color,
        ), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_map, tab_status = st.tabs(["🗺️ Watershed Map", "📋 Status Table"])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — WATERSHED MAP
# ═══════════════════════════════════════════════════════════════════════════════
with tab_map:
    m = folium.Map(location=[41.75, 1.85], zoom_start=9, tiles=None)

    # ── Tile layers ──
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri World Imagery", name="🛰️ Satellite", overlay=False, control=True,
    ).add_to(m)
    folium.TileLayer(
        tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        attr="OpenTopoMap", name="🗻 Topographic", overlay=False, control=True,
    ).add_to(m)
    folium.TileLayer("CartoDB dark_matter", name="🗺️ Dark", overlay=False, control=True).add_to(m)

    # ── Watershed polygon ──
    folium.GeoJson(
        WATERSHED_GEOJSON,
        name="🌍 Watershed boundary",
        style_function=lambda _: {
            "fillColor":   BLUE_40,
            "fillOpacity": 0.07,
            "color":       BLUE_40,
            "weight":      2,
            "dashArray":   "6 4",
        },
        tooltip=folium.GeoJsonTooltip(fields=["name"], aliases=[""]),
    ).add_to(m)

    # ── River paths ──
    def river_style(feature):
        t = feature["properties"].get("type", "tributary")
        return {
            "color":   "#4589ff" if t == "main" else "#78a9ff",
            "weight":  3 if t == "main" else 1.8,
            "opacity": 0.80,
        }

    folium.GeoJson(
        RIVERS_GEOJSON,
        name="🌊 River network",
        style_function=river_style,
        tooltip=folium.GeoJsonTooltip(fields=["name"], aliases=[""]),
    ).add_to(m)

    # ── Gauge station markers (Carbon KPI card style) ──
    gauge_fg = folium.FeatureGroup(name="💧 Gauge stations", show=True)
    for s in gauge_stations:
        info = gauge_latest[s["id"]]
        flow = info["flow"]
        trend = info["trend"]
        alert_lvl, color = gauge_alert(flow)
        flow_str   = f"{flow:.1f}" if not np.isnan(flow) else "—"
        short_name = s["name"].split(" (")[0][:16]
        river_name = s.get("river", "")

        card_html = map_kpi(label=f"💧 {short_name}",
                            value=f"{flow_str} m³/s",
                            trend=trend, color=color)

        popup_html = f"""
        <div style="font-family:{FONT_SANS};min-width:180px;background:{LAYER_01};
                    padding:8px;border-top:3px solid {color}">
          <b style="font-size:13px;color:{TEXT_PRIMARY}">{s['name']}</b><br>
          <span style="color:{TEXT_SECONDARY};font-size:11px">{river_name} · {s.get('sub_basin','').replace('_',' ').title()}</span>
          <hr style="border-color:{BORDER};margin:6px 0">
          <table style="font-size:12px;width:100%;color:{TEXT_PRIMARY}">
            <tr><td style="color:{TEXT_SECONDARY}">Flow</td><td>{flow_str} m³/s {trend}</td></tr>
            <tr><td style="color:{TEXT_SECONDARY}">Stage</td>
                <td>{"%.3f m" % info['level'] if not np.isnan(info['level']) else "—"}</td></tr>
            <tr><td style="color:{TEXT_SECONDARY}">Status</td>
                <td><span style="color:{color}">{status_label(alert_lvl)}</span></td></tr>
          </table>
        </div>"""

        folium.Marker(
            location=[s["lat"], s["lon"]],
            icon=folium.DivIcon(
                html=card_html,
                icon_size=(110, 52),
                icon_anchor=(55, 26),
            ),
            popup=folium.Popup(popup_html, max_width=220),
        ).add_to(gauge_fg)
    gauge_fg.add_to(m)

    # ── Reservoir markers ──
    res_fg = folium.FeatureGroup(name="🏔️ Reservoirs", show=True)
    for r in reservoirs:
        v   = res_latest[r["id"]]
        pct = v["pct"]
        vol = v["vol"]
        _, color = res_alert(pct)
        pct_str = f"{pct:.0f}%" if not np.isnan(pct) else "—"
        vol_str = f"{vol:.1f} hm³" if not np.isnan(vol) else "—"
        short   = r["name"][:14]

        card_html = map_kpi(label=f"🏔️ {short}",
                            value=pct_str,
                            trend=vol_str, color=color)

        popup_html = f"""
        <div style="font-family:{FONT_SANS};min-width:180px;background:{LAYER_01};
                    padding:8px;border-top:3px solid {color}">
          <b style="font-size:13px;color:{TEXT_PRIMARY}">🏔️ {r['name']}</b><br>
          <span style="color:{TEXT_SECONDARY};font-size:11px">{r.get('river','')} reservoir</span>
          <hr style="border-color:{BORDER};margin:6px 0">
          <table style="font-size:12px;width:100%;color:{TEXT_PRIMARY}">
            <tr><td style="color:{TEXT_SECONDARY}">Storage</td><td>{pct_str}</td></tr>
            <tr><td style="color:{TEXT_SECONDARY}">Volume</td><td>{vol_str}</td></tr>
            <tr><td style="color:{TEXT_SECONDARY}">Capacity</td>
                <td>{r.get('capacity_hm3','?')} hm³</td></tr>
          </table>
        </div>"""

        folium.Marker(
            location=[r["lat"], r["lon"]],
            icon=folium.DivIcon(
                html=card_html,
                icon_size=(95, 52),
                icon_anchor=(47, 26),
            ),
            popup=folium.Popup(popup_html, max_width=220),
        ).add_to(res_fg)
    res_fg.add_to(m)

    # ── Meteo station markers ──
    meteo_fg = folium.FeatureGroup(name="⛅ Meteo stations", show=True)
    for mt in meteo_stations:
        info  = meteo_latest[mt["id"]]
        temp  = info["temp"]
        prec  = info["precip"]
        temp_str = f"{temp:.1f}°C" if not np.isnan(temp) else "—"
        prec_str = f"{prec:.1f} mm" if not np.isnan(prec) else "—"
        short    = mt["name"][:12]

        card_html = map_kpi(label=f"⛅ {short}",
                            value=temp_str,
                            trend=f"💧 {prec_str}", color=C_NORMAL)

        popup_html = f"""
        <div style="font-family:{FONT_SANS};background:{LAYER_01};
                    padding:8px;border-top:3px solid {C_NORMAL}">
          <b style="color:{TEXT_PRIMARY}">⛅ {mt['name']}</b><br>
          <span style="color:{TEXT_SECONDARY};font-size:11px">AEMET station {mt['id']}</span>
          <hr style="border-color:{BORDER};margin:6px 0">
          <span style="font-size:12px;color:{TEXT_PRIMARY}">🌡️ {temp_str} &nbsp; 💧 {prec_str}</span>
        </div>"""

        folium.Marker(
            location=[mt["lat"], mt["lon"]],
            icon=folium.DivIcon(
                html=card_html,
                icon_size=(90, 50),
                icon_anchor=(45, 25),
            ),
            popup=folium.Popup(popup_html, max_width=200),
        ).add_to(meteo_fg)
    meteo_fg.add_to(m)

    folium.LayerControl(position="topright", collapsed=False).add_to(m)

    st_folium(m, use_container_width=True, height=620, returned_objects=[])

    # Legend — Carbon style
    st.markdown(f"""
<div style="display:flex;gap:1.5rem;flex-wrap:wrap;font-size:12px;font-family:{FONT_SANS};
            color:{TEXT_SECONDARY};margin-top:8px;padding-top:8px;border-top:1px solid {BORDER}">
  <span>💧 <b style="color:{TEXT_PRIMARY}">Gauge</b> — flow m³/s</span>
  <span>🏔️ <b style="color:{TEXT_PRIMARY}">Reservoir</b> — % capacity</span>
  <span>⛅ <b style="color:{TEXT_PRIMARY}">Weather</b> — temperature</span>
  <span style="color:{C_CRITICAL}">● Flood warning</span>
  <span style="color:{C_WATCH}">● Flood watch</span>
  <span style="color:{C_NORMAL}">● Normal</span>
  <span style="color:{C_LOW_FLOW}">● Low flow</span>
  <span style="color:{C_NODATA}">● No data</span>
</div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — STATUS TABLE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_status:
    st.markdown(section_label("All stations — current status"), unsafe_allow_html=True)

    rows = []
    for s in sorted_gauges:
        info  = gauge_latest[s["id"]]
        flow  = info["flow"]
        level = info["level"]
        alv, _ = gauge_alert(flow)
        rows.append({
            "Type":     "💧 Gauge",
            "Name":     s["name"],
            "River":    s.get("river", "—"),
            "Value":    f"{flow:.2f} m³/s" if not np.isnan(flow) else "—",
            "Stage":    f"{level:.3f} m"   if not np.isnan(level) else "—",
            "Trend":    info["trend"],
            "Status":   status_label(alv),
        })

    for r in reservoirs:
        v   = res_latest[r["id"]]
        pct = v["pct"]
        vol = v["vol"]
        alv, _ = res_alert(pct)
        rows.append({
            "Type":   "🏔️ Reservoir",
            "Name":   r["name"],
            "River":  r.get("river", "—"),
            "Value":  f"{pct:.1f}%" if not np.isnan(pct) else "—",
            "Stage":  f"{vol:.1f} hm³" if not np.isnan(vol) else "—",
            "Trend":  "—",
            "Status": status_label(alv),
        })

    for mt in meteo_stations:
        info = meteo_latest[mt["id"]]
        temp = info["temp"]
        rows.append({
            "Type":   "⛅ Meteo",
            "Name":   mt["name"],
            "River":  "—",
            "Value":  f"{temp:.1f} °C" if not np.isnan(temp) else "—",
            "Stage":  "—",
            "Trend":  "—",
            "Status": "Active" if not np.isnan(temp) else "No data",
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.caption("💧 Gauge data: ACA Sentilo · ⛅ Meteo: AEMET OpenData · Cache refreshes every 30 min.")
