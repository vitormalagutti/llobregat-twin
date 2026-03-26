"""
Page 1 — Overview: Watershed Map + System Status

Tabs:
  🗺️ Watershed Map  — Folium with watershed polygon, river paths, custom
                       station icons (KPI card style), satellite/topo/clean tiles
  📋 Status Table   — full status for all stations
"""
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from pathlib import Path
import yaml
import numpy as np
import json

st.set_page_config(page_title="Overview — Llobregat", layout="wide")

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
    if pd.isna(flow):                              return "no_data",  "#95a5a6"
    if flood_warn  and flow >= flood_warn:         return "critical", "#c0392b"
    if flood_watch and flow >= flood_watch:        return "watch",    "#e67e22"
    if low_flow    and flow <= low_flow:           return "low_flow", "#8e44ad"
    return "normal", "#27ae60"

def res_alert(pct):
    if np.isnan(pct):           return "no_data",  "#95a5a6"
    if pct <= res_crit:         return "critical", "#c0392b"
    if pct <= res_low:          return "watch",    "#e67e22"
    if pct >= 80:               return "full",     "#27ae60"
    return "normal", "#0096c7"

# ── Collect latest values (BCN-first: sort lower → upper) ─────────────────────
# Sort gauges: lower_llobregat first (closest to BCN), then others
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

# ── Hero banner ────────────────────────────────────────────────────────────────
sjd_flow_str = f"{sjd_flow:.1f} m³/s" if not np.isnan(sjd_flow) else "—"
sys_pct_str  = f"{sys_pct:.1f}%" if not np.isnan(sys_pct) else "—"

st.markdown(f"""
<div style="background:linear-gradient(135deg,#023e8a 0%,#0096c7 60%,#48cae4 100%);
            padding:1.4rem 2rem;border-radius:12px;margin-bottom:1rem;
            display:flex;align-items:center;justify-content:space-between">
  <div>
    <h2 style="color:white;margin:0;font-size:1.8rem">💧 Llobregat Watershed</h2>
    <p style="color:#caf0f8;margin:0.2rem 0 0;font-size:0.9rem">
      Live hydrological monitoring · {n_active} of {len(gauge_stations)} gauges active
    </p>
  </div>
  <div style="display:flex;gap:2rem;text-align:center">
    <div>
      <div style="color:#caf0f8;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.05em">
        Sant Joan Despí
      </div>
      <div style="color:white;font-size:1.6rem;font-weight:800">{sjd_flow_str} {sjd_trend}</div>
    </div>
    <div>
      <div style="color:#caf0f8;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.05em">
        Reservoir system
      </div>
      <div style="color:white;font-size:1.6rem;font-weight:800">{sys_pct_str}</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── KPI strip (BCN-first) ──────────────────────────────────────────────────────
# Show top 6 BCN-area stations
top_stations = sorted_gauges[:6]
kpi_cols = st.columns(6)
for i, s in enumerate(top_stations):
    info = gauge_latest[s["id"]]
    flow = info["flow"]
    _, color = gauge_alert(flow)
    flow_str   = f"{flow:.1f} m³/s" if not np.isnan(flow) else "—"
    short_name = s["name"].split(" (")[0][:16]
    with kpi_cols[i]:
        st.markdown(f"""
<div style="background:#0d1b2a;border:2px solid {color};border-radius:10px;
            padding:0.6rem;text-align:center">
  <div style="color:#90e0ef;font-size:0.65rem;font-weight:700;text-transform:uppercase">{short_name}</div>
  <div style="color:white;font-size:1.2rem;font-weight:800;margin:0.2rem 0">{flow_str}</div>
  <div style="color:{color};font-size:0.85rem">{info['trend']}</div>
</div>""", unsafe_allow_html=True)

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
    folium.TileLayer("CartoDB positron", name="🗺️ Clean", overlay=False, control=True).add_to(m)

    # ── Watershed polygon ──
    folium.GeoJson(
        WATERSHED_GEOJSON,
        name="🌍 Watershed boundary",
        style_function=lambda _: {
            "fillColor":   "#0096c7",
            "fillOpacity": 0.07,
            "color":       "#0096c7",
            "weight":      2,
            "dashArray":   "6 4",
        },
        tooltip=folium.GeoJsonTooltip(fields=["name"], aliases=[""]),
    ).add_to(m)

    # ── River paths ──
    def river_style(feature):
        t = feature["properties"].get("type", "tributary")
        return {
            "color":   "#1e90ff" if t == "main" else "#48cae4",
            "weight":  3 if t == "main" else 1.8,
            "opacity": 0.75,
        }

    folium.GeoJson(
        RIVERS_GEOJSON,
        name="🌊 River network",
        style_function=river_style,
        tooltip=folium.GeoJsonTooltip(fields=["name"], aliases=[""]),
    ).add_to(m)

    # ── Gauge station markers (KPI card style) ──
    gauge_fg = folium.FeatureGroup(name="💧 Gauge stations", show=True)
    for s in gauge_stations:
        info = gauge_latest[s["id"]]
        flow = info["flow"]
        trend = info["trend"]
        alert_lvl, color = gauge_alert(flow)
        flow_str   = f"{flow:.1f}" if not np.isnan(flow) else "—"
        short_name = s["name"].split(" (")[0][:16]
        river_name = s.get("river", "")

        # KPI card DivIcon
        card_html = f"""
        <div style="background:#0d1b2a;border:2px solid {color};border-radius:8px;
                    padding:4px 8px;min-width:88px;text-align:center;white-space:nowrap;
                    box-shadow:2px 3px 8px rgba(0,0,0,0.7);font-family:sans-serif">
          <div style="color:#90e0ef;font-size:9px;font-weight:700;text-transform:uppercase;
                      letter-spacing:0.04em">💧 {short_name}</div>
          <div style="color:white;font-size:15px;font-weight:900;margin:1px 0;
                      line-height:1">{flow_str} m³/s</div>
          <div style="color:{color};font-size:11px">{trend}</div>
        </div>"""

        popup_html = f"""
        <div style="font-family:sans-serif;min-width:180px">
          <b style="font-size:13px">{s['name']}</b><br>
          <span style="color:grey;font-size:11px">{river_name} · {s.get('sub_basin','').replace('_',' ').title()}</span>
          <hr style="margin:4px 0">
          <table style="font-size:12px;width:100%">
            <tr><td><b>Flow</b></td><td>{flow_str} m³/s {trend}</td></tr>
            <tr><td><b>Stage</b></td>
                <td>{"%.3f m" % info['level'] if not np.isnan(info['level']) else "—"}</td></tr>
            <tr><td><b>Status</b></td>
                <td><span style="color:{color}">{alert_lvl.replace('_',' ').title()}</span></td></tr>
          </table>
        </div>"""

        folium.Marker(
            location=[s["lat"], s["lon"]],
            icon=folium.DivIcon(
                html=card_html,
                icon_size=(95, 58),
                icon_anchor=(47, 29),
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

        card_html = f"""
        <div style="background:#03045e;border:2px solid {color};border-radius:8px;
                    padding:4px 8px;min-width:80px;text-align:center;white-space:nowrap;
                    box-shadow:2px 3px 8px rgba(0,0,0,0.7);font-family:sans-serif">
          <div style="color:#90e0ef;font-size:9px;font-weight:700;text-transform:uppercase">
            🏔️ {short}</div>
          <div style="color:white;font-size:15px;font-weight:900;margin:1px 0;line-height:1">
            {pct_str}</div>
          <div style="color:grey;font-size:9px">{vol_str}</div>
        </div>"""

        popup_html = f"""
        <div style="font-family:sans-serif;min-width:180px">
          <b style="font-size:13px">🏔️ {r['name']}</b><br>
          <span style="color:grey;font-size:11px">{r.get('river','')} reservoir</span>
          <hr style="margin:4px 0">
          <table style="font-size:12px;width:100%">
            <tr><td><b>Storage</b></td><td>{pct_str}</td></tr>
            <tr><td><b>Volume</b></td><td>{vol_str}</td></tr>
            <tr><td><b>Capacity</b></td>
                <td>{r.get('capacity_hm3','?')} hm³</td></tr>
          </table>
        </div>"""

        folium.Marker(
            location=[r["lat"], r["lon"]],
            icon=folium.DivIcon(
                html=card_html,
                icon_size=(85, 58),
                icon_anchor=(42, 29),
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

        card_html = f"""
        <div style="background:#1a1a2e;border:2px solid #2ca02c;border-radius:8px;
                    padding:4px 8px;min-width:75px;text-align:center;white-space:nowrap;
                    box-shadow:2px 3px 8px rgba(0,0,0,0.7);font-family:sans-serif">
          <div style="color:#90e0ef;font-size:9px;font-weight:700;text-transform:uppercase">
            ⛅ {short}</div>
          <div style="color:white;font-size:14px;font-weight:900;margin:1px 0;line-height:1">
            {temp_str}</div>
          <div style="color:grey;font-size:9px">💧 {prec_str}</div>
        </div>"""

        popup_html = f"""
        <div style="font-family:sans-serif">
          <b>⛅ {mt['name']}</b><br>
          <span style="color:grey;font-size:11px">AEMET station {mt['id']}</span>
          <hr style="margin:4px 0">
          <span style="font-size:12px">🌡️ {temp_str} &nbsp; 💧 {prec_str}</span>
        </div>"""

        folium.Marker(
            location=[mt["lat"], mt["lon"]],
            icon=folium.DivIcon(
                html=card_html,
                icon_size=(80, 55),
                icon_anchor=(40, 27),
            ),
            popup=folium.Popup(popup_html, max_width=200),
        ).add_to(meteo_fg)
    meteo_fg.add_to(m)

    folium.LayerControl(position="topright", collapsed=False).add_to(m)

    st_folium(m, use_container_width=True, height=620, returned_objects=[])

    # Legend
    st.markdown("""
<div style="display:flex;gap:1rem;flex-wrap:wrap;font-size:0.8rem;margin-top:0.5rem">
  <span>💧 <b>Gauge station</b> — flow m³/s</span>
  <span>🏔️ <b>Reservoir</b> — % capacity</span>
  <span>⛅ <b>Weather station</b> — temperature</span>
  <span style="color:#c0392b">🔴 Flood warning</span>
  <span style="color:#e67e22">🟠 Flood watch</span>
  <span style="color:#27ae60">🟢 Normal</span>
  <span style="color:#8e44ad">🟣 Low flow</span>
  <span style="color:#95a5a6">⚫ No data</span>
</div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — STATUS TABLE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_status:
    st.subheader("All stations — current status")

    BADGE = {
        "critical": "🔴 Flood warning",
        "watch":    "🟠 Watch",
        "normal":   "🟢 Normal",
        "low_flow": "🟣 Low flow",
        "no_data":  "⚫ No data",
        "full":     "🟢 Full",
    }

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
            "Status":   BADGE.get(alv, alv),
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
            "Status": BADGE.get(alv, alv),
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
            "Status": "🟢 Active" if not np.isnan(temp) else "⚫ No data",
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.caption("💧 Gauge data: ACA Sentilo · ⛅ Meteo: AEMET OpenData · Cache refreshes every 30 min.")
