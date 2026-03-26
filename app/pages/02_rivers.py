"""
Page 2 — River Flows

Layout:
- Hero: Sant Joan Despí (main channel near Barcelona) with large KPI
- Full-width Folium map with KPI card markers at each station position
- Tabs: hydrograph detail | multi-station comparison | sub-basin table
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import folium
from streamlit_folium import st_folium
from pathlib import Path
import yaml
import numpy as np

st.set_page_config(page_title="River Flows — Llobregat", layout="wide")

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

C_WATER  = "#0096c7"
C_FLOOD  = "#c0392b"
C_WATCH  = "#e67e22"
C_LOW    = "#8e44ad"
C_NORMAL = "#27ae60"
C_NODATA = "#95a5a6"

BASIN_ORDER = {"lower_llobregat": 0, "anoia": 1, "middle_llobregat": 2,
               "cardener": 3, "upper_llobregat": 4, "other": 5}

SUB_BASIN_LABELS = {
    "upper_llobregat": "Upper Llobregat",
    "middle_llobregat": "Middle Llobregat",
    "lower_llobregat": "Lower Llobregat (Barcelona)",
    "cardener": "Cardener",
    "anoia": "Anoia",
}

@st.cache_data(ttl=1800)
def load_gauge_data(station_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"flow_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

@st.cache_data(ttl=3600)
def load_metadata() -> tuple[list, dict]:
    meta_path  = CONFIG_DIR / "station_metadata.yaml"
    thresh_path = CONFIG_DIR / "thresholds.yaml"
    stations, thresholds = [], {}
    if meta_path.exists():
        with open(meta_path) as f:
            stations = yaml.safe_load(f).get("gauge_stations", [])
    if thresh_path.exists():
        with open(thresh_path) as f:
            thresholds = yaml.safe_load(f)
    return stations, thresholds

stations, thresholds = load_metadata()
flow_thresh = thresholds.get("flow_alert_m3s", {}).get("defaults", {})
flood_warn  = flow_thresh.get("flood_warning")
flood_watch = flow_thresh.get("flood_watch")
low_flow    = flow_thresh.get("low_flow_warning")

if not stations:
    st.warning("No gauge stations configured.")
    st.stop()

# BCN-first ordering
sorted_stations = sorted(stations,
    key=lambda s: (BASIN_ORDER.get(s.get("sub_basin", "other"), 5), s.get("priority", 9)))

def flow_alert(flow):
    if pd.isna(flow):                              return "no_data",  C_NODATA
    if flood_warn  and flow >= flood_warn:         return "critical", C_FLOOD
    if flood_watch and flow >= flood_watch:        return "watch",    C_WATCH
    if low_flow    and flow <= low_flow:           return "low_flow", C_LOW
    return "normal", C_NORMAL

def get_latest(station_id: str):
    df = load_gauge_data(station_id)
    if df.empty:
        return np.nan, np.nan, "→"
    df2 = df.sort_values("timestamp")
    flow  = float(df2["flow_m3s"].iloc[-1]) if "flow_m3s" in df2.columns else np.nan
    level = float(df2["level_m"].iloc[-1])  if "level_m"  in df2.columns else np.nan
    prev  = df2["flow_m3s"].iloc[-min(len(df2), 12)] if len(df2) >= 2 else flow
    trend = "↑" if flow > prev*1.05 else ("↓" if flow < prev*0.95 else "→")
    return flow, level, trend

station_latest = {s["id"]: dict(zip(("flow","level","trend"), get_latest(s["id"])))
                  for s in stations}

# ── Sant Joan Despí hero ───────────────────────────────────────────────────────
sjd = next((s for s in stations if "Sant Joan Desp" in s["name"]), sorted_stations[0])
sjd_info = station_latest[sjd["id"]]
sjd_flow  = sjd_info["flow"]
sjd_level = sjd_info["level"]
sjd_trend = sjd_info["trend"]
sjd_alert, sjd_color = flow_alert(sjd_flow)

BADGE_LABELS = {"critical": "🔴 Flood warning", "watch": "🟠 Flood watch",
                "normal": "🟢 Normal", "low_flow": "🟣 Low flow", "no_data": "⚫ No data"}

# System alert
all_alerts = [flow_alert(v["flow"])[0] for v in station_latest.values()]
sys_alert  = ("critical" if "critical" in all_alerts else
              "watch"    if "watch"    in all_alerts else
              "no_data"  if all(a == "no_data" for a in all_alerts) else
              "low_flow" if "low_flow" in all_alerts else "normal")
_, sys_color = flow_alert(0 if sys_alert == "normal" else
                          (flood_warn or 999) if sys_alert == "critical" else np.nan)
_, sys_color = {"critical": (None, C_FLOOD), "watch": (None, C_WATCH),
                "low_flow": (None, C_LOW), "normal": (None, C_NORMAL),
                "no_data": (None, C_NODATA)}[sys_alert]

n_active = sum(1 for v in station_latest.values() if not np.isnan(v["flow"]))

st.markdown(f"""
<div style="background:linear-gradient(135deg,#023e8a,#0077b6);
            padding:1.4rem 2rem;border-radius:12px;margin-bottom:0.8rem;
            display:flex;align-items:center;justify-content:space-between">
  <div>
    <h1 style="color:white;margin:0;font-size:1.8rem">🌊 River Flows</h1>
    <p style="color:#90e0ef;margin:0.3rem 0 0;font-size:0.9rem">
      {n_active} of {len(stations)} gauge stations active · Main channel: Sant Joan Despí
    </p>
  </div>
  <div style="text-align:right">
    <div style="background:{sys_color};color:white;padding:0.4rem 1rem;
                border-radius:20px;font-weight:700;font-size:0.95rem">
      {BADGE_LABELS[sys_alert]}
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Sant Joan Despí featured KPI ───────────────────────────────────────────────
col_hero, col_rest = st.columns([1, 2])
with col_hero:
    sjd_flow_str = f"{sjd_flow:.2f} m³/s" if not np.isnan(sjd_flow) else "—"
    sjd_lvl_str  = f"{sjd_level:.3f} m"   if not np.isnan(sjd_level) else "—"
    st.markdown(f"""
<div style="background:#0d1b2a;border:3px solid {sjd_color};border-radius:12px;
            padding:1rem 1.2rem;text-align:center">
  <div style="color:#90e0ef;font-size:0.75rem;font-weight:700;text-transform:uppercase;
              letter-spacing:0.06em">📍 Sant Joan Despí · Main channel (Barcelona)</div>
  <div style="color:white;font-size:2.2rem;font-weight:900;margin:0.4rem 0">
    {sjd_flow_str} {sjd_trend}
  </div>
  <div style="color:#aaa;font-size:0.9rem">Stage: {sjd_lvl_str}</div>
  <div style="margin-top:0.5rem">
    <span style="background:{sjd_color};color:white;padding:3px 12px;border-radius:12px;
                 font-size:0.8rem;font-weight:600">{BADGE_LABELS[sjd_alert]}</span>
  </div>
</div>""", unsafe_allow_html=True)

with col_rest:
    # Mini KPI strip for other BCN-area stations
    bcn_stations = [s for s in sorted_stations
                    if s["id"] != sjd["id"] and
                    s.get("sub_basin") in ("lower_llobregat", "anoia")][:4]
    if bcn_stations:
        st.markdown("<div style='color:#90e0ef;font-size:0.75rem;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:0.4rem'>Other lower basin stations</div>", unsafe_allow_html=True)
        c2 = st.columns(len(bcn_stations))
        for i, s in enumerate(bcn_stations):
            info  = station_latest[s["id"]]
            flow  = info["flow"]
            _, c  = flow_alert(flow)
            flow_s = f"{flow:.1f}" if not np.isnan(flow) else "—"
            short  = s["name"].split(" (")[0][:14]
            with c2[i]:
                st.markdown(f"""
<div style="background:#0d1b2a;border:2px solid {c};border-radius:8px;
            padding:0.5rem;text-align:center">
  <div style="color:#90e0ef;font-size:0.6rem;font-weight:700;text-transform:uppercase">{short}</div>
  <div style="color:white;font-size:1.1rem;font-weight:800">{flow_s} m³/s</div>
  <div style="color:{c};font-size:0.8rem">{info['trend']}</div>
</div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Interactive map with KPI card markers ──────────────────────────────────────
st.markdown("#### 📍 Station map — click a marker for details")

river_coords = {
    "Llobregat": [[1.97,42.21],[1.88,42.12],[1.878,42.08],[1.880,41.86],
                  [1.858,41.65],[1.948,41.47],[1.933,41.48],[2.023,41.39],
                  [2.047,41.35],[2.070,41.30]],
    "Cardener":  [[1.583,42.10],[1.606,41.96],[1.696,41.92],
                  [1.763,41.81],[1.830,41.73],[1.858,41.65]],
    "Anoia":     [[1.788,41.44],[1.929,41.48],[1.948,41.47]],
}

m = folium.Map(location=[41.72, 1.88], zoom_start=9, tiles=None)

folium.TileLayer("CartoDB positron", name="🗺️ Clean", overlay=False, control=True).add_to(m)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri", name="🛰️ Satellite", overlay=False, control=True,
).add_to(m)

# River paths
river_colors = {"Llobregat": "#1e90ff", "Cardener": "#48cae4", "Anoia": "#90e0ef"}
for river_name, coords in river_coords.items():
    folium.PolyLine(
        locations=[[c[1], c[0]] for c in coords],
        color=river_colors.get(river_name, "#48cae4"),
        weight=3 if river_name == "Llobregat" else 2,
        opacity=0.7,
        tooltip=river_name,
    ).add_to(m)

# Station KPI card markers
for s in sorted_stations:
    info  = station_latest[s["id"]]
    flow  = info["flow"]
    trend = info["trend"]
    level = info["level"]
    _, color = flow_alert(flow)
    alert_lbl, _ = flow_alert(flow)
    flow_str  = f"{flow:.1f}" if not np.isnan(flow) else "—"
    level_str = f"{level:.3f} m" if not np.isnan(level) else "—"
    short     = s["name"].split(" (")[0][:15]
    is_sjd    = s["id"] == sjd["id"]

    # Slightly larger card for Sant Joan Despí
    border_w = "3px" if is_sjd else "2px"
    font_sz  = "16px" if is_sjd else "14px"

    card_html = f"""
    <div style="background:#0d1b2a;border:{border_w} solid {color};border-radius:8px;
                padding:4px 8px;min-width:90px;text-align:center;white-space:nowrap;
                box-shadow:2px 3px 10px rgba(0,0,0,0.75);font-family:sans-serif">
      <div style="color:#90e0ef;font-size:9px;font-weight:700;text-transform:uppercase;
                  letter-spacing:0.03em">💧 {short}</div>
      <div style="color:white;font-size:{font_sz};font-weight:900;margin:1px 0;line-height:1">
        {flow_str} m³/s</div>
      <div style="color:{color};font-size:11px">{trend}</div>
    </div>"""

    popup_html = f"""
    <div style="font-family:sans-serif;min-width:190px">
      <b style="font-size:13px">{s['name']}</b><br>
      <span style="color:grey;font-size:11px">{s.get('river','')} ·
        {s.get('sub_basin','').replace('_',' ').title()}</span>
      <hr style="margin:5px 0">
      <table style="font-size:12px;width:100%">
        <tr><td><b>Flow</b></td><td>{flow_str} m³/s {trend}</td></tr>
        <tr><td><b>Stage</b></td><td>{level_str}</td></tr>
        <tr><td><b>Alert</b></td>
            <td style="color:{color}">{alert_lbl.replace('_',' ').title()}</td></tr>
        <tr><td><b>ID</b></td><td style="color:grey">{s['id']}</td></tr>
      </table>
    </div>"""

    folium.Marker(
        location=[s["lat"], s["lon"]],
        icon=folium.DivIcon(html=card_html, icon_size=(95, 56), icon_anchor=(47, 28)),
        popup=folium.Popup(popup_html, max_width=220),
    ).add_to(m)

folium.LayerControl(position="topright", collapsed=True).add_to(m)
st_folium(m, use_container_width=True, height=500, returned_objects=[])

st.divider()

# ── Tabs: detail / comparison / sub-basin ─────────────────────────────────────
tab_detail, tab_compare, tab_basin = st.tabs([
    "📈 Station Detail",
    "🔀 Multi-station Comparison",
    "🗂️ Sub-basin Summary",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: Station detail — default to Sant Joan Despí
# ═══════════════════════════════════════════════════════════════════════════════
with tab_detail:
    station_options = {s["name"]: s["id"] for s in sorted_stations}
    default_idx = list(station_options.keys()).index(sjd["name"]) if sjd["name"] in station_options else 0
    selected_name = st.selectbox("Select gauge station", list(station_options.keys()),
                                  index=default_idx)
    selected_id   = station_options[selected_name]
    selected_meta = next(s for s in stations if s["id"] == selected_id)

    df = load_gauge_data(selected_id)
    if df.empty:
        st.warning(f"No cached data for **{selected_name}** ({selected_id}).")
    else:
        if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["ts"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
        else:
            df["ts"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("ts")

        latest   = df.iloc[-1]
        lat_flow = latest.get("flow_m3s", np.nan)
        lat_lvl  = latest.get("level_m",  np.nan)
        alv, _   = flow_alert(lat_flow)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Latest flow",  f"{lat_flow:.2f} m³/s" if not pd.isna(lat_flow) else "—")
        c2.metric("Latest stage", f"{lat_lvl:.3f} m"     if not pd.isna(lat_lvl)  else "—")
        df_24h = df[df["ts"] >= df["ts"].max() - pd.Timedelta(hours=24)]
        if not df_24h.empty and "flow_m3s" in df_24h.columns:
            c3.metric("24h peak",  f"{df_24h['flow_m3s'].max():.2f} m³/s")
            c4.metric("24h mean",  f"{df_24h['flow_m3s'].mean():.2f} m³/s")

        fig = go.Figure()
        y_max  = df["flow_m3s"].max() if "flow_m3s" in df.columns else 10
        y_ceil = max(y_max * 1.2, 5)

        if flood_warn:
            fig.add_hrect(y0=flood_warn, y1=y_ceil, fillcolor="rgba(192,57,43,0.08)",
                          line_width=0, annotation_text="🔴 Flood warning",
                          annotation_position="top left", annotation_font_color=C_FLOOD)
        if flood_watch and flood_warn:
            fig.add_hrect(y0=flood_watch, y1=flood_warn, fillcolor="rgba(230,126,34,0.06)",
                          line_width=0, annotation_text="🟠 Watch",
                          annotation_position="top left", annotation_font_color=C_WATCH)
        if low_flow:
            fig.add_hrect(y0=0, y1=low_flow, fillcolor="rgba(142,68,173,0.06)",
                          line_width=0, annotation_text="🟣 Low flow",
                          annotation_position="bottom left", annotation_font_color=C_LOW)

        fig.add_trace(go.Scatter(
            x=df["ts"], y=df["flow_m3s"], mode="lines", name="Flow",
            fill="tozeroy", line=dict(color=C_WATER, width=2),
            fillcolor="rgba(0,150,199,0.15)",
            hovertemplate="%{x|%d %b %H:%M}<br><b>%{y:.2f} m³/s</b><extra></extra>",
        ))
        if flood_warn:
            fig.add_hline(y=flood_warn, line_dash="dash", line_color=C_FLOOD,
                          annotation_text="Flood warning", annotation_position="top right")
        if flood_watch:
            fig.add_hline(y=flood_watch, line_dash="dot", line_color=C_WATCH,
                          annotation_text="Watch", annotation_position="top right")
        if low_flow:
            fig.add_hline(y=low_flow, line_dash="dot", line_color=C_LOW,
                          annotation_text="Low flow", annotation_position="bottom right")

        fig.update_layout(xaxis_title="Time", yaxis_title="Flow (m³/s)",
                          yaxis=dict(range=[0, y_ceil]), hovermode="x unified",
                          height=400, margin=dict(t=20, b=40), template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

        if "level_m" in df.columns and not df["level_m"].isna().all():
            fig2 = go.Figure(go.Scatter(
                x=df["ts"], y=df["level_m"], mode="lines", fill="tozeroy",
                line=dict(color="#2ca02c", width=1.8), fillcolor="rgba(44,160,44,0.12)",
                hovertemplate="%{x|%d %b %H:%M}<br><b>%{y:.3f} m</b><extra></extra>",
            ))
            fig2.update_layout(xaxis_title="Time", yaxis_title="Stage (m)",
                               height=280, margin=dict(t=20, b=40), template="plotly_white")
            st.plotly_chart(fig2, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: Multi-station comparison
# ═══════════════════════════════════════════════════════════════════════════════
with tab_compare:
    all_names = [s["name"] for s in sorted_stations]
    p1_names  = [s["name"] for s in sorted_stations if s.get("priority", 9) == 1]
    selected_multi = st.multiselect("Stations to compare", all_names, default=p1_names)

    if selected_multi:
        sel_map = {s["name"]: s["id"] for s in stations if s["name"] in selected_multi}
        fig_c   = go.Figure()
        colors  = px.colors.qualitative.Plotly
        for idx, (name, sid) in enumerate(sel_map.items()):
            df_s = load_gauge_data(sid)
            if df_s.empty:
                continue
            if pd.api.types.is_datetime64_any_dtype(df_s["timestamp"]):
                df_s["ts"] = df_s["timestamp"].dt.tz_convert("Europe/Madrid")
            else:
                df_s["ts"] = pd.to_datetime(df_s["timestamp"])
            fig_c.add_trace(go.Scatter(
                x=df_s.sort_values("ts")["ts"],
                y=df_s.sort_values("ts")["flow_m3s"],
                mode="lines", name=name.split(" (")[0],
                line=dict(color=colors[idx % len(colors)], width=1.8),
                hovertemplate=f"<b>{name.split('(')[0].strip()}</b><br>%{{y:.2f}} m³/s<extra></extra>",
            ))
        if flood_warn:
            fig_c.add_hline(y=flood_warn, line_dash="dash", line_color=C_FLOOD,
                            annotation_text="Flood warning")
        fig_c.update_layout(xaxis_title="Time", yaxis_title="Flow (m³/s)",
                             hovermode="x unified", height=460,
                             template="plotly_white",
                             legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig_c, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: Sub-basin summary
# ═══════════════════════════════════════════════════════════════════════════════
with tab_basin:
    basins: dict[str, list] = {}
    for s in sorted_stations:
        b = s.get("sub_basin", "other")
        basins.setdefault(b, []).append(s)

    for basin_key, basin_stations in basins.items():
        label = SUB_BASIN_LABELS.get(basin_key, basin_key.replace("_", " ").title())
        with st.expander(f"**{label}** ({len(basin_stations)} stations)", expanded=(basin_key == "lower_llobregat")):
            rows = []
            for s in basin_stations:
                info  = station_latest[s["id"]]
                flow  = info["flow"]
                alv, _ = flow_alert(flow)
                badge = {"critical": "🔴 Flood warning", "watch": "🟠 Watch",
                         "low_flow": "🟣 Low flow", "normal": "🟢 Normal",
                         "no_data": "⚫ No data"}
                rows.append({
                    "Station":     s["name"],
                    "River":       s.get("river", "—"),
                    "Flow (m³/s)": f"{flow:.2f}" if not np.isnan(flow) else "—",
                    "Stage (m)":   f"{info['level']:.3f}" if not np.isnan(info["level"]) else "—",
                    "Trend":       info["trend"],
                    "Status":      badge.get(alv, alv),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
