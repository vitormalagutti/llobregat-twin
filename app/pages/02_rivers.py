"""
Page 2 — River Flows
IBM Carbon Design System — Gray 100 dark theme

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

st.set_page_config(page_title="River Flows — Llobregat", layout="wide")
inject()

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

BASIN_ORDER = {"lower_llobregat": 0, "anoia": 1, "middle_llobregat": 2,
               "cardener": 3, "upper_llobregat": 4, "other": 5}

SUB_BASIN_LABELS = {
    "upper_llobregat":  "Upper Llobregat",
    "middle_llobregat": "Middle Llobregat",
    "lower_llobregat":  "Lower Llobregat (Barcelona)",
    "cardener":         "Cardener",
    "anoia":            "Anoia",
}

# ── Plotly dark template matching Carbon ──────────────────────────────────────
DARK_LAYOUT = dict(
    paper_bgcolor=BG,
    plot_bgcolor=LAYER_01,
    font=dict(family=FONT_SANS, color=TEXT_SECONDARY),
    xaxis=dict(gridcolor=LAYER_02, linecolor=BORDER, tickcolor=TEXT_DISABLED),
    yaxis=dict(gridcolor=LAYER_02, linecolor=BORDER, tickcolor=TEXT_DISABLED),
)

@st.cache_data(ttl=1800)
def load_gauge_data(station_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"flow_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

@st.cache_data(ttl=3600)
def load_metadata() -> tuple[list, dict]:
    meta_path   = CONFIG_DIR / "station_metadata.yaml"
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
    if flood_warn  and flow >= flood_warn:         return "critical", C_CRITICAL
    if flood_watch and flow >= flood_watch:        return "watch",    C_WATCH
    if low_flow    and flow <= low_flow:           return "low_flow", C_LOW_FLOW
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
sjd      = next((s for s in stations if "Sant Joan Desp" in s["name"]), sorted_stations[0])
sjd_info = station_latest[sjd["id"]]
sjd_flow  = sjd_info["flow"]
sjd_level = sjd_info["level"]
sjd_trend = sjd_info["trend"]
sjd_alert, sjd_color = flow_alert(sjd_flow)

# System alert
all_alerts = [flow_alert(v["flow"])[0] for v in station_latest.values()]
sys_alert  = ("critical" if "critical" in all_alerts else
              "watch"    if "watch"    in all_alerts else
              "no_data"  if all(a == "no_data" for a in all_alerts) else
              "low_flow" if "low_flow" in all_alerts else "normal")
_, sys_color = {"critical": (None, C_CRITICAL), "watch": (None, C_WATCH),
                "low_flow": (None, C_LOW_FLOW), "normal": (None, C_NORMAL),
                "no_data":  (None, C_NODATA)}[sys_alert]

n_active = sum(1 for v in station_latest.values() if not np.isnan(v["flow"]))

# ── Hero banner (Carbon) ───────────────────────────────────────────────────────
sjd_flow_str = f"{sjd_flow:.2f} m³/s" if not np.isnan(sjd_flow) else "—"
sjd_lvl_str  = f"{sjd_level:.3f} m"   if not np.isnan(sjd_level) else "—"

st.markdown(hero(
    title="River Flows",
    subtitle=f"{n_active} of {len(stations)} gauge stations active · Main channel: Sant Joan Despí",
    status_text=status_label(sys_alert),
    status_color=sys_color,
), unsafe_allow_html=True)

# ── Sant Joan Despí featured KPI + other BCN stations ─────────────────────────
col_hero, col_rest = st.columns([1, 2])
with col_hero:
    st.markdown(f"""
<div style="background:{LAYER_01};border-left:4px solid {sjd_color};
            padding:1.2rem 1.4rem;margin-bottom:0.5rem">
  <div style="font-family:{FONT_SANS};color:{TEXT_SECONDARY};font-size:11px;
              font-weight:600;text-transform:uppercase;letter-spacing:0.08em;
              margin-bottom:6px">📍 Sant Joan Despí · Main channel</div>
  <div style="font-family:{FONT_MONO};color:{TEXT_PRIMARY};font-size:2rem;
              font-weight:400;line-height:1.1">{sjd_flow_str}</div>
  <div style="font-family:{FONT_MONO};color:{TEXT_SECONDARY};font-size:0.9rem;
              margin-top:4px">{sjd_trend} &nbsp; Stage: {sjd_lvl_str}</div>
  <div style="margin-top:10px">{badge(status_label(sjd_alert), sjd_color)}</div>
</div>""", unsafe_allow_html=True)

with col_rest:
    bcn_stations = [s for s in sorted_stations
                    if s["id"] != sjd["id"] and
                    s.get("sub_basin") in ("lower_llobregat", "anoia")][:4]
    if bcn_stations:
        st.markdown(f"<div style='font-family:{FONT_SANS};color:{TEXT_SECONDARY};font-size:11px;"
                    f"font-weight:600;text-transform:uppercase;letter-spacing:0.08em;"
                    f"margin-bottom:8px'>Other lower basin stations</div>", unsafe_allow_html=True)
        c2 = st.columns(len(bcn_stations))
        for i, s in enumerate(bcn_stations):
            info  = station_latest[s["id"]]
            flow  = info["flow"]
            _, c  = flow_alert(flow)
            flow_s = f"{flow:.1f}" if not np.isnan(flow) else "—"
            short  = s["name"].split(" (")[0][:14]
            with c2[i]:
                st.markdown(kpi_card(
                    label=short,
                    value=f"{flow_s} m³/s",
                    trend=info["trend"],
                    color=c,
                ), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Interactive map with KPI card markers ──────────────────────────────────────
st.markdown(f"<p style='font-family:{FONT_SANS};color:{TEXT_SECONDARY};font-size:13px;"
            f"margin-bottom:8px'>📍 Station map — click a marker for details</p>",
            unsafe_allow_html=True)

river_coords = {
    "Llobregat": [[1.97,42.21],[1.88,42.12],[1.878,42.08],[1.880,41.86],
                  [1.858,41.65],[1.948,41.47],[1.933,41.48],[2.023,41.39],
                  [2.047,41.35],[2.070,41.30]],
    "Cardener":  [[1.583,42.10],[1.606,41.96],[1.696,41.92],
                  [1.763,41.81],[1.830,41.73],[1.858,41.65]],
    "Anoia":     [[1.788,41.44],[1.929,41.48],[1.948,41.47]],
}

m = folium.Map(location=[41.72, 1.88], zoom_start=9, tiles=None)
folium.TileLayer("CartoDB dark_matter", name="🗺️ Dark", overlay=False, control=True).add_to(m)
folium.TileLayer("CartoDB positron",    name="🗺️ Light", overlay=False, control=True).add_to(m)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri", name="🛰️ Satellite", overlay=False, control=True,
).add_to(m)

# River paths
river_colors = {"Llobregat": "#4589ff", "Cardener": "#78a9ff", "Anoia": "#a6c8ff"}
for river_name, coords in river_coords.items():
    folium.PolyLine(
        locations=[[c[1], c[0]] for c in coords],
        color=river_colors.get(river_name, BLUE_40),
        weight=3 if river_name == "Llobregat" else 2,
        opacity=0.8,
        tooltip=river_name,
    ).add_to(m)

# Station KPI card markers (Carbon style)
for s in sorted_stations:
    info    = station_latest[s["id"]]
    flow    = info["flow"]
    trend   = info["trend"]
    level   = info["level"]
    alert_lbl, color = flow_alert(flow)
    flow_str  = f"{flow:.1f}" if not np.isnan(flow) else "—"
    level_str = f"{level:.3f} m" if not np.isnan(level) else "—"
    short     = s["name"].split(" (")[0][:15]
    is_sjd    = s["id"] == sjd["id"]

    card_html = map_kpi(
        label=f"💧 {short}",
        value=f"{flow_str} m³/s",
        trend=trend,
        color=color,
    )

    popup_html = f"""
    <div style="font-family:{FONT_SANS};min-width:190px;background:{LAYER_01};
                padding:10px;border-top:3px solid {color}">
      <b style="font-size:13px;color:{TEXT_PRIMARY}">{s['name']}</b><br>
      <span style="color:{TEXT_SECONDARY};font-size:11px">{s.get('river','')} ·
        {s.get('sub_basin','').replace('_',' ').title()}</span>
      <hr style="border-color:{BORDER};margin:6px 0">
      <table style="font-size:12px;width:100%;color:{TEXT_PRIMARY}">
        <tr><td style="color:{TEXT_SECONDARY}">Flow</td><td>{flow_str} m³/s {trend}</td></tr>
        <tr><td style="color:{TEXT_SECONDARY}">Stage</td><td>{level_str}</td></tr>
        <tr><td style="color:{TEXT_SECONDARY}">Status</td>
            <td style="color:{color}">{status_label(alert_lbl)}</td></tr>
        <tr><td style="color:{TEXT_SECONDARY}">ID</td>
            <td style="color:{TEXT_DISABLED}">{s['id']}</td></tr>
      </table>
    </div>"""

    folium.Marker(
        location=[s["lat"], s["lon"]],
        icon=folium.DivIcon(html=card_html, icon_size=(110, 52), icon_anchor=(55, 26)),
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

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Latest flow",  f"{lat_flow:.2f} m³/s" if not pd.isna(lat_flow) else "—")
        c2.metric("Latest stage", f"{lat_lvl:.3f} m"     if not pd.isna(lat_lvl)  else "—")
        df_24h = df[df["ts"] >= df["ts"].max() - pd.Timedelta(hours=24)]
        if not df_24h.empty and "flow_m3s" in df_24h.columns:
            c3.metric("24h peak",  f"{df_24h['flow_m3s'].max():.2f} m³/s")
            c4.metric("24h mean",  f"{df_24h['flow_m3s'].mean():.2f} m³/s")

        y_max  = df["flow_m3s"].max() if "flow_m3s" in df.columns else 10
        y_ceil = max(y_max * 1.2, 5)

        fig = go.Figure()
        if flood_warn:
            fig.add_hrect(y0=flood_warn, y1=y_ceil, fillcolor=f"rgba(218,30,40,0.07)",
                          line_width=0, annotation_text="Flood warning",
                          annotation_position="top left",
                          annotation_font_color=C_CRITICAL)
        if flood_watch and flood_warn:
            fig.add_hrect(y0=flood_watch, y1=flood_warn, fillcolor=f"rgba(241,194,27,0.06)",
                          line_width=0, annotation_text="Watch",
                          annotation_position="top left",
                          annotation_font_color=C_WATCH)
        if low_flow:
            fig.add_hrect(y0=0, y1=low_flow, fillcolor=f"rgba(120,169,255,0.06)",
                          line_width=0, annotation_text="Low flow",
                          annotation_position="bottom left",
                          annotation_font_color=C_LOW_FLOW)

        fig.add_trace(go.Scatter(
            x=df["ts"], y=df["flow_m3s"], mode="lines", name="Flow",
            fill="tozeroy",
            line=dict(color=BLUE_40, width=2),
            fillcolor=f"rgba(120,169,255,0.12)",
            hovertemplate="%{x|%d %b %H:%M}<br><b>%{y:.2f} m³/s</b><extra></extra>",
        ))
        if flood_warn:
            fig.add_hline(y=flood_warn, line_dash="dash", line_color=C_CRITICAL, line_width=1,
                          annotation_text="Flood warning", annotation_position="top right",
                          annotation_font_color=C_CRITICAL)
        if flood_watch:
            fig.add_hline(y=flood_watch, line_dash="dot", line_color=C_WATCH, line_width=1,
                          annotation_text="Watch", annotation_position="top right",
                          annotation_font_color=C_WATCH)
        if low_flow:
            fig.add_hline(y=low_flow, line_dash="dot", line_color=C_LOW_FLOW, line_width=1,
                          annotation_text="Low flow", annotation_position="bottom right",
                          annotation_font_color=C_LOW_FLOW)

        fig.update_layout(
            **DARK_LAYOUT,
            xaxis_title="Time", yaxis_title="Flow (m³/s)",
            yaxis=dict(**DARK_LAYOUT["yaxis"], range=[0, y_ceil]),
            hovermode="x unified", height=400, margin=dict(t=20, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)

        if "level_m" in df.columns and not df["level_m"].isna().all():
            fig2 = go.Figure(go.Scatter(
                x=df["ts"], y=df["level_m"], mode="lines", fill="tozeroy",
                line=dict(color=C_NORMAL, width=2),
                fillcolor="rgba(36,161,72,0.10)",
                hovertemplate="%{x|%d %b %H:%M}<br><b>%{y:.3f} m</b><extra></extra>",
            ))
            fig2.update_layout(
                **DARK_LAYOUT,
                xaxis_title="Time", yaxis_title="Stage (m)",
                height=280, margin=dict(t=20, b=40),
            )
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
        # Carbon-compatible sequential palette
        pal = ["#78a9ff","#33b1ff","#42be65","#f1c21b","#ff7eb6","#be95ff","#fa4d56"]
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
                line=dict(color=pal[idx % len(pal)], width=2),
                hovertemplate=f"<b>{name.split('(')[0].strip()}</b><br>%{{y:.2f}} m³/s<extra></extra>",
            ))
        if flood_warn:
            fig_c.add_hline(y=flood_warn, line_dash="dash", line_color=C_CRITICAL,
                            annotation_text="Flood warning",
                            annotation_font_color=C_CRITICAL)
        fig_c.update_layout(
            **DARK_LAYOUT,
            xaxis_title="Time", yaxis_title="Flow (m³/s)",
            hovermode="x unified", height=460, margin=dict(t=20, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        font=dict(color=TEXT_SECONDARY)),
        )
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
        with st.expander(f"**{label}** ({len(basin_stations)} stations)",
                         expanded=(basin_key == "lower_llobregat")):
            rows = []
            for s in basin_stations:
                info  = station_latest[s["id"]]
                flow  = info["flow"]
                alv, _ = flow_alert(flow)
                rows.append({
                    "Station":     s["name"],
                    "River":       s.get("river", "—"),
                    "Flow (m³/s)": f"{flow:.2f}" if not np.isnan(flow) else "—",
                    "Stage (m)":   f"{info['level']:.3f}" if not np.isnan(info["level"]) else "—",
                    "Trend":       info["trend"],
                    "Status":      status_label(alv),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
