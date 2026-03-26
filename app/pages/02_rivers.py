"""
Page 2 — River Flows  (gamified redesign)

Displays:
- Hero banner with system-level alert status
- KPI strip: all gauges with colour-coded flow status
- Tab 1: Single-station deep dive (hydrograph + stage + alert bands)
- Tab 2: Multi-station comparison chart (all gauges overlaid)
- Tab 3: Sub-basin breakdown table with trend arrows

Data source: reads flow_*.parquet from data/cache/ only.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import yaml
import numpy as np

st.set_page_config(page_title="River Flows — Llobregat", layout="wide")

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

# ── Colour system ──────────────────────────────────────────────────────────────
C_WATER   = "#0096c7"
C_FLOOD   = "#c0392b"
C_WATCH   = "#e67e22"
C_LOW     = "#8e44ad"
C_NORMAL  = "#27ae60"
C_NODATA  = "#95a5a6"

SUB_BASIN_LABELS = {
    "upper_llobregat": "Upper Llobregat",
    "middle_llobregat": "Middle Llobregat",
    "lower_llobregat": "Lower Llobregat",
    "cardener": "Cardener",
    "anoia": "Anoia",
}

# ── Data loaders ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800)
def load_gauge_data(station_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"flow_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

@st.cache_data(ttl=3600)
def load_metadata() -> tuple[list, dict]:
    meta_path = CONFIG_DIR / "station_metadata.yaml"
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
    st.warning("No gauge stations configured. Check config/station_metadata.yaml.")
    st.stop()

# ── Helper: get latest flow for a station ──────────────────────────────────────
def get_latest(station_id: str):
    df = load_gauge_data(station_id)
    if df.empty:
        return None, None, None  # flow, level, trend
    df2 = df.sort_values("timestamp")
    latest_flow = df2["flow_m3s"].iloc[-1] if "flow_m3s" in df2.columns else np.nan
    latest_lvl  = df2["level_m"].iloc[-1]  if "level_m"  in df2.columns else np.nan
    # 6-hour trend
    if len(df2) >= 2:
        prev = df2["flow_m3s"].iloc[-min(len(df2), 12)]
        trend = "↑" if latest_flow > prev * 1.05 else ("↓" if latest_flow < prev * 0.95 else "→")
    else:
        trend = "→"
    return latest_flow, latest_lvl, trend

def flow_alert_level(flow):
    if pd.isna(flow):      return "no_data"
    if flood_warn and flow >= flood_warn:  return "critical"
    if flood_watch and flow >= flood_watch: return "watch"
    if low_flow and flow <= low_flow:       return "low_flow"
    return "normal"

def alert_badge(level: str) -> str:
    cfg = {
        "critical": (C_FLOOD,  "🔴 Flood warning"),
        "watch":    (C_WATCH,  "🟠 Flood watch"),
        "normal":   (C_NORMAL, "🟢 Normal"),
        "low_flow": (C_LOW,    "🟣 Low flow"),
        "no_data":  (C_NODATA, "⚫ No data"),
    }
    color, label = cfg.get(level, (C_NODATA, level))
    return (f'<span style="background:{color};color:white;'
            f'padding:3px 10px;border-radius:12px;font-size:0.8rem;'
            f'font-weight:600">{label}</span>')

# ── Gather all latest values ───────────────────────────────────────────────────
station_latest = {}
for s in stations:
    flow, lvl, trend = get_latest(s["id"])
    station_latest[s["id"]] = {"flow": flow, "level": lvl, "trend": trend,
                                "alert": flow_alert_level(flow)}

# Determine overall system alert
all_alerts = [v["alert"] for v in station_latest.values()]
if "critical" in all_alerts:
    sys_alert, sys_color, sys_emoji = "FLOOD WARNING", C_FLOOD, "🔴"
elif "watch" in all_alerts:
    sys_alert, sys_color, sys_emoji = "FLOOD WATCH",  C_WATCH, "🟠"
elif all(a == "no_data" for a in all_alerts):
    sys_alert, sys_color, sys_emoji = "NO DATA",      C_NODATA, "⚫"
elif "low_flow" in all_alerts:
    sys_alert, sys_color, sys_emoji = "LOW FLOW",     C_LOW, "🟣"
else:
    sys_alert, sys_color, sys_emoji = "NORMAL",       C_NORMAL, "🟢"

# Active stations count
n_active = sum(1 for v in station_latest.values() if v["alert"] != "no_data")
n_total  = len(stations)

# ── Hero banner ────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="background:linear-gradient(135deg,#023e8a,#0077b6);
            padding:1.6rem 2rem;border-radius:12px;margin-bottom:1.2rem;
            display:flex;align-items:center;justify-content:space-between">
  <div>
    <h1 style="color:white;margin:0;font-size:1.9rem">🌊 River Flows</h1>
    <p style="color:#90e0ef;margin:0.3rem 0 0;font-size:0.95rem">
      {n_active} of {n_total} gauge stations active &nbsp;·&nbsp; Llobregat watershed
    </p>
  </div>
  <div style="text-align:right">
    <div style="background:{sys_color};color:white;padding:0.5rem 1.2rem;
                border-radius:20px;font-weight:700;font-size:1rem">
      {sys_emoji} System: {sys_alert}
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── KPI strip — all stations ───────────────────────────────────────────────────
st.markdown("#### Live gauge readings")
kpi_cols = st.columns(min(len(stations), 6))
for i, stn in enumerate(stations[:6]):
    info = station_latest[stn["id"]]
    flow_val = info["flow"]
    trend    = info["trend"]
    alert    = info["alert"]
    badge_colors = {"critical": C_FLOOD, "watch": C_WATCH,
                    "low_flow": C_LOW, "normal": C_NORMAL, "no_data": C_NODATA}
    border_col = badge_colors[alert]
    flow_str = f"{flow_val:.2f} m³/s" if not pd.isna(flow_val) else "— m³/s"
    short_name = stn["name"].split(" (")[0][:18]
    with kpi_cols[i % 6]:
        st.markdown(f"""
<div style="background:#1a2a3a;border:2px solid {border_col};
            border-radius:10px;padding:0.8rem;text-align:center;margin-bottom:0.5rem">
  <div style="color:#90e0ef;font-size:0.7rem;font-weight:600;text-transform:uppercase;
              letter-spacing:0.05em">{short_name}</div>
  <div style="color:white;font-size:1.3rem;font-weight:700;margin:0.25rem 0">
    {flow_str}
  </div>
  <div style="color:{border_col};font-size:1rem">{trend}</div>
</div>
""", unsafe_allow_html=True)

# Second row for remaining stations if any
if len(stations) > 6:
    kpi_cols2 = st.columns(min(len(stations) - 6, 6))
    for i, stn in enumerate(stations[6:12]):
        info = station_latest[stn["id"]]
        flow_val = info["flow"]
        trend    = info["trend"]
        alert    = info["alert"]
        badge_colors = {"critical": C_FLOOD, "watch": C_WATCH,
                        "low_flow": C_LOW, "normal": C_NORMAL, "no_data": C_NODATA}
        border_col = badge_colors[alert]
        flow_str = f"{flow_val:.2f} m³/s" if not pd.isna(flow_val) else "— m³/s"
        short_name = stn["name"].split(" (")[0][:18]
        with kpi_cols2[i % 6]:
            st.markdown(f"""
<div style="background:#1a2a3a;border:2px solid {border_col};
            border-radius:10px;padding:0.8rem;text-align:center;margin-bottom:0.5rem">
  <div style="color:#90e0ef;font-size:0.7rem;font-weight:600;text-transform:uppercase;
              letter-spacing:0.05em">{short_name}</div>
  <div style="color:white;font-size:1.3rem;font-weight:700;margin:0.25rem 0">
    {flow_str}
  </div>
  <div style="color:{border_col};font-size:1rem">{trend}</div>
</div>
""", unsafe_allow_html=True)

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_detail, tab_compare, tab_basin = st.tabs([
    "📈 Station Detail",
    "🔀 Multi-station Comparison",
    "🗂️ Sub-basin Summary",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: Station detail
# ═══════════════════════════════════════════════════════════════════════════════
with tab_detail:
    station_options = {s["name"]: s["id"] for s in stations}
    selected_name = st.selectbox("Select gauge station", list(station_options.keys()))
    selected_id   = station_options[selected_name]
    selected_meta = next(s for s in stations if s["id"] == selected_id)

    df = load_gauge_data(selected_id)

    if df.empty:
        st.warning(
            f"No cached data for **{selected_name}** ({selected_id}). "
            "Run `python -m data.fetchers.refresh_all` to populate the cache."
        )
    else:
        # Timezone convert
        if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["ts"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
        else:
            df["ts"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("ts")

        latest = df.iloc[-1]
        latest_flow  = latest.get("flow_m3s", np.nan)
        latest_level = latest.get("level_m",  np.nan)
        alert_lvl    = flow_alert_level(latest_flow)

        # Station header with badge
        badge_html = alert_badge(alert_lvl)
        sub = SUB_BASIN_LABELS.get(selected_meta.get("sub_basin", ""), "")
        st.markdown(f"""
<div style="display:flex;align-items:center;gap:1rem;margin-bottom:1rem">
  <div>
    <h3 style="margin:0">{selected_name}</h3>
    <span style="color:grey;font-size:0.85rem">{sub} · ID: {selected_id}</span>
  </div>
  <div>{badge_html}</div>
</div>
""", unsafe_allow_html=True)

        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Latest flow", f"{latest_flow:.2f} m³/s" if not pd.isna(latest_flow) else "—")
        col2.metric("Latest stage", f"{latest_level:.2f} m" if not pd.isna(latest_level) else "—")
        # 24h stats
        cutoff_24h = df["ts"].max() - pd.Timedelta(hours=24)
        df_24h = df[df["ts"] >= cutoff_24h]
        if not df_24h.empty and "flow_m3s" in df_24h.columns:
            max_24h = df_24h["flow_m3s"].max()
            mean_24h = df_24h["flow_m3s"].mean()
            col3.metric("24h peak flow", f"{max_24h:.2f} m³/s")
            col4.metric("24h mean flow", f"{mean_24h:.2f} m³/s")
        else:
            col3.metric("Records", f"{len(df):,}")
            col4.metric("Quality", str(latest.get("quality_flag", "—")))

        # ── Flow hydrograph with alert bands ──────────────────────────────────
        st.subheader("Flow hydrograph")
        fig = go.Figure()

        y_max = df["flow_m3s"].max() if "flow_m3s" in df.columns else 100
        y_ceil = max(y_max * 1.2, 10)

        # Alert zone bands
        if flood_warn:
            fig.add_hrect(y0=flood_warn, y1=y_ceil,
                          fillcolor="rgba(192,57,43,0.08)", line_width=0,
                          annotation_text="🔴 Flood warning zone",
                          annotation_position="top left",
                          annotation_font_color=C_FLOOD)
        if flood_watch and flood_warn:
            fig.add_hrect(y0=flood_watch, y1=flood_warn,
                          fillcolor="rgba(230,126,34,0.06)", line_width=0,
                          annotation_text="🟠 Watch zone",
                          annotation_position="top left",
                          annotation_font_color=C_WATCH)
        if low_flow:
            fig.add_hrect(y0=0, y1=low_flow,
                          fillcolor="rgba(142,68,173,0.06)", line_width=0,
                          annotation_text="🟣 Low flow zone",
                          annotation_position="bottom left",
                          annotation_font_color=C_LOW)

        # Flow line with fill
        fig.add_trace(go.Scatter(
            x=df["ts"], y=df["flow_m3s"],
            mode="lines", name="Flow (m³/s)",
            fill="tozeroy",
            line=dict(color=C_WATER, width=2),
            fillcolor="rgba(0,150,199,0.15)",
            hovertemplate="%{x|%d %b %H:%M}<br><b>%{y:.2f} m³/s</b><extra></extra>",
        ))

        # Threshold dashed lines
        if flood_warn:
            fig.add_hline(y=flood_warn, line_dash="dash", line_color=C_FLOOD,
                          annotation_text="Flood warning", annotation_position="top right")
        if flood_watch:
            fig.add_hline(y=flood_watch, line_dash="dot", line_color=C_WATCH,
                          annotation_text="Flood watch", annotation_position="top right")
        if low_flow:
            fig.add_hline(y=low_flow, line_dash="dot", line_color=C_LOW,
                          annotation_text="Low flow", annotation_position="bottom right")

        fig.update_layout(
            xaxis_title="Time (Europe/Madrid)",
            yaxis_title="Flow (m³/s)",
            yaxis=dict(range=[0, y_ceil]),
            hovermode="x unified",
            height=420,
            margin=dict(t=20, b=40),
            template="plotly_white",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Stage hydrograph ──────────────────────────────────────────────────
        if "level_m" in df.columns and not df["level_m"].isna().all():
            st.subheader("Stage (water level)")
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(
                x=df["ts"], y=df["level_m"],
                mode="lines", fill="tozeroy", name="Stage (m)",
                line=dict(color="#2ca02c", width=1.8),
                fillcolor="rgba(44,160,44,0.12)",
                hovertemplate="%{x|%d %b %H:%M}<br><b>%{y:.3f} m</b><extra></extra>",
            ))
            fig2.update_layout(
                xaxis_title="Time (Europe/Madrid)",
                yaxis_title="Stage (m)",
                height=300,
                margin=dict(t=20, b=40),
                template="plotly_white",
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.caption(
            f"⚠️ Data from cache only. Quality: `{latest.get('quality_flag', 'unknown')}`"
        )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: Multi-station comparison
# ═══════════════════════════════════════════════════════════════════════════════
with tab_compare:
    st.subheader("Multi-station flow comparison")

    # Station multiselect — default to priority-1 stations
    priority_1 = [s["name"] for s in stations if s.get("priority", 99) == 1]
    all_names  = [s["name"] for s in stations]
    selected_multi = st.multiselect(
        "Select stations to compare", all_names,
        default=priority_1,
    )

    if not selected_multi:
        st.info("Select at least one station above.")
    else:
        sel_ids = {s["name"]: s["id"] for s in stations if s["name"] in selected_multi}

        fig_comp = go.Figure()
        colors = px.colors.qualitative.Plotly

        for idx, (name, sid) in enumerate(sel_ids.items()):
            df_s = load_gauge_data(sid)
            if df_s.empty:
                continue
            if pd.api.types.is_datetime64_any_dtype(df_s["timestamp"]):
                df_s["ts"] = df_s["timestamp"].dt.tz_convert("Europe/Madrid")
            else:
                df_s["ts"] = pd.to_datetime(df_s["timestamp"])
            df_s = df_s.sort_values("ts")
            color = colors[idx % len(colors)]
            short = name.split(" (")[0]
            fig_comp.add_trace(go.Scatter(
                x=df_s["ts"], y=df_s["flow_m3s"],
                mode="lines", name=short,
                line=dict(color=color, width=1.8),
                hovertemplate=f"<b>{short}</b><br>%{{x|%d %b %H:%M}}<br>%{{y:.2f}} m³/s<extra></extra>",
            ))

        # Add threshold lines
        if flood_warn:
            fig_comp.add_hline(y=flood_warn, line_dash="dash", line_color=C_FLOOD,
                               annotation_text="Flood warning", annotation_position="top right")
        if flood_watch:
            fig_comp.add_hline(y=flood_watch, line_dash="dot", line_color=C_WATCH,
                               annotation_text="Flood watch", annotation_position="top right")
        if low_flow:
            fig_comp.add_hline(y=low_flow, line_dash="dot", line_color=C_LOW,
                               annotation_text="Low flow", annotation_position="bottom right")

        fig_comp.update_layout(
            xaxis_title="Time (Europe/Madrid)",
            yaxis_title="Flow (m³/s)",
            hovermode="x unified",
            height=480,
            margin=dict(t=20, b=40),
            template="plotly_white",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        )
        st.plotly_chart(fig_comp, use_container_width=True)

        # Summary stats table
        rows = []
        for name in selected_multi:
            sid = sel_ids.get(name)
            if not sid:
                continue
            df_s = load_gauge_data(sid)
            if df_s.empty:
                rows.append({"Station": name, "Latest (m³/s)": "—", "Max (m³/s)": "—",
                             "Mean (m³/s)": "—", "Alert": "⚫ No data"})
                continue
            df_s = df_s.sort_values("timestamp")
            flow_col = df_s["flow_m3s"] if "flow_m3s" in df_s.columns else pd.Series(dtype=float)
            lat_f = float(flow_col.iloc[-1]) if len(flow_col) else np.nan
            badge_map = {"critical": "🔴", "watch": "🟠", "low_flow": "🟣",
                         "normal": "🟢", "no_data": "⚫"}
            alv = flow_alert_level(lat_f)
            rows.append({
                "Station":      name,
                "Latest (m³/s)": f"{lat_f:.2f}" if not pd.isna(lat_f) else "—",
                "Max (m³/s)":   f"{flow_col.max():.2f}" if len(flow_col) else "—",
                "Mean (m³/s)":  f"{flow_col.mean():.2f}" if len(flow_col) else "—",
                "Alert":        f"{badge_map[alv]} {alv.replace('_',' ').title()}",
            })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: Sub-basin breakdown
# ═══════════════════════════════════════════════════════════════════════════════
with tab_basin:
    st.subheader("Sub-basin breakdown")

    # Group stations by sub-basin
    basins: dict[str, list] = {}
    for s in stations:
        b = s.get("sub_basin", "other")
        basins.setdefault(b, []).append(s)

    for basin_key, basin_stations in basins.items():
        basin_label = SUB_BASIN_LABELS.get(basin_key, basin_key.replace("_", " ").title())
        with st.expander(f"**{basin_label}** ({len(basin_stations)} stations)", expanded=True):
            rows = []
            for s in basin_stations:
                info = station_latest[s["id"]]
                flow_v = info["flow"]
                lvl_v  = info["level"]
                trend  = info["trend"]
                alert  = info["alert"]
                badge_map = {"critical": "🔴 Flood warning", "watch": "🟠 Watch",
                             "low_flow": "🟣 Low flow", "normal": "🟢 Normal",
                             "no_data": "⚫ No data"}
                rows.append({
                    "Station":      s["name"],
                    "River":        s.get("river", "—"),
                    "Flow (m³/s)":  f"{flow_v:.2f}" if not pd.isna(flow_v) else "—",
                    "Stage (m)":    f"{lvl_v:.3f}" if not pd.isna(lvl_v) else "—",
                    "Trend":        trend,
                    "Status":       badge_map.get(alert, alert),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.caption("⚠️ All data from cache only. Trend = 6-point rolling direction.")
