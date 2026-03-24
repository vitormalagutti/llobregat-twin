"""
Page 2 — River Flows

Displays:
- Station selector
- Flow hydrograph (last 7 days)
- Stage (water level) hydrograph
- Multi-station comparison chart
- Alert threshold overlays

Data source: reads flow_*.parquet from data/cache/ only.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import yaml
import numpy as np

st.set_page_config(page_title="River Flows — Llobregat", layout="wide")
st.title("River Flows")

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

@st.cache_data(ttl=1800)
def load_gauge_data(station_id: str) -> pd.DataFrame:
    """Load most recent cached parquet for a gauge station."""
    files = sorted(CACHE_DIR.glob(f"flow_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

@st.cache_data(ttl=3600)
def load_station_metadata() -> list:
    meta_path = CONFIG_DIR / "station_metadata.yaml"
    if not meta_path.exists():
        return []
    with open(meta_path) as f:
        data = yaml.safe_load(f)
    return data.get("gauge_stations", [])

@st.cache_data(ttl=3600)
def load_thresholds() -> dict:
    thresh_path = CONFIG_DIR / "thresholds.yaml"
    if not thresh_path.exists():
        return {}
    with open(thresh_path) as f:
        return yaml.safe_load(f)

stations = load_station_metadata()
thresholds = load_thresholds()
defaults = thresholds.get("flow_alert_m3s", {}).get("defaults", {})

if not stations:
    st.warning("No gauge stations configured. Check config/station_metadata.yaml.")
    st.stop()

station_options = {s["name"]: s["id"] for s in stations}
selected_name = st.selectbox("Select gauge station", list(station_options.keys()))
selected_id = station_options[selected_name]

df = load_gauge_data(selected_id)

if df.empty:
    st.warning(
        f"No cached data for **{selected_name}** ({selected_id}). "
        "Run `python -m data.fetchers.refresh_all` to populate the cache."
    )
    st.stop()

# Convert to local time for display
if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
    df["timestamp_local"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
else:
    df["timestamp_local"] = pd.to_datetime(df["timestamp"])

df = df.sort_values("timestamp_local")

col1, col2, col3 = st.columns(3)
latest = df.iloc[-1]
col1.metric("Latest flow", f"{latest['flow_m3s']:.2f} m³/s" if not pd.isna(latest.get("flow_m3s")) else "—")
col2.metric("Latest stage", f"{latest['level_m']:.2f} m" if not pd.isna(latest.get("level_m")) else "—")
col3.metric("Records loaded", f"{len(df):,}")

# ── Flow hydrograph ────────────────────────────────────────────────────────────
st.subheader(f"Flow hydrograph — {selected_name}")
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df["timestamp_local"], y=df["flow_m3s"],
    mode="lines", name="Flow (m³/s)",
    line=dict(color="#1f77b4", width=1.5),
))

# Threshold overlays
if defaults.get("flood_warning"):
    fig.add_hline(y=defaults["flood_warning"], line_dash="dash", line_color="red",
                  annotation_text="Flood warning", annotation_position="top right")
if defaults.get("flood_watch"):
    fig.add_hline(y=defaults["flood_watch"], line_dash="dot", line_color="orange",
                  annotation_text="Flood watch", annotation_position="top right")
if defaults.get("low_flow_warning"):
    fig.add_hline(y=defaults["low_flow_warning"], line_dash="dot", line_color="purple",
                  annotation_text="Low flow", annotation_position="bottom right")

fig.update_layout(
    xaxis_title="Time (Europe/Madrid)",
    yaxis_title="Flow (m³/s)",
    hovermode="x unified",
    height=400,
    margin=dict(t=20, b=40),
)
st.plotly_chart(fig, use_container_width=True)

# ── Stage hydrograph ───────────────────────────────────────────────────────────
if "level_m" in df.columns and not df["level_m"].isna().all():
    st.subheader(f"Stage (water level) — {selected_name}")
    fig2 = px.line(df, x="timestamp_local", y="level_m",
                   labels={"timestamp_local": "Time", "level_m": "Stage (m)"},
                   color_discrete_sequence=["#2ca02c"])
    fig2.update_layout(height=300, margin=dict(t=20, b=40))
    st.plotly_chart(fig2, use_container_width=True)

st.caption(
    f"⚠️ Data shown from cache only. "
    f"Quality flag: `{latest.get('quality_flag', 'unknown')}`"
)
