"""
Page 4 — Meteorology

Displays:
- Precipitation totals (bar chart)
- Temperature time series (min/max/mean)
- Wind speed and direction
- Station selector

Data source: reads meteo_*.parquet from data/cache/ only.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import yaml
import numpy as np

st.set_page_config(page_title="Meteorology — Llobregat", layout="wide")
st.title("Meteorology")

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

@st.cache_data(ttl=1800)
def load_meteo_data(station_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"meteo_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

@st.cache_data(ttl=3600)
def load_meteo_stations() -> list:
    meta_path = CONFIG_DIR / "station_metadata.yaml"
    if not meta_path.exists():
        return []
    with open(meta_path) as f:
        return yaml.safe_load(f).get("meteo_stations", [])

stations = load_meteo_stations()
if not stations:
    st.warning("No meteo stations configured. Check config/station_metadata.yaml.")
    st.stop()

station_options = {s["name"]: s["id"] for s in stations}
selected_name = st.selectbox("Select meteorological station", list(station_options.keys()))
selected_id = station_options[selected_name]

df = load_meteo_data(selected_id)

if df.empty:
    st.warning(
        f"No cached data for **{selected_name}** ({selected_id}). "
        "Run `python -m data.fetchers.refresh_all` to populate the cache."
    )
    st.stop()

if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
    df["timestamp_local"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
else:
    df["timestamp_local"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp_local")

# ── Summary metrics ────────────────────────────────────────────────────────────
latest = df.iloc[-1]
col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest precip", f"{latest.get('precip_mm', np.nan):.1f} mm" if not pd.isna(latest.get("precip_mm")) else "—")
col2.metric("Temperature", f"{latest.get('temp_c', np.nan):.1f} °C" if not pd.isna(latest.get("temp_c")) else "—")
col3.metric("Wind speed", f"{latest.get('wind_speed_ms', np.nan):.1f} m/s" if not pd.isna(latest.get("wind_speed_ms")) else "—")
col4.metric("Humidity", f"{latest.get('humidity_pct', np.nan):.0f}%" if not pd.isna(latest.get("humidity_pct")) else "—")

# ── Precipitation bar chart ────────────────────────────────────────────────────
if "precip_mm" in df.columns and not df["precip_mm"].isna().all():
    st.subheader("Precipitation")
    fig = px.bar(df, x="timestamp_local", y="precip_mm",
                 labels={"timestamp_local": "Time", "precip_mm": "Precipitation (mm)"},
                 color_discrete_sequence=["#1f77b4"])
    fig.update_layout(height=300, margin=dict(t=20, b=40))
    st.plotly_chart(fig, use_container_width=True)

# ── Temperature time series ────────────────────────────────────────────────────
temp_cols = [c for c in ["temp_min_c", "temp_c", "temp_max_c"] if c in df.columns]
if temp_cols and not df[temp_cols].isna().all().all():
    st.subheader("Temperature")
    fig2 = go.Figure()
    colors = {"temp_min_c": "#aec7e8", "temp_c": "#d62728", "temp_max_c": "#ff9896"}
    labels = {"temp_min_c": "Min", "temp_c": "Mean", "temp_max_c": "Max"}
    for col in temp_cols:
        fig2.add_trace(go.Scatter(
            x=df["timestamp_local"], y=df[col],
            mode="lines", name=labels.get(col, col),
            line=dict(color=colors.get(col, "gray")),
        ))
    fig2.update_layout(
        yaxis_title="Temperature (°C)", xaxis_title="Time",
        height=300, margin=dict(t=20, b=40), hovermode="x unified",
    )
    st.plotly_chart(fig2, use_container_width=True)

# ── Wind ──────────────────────────────────────────────────────────────────────
if "wind_speed_ms" in df.columns and not df["wind_speed_ms"].isna().all():
    st.subheader("Wind speed")
    fig3 = px.line(df, x="timestamp_local", y="wind_speed_ms",
                   labels={"timestamp_local": "Time", "wind_speed_ms": "Wind speed (m/s)"},
                   color_discrete_sequence=["#2ca02c"])
    fig3.update_layout(height=250, margin=dict(t=20, b=40))
    st.plotly_chart(fig3, use_container_width=True)

st.caption("⚠️ Data shown from cache only. Source: AEMET OpenData.")
