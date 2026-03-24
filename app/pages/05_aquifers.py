"""
Page 5 — Aquifers (Piezometric levels)

Displays piezometric levels for the Baix Llobregat alluvial aquifer.
Data availability is patchy — all empty/missing states are handled gracefully.

Data source: reads piezo_*.parquet from data/cache/ only.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import yaml
import numpy as np

st.set_page_config(page_title="Aquifers — Llobregat", layout="wide")
st.title("Aquifer Piezometric Levels")

st.info(
    "**Baix Llobregat alluvial aquifer** — The lower 30 km of the Llobregat valley "
    "hosts a major alluvial aquifer that supplies Barcelona's industrial belt. "
    "Piezometric data availability is irregular; the dashboard degrades gracefully "
    "when data is missing."
)

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

@st.cache_data(ttl=1800)
def load_piezo_data(station_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"piezo_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

@st.cache_data(ttl=3600)
def load_piezo_stations() -> list:
    meta_path = CONFIG_DIR / "station_metadata.yaml"
    if not meta_path.exists():
        return []
    with open(meta_path) as f:
        data = yaml.safe_load(f)
    return data.get("piezo_stations") or []

stations = load_piezo_stations()

if not stations:
    st.warning(
        "No piezometric stations are configured yet. "
        "Once ACA piezometric data has been explored and station IDs confirmed, "
        "add them to `config/station_metadata.yaml` under `piezo_stations`."
    )
    st.caption(
        "The ACA piezometric endpoint sometimes returns HTTP 204 (no content) "
        "for stations with no recent readings. This is treated as missing data, not an error."
    )
    st.stop()

station_options = {s["name"]: s["id"] for s in stations}
selected_name = st.selectbox("Select piezometric station", list(station_options.keys()))
selected_id = station_options[selected_name]

df = load_piezo_data(selected_id)

if df.empty:
    st.warning(
        f"No cached data for **{selected_name}** ({selected_id}). "
        "The ACA piezometric network has irregular availability. "
        "Run `python -m data.fetchers.refresh_all` to try fetching the latest data."
    )
    st.stop()

if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
    df["timestamp_local"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
else:
    df["timestamp_local"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp_local")

latest = df.iloc[-1]
col1, col2 = st.columns(2)
col1.metric(
    "Depth to water table",
    f"{latest.get('depth_m', np.nan):.2f} m" if not pd.isna(latest.get("depth_m")) else "—",
)
col2.metric(
    "Piezometric level",
    f"{latest.get('level_masl', np.nan):.2f} m a.s.l." if not pd.isna(latest.get("level_masl")) else "—",
)

# ── Level time series ──────────────────────────────────────────────────────────
if "level_masl" in df.columns and not df["level_masl"].isna().all():
    fig = px.line(df, x="timestamp_local", y="level_masl",
                  labels={"timestamp_local": "Time", "level_masl": "Level (m a.s.l.)"},
                  color_discrete_sequence=["#8c564b"])
    fig.update_layout(
        title=f"Piezometric level — {selected_name}",
        height=400, margin=dict(t=40, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

if "depth_m" in df.columns and not df["depth_m"].isna().all():
    fig2 = px.line(df, x="timestamp_local", y="depth_m",
                   labels={"timestamp_local": "Time", "depth_m": "Depth to water (m)"},
                   color_discrete_sequence=["#e377c2"])
    # Invert y-axis so shallower water table = higher on chart
    fig2.update_yaxes(autorange="reversed")
    fig2.update_layout(
        title=f"Depth to water table — {selected_name}",
        height=300, margin=dict(t=40, b=40),
    )
    st.plotly_chart(fig2, use_container_width=True)

st.caption(
    "⚠️ Data shown from cache only. Source: ACA piezometric network. "
    "Alert thresholds for this station are not yet calibrated — see config/thresholds.yaml."
)
