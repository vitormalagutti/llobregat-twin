"""
Page 3 — Reservoirs

Displays:
- Reservoir storage levels (hm3 and % capacity)
- Trend over available history
- Alert status badges

Data source: reads reservoir_*.parquet from data/cache/ only.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import yaml
import numpy as np

st.set_page_config(page_title="Reservoirs — Llobregat", layout="wide")
st.title("Reservoir Levels")

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

@st.cache_data(ttl=1800)
def load_reservoir_data(reservoir_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"reservoir_{reservoir_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

@st.cache_data(ttl=3600)
def load_metadata() -> tuple[list, dict]:
    meta_path = CONFIG_DIR / "station_metadata.yaml"
    thresh_path = CONFIG_DIR / "thresholds.yaml"
    reservoirs, thresholds = [], {}
    if meta_path.exists():
        with open(meta_path) as f:
            reservoirs = yaml.safe_load(f).get("reservoirs", [])
    if thresh_path.exists():
        with open(thresh_path) as f:
            thresholds = yaml.safe_load(f)
    return reservoirs, thresholds

reservoirs, thresholds = load_metadata()
res_thresholds = thresholds.get("reservoir_alert_pct", {})

if not reservoirs:
    st.warning("No reservoirs configured. Check config/station_metadata.yaml.")
    st.stop()

# ── Summary metrics ────────────────────────────────────────────────────────────
st.subheader("Current storage summary")
cols = st.columns(len(reservoirs))
for i, res in enumerate(reservoirs):
    df = load_reservoir_data(res["id"])
    with cols[i]:
        if df.empty:
            st.metric(res["name"], "No data", help=f"ID: {res['id']}")
        else:
            latest = df.sort_values("timestamp").iloc[-1]
            pct = latest.get("pct_capacity", np.nan)
            vol = latest.get("volume_hm3", np.nan)
            cap = latest.get("capacity_hm3", res.get("capacity_hm3", np.nan))
            label = f"{pct:.1f}%" if not pd.isna(pct) else "—"
            delta_label = f"{vol:.1f} / {cap:.0f} hm³" if (not pd.isna(vol) and not pd.isna(cap)) else None
            crit = res_thresholds.get("critically_low", 20)
            low = res_thresholds.get("low", 40)
            delta_color = "inverse" if (not pd.isna(pct) and pct <= crit) else "off"
            st.metric(res["name"], label, delta=delta_label, delta_color=delta_color)

# ── Per-reservoir time series ──────────────────────────────────────────────────
selected_res_name = st.selectbox("Select reservoir", [r["name"] for r in reservoirs])
selected_res = next(r for r in reservoirs if r["name"] == selected_res_name)

df = load_reservoir_data(selected_res["id"])

if df.empty:
    st.warning(
        f"No cached data for **{selected_res_name}** ({selected_res['id']}). "
        "Run `python -m data.fetchers.refresh_all` to populate the cache."
    )
    st.stop()

if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
    df["timestamp_local"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
else:
    df["timestamp_local"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp_local")

# % capacity chart
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df["timestamp_local"], y=df["pct_capacity"],
    mode="lines", fill="tozeroy",
    name="% capacity",
    line=dict(color="#1f77b4"),
    fillcolor="rgba(31,119,180,0.15)",
))
if res_thresholds.get("low"):
    fig.add_hline(y=res_thresholds["low"], line_dash="dot", line_color="orange",
                  annotation_text="Watch threshold", annotation_position="top right")
if res_thresholds.get("critically_low"):
    fig.add_hline(y=res_thresholds["critically_low"], line_dash="dash", line_color="red",
                  annotation_text="Critical threshold", annotation_position="top right")
fig.update_layout(
    title=f"{selected_res_name} — storage as % of capacity",
    yaxis=dict(range=[0, 100], title="% capacity"),
    xaxis_title="Time (Europe/Madrid)",
    height=400, margin=dict(t=40, b=40),
)
st.plotly_chart(fig, use_container_width=True)

# Absolute volume chart
if "volume_hm3" in df.columns:
    fig2 = px.area(df, x="timestamp_local", y="volume_hm3",
                   labels={"timestamp_local": "Time", "volume_hm3": "Volume (hm³)"},
                   color_discrete_sequence=["#aec7e8"])
    fig2.update_layout(title=f"{selected_res_name} — stored volume",
                       height=300, margin=dict(t=40, b=40))
    st.plotly_chart(fig2, use_container_width=True)

st.caption("⚠️ Data shown from cache only. Capacity figures from station metadata.")
