"""
Page 3 — Reservoirs  (gamified redesign)

Displays:
- Hero banner with combined system storage %
- Speedometer gauge indicators for each reservoir
- Combined system storage bar
- Per-reservoir time series with threshold bands
- Volume trend charts

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

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

# ── Colour system ──────────────────────────────────────────────────────────────
C_WATER   = "#0096c7"
C_FULL    = "#27ae60"
C_OK      = "#48cae4"
C_WATCH   = "#e67e22"
C_CRIT    = "#c0392b"
C_NODATA  = "#95a5a6"

# ── Data loaders ───────────────────────────────────────────────────────────────
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
res_thresh = thresholds.get("reservoir_alert_pct", {})
PCT_CRIT   = res_thresh.get("critically_low", 20)
PCT_LOW    = res_thresh.get("low", 40)

if not reservoirs:
    st.warning("No reservoirs configured. Check config/station_metadata.yaml.")
    st.stop()

# ── Gather latest values ───────────────────────────────────────────────────────
res_latest = {}
for r in reservoirs:
    df = load_reservoir_data(r["id"])
    if df.empty:
        res_latest[r["id"]] = {"pct": np.nan, "vol": np.nan, "cap": r.get("capacity_hm3", np.nan)}
        continue
    row = df.sort_values("timestamp").iloc[-1]
    pct = row.get("pct_capacity", np.nan)
    vol = row.get("volume_hm3",  np.nan)
    cap = row.get("capacity_hm3", r.get("capacity_hm3", np.nan))
    res_latest[r["id"]] = {"pct": float(pct) if not pd.isna(pct) else np.nan,
                            "vol": float(vol) if not pd.isna(vol) else np.nan,
                            "cap": float(cap) if not pd.isna(cap) else np.nan}

# System totals
total_vol = sum(v["vol"] for v in res_latest.values() if not np.isnan(v["vol"]))
total_cap = sum(v["cap"] for v in res_latest.values() if not np.isnan(v["cap"]))
sys_pct   = (total_vol / total_cap * 100) if total_cap > 0 else np.nan

def pct_color(pct):
    if np.isnan(pct):      return C_NODATA
    if pct <= PCT_CRIT:    return C_CRIT
    if pct <= PCT_LOW:     return C_WATCH
    if pct >= 80:          return C_FULL
    return C_OK

def pct_status(pct):
    if np.isnan(pct):      return "⚫ No data"
    if pct <= PCT_CRIT:    return "🔴 Critical"
    if pct <= PCT_LOW:     return "🟠 Watch"
    if pct >= 80:          return "🟢 Full"
    return "🟢 Normal"

# ── Hero banner ────────────────────────────────────────────────────────────────
sys_col   = pct_color(sys_pct)
sys_stat  = pct_status(sys_pct)
sys_pct_str = f"{sys_pct:.1f}%" if not np.isnan(sys_pct) else "—"
total_vol_str = f"{total_vol:.1f} hm³" if total_vol else "—"
total_cap_str = f"{total_cap:.0f} hm³" if total_cap else "—"

st.markdown(f"""
<div style="background:linear-gradient(135deg,#03045e,#0077b6);
            padding:1.6rem 2rem;border-radius:12px;margin-bottom:1.2rem;
            display:flex;align-items:center;justify-content:space-between">
  <div>
    <h1 style="color:white;margin:0;font-size:1.9rem">💧 Reservoir Storage</h1>
    <p style="color:#90e0ef;margin:0.3rem 0 0;font-size:0.95rem">
      {len(reservoirs)} reservoirs monitored &nbsp;·&nbsp;
      {total_vol_str} stored of {total_cap_str} total capacity
    </p>
  </div>
  <div style="text-align:right">
    <div style="background:{sys_col};color:white;padding:0.5rem 1.2rem;
                border-radius:20px;font-weight:700;font-size:1rem">
      {sys_stat}
    </div>
    <div style="color:#90e0ef;font-size:0.85rem;margin-top:0.4rem">
      System: <strong style="color:white;font-size:1.1rem">{sys_pct_str}</strong>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── System storage combined bar ────────────────────────────────────────────────
if total_cap > 0:
    bar_fig = go.Figure()
    colors_bar = [pct_color(res_latest[r["id"]]["pct"]) for r in reservoirs]
    volumes    = [res_latest[r["id"]]["vol"] for r in reservoirs]
    names      = [r["name"] for r in reservoirs]
    caps       = [res_latest[r["id"]]["cap"] for r in reservoirs]

    for i, r in enumerate(reservoirs):
        v = res_latest[r["id"]]
        bar_fig.add_trace(go.Bar(
            name=r["name"],
            y=["System storage"],
            x=[v["vol"] if not np.isnan(v["vol"]) else 0],
            orientation="h",
            marker_color=colors_bar[i],
            text=f"{r['name']}: {v['vol']:.1f} hm³" if not np.isnan(v["vol"]) else r["name"],
            textposition="inside",
            hovertemplate=f"<b>{r['name']}</b><br>Volume: %{{x:.1f}} hm³<extra></extra>",
        ))

    # Capacity outline
    bar_fig.add_trace(go.Bar(
        name="Remaining capacity",
        y=["System storage"],
        x=[max(0, total_cap - total_vol)],
        orientation="h",
        marker_color="rgba(255,255,255,0.08)",
        marker_line=dict(color="rgba(255,255,255,0.3)", width=1),
        hovertemplate="Remaining capacity: %{x:.1f} hm³<extra></extra>",
    ))

    bar_fig.update_layout(
        barmode="stack",
        height=90,
        margin=dict(t=5, b=5, l=10, r=10),
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False, range=[0, total_cap]),
        yaxis=dict(showgrid=False),
        font_color="white",
    )
    st.plotly_chart(bar_fig, use_container_width=True)

# ── Speedometer gauges ─────────────────────────────────────────────────────────
st.subheader("Storage gauges")
gauge_cols = st.columns(len(reservoirs))
for i, res in enumerate(reservoirs):
    v   = res_latest[res["id"]]
    pct = v["pct"]
    vol = v["vol"]
    cap = v["cap"]
    with gauge_cols[i]:
        gauge_fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=pct if not np.isnan(pct) else 0,
            title={
                "text": (
                    f"<b>{res['name']}</b><br>"
                    f"<span style='font-size:0.75em;color:grey'>"
                    f"{vol:.1f} / {cap:.0f} hm³"
                    f"</span>"
                ) if not np.isnan(vol) else f"<b>{res['name']}</b>",
                "font": {"size": 14},
            },
            number={"suffix": "%", "valueformat": ".1f",
                    "font": {"size": 28}},
            gauge={
                "axis": {
                    "range": [0, 100],
                    "tickwidth": 1,
                    "tickvals": [0, PCT_CRIT, PCT_LOW, 60, 80, 100],
                    "ticktext": ["0", f"{PCT_CRIT}", f"{PCT_LOW}", "60", "80", "100"],
                },
                "bar": {"color": pct_color(pct), "thickness": 0.25},
                "bgcolor": "rgba(0,0,0,0)",
                "borderwidth": 2,
                "bordercolor": "rgba(255,255,255,0.1)",
                "steps": [
                    {"range": [0, PCT_CRIT],         "color": "rgba(192,57,43,0.25)"},
                    {"range": [PCT_CRIT, PCT_LOW],    "color": "rgba(230,126,34,0.20)"},
                    {"range": [PCT_LOW, 80],          "color": "rgba(72,202,228,0.15)"},
                    {"range": [80, 100],              "color": "rgba(39,174,96,0.20)"},
                ],
                "threshold": {
                    "line": {"color": C_CRIT, "width": 3},
                    "thickness": 0.8,
                    "value": PCT_CRIT,
                },
            },
        ))
        gauge_fig.update_layout(
            height=260,
            margin=dict(t=60, b=10, l=20, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="white",
        )
        st.plotly_chart(gauge_fig, use_container_width=True)

        # Status badge under gauge
        status = pct_status(pct)
        col_hex = pct_color(pct)
        st.markdown(
            f'<div style="text-align:center;margin-top:-1rem">'
            f'<span style="background:{col_hex};color:white;padding:3px 12px;'
            f'border-radius:12px;font-size:0.8rem;font-weight:600">{status}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

st.divider()

# ── Per-reservoir time series ──────────────────────────────────────────────────
st.subheader("Storage history")
tab_pct, tab_vol = st.tabs(["% Capacity over time", "Volume (hm³) over time"])

with tab_pct:
    fig_pct = go.Figure()

    # Alert bands
    fig_pct.add_hrect(y0=0, y1=PCT_CRIT,
                      fillcolor="rgba(192,57,43,0.08)", line_width=0,
                      annotation_text="🔴 Critical zone", annotation_position="bottom left",
                      annotation_font_color=C_CRIT)
    fig_pct.add_hrect(y0=PCT_CRIT, y1=PCT_LOW,
                      fillcolor="rgba(230,126,34,0.06)", line_width=0,
                      annotation_text="🟠 Watch zone", annotation_position="bottom left",
                      annotation_font_color=C_WATCH)
    fig_pct.add_hrect(y0=80, y1=100,
                      fillcolor="rgba(39,174,96,0.06)", line_width=0,
                      annotation_text="🟢 Full zone", annotation_position="top left",
                      annotation_font_color=C_FULL)

    colors_ts = ["#0096c7", "#48cae4", "#90e0ef"]
    for idx, res in enumerate(reservoirs):
        df = load_reservoir_data(res["id"])
        if df.empty or "pct_capacity" not in df.columns:
            continue
        if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["ts"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
        else:
            df["ts"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("ts")
        c = colors_ts[idx % len(colors_ts)]
        fig_pct.add_trace(go.Scatter(
            x=df["ts"], y=df["pct_capacity"],
            mode="lines", name=res["name"],
            line=dict(color=c, width=2),
            hovertemplate=f"<b>{res['name']}</b><br>%{{x|%d %b %H:%M}}<br>%{{y:.1f}}%<extra></extra>",
        ))

    # Threshold lines
    fig_pct.add_hline(y=PCT_LOW, line_dash="dot", line_color=C_WATCH,
                      annotation_text="Watch threshold", annotation_position="top right")
    fig_pct.add_hline(y=PCT_CRIT, line_dash="dash", line_color=C_CRIT,
                      annotation_text="Critical threshold", annotation_position="top right")

    fig_pct.update_layout(
        yaxis=dict(range=[0, 100], title="% of capacity"),
        xaxis_title="Time (Europe/Madrid)",
        hovermode="x unified",
        height=420,
        margin=dict(t=20, b=40),
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_pct, use_container_width=True)

with tab_vol:
    fig_vol = go.Figure()
    fill_colors = ["rgba(0,150,199,0.15)", "rgba(72,202,228,0.12)", "rgba(144,224,239,0.12)"]
    colors_v    = ["#0096c7", "#48cae4", "#90e0ef"]

    for idx, res in enumerate(reservoirs):
        df = load_reservoir_data(res["id"])
        if df.empty or "volume_hm3" not in df.columns:
            continue
        if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["ts"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
        else:
            df["ts"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("ts")
        cap = res.get("capacity_hm3", np.nan)
        c   = colors_v[idx % len(colors_v)]
        fc  = fill_colors[idx % len(fill_colors)]
        fig_vol.add_trace(go.Scatter(
            x=df["ts"], y=df["volume_hm3"],
            mode="lines", name=res["name"],
            fill="tozeroy",
            line=dict(color=c, width=2),
            fillcolor=fc,
            hovertemplate=(
                f"<b>{res['name']}</b><br>%{{x|%d %b %H:%M}}<br>"
                f"%{{y:.1f}} hm³"
                + (f" / {cap:.0f} hm³" if not np.isnan(cap) else "")
                + "<extra></extra>"
            ),
        ))
        # Capacity reference line
        if not np.isnan(cap):
            fig_vol.add_hline(y=cap, line_dash="dash", line_color=c,
                              opacity=0.5,
                              annotation_text=f"{res['name']} capacity ({cap:.0f} hm³)",
                              annotation_position="top right")

    fig_vol.update_layout(
        yaxis_title="Volume (hm³)",
        xaxis_title="Time (Europe/Madrid)",
        hovermode="x unified",
        height=420,
        margin=dict(t=20, b=40),
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_vol, use_container_width=True)

# ── Summary table ──────────────────────────────────────────────────────────────
st.subheader("Reservoir summary")
summary_rows = []
for res in reservoirs:
    v = res_latest[res["id"]]
    pct = v["pct"]
    vol = v["vol"]
    cap = v["cap"]
    summary_rows.append({
        "Reservoir":    res["name"],
        "River":        res.get("river", "—"),
        "Capacity (hm³)": f"{cap:.0f}" if not np.isnan(cap) else "—",
        "Volume (hm³)": f"{vol:.1f}" if not np.isnan(vol) else "—",
        "% Full":       f"{pct:.1f}%" if not np.isnan(pct) else "—",
        "Status":       pct_status(pct),
    })
st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)
st.caption("⚠️ Data shown from cache only. Capacity figures from station metadata.")
