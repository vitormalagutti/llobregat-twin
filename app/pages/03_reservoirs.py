"""
Page 3 — Reservoirs
IBM Carbon Design System — Gray 100 dark theme

Displays:
- Hero banner with combined system storage %
- Speedometer gauge indicators for each reservoir
- Combined system storage bar
- Per-reservoir time series with threshold bands
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import yaml
import numpy as np
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from app.carbon import (
    inject, hero, kpi_card, badge, section_label,
    status_color, status_label,
    BG, LAYER_01, LAYER_02, BORDER,
    TEXT_PRIMARY, TEXT_SECONDARY, TEXT_DISABLED,
    FONT_SANS, FONT_MONO,
    BLUE_40, C_CRITICAL, C_WATCH, C_NORMAL, C_LOW_FLOW, C_NODATA,
)

st.set_page_config(page_title="Reservoirs — Llobregat", layout="wide")
inject()

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

# Carbon-compatible reservoir palette
C_FULL  = C_NORMAL
C_OK    = BLUE_40
C_WATER = "#33b1ff"

# ── Plotly dark template ───────────────────────────────────────────────────────
DARK_LAYOUT = dict(
    paper_bgcolor=BG,
    plot_bgcolor=LAYER_01,
    font=dict(family=FONT_SANS, color=TEXT_SECONDARY),
    xaxis=dict(gridcolor=LAYER_02, linecolor=BORDER, tickcolor=TEXT_DISABLED),
    yaxis=dict(gridcolor=LAYER_02, linecolor=BORDER, tickcolor=TEXT_DISABLED),
)

@st.cache_data(ttl=1800)
def load_reservoir_data(reservoir_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"reservoir_{reservoir_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

@st.cache_data(ttl=3600)
def load_metadata() -> tuple[list, dict]:
    meta_path   = CONFIG_DIR / "station_metadata.yaml"
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
    vol = row.get("volume_hm3",   np.nan)
    cap = row.get("capacity_hm3", r.get("capacity_hm3", np.nan))
    res_latest[r["id"]] = {
        "pct": float(pct) if not pd.isna(pct) else np.nan,
        "vol": float(vol) if not pd.isna(vol) else np.nan,
        "cap": float(cap) if not pd.isna(cap) else np.nan,
    }

# System totals
total_vol = sum(v["vol"] for v in res_latest.values() if not np.isnan(v["vol"]))
total_cap = sum(v["cap"] for v in res_latest.values() if not np.isnan(v["cap"]))
sys_pct   = (total_vol / total_cap * 100) if total_cap > 0 else np.nan

def pct_color(pct):
    if np.isnan(pct):    return C_NODATA
    if pct <= PCT_CRIT:  return C_CRITICAL
    if pct <= PCT_LOW:   return C_WATCH
    if pct >= 80:        return C_FULL
    return C_OK

def pct_status(pct):
    if np.isnan(pct):    return "no_data"
    if pct <= PCT_CRIT:  return "critical"
    if pct <= PCT_LOW:   return "watch"
    if pct >= 80:        return "full"
    return "normal"

# ── Hero banner (Carbon) ───────────────────────────────────────────────────────
sys_col       = pct_color(sys_pct)
sys_stat      = pct_status(sys_pct)
sys_pct_str   = f"{sys_pct:.1f}%" if not np.isnan(sys_pct) else "—"
total_vol_str = f"{total_vol:.1f} hm³" if total_vol else "—"
total_cap_str = f"{total_cap:.0f} hm³" if total_cap else "—"

st.markdown(hero(
    title="Reservoir Storage",
    subtitle=f"{len(reservoirs)} reservoirs monitored · {total_vol_str} stored of {total_cap_str} total capacity",
    right_label="System storage",
    right_value=sys_pct_str,
    status_text=status_label(sys_stat),
    status_color=sys_col,
), unsafe_allow_html=True)

# ── System storage combined bar ────────────────────────────────────────────────
if total_cap > 0:
    bar_fig = go.Figure()
    for r in reservoirs:
        v = res_latest[r["id"]]
        bar_fig.add_trace(go.Bar(
            name=r["name"],
            y=["System storage"],
            x=[v["vol"] if not np.isnan(v["vol"]) else 0],
            orientation="h",
            marker_color=pct_color(v["pct"]),
            text=f"{r['name']}: {v['vol']:.1f} hm³" if not np.isnan(v["vol"]) else r["name"],
            textposition="inside",
            textfont=dict(family=FONT_SANS, color=TEXT_PRIMARY, size=11),
            hovertemplate=f"<b>{r['name']}</b><br>Volume: %{{x:.1f}} hm³<extra></extra>",
        ))
    # Remaining capacity
    bar_fig.add_trace(go.Bar(
        name="Remaining",
        y=["System storage"],
        x=[max(0, total_cap - total_vol)],
        orientation="h",
        marker_color=LAYER_02,
        marker_line=dict(color=BORDER, width=1),
        hovertemplate="Remaining: %{x:.1f} hm³<extra></extra>",
    ))
    bar_fig.update_layout(
        barmode="stack", height=80,
        margin=dict(t=5, b=5, l=10, r=10),
        showlegend=False,
        paper_bgcolor=BG, plot_bgcolor=BG,
        xaxis=dict(showgrid=False, showticklabels=False, range=[0, total_cap]),
        yaxis=dict(showgrid=False, tickfont=dict(color=TEXT_SECONDARY)),
        font=dict(family=FONT_SANS, color=TEXT_SECONDARY),
    )
    st.plotly_chart(bar_fig, use_container_width=True)

# ── Speedometer gauges ─────────────────────────────────────────────────────────
st.markdown(section_label("Storage gauges"), unsafe_allow_html=True)
gauge_cols = st.columns(len(reservoirs))

for i, res in enumerate(reservoirs):
    v   = res_latest[res["id"]]
    pct = v["pct"]
    vol = v["vol"]
    cap = v["cap"]
    col_hex = pct_color(pct)
    stat    = pct_status(pct)

    with gauge_cols[i]:
        gauge_fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=pct if not np.isnan(pct) else 0,
            title={
                "text": (
                    f"<b style='font-family:{FONT_SANS}'>{res['name']}</b><br>"
                    f"<span style='font-size:0.75em;color:{TEXT_SECONDARY}'>"
                    f"{vol:.1f} / {cap:.0f} hm³"
                    f"</span>"
                ) if not np.isnan(vol) else f"<b>{res['name']}</b>",
                "font": {"size": 13, "color": TEXT_PRIMARY, "family": FONT_SANS},
            },
            number={
                "suffix": "%", "valueformat": ".1f",
                "font": {"size": 30, "family": FONT_MONO, "color": col_hex},
            },
            gauge={
                "axis": {
                    "range": [0, 100],
                    "tickwidth": 1,
                    "tickcolor": TEXT_DISABLED,
                    "tickvals":  [0, PCT_CRIT, PCT_LOW, 60, 80, 100],
                    "ticktext":  ["0", f"{PCT_CRIT}", f"{PCT_LOW}", "60", "80", "100"],
                    "tickfont":  {"color": TEXT_SECONDARY, "family": FONT_MONO, "size": 10},
                },
                "bar": {"color": col_hex, "thickness": 0.22},
                "bgcolor": "rgba(0,0,0,0)",
                "borderwidth": 1,
                "bordercolor": BORDER,
                "steps": [
                    {"range": [0, PCT_CRIT],       "color": "rgba(218,30,40,0.18)"},
                    {"range": [PCT_CRIT, PCT_LOW],  "color": "rgba(241,194,27,0.14)"},
                    {"range": [PCT_LOW, 80],        "color": "rgba(120,169,255,0.10)"},
                    {"range": [80, 100],            "color": "rgba(36,161,72,0.14)"},
                ],
                "threshold": {
                    "line": {"color": C_CRITICAL, "width": 2},
                    "thickness": 0.75,
                    "value": PCT_CRIT,
                },
            },
        ))
        gauge_fig.update_layout(
            height=260,
            margin=dict(t=60, b=10, l=20, r=20),
            paper_bgcolor=BG,
            font=dict(color=TEXT_SECONDARY, family=FONT_SANS),
        )
        st.plotly_chart(gauge_fig, use_container_width=True)

        # Status badge under gauge
        st.markdown(
            f'<div style="text-align:center;margin-top:-0.8rem;margin-bottom:0.5rem">'
            f'{badge(status_label(stat), col_hex)}</div>',
            unsafe_allow_html=True,
        )

st.divider()

# ── Per-reservoir time series ──────────────────────────────────────────────────
st.markdown(section_label("Storage history"), unsafe_allow_html=True)
tab_pct, tab_vol = st.tabs(["% Capacity over time", "Volume (hm³) over time"])

PALETTE = ["#78a9ff", "#33b1ff", "#42be65"]

with tab_pct:
    fig_pct = go.Figure()
    fig_pct.add_hrect(y0=0, y1=PCT_CRIT,
                      fillcolor="rgba(218,30,40,0.07)", line_width=0,
                      annotation_text="Critical zone", annotation_position="bottom left",
                      annotation_font_color=C_CRITICAL)
    fig_pct.add_hrect(y0=PCT_CRIT, y1=PCT_LOW,
                      fillcolor="rgba(241,194,27,0.05)", line_width=0,
                      annotation_text="Watch zone", annotation_position="bottom left",
                      annotation_font_color=C_WATCH)
    fig_pct.add_hrect(y0=80, y1=100,
                      fillcolor="rgba(36,161,72,0.05)", line_width=0,
                      annotation_text="Full zone", annotation_position="top left",
                      annotation_font_color=C_FULL)

    for idx, res in enumerate(reservoirs):
        df = load_reservoir_data(res["id"])
        if df.empty or "pct_capacity" not in df.columns:
            continue
        if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["ts"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
        else:
            df["ts"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("ts")
        c = PALETTE[idx % len(PALETTE)]
        fig_pct.add_trace(go.Scatter(
            x=df["ts"], y=df["pct_capacity"],
            mode="lines", name=res["name"],
            line=dict(color=c, width=2),
            hovertemplate=f"<b>{res['name']}</b><br>%{{x|%d %b %H:%M}}<br>%{{y:.1f}}%<extra></extra>",
        ))

    fig_pct.add_hline(y=PCT_LOW, line_dash="dot", line_color=C_WATCH, line_width=1,
                      annotation_text="Watch threshold", annotation_position="top right",
                      annotation_font_color=C_WATCH)
    fig_pct.add_hline(y=PCT_CRIT, line_dash="dash", line_color=C_CRITICAL, line_width=1,
                      annotation_text="Critical threshold", annotation_position="top right",
                      annotation_font_color=C_CRITICAL)

    fig_pct.update_layout(
        **DARK_LAYOUT,
        yaxis=dict(**DARK_LAYOUT["yaxis"], range=[0, 100], title="% of capacity"),
        xaxis_title="Time (Europe/Madrid)",
        hovermode="x unified", height=420, margin=dict(t=20, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    font=dict(color=TEXT_SECONDARY)),
    )
    st.plotly_chart(fig_pct, use_container_width=True)

with tab_vol:
    fig_vol = go.Figure()
    fill_colors = ["rgba(120,169,255,0.13)", "rgba(51,177,255,0.10)", "rgba(66,190,101,0.10)"]

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
        c   = PALETTE[idx % len(PALETTE)]
        fc  = fill_colors[idx % len(fill_colors)]
        fig_vol.add_trace(go.Scatter(
            x=df["ts"], y=df["volume_hm3"],
            mode="lines", name=res["name"],
            fill="tozeroy", line=dict(color=c, width=2),
            fillcolor=fc,
            hovertemplate=(
                f"<b>{res['name']}</b><br>%{{x|%d %b %H:%M}}<br>"
                f"%{{y:.1f}} hm³"
                + (f" / {cap:.0f} hm³" if not np.isnan(cap) else "")
                + "<extra></extra>"
            ),
        ))
        if not np.isnan(cap):
            fig_vol.add_hline(y=cap, line_dash="dash", line_color=c, line_width=1, opacity=0.4,
                              annotation_text=f"{res['name']} cap ({cap:.0f} hm³)",
                              annotation_position="top right",
                              annotation_font_color=c)

    fig_vol.update_layout(
        **DARK_LAYOUT,
        yaxis_title="Volume (hm³)", xaxis_title="Time (Europe/Madrid)",
        hovermode="x unified", height=420, margin=dict(t=20, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    font=dict(color=TEXT_SECONDARY)),
    )
    st.plotly_chart(fig_vol, use_container_width=True)

# ── Summary table ──────────────────────────────────────────────────────────────
st.markdown(section_label("Reservoir summary"), unsafe_allow_html=True)
summary_rows = []
for res in reservoirs:
    v = res_latest[res["id"]]
    pct, vol, cap = v["pct"], v["vol"], v["cap"]
    summary_rows.append({
        "Reservoir":       res["name"],
        "River":           res.get("river", "—"),
        "Capacity (hm³)":  f"{cap:.0f}" if not np.isnan(cap) else "—",
        "Volume (hm³)":    f"{vol:.1f}" if not np.isnan(vol) else "—",
        "% Full":          f"{pct:.1f}%" if not np.isnan(pct) else "—",
        "Status":          status_label(pct_status(pct)),
    })
st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)
st.caption("⚠️ Data shown from cache only. Capacity figures from station metadata.")
