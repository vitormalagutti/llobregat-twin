"""
Page 4 — Meteorology

- Hero KPI strip: all active meteo stations (BCN-first)
- 7-day AEMET forecast cards for the nearest active municipio
- Historical charts: precipitation, temperature range, wind
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import yaml
import numpy as np
import httpx
import json
import sys

_APP_DIR = Path(__file__).parent.parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))
from carbon import (inject, hero, kpi_card, badge,
                    BG, LAYER_01, LAYER_02, BORDER, TEXT_PRIMARY, TEXT_SECONDARY,
                    BLUE_40, C_CRITICAL, C_WATCH, C_NORMAL, C_NODATA,
                    FONT_MONO)

st.set_page_config(page_title="Meteorology — Llobregat", layout="wide")
inject()

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
AEMET_BASE = "https://opendata.aemet.es/opendata/api"

# ── API key loader ─────────────────────────────────────────────────────────────
def _get_aemet_key() -> str:
    import os
    key = os.environ.get("AEMET_API_KEY", "")
    if key:
        return key
    try:
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib
        p = Path(__file__).parent.parent.parent / ".streamlit" / "secrets.toml"
        if p.exists():
            with open(p, "rb") as f:
                return tomllib.load(f).get("AEMET_API_KEY", "")
    except Exception:
        pass
    # Streamlit Cloud secrets
    try:
        return st.secrets.get("AEMET_API_KEY", "")
    except Exception:
        return ""

# ── Metadata ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_meteo_stations() -> list:
    p = CONFIG_DIR / "station_metadata.yaml"
    if not p.exists():
        return []
    with open(p) as f:
        return yaml.safe_load(f).get("meteo_stations", [])

@st.cache_data(ttl=1800)
def load_meteo_data(station_id: str) -> pd.DataFrame:
    """Load ALL cached parquet files for a meteo station (full history)."""
    files = sorted(CACHE_DIR.glob(f"meteo_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)

# ── AEMET 7-day forecast (live fetch, 30-min cache) ───────────────────────────
SKY_ICONS = {
    "11": "☀️", "12": "🌤️", "13": "⛅", "14": "🌥️",
    "15": "☁️", "16": "☁️", "17": "🌥️",
    "23": "🌦️", "24": "🌧️", "25": "🌧️", "26": "🌧️",
    "33": "🌤️", "34": "🌥️", "35": "☁️", "36": "🌥️",
    "43": "🌦️", "44": "🌧️", "45": "🌧️", "46": "🌧️",
    "51": "🌨️", "52": "🌨️", "53": "❄️", "54": "❄️",
    "61": "❄️", "62": "❄️", "63": "🌨️",
    "71": "⛈️", "72": "⛈️", "73": "⛈️",
    "81": "🌫️", "82": "🌫️",
}

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_forecast(municipio_code: str, api_key: str) -> tuple[list[dict], dict | None]:
    """
    Fetch AEMET 7-day daily forecast for a municipality.
    Returns (days, error_info) — error_info is None on success, or a dict with
    {url, http_status, estado, descripcion, exception} on failure.
    """
    if not api_key or not municipio_code:
        return [], {"url": "—", "http_status": None, "estado": None,
                    "descripcion": "API key or municipio_code missing.", "exception": None}
    url1 = f"{AEMET_BASE}/prediccion/especifica/municipio/diaria/{municipio_code}"
    try:
        r1 = httpx.get(url1, params={"api_key": api_key},
                        headers={"api_key": api_key}, timeout=20)
        if r1.status_code != 200:
            return [], {"url": url1, "http_status": r1.status_code, "estado": None,
                        "descripcion": f"Step-1 HTTP {r1.status_code}",
                        "exception": None}
        # AEMET responses are ISO-8859-1 (Latin-1) — decode explicitly, not UTF-8
        j1 = json.loads(r1.content.decode("latin-1"))
        estado = j1.get("estado")
        descripcion = j1.get("descripcion", "")
        datos_url = j1.get("datos")
        if not datos_url:
            return [], {"url": url1, "http_status": r1.status_code,
                        "estado": estado, "descripcion": descripcion or "No 'datos' URL in response.",
                        "exception": None}

        r2 = httpx.get(datos_url, timeout=30)
        if r2.status_code != 200:
            return [], {"url": datos_url, "http_status": r2.status_code, "estado": estado,
                        "descripcion": f"Step-2 HTTP {r2.status_code}",
                        "exception": None}
        # Step-2 data payload is also Latin-1
        raw = json.loads(r2.content.decode("latin-1"))
        if not raw or not isinstance(raw, list):
            return [], {"url": datos_url, "http_status": r2.status_code, "estado": estado,
                        "descripcion": f"Unexpected step-2 payload type: {type(raw).__name__}",
                        "exception": None}

        pred = raw[0].get("prediccion", {}).get("dia", [])
        days = []
        for day in pred:
            fecha = str(day.get("fecha", ""))[:10]
            try:
                dt = pd.Timestamp(fecha)
                weekday = dt.strftime("%a")
                date_str = dt.strftime("%d %b")
            except Exception:
                weekday = fecha
                date_str = fecha

            # Temperature
            temp_obj = day.get("temperatura", {})
            t_max = temp_obj.get("maxima")
            t_min = temp_obj.get("minima")

            # Precipitation probability (00-24 period)
            prob_list = day.get("probPrecipitacion", [])
            precip_pct = None
            for p in prob_list:
                if p.get("periodo") == "00-24":
                    precip_pct = p.get("value")
                    break
            if precip_pct is None and prob_list:
                precip_pct = prob_list[0].get("value")

            # Precipitation amount
            prec_list = day.get("precipitacion", [])
            prec_mm = None
            for p in prec_list:
                if p.get("periodo") == "00-24":
                    prec_mm = p.get("value")
                    break
            if prec_mm is None and prec_list:
                prec_mm = prec_list[0].get("value")

            # Sky state
            sky_list = day.get("estadoCielo", [])
            sky_code = None
            sky_desc = ""
            for sk in sky_list:
                if sk.get("periodo") == "00-24":
                    sky_code = str(sk.get("value", ""))
                    sky_desc = sk.get("descripcion", "")
                    break
            if not sky_code and sky_list:
                sky_code = str(sky_list[0].get("value", ""))
                sky_desc = sky_list[0].get("descripcion", "")
            sky_icon = SKY_ICONS.get(sky_code, "🌡️")

            # Wind
            wind_list = day.get("viento", [])
            wind_spd = None
            wind_dir = ""
            for w in wind_list:
                if w.get("periodo") == "00-24":
                    wind_spd = w.get("velocidad")
                    wind_dir = w.get("direccion", "")
                    break
            if wind_spd is None and wind_list:
                wind_spd = wind_list[0].get("velocidad")
                wind_dir = wind_list[0].get("direccion", "")

            days.append({
                "weekday":    weekday,
                "date_str":   date_str,
                "sky_icon":   sky_icon,
                "sky_desc":   sky_desc,
                "t_max":      t_max,
                "t_min":      t_min,
                "precip_pct": precip_pct,
                "prec_mm":    prec_mm,
                "wind_spd":   wind_spd,
                "wind_dir":   wind_dir,
            })
        return days[:7], None

    except Exception as exc:
        return [], {"url": url1, "http_status": None, "estado": None,
                    "descripcion": "Exception during fetch.", "exception": str(exc)}

# ── Load stations ──────────────────────────────────────────────────────────────
meteo_stations = load_meteo_stations()
if not meteo_stations:
    st.warning("No meteo stations configured.")
    st.stop()

# BCN-first
BASIN_ORDER = {"lower_llobregat": 0, "anoia": 1, "middle_llobregat": 2,
               "cardener": 3, "upper_llobregat": 4}
sorted_stations = sorted(meteo_stations,
    key=lambda s: (BASIN_ORDER.get(s.get("sub_basin", "other"), 5), s.get("priority", 9)))

# Collect latest for each station
station_latest = {}
for mt in meteo_stations:
    df = load_meteo_data(mt["id"])
    if df.empty:
        station_latest[mt["id"]] = {"temp": np.nan, "precip": np.nan, "wind": np.nan}
        continue
    row = df.sort_values("timestamp").iloc[-1]
    station_latest[mt["id"]] = {
        "temp":   float(row.get("temp_c",    np.nan)) if not pd.isna(row.get("temp_c"))    else np.nan,
        "precip": float(row.get("precip_mm", np.nan)) if not pd.isna(row.get("precip_mm")) else np.nan,
        "wind":   float(row.get("wind_speed_ms", np.nan)) if not pd.isna(row.get("wind_speed_ms")) else np.nan,
    }

active_stations = [m for m in meteo_stations if not np.isnan(station_latest[m["id"]]["temp"])]
n_active = len(active_stations)

# Find the hero station (first sorted = BCN/lower_llobregat priority 1)
hero_stn = sorted_stations[0] if sorted_stations else None
hero_info = station_latest[hero_stn["id"]] if hero_stn else {}
hero_temp = f"{hero_info.get('temp', np.nan):.1f} °C" if not np.isnan(hero_info.get("temp", np.nan)) else "—"
hero_precip = f"{hero_info.get('precip', np.nan):.1f} mm" if not np.isnan(hero_info.get("precip", np.nan)) else "—"

# ── Hero banner ────────────────────────────────────────────────────────────────
st.markdown(hero(
    title="⛅ Meteorology",
    subtitle=f"{n_active} AEMET stations active · Llobregat watershed",
    right_label=hero_stn["name"] if hero_stn else "",
    right_value=hero_temp,
    right_label2="Latest precip",
    right_value2=hero_precip,
), unsafe_allow_html=True)

# ── Station KPI strip ──────────────────────────────────────────────────────────
active_sorted = [m for m in sorted_stations
                 if not np.isnan(station_latest[m["id"]]["temp"])]
if active_sorted:
    cols = st.columns(len(active_sorted))
    for i, mt in enumerate(active_sorted):
        info = station_latest[mt["id"]]
        with cols[i]:
            temp_s  = f"{info['temp']:.1f} °C"  if not np.isnan(info["temp"])   else "—"
            prec_s  = f"{info['precip']:.1f} mm" if not np.isnan(info["precip"]) else "—"
            wind_s  = f"{info['wind']:.1f} m/s"  if not np.isnan(info["wind"])   else "—"
            st.markdown(kpi_card(
                label=f"⛅ {mt['name']}",
                value=temp_s,
                sub=f"💧 {prec_s}  🌬️ {wind_s}",
                color=C_NORMAL,
            ), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Station selector ───────────────────────────────────────────────────────────
# BCN-closest station first in the dropdown
station_options = {s["name"]: s["id"] for s in sorted_stations}
selected_name = st.selectbox("Select meteorological station for detail view",
                              list(station_options.keys()))
selected_id   = station_options[selected_name]
selected_meta = next(s for s in meteo_stations if s["id"] == selected_id)
municipio_code = selected_meta.get("municipio_code", "")

# ── 7-day forecast section ─────────────────────────────────────────────────────
st.subheader(f"📅 7-day forecast — {selected_name} area")

aemet_key = _get_aemet_key()
if not aemet_key:
    st.info("AEMET API key not configured — forecast unavailable. Add `AEMET_API_KEY` to secrets.")
elif not municipio_code:
    st.info("Municipality code not set for this station — forecast unavailable.")
else:
    with st.spinner("Loading forecast…"):
        forecast_days, forecast_err = fetch_forecast(municipio_code, aemet_key)

    if not forecast_days:
        st.warning("⚠️ Forecast data unavailable for this station.")
        if forecast_err:
            with st.expander("🔍 AEMET diagnostic — click to see why", expanded=True):
                st.markdown(f"""
| Field | Value |
|---|---|
| **URL** | `{forecast_err.get('url', '—')}` |
| **HTTP status** | `{forecast_err.get('http_status', '—')}` |
| **AEMET estado** | `{forecast_err.get('estado', '—')}` |
| **Description** | {forecast_err.get('descripcion', '—')} |
| **Exception** | `{forecast_err.get('exception') or 'none'}` |
""")
    else:
        # Forecast day cards
        day_cols = st.columns(len(forecast_days))
        for i, day in enumerate(forecast_days):
            t_max = day["t_max"]
            t_min = day["t_min"]
            pct   = day["precip_pct"]
            prec  = day["prec_mm"]
            sky   = day["sky_icon"]

            t_max_s = f"{t_max}°" if t_max is not None else "—"
            t_min_s = f"{t_min}°" if t_min is not None else "—"
            pct_s   = f"{pct}%"   if pct   is not None else ""
            prec_s  = f"{prec} mm" if prec is not None and float(prec) > 0 else ""

            # Color by temperature max
            if t_max is not None:
                t_color = ("#c0392b" if t_max >= 30 else
                           "#e67e22" if t_max >= 22 else
                           "#2ca02c" if t_max >= 15 else
                           "#0096c7" if t_max >= 8  else "#8e44ad")
            else:
                t_color = "#95a5a6"

            with day_cols[i]:
                st.markdown(f"""
<div style="background:{LAYER_01};border-bottom:2px solid {t_color};
            padding:10px 8px;text-align:center;font-family:'IBM Plex Mono',monospace">
  <div style="color:{TEXT_SECONDARY};font-size:10px;font-weight:400;letter-spacing:0.32px;text-transform:uppercase">{day['weekday']}</div>
  <div style="color:{TEXT_SECONDARY};font-size:9px">{day['date_str']}</div>
  <div style="font-size:1.6rem;margin:0.3rem 0">{sky}</div>
  <div style="color:{t_color};font-size:1.1rem;font-weight:400">{t_max_s}</div>
  <div style="color:{BLUE_40};font-size:0.85rem">{t_min_s}</div>
  <div style="color:{BLUE_40};font-size:0.7rem;margin-top:0.3rem">
    {'💧 ' + pct_s if pct_s else ''}
  </div>
  <div style="color:{TEXT_SECONDARY};font-size:0.65rem">{prec_s}</div>
</div>""", unsafe_allow_html=True)

        # Temperature range chart from forecast
        dates   = [d["date_str"] for d in forecast_days if d["t_max"] is not None]
        t_maxes = [d["t_max"]    for d in forecast_days if d["t_max"] is not None]
        t_mins  = [d["t_min"]    for d in forecast_days if d["t_min"] is not None]
        if dates and t_maxes and t_mins:
            fig_fc = go.Figure()
            fig_fc.add_trace(go.Scatter(
                x=dates, y=t_maxes, name="Max temp",
                line=dict(color="rgba(214,39,40,0.5)", width=1), showlegend=False,
            ))
            fig_fc.add_trace(go.Scatter(
                x=dates, y=t_mins, name="Temp range",
                fill="tonexty", fillcolor="rgba(214,39,40,0.12)",
                line=dict(color="rgba(214,39,40,0.5)", width=1),
            ))
            fig_fc.add_trace(go.Bar(
                x=[d["date_str"] for d in forecast_days if d.get("prec_mm") is not None],
                y=[float(d["prec_mm"]) for d in forecast_days if d.get("prec_mm") is not None],
                name="Precip (mm)", yaxis="y2",
                marker_color="rgba(0,119,182,0.6)",
            ))
            fig_fc.update_layout(
                yaxis=dict(title="Temperature (°C)"),
                yaxis2=dict(title="Precip (mm)", overlaying="y", side="right", showgrid=False),
                height=300, margin=dict(t=20, b=40),
                template="plotly_dark",
                paper_bgcolor=LAYER_01, plot_bgcolor=LAYER_02,
                font=dict(family=FONT_MONO, color=TEXT_PRIMARY),
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_fc, use_container_width=True)

st.divider()

# ── Historical charts ──────────────────────────────────────────────────────────
st.subheader(f"📊 Historical observations — {selected_name}")

df = load_meteo_data(selected_id)
if df.empty:
    st.warning(f"No cached data for **{selected_name}** ({selected_id}).")
    st.stop()

if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
    df["ts"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
else:
    df["ts"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("ts")

latest = df.iloc[-1]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Latest temp",   f"{latest.get('temp_c',    np.nan):.1f} °C" if not pd.isna(latest.get("temp_c"))    else "—")
c2.metric("Latest precip", f"{latest.get('precip_mm', np.nan):.1f} mm" if not pd.isna(latest.get("precip_mm")) else "—")
c3.metric("Wind speed",    f"{latest.get('wind_speed_ms', np.nan):.1f} m/s" if not pd.isna(latest.get("wind_speed_ms")) else "—")
c4.metric("Humidity",      f"{latest.get('humidity_pct', np.nan):.0f}%" if not pd.isna(latest.get("humidity_pct")) else "—")

# Temperature range chart
temp_cols = [c for c in ["temp_min_c", "temp_c", "temp_max_c"] if c in df.columns]
if temp_cols and not df[temp_cols].isna().all().all():
    fig_t = go.Figure()
    if "temp_max_c" in df.columns and "temp_min_c" in df.columns:
        fig_t.add_trace(go.Scatter(
            x=df["ts"], y=df["temp_max_c"], name="Max", showlegend=False,
            line=dict(color="rgba(214,39,40,0.3)", width=1),
        ))
        fig_t.add_trace(go.Scatter(
            x=df["ts"], y=df["temp_min_c"], name="Temp range",
            fill="tonexty", fillcolor="rgba(214,39,40,0.1)",
            line=dict(color="rgba(214,39,40,0.3)", width=1),
        ))
    if "temp_c" in df.columns:
        fig_t.add_trace(go.Scatter(
            x=df["ts"], y=df["temp_c"], name="Mean temperature",
            line=dict(color="#d62728", width=2.5),
        ))
    fig_t.update_layout(
        yaxis_title="Temperature (°C)", xaxis_title="Time",
        height=300, margin=dict(t=20, b=40), hovermode="x unified",
        template="plotly_dark",
        paper_bgcolor=LAYER_01, plot_bgcolor=LAYER_02,
        font=dict(family=FONT_MONO, color=TEXT_PRIMARY),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_t, use_container_width=True)

# Precipitation
if "precip_mm" in df.columns and not df["precip_mm"].isna().all():
    fig_p = go.Figure()
    fig_p.add_trace(go.Bar(
        x=df["ts"], y=df["precip_mm"], name="Daily precip",
        marker_color="#023e8a", opacity=0.8,
    ))
    fig_p.add_trace(go.Scatter(
        x=df["ts"], y=df["precip_mm"].cumsum(), name="Cumulative",
        line=dict(color="#e63946", width=2), yaxis="y2",
    ))
    fig_p.update_layout(
        yaxis=dict(title="Daily precip (mm)"),
        yaxis2=dict(title="Cumulative (mm)", overlaying="y", side="right", showgrid=False),
        height=320, margin=dict(t=20, b=40), hovermode="x unified",
        template="plotly_dark",
        paper_bgcolor=LAYER_01, plot_bgcolor=LAYER_02,
        font=dict(family=FONT_MONO, color=TEXT_PRIMARY),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_p, use_container_width=True)

# Wind
if "wind_speed_ms" in df.columns and not df["wind_speed_ms"].isna().all():
    fig_w = px.line(df, x="ts", y="wind_speed_ms",
                    labels={"ts": "Time", "wind_speed_ms": "Wind speed (m/s)"},
                    color_discrete_sequence=[C_NORMAL])
    fig_w.update_layout(height=250, margin=dict(t=20, b=40),
                        template="plotly_dark",
                        paper_bgcolor=LAYER_01, plot_bgcolor=LAYER_02,
                        font=dict(family=FONT_MONO, color=TEXT_PRIMARY))
    st.plotly_chart(fig_w, use_container_width=True)

st.caption("⚠️ Historical data from cache only · Forecast: AEMET OpenData (live)")
