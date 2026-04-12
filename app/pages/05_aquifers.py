"""
Page 5 — Aquifers (Piezometric levels)

The Baix Llobregat alluvial aquifer is one of the most important
groundwater bodies in Catalonia. ACA monitors it via a network of
piezometric stations. Station IDs need to be discovered before data
can be shown here.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
from pathlib import Path
import yaml
import numpy as np
import json
import sys

_APP_DIR = Path(__file__).parent.parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))
from carbon import (inject, hero, kpi_card,
                    BG, LAYER_01, LAYER_02, BORDER, TEXT_PRIMARY, TEXT_SECONDARY,
                    BLUE_40, C_NORMAL, C_NODATA, FONT_MONO)

st.set_page_config(page_title="Aquifers — Llobregat", layout="wide")
inject()

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
SHPS_DIR   = Path(__file__).parent.parent.parent / "shps"

# ── Hero ───────────────────────────────────────────────────────────────────────
st.markdown(hero(
    title="🪨 Aquifer Monitoring",
    subtitle="Baix Llobregat alluvial aquifer · Piezometric levels · Barcelona metropolitan area",
), unsafe_allow_html=True)

# ── Context cards ──────────────────────────────────────────────────────────────
C_PIEZO = "#a56eff"  # Purple accent for aquifer page
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(kpi_card(label="Aquifer area", value="~30 km",
                         sub="Lower Llobregat valley length", color=C_PIEZO), unsafe_allow_html=True)
with col2:
    st.markdown(kpi_card(label="Aquifer type", value="Alluvial",
                         sub="Quaternary fluvial deposits", color=C_PIEZO), unsafe_allow_html=True)
with col3:
    st.markdown(kpi_card(label="Key risk", value="Saltwater",
                         sub="Seawater intrusion near delta", color=C_PIEZO), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Load piezometric stations ──────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_piezo_stations() -> list:
    p = CONFIG_DIR / "station_metadata.yaml"
    if not p.exists():
        return []
    with open(p) as f:
        return yaml.safe_load(f).get("piezo_stations") or []

stations = load_piezo_stations()

# ── Aquifer location map ───────────────────────────────────────────────────────
st.subheader("Baix Llobregat Aquifer — Location")

@st.cache_data(ttl=86400)
def load_geojson(name: str) -> dict:
    p = SHPS_DIR / f"{name}.geojson"
    if not p.exists():
        return {"type": "FeatureCollection", "features": []}
    with open(p) as f:
        return json.load(f)

# Colour-code aquifer zones by the 'tipo' field
TIPO_STYLES = {
    "modelo":      {"fillColor": "#9b00b3", "fillOpacity": 0.20, "color": "#9b00b3", "weight": 2},
    "Aquitard":    {"fillColor": "#e67e22", "fillOpacity": 0.15, "color": "#e67e22", "weight": 1.5, "dashArray": "4 3"},
    "Plan de BCN": {"fillColor": "#0096c7", "fillOpacity": 0.12, "color": "#0096c7", "weight": 1.5, "dashArray": "4 3"},
}

def aquifer_style(feature):
    tipo = feature.get("properties", {}).get("tipo", "")
    return TIPO_STYLES.get(tipo, {"fillColor": "#9b00b3", "fillOpacity": 0.15, "color": "#9b00b3", "weight": 2})

m = folium.Map(location=[41.35, 2.00], zoom_start=10, tiles=None)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri", name="🛰️ Satellite", overlay=False, control=False,
).add_to(m)

# Real aquifer model from shapefile — 3 zones: modelo (purple), Aquitard (orange), Plan de BCN (blue)
folium.GeoJson(
    load_geojson("modelo_flujo"),
    name="🪨 Aquifer model zones",
    style_function=aquifer_style,
    tooltip=folium.GeoJsonTooltip(fields=["tipo"], aliases=["Zone:"]),
).add_to(m)

# Real river drainage network
folium.GeoJson(
    load_geojson("Drenaje"),
    name="🌊 River network",
    style_function=lambda _: {"color": "#1e90ff", "weight": 2, "opacity": 0.7},
    tooltip="Llobregat drainage network",
).add_to(m)

# Sant Joan Despí marker (main gauge)
folium.Marker(
    location=[41.352, 2.047],
    icon=folium.DivIcon(
        html="""<div style="background:#0d1b2a;border:2px solid #0096c7;border-radius:6px;
                           padding:3px 7px;white-space:nowrap;font-size:10px;color:white;
                           font-family:sans-serif">💧 Sant Joan Despí</div>""",
        icon_size=(130, 28), icon_anchor=(65, 14),
    ),
    popup="Sant Joan Despí river gauge — main channel monitoring point",
).add_to(m)

# Key towns
for name, lat, lon in [("Barcelona", 41.383, 2.176), ("El Prat", 41.326, 2.095),
                        ("Cornellà", 41.355, 2.076), ("Sant Boi", 41.343, 2.038)]:
    folium.CircleMarker(
        location=[lat, lon], radius=4,
        color="white", fill=True, fill_color="#666", fill_opacity=0.7,
        tooltip=name,
    ).add_to(m)
    folium.Marker(
        location=[lat, lon],
        icon=folium.DivIcon(
            html=f'<div style="font-size:10px;color:#333;font-family:sans-serif;'
                 f'font-weight:600;margin-left:6px">{name}</div>',
            icon_size=(80, 18), icon_anchor=(0, 9),
        ),
    ).add_to(m)

if stations:
    for s in stations:
        folium.Marker(
            location=[s["lat"], s["lon"]],
            icon=folium.DivIcon(
                html=f"""<div style="background:#1a0026;border:2px solid #9b00b3;
                                   border-radius:6px;padding:3px 7px;white-space:nowrap;
                                   font-size:10px;color:white;font-family:sans-serif">
                          🪨 {s['name']}</div>""",
                icon_size=(120, 28), icon_anchor=(60, 14),
            ),
            popup=f"Piezometric station: {s['id']}",
        ).add_to(m)

folium.LayerControl(position="topright", collapsed=True).add_to(m)
st_folium(m, use_container_width=True, height=480, returned_objects=[])

# ── Status ─────────────────────────────────────────────────────────────────────
if not stations:
    st.info("""
**Piezometric station IDs not yet configured.**

The ACA network does publish piezometric data via Sentilo (`componentType=piezometre`),
but the station IDs first need to be discovered and verified. Run:

```bash
python -m data.fetchers.discover_stations
```

(modify the `component_type` variable to `'piezometre'`) — this will write a
CSV of all active piezometric components. Then add the verified IDs to
`config/station_metadata.yaml` under `piezo_stations`.
""")
    st.stop()

# ── If we have stations, show the data ────────────────────────────────────────
@st.cache_data(ttl=1800)
def load_piezo_data(station_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"piezo_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

station_options = {s["name"]: s["id"] for s in stations}
selected_name   = st.selectbox("Select piezometric station", list(station_options.keys()))
selected_id     = station_options[selected_name]

df = load_piezo_data(selected_id)
if df.empty:
    st.warning(f"No data cached for **{selected_name}** ({selected_id}).")
    st.stop()

if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
    df["ts"] = df["timestamp"].dt.tz_convert("Europe/Madrid")
else:
    df["ts"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("ts")

latest = df.iloc[-1]
col1, col2 = st.columns(2)
col1.metric("Depth to water table",
            f"{latest.get('depth_m', np.nan):.2f} m" if not pd.isna(latest.get("depth_m")) else "—")
col2.metric("Piezometric level",
            f"{latest.get('level_masl', np.nan):.2f} m a.s.l." if not pd.isna(latest.get("level_masl")) else "—")

if "level_masl" in df.columns and not df["level_masl"].isna().all():
    fig = go.Figure(go.Scatter(
        x=df["ts"], y=df["level_masl"], mode="lines",
        fill="tozeroy", line=dict(color="#9b00b3", width=2),
        fillcolor="rgba(155,0,179,0.10)",
        hovertemplate="%{x|%d %b %H:%M}<br><b>%{y:.2f} m a.s.l.</b><extra></extra>",
    ))
    fig.update_layout(yaxis_title="Level (m a.s.l.)", xaxis_title="Time",
                      height=360, margin=dict(t=20, b=40),
                      template="plotly_dark",
                      paper_bgcolor=LAYER_01, plot_bgcolor=LAYER_02,
                      font=dict(family=FONT_MONO, color=TEXT_PRIMARY))
    st.plotly_chart(fig, use_container_width=True)

st.caption("⚠️ Data from cache only · Source: ACA piezometric network")
