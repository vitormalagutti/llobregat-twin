"""
Page 5 — Aquifers (Piezometric levels)
IBM Carbon Design System — Gray 100 dark theme

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
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from app.carbon import (
    inject, hero, kpi_card, map_kpi, badge, section_label,
    BG, LAYER_01, LAYER_02, BORDER,
    TEXT_PRIMARY, TEXT_SECONDARY, TEXT_DISABLED,
    FONT_SANS, FONT_MONO,
    BLUE_40, C_CRITICAL, C_WATCH, C_NORMAL, C_LOW_FLOW, C_NODATA,
)

st.set_page_config(page_title="Aquifers — Llobregat", layout="wide")
inject()

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

# Aquifer accent — a blue-purple that works within Carbon
C_AQUIFER = "#be95ff"   # Carbon Purple 40 (light purple on dark)
C_AQUIFER_DIM = "#6929c4"  # Carbon Purple 70

# ── Plotly dark template ───────────────────────────────────────────────────────
DARK_LAYOUT = dict(
    paper_bgcolor=BG,
    plot_bgcolor=LAYER_01,
    font=dict(family=FONT_SANS, color=TEXT_SECONDARY),
    xaxis=dict(gridcolor=LAYER_02, linecolor=BORDER, tickcolor=TEXT_DISABLED),
    yaxis=dict(gridcolor=LAYER_02, linecolor=BORDER, tickcolor=TEXT_DISABLED),
)

# ── Hero banner (Carbon, purple accent) ───────────────────────────────────────
st.markdown(f"""
<div style="background:{LAYER_01};border-left:4px solid {C_AQUIFER};
            padding:1.4rem 2rem;margin-bottom:1.2rem;
            display:flex;align-items:center;justify-content:space-between">
  <div>
    <h1 style="font-family:{FONT_SANS};font-size:1.8rem;font-weight:600;
               color:{TEXT_PRIMARY};margin:0">🪨 Aquifer Monitoring</h1>
    <p style="font-family:{FONT_SANS};color:{TEXT_SECONDARY};
              margin:0.3rem 0 0;font-size:0.9rem">
      Baix Llobregat alluvial aquifer · Piezometric levels · Barcelona metropolitan area
    </p>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Context KPI cards ──────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(kpi_card(
        label="Aquifer area",
        value="~30 km",
        trend="Lower Llobregat valley length",
        color=C_AQUIFER,
    ), unsafe_allow_html=True)
with col2:
    st.markdown(kpi_card(
        label="Aquifer type",
        value="Alluvial",
        trend="Quaternary fluvial deposits",
        color=C_AQUIFER,
    ), unsafe_allow_html=True)
with col3:
    st.markdown(kpi_card(
        label="Key risk",
        value="Saltwater",
        trend="Seawater intrusion near delta",
        color=C_CRITICAL,
    ), unsafe_allow_html=True)

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
st.markdown(section_label("Baix Llobregat Aquifer — Location"), unsafe_allow_html=True)

m = folium.Map(location=[41.35, 2.00], zoom_start=10, tiles=None)
folium.TileLayer("CartoDB dark_matter", name="🗺️ Dark",  overlay=False, control=True).add_to(m)
folium.TileLayer("CartoDB positron",    name="🗺️ Light", overlay=False, control=True).add_to(m)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri", name="🛰️ Satellite", overlay=False, control=True,
).add_to(m)

# Aquifer extent polygon
aquifer_polygon = {
    "type": "FeatureCollection",
    "features": [{
        "type": "Feature",
        "properties": {"name": "Baix Llobregat alluvial aquifer"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [1.88, 41.55], [1.92, 41.50], [1.98, 41.40], [2.05, 41.35],
                [2.10, 41.32], [2.12, 41.30], [2.07, 41.28], [1.98, 41.30],
                [1.90, 41.38], [1.85, 41.48], [1.86, 41.54], [1.88, 41.55]
            ]]
        }
    }]
}

folium.GeoJson(
    aquifer_polygon,
    style_function=lambda _: {
        "fillColor":   C_AQUIFER,
        "fillOpacity": 0.14,
        "color":       C_AQUIFER,
        "weight":      2,
        "dashArray":   "5 3",
    },
    tooltip=folium.GeoJsonTooltip(fields=["name"], aliases=[""]),
).add_to(m)

# Llobregat river path
folium.PolyLine(
    locations=[[41.55, 1.93], [41.48, 1.93], [41.39, 2.02], [41.35, 2.05], [41.30, 2.07]],
    color=BLUE_40, weight=3, opacity=0.8, tooltip="Llobregat",
).add_to(m)

# Sant Joan Despí marker (Carbon card style)
folium.Marker(
    location=[41.352, 2.047],
    icon=folium.DivIcon(
        html=map_kpi(label="💧 Sant Joan Despí", value="Main gauge", color=BLUE_40),
        icon_size=(140, 50), icon_anchor=(70, 25),
    ),
    popup="Sant Joan Despí river gauge — main channel monitoring point",
).add_to(m)

# Key towns (minimal Carbon labels)
for name, lat, lon in [
    ("Barcelona", 41.383, 2.176),
    ("El Prat",   41.326, 2.095),
    ("Cornellà",  41.355, 2.076),
    ("Sant Boi",  41.343, 2.038),
]:
    folium.CircleMarker(
        location=[lat, lon], radius=4,
        color=BORDER, fill=True, fill_color=TEXT_DISABLED, fill_opacity=0.8,
        tooltip=name,
    ).add_to(m)
    folium.Marker(
        location=[lat, lon],
        icon=folium.DivIcon(
            html=f'<div style="font-family:{FONT_SANS};font-size:10px;'
                 f'color:{TEXT_SECONDARY};font-weight:600;'
                 f'margin-left:8px;white-space:nowrap">{name}</div>',
            icon_size=(80, 18), icon_anchor=(0, 9),
        ),
    ).add_to(m)

# Piezometric station markers (if configured)
if stations:
    for s in stations:
        folium.Marker(
            location=[s["lat"], s["lon"]],
            icon=folium.DivIcon(
                html=map_kpi(label=f"🪨 {s['name'][:14]}", value="Piezo", color=C_AQUIFER),
                icon_size=(120, 50), icon_anchor=(60, 25),
            ),
            popup=f"Piezometric station: {s['id']}",
        ).add_to(m)

folium.LayerControl(position="topright", collapsed=True).add_to(m)
st_folium(m, use_container_width=True, height=480, returned_objects=[])

# ── Status ─────────────────────────────────────────────────────────────────────
if not stations:
    st.markdown(f"""
<div style="background:{LAYER_01};border-left:4px solid {C_AQUIFER};
            padding:1.2rem 1.4rem;margin-top:1rem;font-family:{FONT_SANS}">
  <div style="color:{C_AQUIFER};font-size:11px;font-weight:600;
              text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px">
    Piezometric stations not yet configured
  </div>
  <div style="color:{TEXT_PRIMARY};font-size:14px;margin-bottom:8px">
    The ACA network publishes piezometric data via Sentilo
    (<code style="color:{C_AQUIFER};background:{LAYER_02};padding:2px 6px">componentType=piezometre</code>),
    but station IDs first need to be discovered and verified.
  </div>
  <div style="color:{TEXT_SECONDARY};font-size:13px">
    Run <code style="color:{BLUE_40};background:{LAYER_02};padding:2px 6px">python -m data.fetchers.discover_stations</code>
    (set <code style="background:{LAYER_02};padding:2px 4px">component_type = 'piezometre'</code>),
    then add the verified IDs to
    <code style="background:{LAYER_02};padding:2px 4px">config/station_metadata.yaml</code>
    under <code style="background:{LAYER_02};padding:2px 4px">piezo_stations</code>.
  </div>
</div>
""", unsafe_allow_html=True)
    st.stop()

# ── If we have stations, show the data ────────────────────────────────────────
@st.cache_data(ttl=1800)
def load_piezo_data(station_id: str) -> pd.DataFrame:
    files = sorted(CACHE_DIR.glob(f"piezo_{station_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])

st.divider()
st.markdown(section_label("Piezometric data"), unsafe_allow_html=True)

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
        fill="tozeroy",
        line=dict(color=C_AQUIFER, width=2),
        fillcolor="rgba(190,149,255,0.10)",
        hovertemplate="%{x|%d %b %H:%M}<br><b>%{y:.2f} m a.s.l.</b><extra></extra>",
    ))
    fig.update_layout(
        **DARK_LAYOUT,
        yaxis_title="Level (m a.s.l.)", xaxis_title="Time",
        height=360, margin=dict(t=20, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

st.caption("⚠️ Data from cache only · Source: ACA piezometric network")
