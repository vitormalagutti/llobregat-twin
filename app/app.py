"""
Llobregat Watershed Monitoring Dashboard — Main Entry Point

This is the root Streamlit app file. Streamlit's multi-page convention
auto-discovers pages in app/pages/ and lists them in the sidebar.

Run locally:
    streamlit run app/app.py
"""
import streamlit as st
from pathlib import Path
from datetime import datetime, timezone
import sys

_APP_DIR = Path(__file__).parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))
from carbon import (inject, hero, kpi_card,
                    BG, LAYER_01, TEXT_PRIMARY, TEXT_SECONDARY,
                    BLUE_40, C_NORMAL, FONT_MONO)

st.set_page_config(
    page_title="Llobregat Watershed — Monitoring Dashboard",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject()

# ── Sidebar: branding + cache freshness ───────────────────────────────────────
with st.sidebar:
    st.title("💧 Llobregat")
    st.caption("Watershed Monitoring Dashboard")
    st.divider()

    # Show the freshest cache timestamp across all parquet files
    cache_dir = Path(__file__).parent.parent / "data" / "cache"
    parquet_files = sorted(cache_dir.glob("*.parquet"))
    if parquet_files:
        most_recent = parquet_files[-1]
        # Extract date from filename (format: prefix_stationid_YYYYMMDD.parquet)
        try:
            date_str = most_recent.stem.split("_")[-1]
            cache_date = datetime.strptime(date_str, "%Y%m%d").replace(
                tzinfo=timezone.utc
            )
            st.caption(f"📦 Cache updated: {cache_date.strftime('%d %b %Y')}")
        except (ValueError, IndexError):
            st.caption("📦 Cache: available")
    else:
        st.warning("⚠️ No cached data found. Run `python -m data.fetchers.refresh_all` first.")

    st.divider()
    st.caption(
        "Data: [ACA SDIM](https://aca.gencat.cat) · "
        "[AEMET OpenData](https://opendata.aemet.es)"
    )
    st.caption("⚠️ This is a monitoring dashboard, not a digital twin.")

# ── Landing page content ───────────────────────────────────────────────────────
st.markdown(hero(
    title="Llobregat Watershed",
    subtitle="Hydrological monitoring dashboard · ACA + AEMET live data",
), unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(kpi_card(label="Main stem length", value="~170 km",
                         sub="Source to Barcelona delta", color=BLUE_40), unsafe_allow_html=True)
with col2:
    st.markdown(kpi_card(label="Mean flow (Martorell)", value="~17 m³/s",
                         sub="Long-term average", color=BLUE_40), unsafe_allow_html=True)
with col3:
    st.markdown(kpi_card(label="Watershed area", value="~5,000 km²",
                         sub="Including Cardener & Anoia", color=BLUE_40), unsafe_allow_html=True)

st.markdown(f"""
<div style="background:{LAYER_01};padding:24px 32px;margin-top:24px;border-left:3px solid {BLUE_40}">
  <p style="color:{TEXT_SECONDARY};font-size:14px;margin:0;line-height:1.6">
    Use the sidebar to navigate: <strong style="color:{TEXT_PRIMARY}">Overview</strong> — watershed map ·
    <strong style="color:{TEXT_PRIMARY}">Rivers</strong> — flow hydrographs ·
    <strong style="color:{TEXT_PRIMARY}">Reservoirs</strong> — storage levels ·
    <strong style="color:{TEXT_PRIMARY}">Meteorology</strong> — precipitation & temperature ·
    <strong style="color:{TEXT_PRIMARY}">Aquifers</strong> — piezometric levels
  </p>
  <p style="color:{TEXT_SECONDARY};font-size:12px;margin:8px 0 0;opacity:0.7">
    ⚠️ This displays observed and cached data from ACA and AEMET monitoring networks.
    It is not a simulation model.
  </p>
</div>
""", unsafe_allow_html=True)
