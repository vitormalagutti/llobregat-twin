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

st.set_page_config(
    page_title="Llobregat Watershed — Monitoring Dashboard",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
st.title("Llobregat Watershed Monitoring Dashboard")
st.markdown(
    """
    Welcome. Use the sidebar to navigate between sections:

    - **Overview** — interactive watershed map and system status
    - **Rivers** — gauge flow hydrographs and comparisons
    - **Reservoirs** — storage levels and trends
    - **Meteorology** — precipitation, temperature, and wind
    - **Aquifers** — piezometric levels (Baix Llobregat)

    ---
    > This tool displays observed and recently cached data from the ACA and AEMET
    > monitoring networks. It is **not** a simulation model. Forecasts and
    > rainfall-runoff modelling are planned for a future phase.
    """
)

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Main stem length", "~170 km")
with col2:
    st.metric("Mean flow (Martorell)", "~17 m³/s")
with col3:
    st.metric("Watershed area", "~5,000 km²")
