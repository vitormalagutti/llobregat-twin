# data/fetchers/__init__.py
# Data fetching layer for the Llobregat Watershed Monitoring Dashboard.
# All modules in this package write to data/cache/ as Parquet files.
# They MUST NOT import from or write to app/ (Streamlit layer).
