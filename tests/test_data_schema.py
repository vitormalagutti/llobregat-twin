"""
tests/test_data_schema.py

Tests that verify DataFrames produced by fetchers conform to DATA_SCHEMA.md.

These are integration-level schema tests: given any DataFrame that claims to be
a gauge/reservoir/meteo/piezo record, assert it has the required columns,
correct dtypes, and no forbidden sentinel values (0 in place of NaN, -9999, etc.).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from data.fetchers import aca, aemet


# ── Schema validators (reusable) ──────────────────────────────────────────────

def assert_gauge_schema(df: pd.DataFrame) -> None:
    required = ["timestamp", "station_id", "station_name", "flow_m3s", "level_m", "quality_flag"]
    for col in required:
        assert col in df.columns, f"Missing column: {col}"
    if len(df):
        assert df.dtypes["flow_m3s"] == "float64"
        assert df.dtypes["level_m"] == "float64"
        # No sentinel -9999 values
        assert not (df["flow_m3s"] == -9999).any(), "Found -9999 sentinel in flow_m3s"
        assert not (df["level_m"] == -9999).any(), "Found -9999 sentinel in level_m"


def assert_reservoir_schema(df: pd.DataFrame) -> None:
    required = ["timestamp", "reservoir_id", "reservoir_name",
                "volume_hm3", "level_m", "capacity_hm3", "pct_capacity"]
    for col in required:
        assert col in df.columns, f"Missing column: {col}"
    if len(df):
        assert df.dtypes["pct_capacity"] == "float64"
        # pct_capacity must be 0–100 or NaN, never negative or > 200
        valid_mask = pd.notna(df["pct_capacity"])
        if valid_mask.any():
            assert (df.loc[valid_mask, "pct_capacity"] >= 0).all()
            assert (df.loc[valid_mask, "pct_capacity"] <= 200).all()  # 200% allows some tolerance


def assert_meteo_schema(df: pd.DataFrame) -> None:
    required = ["timestamp", "station_id", "station_name",
                "precip_mm", "temp_c", "temp_max_c", "temp_min_c",
                "wind_speed_ms", "wind_dir_deg", "humidity_pct"]
    for col in required:
        assert col in df.columns, f"Missing column: {col}"
    if len(df):
        # Humidity must be 0–100 or NaN
        valid_hum = df["humidity_pct"].dropna()
        if len(valid_hum):
            assert (valid_hum >= 0).all() and (valid_hum <= 100).all()
        # Wind direction must be 0–360 or NaN
        valid_dir = df["wind_dir_deg"].dropna()
        if len(valid_dir):
            assert (valid_dir >= 0).all() and (valid_dir <= 360).all()


def assert_piezo_schema(df: pd.DataFrame) -> None:
    required = ["timestamp", "station_id", "station_name", "depth_m", "level_masl", "aquifer_unit"]
    for col in required:
        assert col in df.columns, f"Missing column: {col}"


# ── Tests using empty DataFrames (always available, no network needed) ────────

class TestEmptyDataFrameSchemaCompliance:
    def test_empty_gauge_schema(self):
        assert_gauge_schema(aca._empty_gauge_df())

    def test_empty_reservoir_schema(self):
        assert_reservoir_schema(aca._empty_reservoir_df())

    def test_empty_piezo_schema(self):
        assert_piezo_schema(aca._empty_piezo_df())

    def test_empty_meteo_schema(self):
        assert_meteo_schema(aemet._empty_meteo_df())


# ── Tests using synthetic populated DataFrames ────────────────────────────────

class TestPopulatedDataFrameSchemaCompliance:
    """Build minimal synthetic DataFrames and assert schema compliance."""

    def test_synthetic_gauge_df(self):
        df = pd.DataFrame([{
            "timestamp": pd.Timestamp("2025-03-01T10:00:00", tz="UTC"),
            "station_id": "E003",
            "station_name": "Martorell",
            "flow_m3s": 5.2,
            "level_m": 1.1,
            "quality_flag": "B",
        }])
        assert_gauge_schema(df)

    def test_synthetic_gauge_with_nan_flow(self):
        df = pd.DataFrame([{
            "timestamp": pd.Timestamp("2025-03-01T10:00:00", tz="UTC"),
            "station_id": "E003",
            "station_name": "Martorell",
            "flow_m3s": np.nan,
            "level_m": np.nan,
            "quality_flag": "unknown",
        }])
        assert_gauge_schema(df)
        assert pd.isna(df["flow_m3s"].iloc[0])

    def test_synthetic_reservoir_pct_range(self):
        df = pd.DataFrame([{
            "timestamp": pd.Timestamp("2025-03-01T06:00:00", tz="UTC"),
            "reservoir_id": "BAELLS",
            "reservoir_name": "La Baells",
            "volume_hm3": 54.5,
            "level_m": 677.5,
            "capacity_hm3": 109.0,
            "pct_capacity": 54.5 / 109.0 * 100,
        }])
        assert_reservoir_schema(df)

    def test_synthetic_meteo_df(self):
        df = pd.DataFrame([{
            "timestamp": pd.Timestamp("2025-03-01T12:00:00", tz="UTC"),
            "station_id": "0149X",
            "station_name": "Manresa",
            "precip_mm": 3.2,
            "temp_c": 15.4,
            "temp_max_c": 18.1,
            "temp_min_c": 12.0,
            "wind_speed_ms": 2.5,
            "wind_dir_deg": 220.0,
            "humidity_pct": 68.0,
        }])
        assert_meteo_schema(df)


# ── NaN convention tests ────────────────────────────────────────────────────────

class TestNaNConvention:
    """Missing data must be np.nan, never 0 or -9999 (per DATA_SCHEMA.md)."""

    def test_zero_is_valid_flow(self):
        """0 m3/s is a real value (dry stream), not a missing data sentinel."""
        df = pd.DataFrame([{
            "timestamp": pd.Timestamp("2025-08-01T12:00:00", tz="UTC"),
            "station_id": "E003",
            "station_name": "Martorell",
            "flow_m3s": 0.0,
            "level_m": 0.05,
            "quality_flag": "B",
        }])
        assert_gauge_schema(df)
        assert df["flow_m3s"].iloc[0] == 0.0  # Zero is valid — low summer flow

    def test_negative_9999_sentinel_fails_check(self):
        """Regression: sentinel -9999 must never appear in cache files."""
        df = pd.DataFrame([{
            "timestamp": pd.Timestamp("2025-03-01T10:00:00", tz="UTC"),
            "station_id": "E003",
            "station_name": "Martorell",
            "flow_m3s": -9999.0,  # This should NOT be in real data
            "level_m": np.nan,
            "quality_flag": "unknown",
        }])
        with pytest.raises(AssertionError, match="-9999"):
            assert_gauge_schema(df)
