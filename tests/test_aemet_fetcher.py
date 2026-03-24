"""
tests/test_aemet_fetcher.py

Unit tests for the AEMET OpenData fetcher module.

Tests cover:
  - Empty DataFrame schema compliance
  - Two-step response pattern parsing
  - Missing / null field handling → np.nan
  - Coordinate conversion (AEMET DMS format → decimal degrees)
  - Missing API key → empty DataFrame, not crash

Run with: pytest tests/test_aemet_fetcher.py -v
"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from data.fetchers import aemet


# ── Schema compliance ──────────────────────────────────────────────────────────

class TestEmptyMeteoSchema:
    def test_empty_df_columns(self):
        df = aemet._empty_meteo_df()
        expected = {
            "timestamp", "station_id", "station_name",
            "precip_mm", "temp_c", "temp_max_c", "temp_min_c",
            "wind_speed_ms", "wind_dir_deg", "humidity_pct",
        }
        assert set(df.columns) == expected

    def test_empty_df_is_empty(self):
        df = aemet._empty_meteo_df()
        assert len(df) == 0

    def test_numeric_cols_are_float64(self):
        df = aemet._empty_meteo_df()
        for col in ["precip_mm", "temp_c", "temp_max_c", "temp_min_c",
                    "wind_speed_ms", "wind_dir_deg", "humidity_pct"]:
            assert df.dtypes[col] == "float64", f"{col} should be float64"


# ── Missing API key ────────────────────────────────────────────────────────────

class TestMissingApiKey:
    def test_no_api_key_returns_empty_df(self):
        """Missing AEMET_API_KEY must return empty DataFrame, never raise."""
        import os
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AEMET_API_KEY", None)
            df = aemet.fetch_aemet_observations("0149X", "Manresa", api_key="")
        assert isinstance(df, pd.DataFrame)
        assert df.empty
        assert "temp_c" in df.columns


# ── Two-step response parsing ─────────────────────────────────────────────────

class TestTwoStepResponse:
    """AEMET returns a redirect URL in step-1; step-2 returns actual data."""

    def _make_two_step_mock_client(self, obs_data: list) -> MagicMock:
        # Step 1 response: contains datos URL
        step1_resp = MagicMock()
        step1_resp.status_code = 200
        step1_resp.raise_for_status.return_value = None
        step1_resp.json.return_value = {
            "estado": 200,
            "datos": "https://opendata.aemet.es/fake-data-url",
            "metadatos": "https://opendata.aemet.es/fake-meta-url",
        }

        # Step 2 response: actual observation data
        step2_resp = MagicMock()
        step2_resp.status_code = 200
        step2_resp.raise_for_status.return_value = None
        step2_resp.json.return_value = obs_data

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = [step1_resp, step2_resp]
        return mock_client

    @patch("data.fetchers.aemet.make_client")
    def test_parses_typical_observation(self, mock_make_client):
        obs = [{
            "fint": "2025-03-01T12:00:00",
            "idema": "0149X",
            "ubi": "MANRESA",
            "prec": "3.2",
            "ta": "15.4",
            "tamax": "18.1",
            "tamin": "12.0",
            "vv": "2.5",
            "dv": "220",
            "hr": "68",
        }]
        mock_make_client.return_value = self._make_two_step_mock_client(obs)
        df = aemet.fetch_aemet_observations("0149X", "Manresa", api_key="test-key")
        assert len(df) == 1
        assert df["precip_mm"].iloc[0] == pytest.approx(3.2)
        assert df["temp_c"].iloc[0] == pytest.approx(15.4)
        assert df["wind_speed_ms"].iloc[0] == pytest.approx(2.5)
        assert df["station_id"].iloc[0] == "0149X"

    @patch("data.fetchers.aemet.make_client")
    def test_null_fields_become_nan(self, mock_make_client):
        obs = [{
            "fint": "2025-03-01T12:00:00",
            "prec": None,
            "ta": "",
            "tamax": "Ip",   # AEMET uses "Ip" for trace precipitation
            "vv": None,
        }]
        mock_make_client.return_value = self._make_two_step_mock_client(obs)
        df = aemet.fetch_aemet_observations("0149X", "Manresa", api_key="test-key")
        assert len(df) == 1
        assert pd.isna(df["precip_mm"].iloc[0])
        assert pd.isna(df["temp_c"].iloc[0])
        assert pd.isna(df["temp_max_c"].iloc[0])  # "Ip" → NaN

    @patch("data.fetchers.aemet.make_client")
    def test_sorted_by_timestamp(self, mock_make_client):
        obs = [
            {"fint": "2025-03-01T14:00:00", "ta": "17.0"},
            {"fint": "2025-03-01T10:00:00", "ta": "13.0"},
            {"fint": "2025-03-01T12:00:00", "ta": "15.0"},
        ]
        mock_make_client.return_value = self._make_two_step_mock_client(obs)
        df = aemet.fetch_aemet_observations("0149X", "Manresa", api_key="test-key")
        ts = df["timestamp"].tolist()
        assert ts == sorted(ts)

    @patch("data.fetchers.aemet.make_client")
    def test_estado_404_returns_empty(self, mock_make_client):
        step1_resp = MagicMock()
        step1_resp.status_code = 200
        step1_resp.raise_for_status.return_value = None
        step1_resp.json.return_value = {"estado": 404, "descripcion": "No data"}

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = step1_resp
        mock_make_client.return_value = mock_client

        df = aemet.fetch_aemet_observations("0149X", "Manresa", api_key="test-key")
        assert df.empty


# ── Coordinate conversion ─────────────────────────────────────────────────────

class TestCoordinateConversion:
    """AEMET coordinates use DDMMSS(N/S/E/W) format."""

    def test_north_latitude(self):
        # 41°33'24"N
        result = aemet._aemet_coord_to_decimal("413324N")
        expected = 41 + 33/60 + 24/3600
        assert result == pytest.approx(expected, rel=1e-5)

    def test_east_longitude(self):
        # 1°49'26"E
        result = aemet._aemet_coord_to_decimal("014926E")
        expected = 1 + 49/60 + 26/3600
        assert result == pytest.approx(expected, rel=1e-5)

    def test_west_longitude_is_negative(self):
        result = aemet._aemet_coord_to_decimal("031200W")
        assert result < 0

    def test_south_latitude_is_negative(self):
        result = aemet._aemet_coord_to_decimal("123456S")
        assert result < 0

    def test_already_decimal_passes_through(self):
        result = aemet._aemet_coord_to_decimal("41.5567")
        assert result == pytest.approx(41.5567)
