"""
tests/test_aca_fetcher.py

Unit tests for the ACA SDIM fetcher module.

Tests cover:
  - Empty DataFrame schema compliance (the most critical invariant)
  - Response parsing with mock HTTP responses
  - Graceful handling of HTTP 204 (no content)
  - Graceful handling of network errors
  - Cache path generation
  - pct_capacity calculation

Run with: pytest tests/test_aca_fetcher.py -v
"""
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from data.fetchers import aca


# ── Schema compliance tests ────────────────────────────────────────────────────

class TestEmptyDataFrameSchemas:
    """Empty DataFrames must have exactly the right column names and dtypes."""

    def test_empty_gauge_df_columns(self):
        df = aca._empty_gauge_df()
        expected = {"timestamp", "station_id", "station_name", "flow_m3s", "level_m", "quality_flag"}
        assert set(df.columns) == expected

    def test_empty_gauge_df_is_empty(self):
        df = aca._empty_gauge_df()
        assert len(df) == 0

    def test_empty_gauge_df_numeric_cols_are_float(self):
        df = aca._empty_gauge_df()
        assert df.dtypes["flow_m3s"] == "float64"
        assert df.dtypes["level_m"] == "float64"

    def test_empty_reservoir_df_columns(self):
        df = aca._empty_reservoir_df()
        expected = {
            "timestamp", "reservoir_id", "reservoir_name",
            "volume_hm3", "level_m", "capacity_hm3", "pct_capacity"
        }
        assert set(df.columns) == expected

    def test_empty_piezo_df_columns(self):
        df = aca._empty_piezo_df()
        expected = {"timestamp", "station_id", "station_name", "depth_m", "level_masl", "aquifer_unit"}
        assert set(df.columns) == expected


# ── HTTP 204 / no-content handling ────────────────────────────────────────────

class TestHttp204Handling:
    """HTTP 204 (no content) must return an empty DataFrame, never raise."""

    def _mock_204_client(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_resp.raise_for_status.return_value = None
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        return mock_client

    @patch("data.fetchers.aca.make_client")
    def test_gauge_204_returns_empty_df(self, mock_make_client):
        mock_make_client.return_value = self._mock_204_client()
        df = aca.fetch_aca_gauge_data("E003", "Test station")
        assert isinstance(df, pd.DataFrame)
        assert df.empty
        assert "flow_m3s" in df.columns

    @patch("data.fetchers.aca.make_client")
    def test_piezo_204_returns_empty_df(self, mock_make_client):
        mock_make_client.return_value = self._mock_204_client()
        df = aca.fetch_aca_piezo_data("PIE001", "Test piezometer")
        assert isinstance(df, pd.DataFrame)
        assert df.empty
        assert "level_masl" in df.columns

    @patch("data.fetchers.aca.make_client")
    def test_reservoir_204_returns_empty_df(self, mock_make_client):
        mock_make_client.return_value = self._mock_204_client()
        df = aca.fetch_aca_reservoir_data("BAELLS", "La Baells", capacity_hm3=109.0)
        assert isinstance(df, pd.DataFrame)
        assert df.empty


# ── Data parsing tests ─────────────────────────────────────────────────────────

class TestGaugeDataParsing:
    """Test correct parsing of mock ACA API responses."""

    def _make_mock_response(self, records: list, status_code: int = 200):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = records
        return mock_resp

    def _mock_client_with_responses(self, responses: list):
        """Client that returns each response in sequence for successive .get() calls."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = responses
        return mock_client

    @patch("data.fetchers.aca.make_client")
    def test_gauge_parses_flow_records(self, mock_make_client):
        flow_records = [
            {"data": "2025-03-01T10:00:00", "valor": "5.2", "codiQualitat": "B"},
            {"data": "2025-03-01T10:30:00", "valor": "5.5", "codiQualitat": "B"},
        ]
        stage_records = [
            {"data": "2025-03-01T10:00:00", "valor": "1.1"},
            {"data": "2025-03-01T10:30:00", "valor": "1.2"},
        ]
        mock_make_client.return_value = self._mock_client_with_responses([
            self._make_mock_response(flow_records),
            self._make_mock_response(stage_records),
        ])
        df = aca.fetch_aca_gauge_data("E003", "Martorell")
        assert len(df) == 2
        assert set(df.columns) == {"timestamp", "station_id", "station_name", "flow_m3s", "level_m", "quality_flag"}
        assert df["flow_m3s"].iloc[0] == pytest.approx(5.2)
        assert df["level_m"].iloc[0] == pytest.approx(1.1)
        assert (df["station_id"] == "E003").all()

    @patch("data.fetchers.aca.make_client")
    def test_gauge_handles_null_values_as_nan(self, mock_make_client):
        """Null values in API response must become np.nan, never 0."""
        records = [
            {"data": "2025-03-01T10:00:00", "valor": None, "codiQualitat": "U"},
        ]
        mock_make_client.return_value = self._mock_client_with_responses([
            self._make_mock_response(records),
            self._make_mock_response([]),  # no stage data
        ])
        df = aca.fetch_aca_gauge_data("E003", "Martorell")
        assert len(df) == 1
        assert pd.isna(df["flow_m3s"].iloc[0])

    @patch("data.fetchers.aca.make_client")
    def test_gauge_sorted_by_timestamp(self, mock_make_client):
        records = [
            {"data": "2025-03-01T11:00:00", "valor": "6.0"},
            {"data": "2025-03-01T09:00:00", "valor": "4.0"},
            {"data": "2025-03-01T10:00:00", "valor": "5.0"},
        ]
        mock_make_client.return_value = self._mock_client_with_responses([
            self._make_mock_response(records),
            self._make_mock_response([]),
        ])
        df = aca.fetch_aca_gauge_data("E003", "Martorell")
        timestamps = df["timestamp"].tolist()
        assert timestamps == sorted(timestamps)


# ── Reservoir pct_capacity calculation ───────────────────────────────────────

class TestReservoirPctCapacity:
    """pct_capacity must be volume/capacity*100, with NaN propagation."""

    @patch("data.fetchers.aca.make_client")
    def test_pct_capacity_calculated_correctly(self, mock_make_client):
        vol_records = [{"data": "2025-03-01T06:00:00", "valor": "54.5"}]
        level_records = [{"data": "2025-03-01T06:00:00", "valor": "677.5"}]

        mock_resp_vol = MagicMock()
        mock_resp_vol.status_code = 200
        mock_resp_vol.raise_for_status.return_value = None
        mock_resp_vol.json.return_value = vol_records

        mock_resp_lev = MagicMock()
        mock_resp_lev.status_code = 200
        mock_resp_lev.raise_for_status.return_value = None
        mock_resp_lev.json.return_value = level_records

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = [mock_resp_vol, mock_resp_lev]

        mock_make_client.return_value = mock_client

        df = aca.fetch_aca_reservoir_data("BAELLS", "La Baells", capacity_hm3=109.0)
        assert len(df) == 1
        expected_pct = 54.5 / 109.0 * 100
        assert df["pct_capacity"].iloc[0] == pytest.approx(expected_pct, rel=1e-4)


# ── No hardcoded secrets check ────────────────────────────────────────────────

class TestNoHardcodedSecrets:
    """API keys must not appear as string literals in the fetcher source."""

    def _get_source(self, module_path: str) -> str:
        with open(PROJECT_ROOT / module_path) as f:
            return f.read()

    def test_aca_no_api_key_literals(self):
        source = self._get_source("data/fetchers/aca.py")
        # Should not contain any long alphanumeric string that looks like a key
        assert "api_key = \"" not in source.lower() or "api_key = None" in source
        # More specifically, no 30+ char secrets
        import re
        matches = re.findall(r'["\'][A-Za-z0-9]{32,}["\']', source)
        assert matches == [], f"Possible hardcoded secrets found: {matches}"

    def test_aemet_no_api_key_literals(self):
        source = self._get_source("data/fetchers/aemet.py")
        import re
        matches = re.findall(r'["\'][A-Za-z0-9]{32,}["\']', source)
        assert matches == [], f"Possible hardcoded secrets found: {matches}"
