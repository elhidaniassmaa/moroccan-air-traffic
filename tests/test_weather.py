"""Tests for the Open-Meteo weather ingestion module."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.batch.weather import (
    fetch_weather_data,
    ingest_weather,
    transform_to_rows,
    write_parquet,
)

# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────


@pytest.fixture
def sample_api_response() -> dict[str, Any]:
    """A realistic Open-Meteo API response for a single location."""
    return {
        "latitude": 33.375,
        "longitude": -7.5,
        "elevation": 200.0,
        "generationtime_ms": 0.05,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "°C",
            "precipitation": "mm",
            "wind_speed_10m": "km/h",
        },
        "hourly": {
            "time": [
                "2025-01-15T00:00",
                "2025-01-15T01:00",
                "2025-01-15T02:00",
                "2025-01-15T03:00",
            ],
            "temperature_2m": [12.5, 11.8, 11.2, 10.9],
            "apparent_temperature": [10.1, 9.5, 8.8, 8.5],
            "precipitation": [0.0, 0.0, 0.2, 0.0],
            "rain": [0.0, 0.0, 0.1, 0.0],
            "wind_speed_10m": [15.2, 14.8, 16.1, 13.5],
            "wind_gusts_10m": [25.0, 23.5, 28.0, 21.0],
            "wind_direction_10m": [220, 215, 230, 210],
            "visibility": [24000, 22000, 18000, 24000],
            "cloud_cover": [30, 45, 60, 25],
            "relative_humidity_2m": [72, 75, 80, 68],
            "pressure_msl": [1018.5, 1018.2, 1017.8, 1018.0],
        },
    }


@pytest.fixture
def empty_api_response() -> dict[str, Any]:
    """An Open-Meteo response with no hourly data."""
    return {
        "latitude": 33.375,
        "longitude": -7.5,
        "elevation": 200.0,
        "hourly": {"time": []},
    }


# ─────────────────────────────────────────────────────────────
# Transform Tests
# ─────────────────────────────────────────────────────────────


class TestTransformToRows:
    """Test the transformation of API responses to flat row dicts."""

    def test_correct_row_count(self, sample_api_response: dict[str, Any]) -> None:
        """Should produce one row per hourly timestamp."""
        rows = transform_to_rows(sample_api_response, "GMMN", "CMN")
        assert len(rows) == 4

    def test_row_has_airport_metadata(self, sample_api_response: dict[str, Any]) -> None:
        """Each row should include airport ICAO and IATA codes."""
        rows = transform_to_rows(sample_api_response, "GMMN", "CMN")
        for row in rows:
            assert row["airport_icao"] == "GMMN"
            assert row["airport_iata"] == "CMN"

    def test_row_has_timestamp(self, sample_api_response: dict[str, Any]) -> None:
        """Each row should have timestamp_utc, date, and hour."""
        rows = transform_to_rows(sample_api_response, "GMMN", "CMN")
        assert rows[0]["timestamp_utc"] == "2025-01-15T00:00"
        assert rows[0]["date"] == "2025-01-15"
        assert rows[0]["hour"] == 0
        assert rows[2]["hour"] == 2

    def test_row_has_weather_vars(self, sample_api_response: dict[str, Any]) -> None:
        """Each row should include all weather variables."""
        rows = transform_to_rows(sample_api_response, "GMMN", "CMN")
        row = rows[0]
        assert row["temperature_2m"] == 12.5
        assert row["precipitation"] == 0.0
        assert row["wind_speed_10m"] == 15.2
        assert row["visibility"] == 24000

    def test_row_has_coordinates(self, sample_api_response: dict[str, Any]) -> None:
        """Each row should carry the API-returned coordinates and elevation."""
        rows = transform_to_rows(sample_api_response, "GMMN", "CMN")
        assert rows[0]["latitude"] == 33.375
        assert rows[0]["longitude"] == -7.5
        assert rows[0]["elevation_m"] == 200.0

    def test_empty_response_returns_empty(self, empty_api_response: dict[str, Any]) -> None:
        """Empty hourly data should return an empty list."""
        rows = transform_to_rows(empty_api_response, "GMMN", "CMN")
        assert rows == []

    def test_missing_variable_returns_none(self, sample_api_response: dict[str, Any]) -> None:
        """If a variable is missing from the response, the row value should be None."""
        del sample_api_response["hourly"]["visibility"]
        rows = transform_to_rows(sample_api_response, "GMMN", "CMN")
        assert rows[0]["visibility"] is None


# ─────────────────────────────────────────────────────────────
# Write Tests
# ─────────────────────────────────────────────────────────────


class TestWriteParquet:
    """Test the Parquet/JSONL writer."""

    def test_write_jsonl_fallback(
        self, sample_api_response: dict[str, Any], tmp_path: Path
    ) -> None:
        """When pyarrow is unavailable, should fall back to JSONL."""
        rows = transform_to_rows(sample_api_response, "GMMN", "CMN")
        output = tmp_path / "test.parquet"

        with patch("ingestion.batch.weather.HAS_PYARROW", False):
            write_parquet(rows, output)

        jsonl_path = output.with_suffix(".jsonl")
        assert jsonl_path.exists()
        lines = jsonl_path.read_text().strip().split("\n")
        assert len(lines) == 4

    def test_write_creates_directories(
        self, sample_api_response: dict[str, Any], tmp_path: Path
    ) -> None:
        """Writer should create parent directories as needed."""
        rows = transform_to_rows(sample_api_response, "GMMN", "CMN")
        output = tmp_path / "deep" / "nested" / "dir" / "weather.parquet"

        with patch("ingestion.batch.weather.HAS_PYARROW", False):
            write_parquet(rows, output)

        assert output.with_suffix(".jsonl").exists()


# ─────────────────────────────────────────────────────────────
# API Fetch Tests (mocked)
# ─────────────────────────────────────────────────────────────


class TestFetchWeatherData:
    """Test the API fetch function with mocked requests."""

    @patch("ingestion.batch.weather.requests.get")
    def test_successful_fetch(
        self, mock_get: MagicMock, sample_api_response: dict[str, Any]
    ) -> None:
        """Successful API call should return parsed JSON."""
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: sample_api_response,
        )
        mock_get.return_value.raise_for_status = MagicMock()

        data = fetch_weather_data(33.37, -7.59, "2025-01-15", "2025-01-15")
        assert "hourly" in data
        assert len(data["hourly"]["time"]) == 4

    @patch("ingestion.batch.weather.requests.get")
    def test_api_error_raises(self, mock_get: MagicMock) -> None:
        """API error response should raise ValueError."""
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"error": True, "reason": "Invalid parameter"},
        )
        mock_get.return_value.raise_for_status = MagicMock()

        with pytest.raises(ValueError, match="Invalid parameter"):
            fetch_weather_data(33.37, -7.59, "2025-01-15", "2025-01-15")

    @patch("ingestion.batch.weather.requests.get")
    @patch("ingestion.batch.weather.time.sleep")
    def test_retry_on_request_error(
        self, mock_sleep: MagicMock, mock_get: MagicMock, sample_api_response: dict[str, Any]
    ) -> None:
        """Should retry on request exceptions."""
        import requests as req

        mock_get.side_effect = [
            req.exceptions.ConnectionError("Connection failed"),
            MagicMock(
                status_code=200,
                json=lambda: sample_api_response,
                raise_for_status=MagicMock(),
            ),
        ]

        data = fetch_weather_data(33.37, -7.59, "2025-01-15", "2025-01-15", max_retries=3)
        assert "hourly" in data
        assert mock_get.call_count == 2

    @patch("ingestion.batch.weather.requests.get")
    @patch("ingestion.batch.weather.time.sleep")
    def test_archive_excludes_visibility(
        self, mock_sleep: MagicMock, mock_get: MagicMock, sample_api_response: dict[str, Any]
    ) -> None:
        """Archive endpoint should not request visibility (ERA5 limitation)."""
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: sample_api_response,
            raise_for_status=MagicMock(),
        )

        fetch_weather_data(33.37, -7.59, "2023-01-01", "2023-01-15", use_archive=True)

        called_params = mock_get.call_args[1]["params"]
        hourly_str = called_params["hourly"]
        assert "visibility" not in hourly_str


# ─────────────────────────────────────────────────────────────
# Integration-ish Tests
# ─────────────────────────────────────────────────────────────


class TestIngestWeather:
    """Test the full ingestion orchestration with mocked API."""

    @patch("ingestion.batch.weather.fetch_weather_data")
    @patch("ingestion.batch.weather.OUTPUT_DIR")
    def test_ingest_all_airports(
        self,
        mock_output_dir: MagicMock,
        mock_fetch: MagicMock,
        sample_api_response: dict[str, Any],
        tmp_path: Path,
    ) -> None:
        """Should attempt to fetch weather for all 5 airports."""
        mock_fetch.return_value = sample_api_response

        with patch("ingestion.batch.weather.OUTPUT_DIR", tmp_path):
            results = ingest_weather(start_date="2025-01-15", end_date="2025-01-15")

        assert len(results) == 5
        assert all(v > 0 for v in results.values())
        assert mock_fetch.call_count == 5
