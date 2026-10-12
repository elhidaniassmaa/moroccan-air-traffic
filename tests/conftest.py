"""conftest.py — Shared pytest fixtures for the test suite."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

SCHEMA_PATH = Path(__file__).parent.parent / "ingestion" / "schemas" / "flight_event.json"


@pytest.fixture
def flight_schema() -> dict[str, Any]:
    """Load the flight_event JSON schema."""
    with open(SCHEMA_PATH) as f:
        return json.load(f)


@pytest.fixture
def valid_event() -> dict[str, Any]:
    """A valid flight event matching the JSON schema."""
    now = int(time.time())
    return {
        "icao24": "c00001",
        "callsign": "RAM123",
        "origin_country": "Morocco",
        "time_position": now,
        "last_contact": now,
        "longitude": -7.59,
        "latitude": 33.37,
        "baro_altitude": 10000.0,
        "on_ground": False,
        "velocity": 250.0,
        "true_track": 45.0,
        "vertical_rate": 0.5,
        "geo_altitude": 10050.0,
        "squawk": "7500",
        "spi": False,
        "position_source": 0,
        "category": 4,
        "ingestion_ts": datetime.now(UTC).isoformat(),
        "event_date": datetime.fromtimestamp(now, tz=UTC).strftime("%Y-%m-%d"),
    }


@pytest.fixture
def sample_state_vector() -> list[Any]:
    """A sample OpenSky state vector (array format) with extended=1."""
    now = int(time.time())
    return [
        "c00001",  # 0: icao24
        "RAM123  ",  # 1: callsign (right-padded)
        "Morocco",  # 2: origin_country
        now,  # 3: time_position
        now,  # 4: last_contact
        -7.59,  # 5: longitude
        33.37,  # 6: latitude
        10000.0,  # 7: baro_altitude
        False,  # 8: on_ground
        250.0,  # 9: velocity
        45.0,  # 10: true_track
        0.5,  # 11: vertical_rate
        None,  # 12: sensors
        10050.0,  # 13: geo_altitude
        "7500",  # 14: squawk
        False,  # 15: spi
        0,  # 16: position_source
        4,  # 17: category (extended)
    ]


@pytest.fixture
def invalid_event_missing_icao() -> dict[str, Any]:
    """A flight event missing the required icao24 field."""
    now = int(time.time())
    return {
        "callsign": "RAM123",
        "origin_country": "Morocco",
        "time_position": now,
        "last_contact": now,
        "longitude": -7.59,
        "latitude": 33.37,
        "on_ground": False,
        "ingestion_ts": datetime.now(UTC).isoformat(),
        "event_date": "2026-01-01",
    }


@pytest.fixture
def invalid_event_bad_icao() -> dict[str, Any]:
    """A flight event with invalid icao24 (uppercase, wrong length)."""
    now = int(time.time())
    return {
        "icao24": "ABCXYZ",  # uppercase not matching ^[0-9a-f]{6}$
        "callsign": "RAM123",
        "origin_country": "Morocco",
        "time_position": now,
        "last_contact": now,
        "longitude": -7.59,
        "latitude": 33.37,
        "on_ground": False,
        "ingestion_ts": datetime.now(UTC).isoformat(),
        "event_date": "2026-01-01",
    }
