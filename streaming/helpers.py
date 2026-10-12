"""
Shared UDFs and helper functions for Spark Structured Streaming jobs.

Contains:
- Haversine distance calculation
- Flight phase detection heuristics
- Schema definitions (requires PySpark — lazy-imported)
"""

from __future__ import annotations

import math
from typing import Any


def _get_flight_event_schema():  # noqa: ANN202
    """Lazy-load the PySpark schema to avoid import errors when PySpark is not installed."""
    from pyspark.sql import types as T  # noqa: N812

    return T.StructType(
        [
            T.StructField("icao24", T.StringType(), nullable=False),
            T.StructField("callsign", T.StringType(), nullable=True),
            T.StructField("origin_country", T.StringType(), nullable=True),
            T.StructField("time_position", T.LongType(), nullable=False),
            T.StructField("last_contact", T.LongType(), nullable=True),
            T.StructField("longitude", T.DoubleType(), nullable=False),
            T.StructField("latitude", T.DoubleType(), nullable=False),
            T.StructField("baro_altitude", T.DoubleType(), nullable=True),
            T.StructField("on_ground", T.BooleanType(), nullable=False),
            T.StructField("velocity", T.DoubleType(), nullable=True),
            T.StructField("true_track", T.DoubleType(), nullable=True),
            T.StructField("vertical_rate", T.DoubleType(), nullable=True),
            T.StructField("geo_altitude", T.DoubleType(), nullable=True),
            T.StructField("squawk", T.StringType(), nullable=True),
            T.StructField("spi", T.BooleanType(), nullable=True),
            T.StructField("position_source", T.IntegerType(), nullable=True),
            T.StructField("category", T.IntegerType(), nullable=True),
            T.StructField("ingestion_ts", T.StringType(), nullable=True),
            T.StructField("event_date", T.StringType(), nullable=True),
        ]
    )


# ─────────────────────────────────────────────────────────────
# Kafka → Bronze schema (JSON parsing)
# ─────────────────────────────────────────────────────────────

# FLIGHT_EVENT_SCHEMA is lazy-loaded to avoid requiring PySpark at import time.
# Use: from streaming.helpers import FLIGHT_EVENT_SCHEMA  (in Spark jobs only)
# For test environments without PySpark, only import haversine_km, detect_flight_phase, etc.


class _LazySchema:
    """Descriptor that lazy-loads the PySpark schema on first access."""

    def __init__(self) -> None:
        self._schema = None

    def __call__(self):  # noqa: ANN204
        if self._schema is None:
            self._schema = _get_flight_event_schema()
        return self._schema


FLIGHT_EVENT_SCHEMA = _LazySchema()


# Airport reference data (embedded for Spark jobs — also available as dbt seed)
AIRPORTS: dict[str, dict[str, Any]] = {
    "GMMN": {
        "name": "Casablanca Mohammed V",
        "iata": "CMN",
        "lat": 33.3675,
        "lon": -7.5900,
        "radius_km": 50,
    },
    "GMMX": {
        "name": "Marrakech Menara",
        "iata": "RAK",
        "lat": 31.6069,
        "lon": -8.0363,
        "radius_km": 45,
    },
    "GMME": {"name": "Rabat-Salé", "iata": "RBA", "lat": 34.0515, "lon": -6.7515, "radius_km": 40},
    "GMAD": {
        "name": "Agadir Al Massira",
        "iata": "AGA",
        "lat": 30.3250,
        "lon": -9.4131,
        "radius_km": 45,
    },
    "GMTT": {
        "name": "Tangier Ibn Battouta",
        "iata": "TNG",
        "lat": 35.7269,
        "lon": -5.9169,
        "radius_km": 40,
    },
}


# ─────────────────────────────────────────────────────────────
# Haversine distance
# ─────────────────────────────────────────────────────────────

EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two points on Earth (km).

    Uses the Haversine formula.

    Args:
        lat1, lon1: First point (decimal degrees).
        lat2, lon2: Second point (decimal degrees).

    Returns:
        Distance in kilometers.
    """
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return EARTH_RADIUS_KM * c


def find_nearest_airport(lat: float, lon: float) -> tuple[str | None, str | None, float | None]:
    """Find the nearest target airport to a given position.

    Returns:
        Tuple of (icao_code, iata_code, distance_km) or (None, None, None)
        if no airport is within its influence radius.
    """
    nearest_icao: str | None = None
    nearest_iata: str | None = None
    min_dist: float | None = None

    for icao_code, airport in AIRPORTS.items():
        dist = haversine_km(lat, lon, airport["lat"], airport["lon"])
        if dist <= airport["radius_km"] and (min_dist is None or dist < min_dist):
            min_dist = dist
            nearest_icao = icao_code
            nearest_iata = airport["iata"]

    return nearest_icao, nearest_iata, min_dist


# ─────────────────────────────────────────────────────────────
# Flight phase detection (heuristic)
# ─────────────────────────────────────────────────────────────

# These thresholds are HEURISTICS derived from general aviation knowledge.
# They are NOT official aviation standards. Documented assumptions:
#
# | Phase      | on_ground | altitude (m) | vertical_rate (m/s) | velocity (m/s) |
# |------------|-----------|--------------|---------------------|----------------|
# | on_ground  | True      | any          | any                 | any            |
# | takeoff    | False     | < 2000       | > 3                 | > 30           |
# | climb      | False     | < 8000       | > 2                 | any            |
# | cruise     | False     | >= 8000      | -2 to 2             | any            |
# | descent    | False     | < 8000       | < -2                | any            |
# | approach   | False     | < 2000       | < -1                | any            |
# | landing    | False     | < 300        | < 0                 | < 80           |
#
# Edge cases: an aircraft at 7500m with vrate=0 is classified as cruise.
# Misclassifications are expected — this is a proxy, not a radar system.

# Configurable thresholds
PHASE_THRESHOLDS = {
    "takeoff_max_alt_m": 2000,
    "takeoff_min_vrate_ms": 3.0,
    "takeoff_min_velocity_ms": 30.0,
    "climb_max_alt_m": 8000,
    "climb_min_vrate_ms": 2.0,
    "cruise_min_alt_m": 8000,
    "cruise_vrate_tolerance_ms": 2.0,
    "descent_max_alt_m": 8000,
    "descent_max_vrate_ms": -2.0,
    "approach_max_alt_m": 2000,
    "approach_max_vrate_ms": -1.0,
    "landing_max_alt_m": 300,
    "landing_max_velocity_ms": 80.0,
}


def detect_flight_phase(
    on_ground: bool,
    baro_altitude: float | None,
    velocity: float | None,
    vertical_rate: float | None,
) -> str:
    """Detect the flight phase based on telemetry heuristics.

    Args:
        on_ground: Whether the aircraft reports being on the ground.
        baro_altitude: Barometric altitude in meters (can be None).
        velocity: Ground speed in m/s (can be None).
        vertical_rate: Vertical rate in m/s (can be None).

    Returns:
        One of: "on_ground", "takeoff", "climb", "cruise", "descent",
        "approach", "landing", "unknown".

    NOTE: This is a HEURISTIC classification — not an official flight phase.
    See PHASE_THRESHOLDS for the configurable boundaries.
    """
    if on_ground:
        return "on_ground"

    # Default None values to neutral
    alt = baro_altitude if baro_altitude is not None else 0.0
    vel = velocity if velocity is not None else 0.0
    vrate = vertical_rate if vertical_rate is not None else 0.0

    t = PHASE_THRESHOLDS

    # Landing: very low, descending, slow
    if alt < t["landing_max_alt_m"] and vrate < 0 and vel < t["landing_max_velocity_ms"]:
        return "landing"

    # Approach: low altitude, descending
    if alt < t["approach_max_alt_m"] and vrate < t["approach_max_vrate_ms"]:
        return "approach"

    # Takeoff: low altitude, climbing fast, reasonable speed
    if (
        alt < t["takeoff_max_alt_m"]
        and vrate > t["takeoff_min_vrate_ms"]
        and vel > t["takeoff_min_velocity_ms"]
    ):
        return "takeoff"

    # Climb: below cruise altitude, climbing
    if alt < t["climb_max_alt_m"] and vrate > t["climb_min_vrate_ms"]:
        return "climb"

    # Descent: below cruise altitude, descending
    if alt < t["descent_max_alt_m"] and vrate < t["descent_max_vrate_ms"]:
        return "descent"

    # Cruise: high altitude, level flight
    if alt >= t["cruise_min_alt_m"] and abs(vrate) <= t["cruise_vrate_tolerance_ms"]:
        return "cruise"

    # High altitude but climbing/descending
    if alt >= t["cruise_min_alt_m"] and vrate > t["cruise_vrate_tolerance_ms"]:
        return "climb"
    if alt >= t["cruise_min_alt_m"] and vrate < -t["cruise_vrate_tolerance_ms"]:
        return "descent"

    return "unknown"
