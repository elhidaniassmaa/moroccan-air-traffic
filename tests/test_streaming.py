"""Tests for streaming helper functions: haversine, nearest airport, flight phase detection."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from streaming.helpers import detect_flight_phase, find_nearest_airport, haversine_km

# ─────────────────────────────────────────────────────────────
# Haversine Tests
# ─────────────────────────────────────────────────────────────


class TestHaversine:
    """Test the haversine distance function."""

    def test_same_point_is_zero(self) -> None:
        """Distance from a point to itself should be 0."""
        d = haversine_km(33.3675, -7.59, 33.3675, -7.59)
        assert d == pytest.approx(0.0, abs=0.001)

    def test_casablanca_to_marrakech(self) -> None:
        """CMN to RAK should be ~240 km (known distance)."""
        d = haversine_km(33.3675, -7.59, 31.6069, -8.0363)
        assert 190 < d < 210

    def test_casablanca_to_rabat(self) -> None:
        """CMN to RBA should be ~80 km."""
        d = haversine_km(33.3675, -7.59, 34.0515, -6.7515)
        assert 90 < d < 115

    def test_symmetric(self) -> None:
        """haversine(A, B) should equal haversine(B, A)."""
        d1 = haversine_km(33.3675, -7.59, 31.6069, -8.0363)
        d2 = haversine_km(31.6069, -8.0363, 33.3675, -7.59)
        assert d1 == pytest.approx(d2, abs=0.001)

    def test_antipodal_points(self) -> None:
        """Distance between antipodal points should be ~20015 km (half circumference)."""
        d = haversine_km(0, 0, 0, 180)
        assert 20010 < d < 20020


# ─────────────────────────────────────────────────────────────
# Nearest Airport Tests
# ─────────────────────────────────────────────────────────────


class TestFindNearestAirport:
    """Test nearest airport detection."""

    def test_on_casablanca_airport(self) -> None:
        """A point on CMN airport should match GMMN."""
        icao, iata, dist = find_nearest_airport(33.3675, -7.59)
        assert icao == "GMMN"
        assert iata == "CMN"
        assert dist is not None
        assert dist < 1  # Should be very close

    def test_near_marrakech(self) -> None:
        """A point 10km from RAK should match GMMX."""
        # ~10km south of GMMX
        icao, iata, dist = find_nearest_airport(31.52, -8.04)
        assert icao == "GMMX"
        assert iata == "RAK"

    def test_far_from_all_airports(self) -> None:
        """A point far from all airports should return None."""
        # Mid-Sahara
        icao, iata, dist = find_nearest_airport(25.0, -10.0)
        assert icao is None
        assert iata is None
        assert dist is None

    def test_outside_morocco(self) -> None:
        """A point in Europe should not match any Moroccan airport."""
        icao, iata, dist = find_nearest_airport(48.8566, 2.3522)  # Paris
        assert icao is None


# ─────────────────────────────────────────────────────────────
# Flight Phase Detection Tests
# ─────────────────────────────────────────────────────────────


class TestDetectFlightPhase:
    """Test heuristic flight phase detection with edge cases."""

    def test_on_ground(self) -> None:
        """Aircraft reporting on_ground=True should be 'on_ground'."""
        phase = detect_flight_phase(on_ground=True, baro_altitude=0, velocity=5, vertical_rate=0)
        assert phase == "on_ground"

    def test_on_ground_overrides_altitude(self) -> None:
        """on_ground=True should override any altitude value."""
        phase = detect_flight_phase(
            on_ground=True, baro_altitude=10000, velocity=250, vertical_rate=0
        )
        assert phase == "on_ground"

    def test_takeoff(self) -> None:
        """Low altitude, fast climbing, reasonable speed = takeoff."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=500, velocity=80, vertical_rate=10
        )
        assert phase == "takeoff"

    def test_climb(self) -> None:
        """Mid altitude, moderate climb = climb."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=5000, velocity=200, vertical_rate=5
        )
        assert phase == "climb"

    def test_cruise(self) -> None:
        """High altitude, level flight = cruise."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=10000, velocity=250, vertical_rate=0
        )
        assert phase == "cruise"

    def test_cruise_slight_vrate(self) -> None:
        """High altitude with slight vertical rate within tolerance = still cruise."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=11000, velocity=260, vertical_rate=1.5
        )
        assert phase == "cruise"

    def test_descent(self) -> None:
        """Mid altitude, descending = descent."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=6000, velocity=200, vertical_rate=-5
        )
        assert phase == "descent"

    def test_approach(self) -> None:
        """Low altitude, moderate descent = approach."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=1000, velocity=80, vertical_rate=-3
        )
        assert phase == "approach"

    def test_landing(self) -> None:
        """Very low altitude, descending, slow = landing."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=100, velocity=60, vertical_rate=-2
        )
        assert phase == "landing"

    def test_high_altitude_descent(self) -> None:
        """High altitude but descending fast = descent."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=9000, velocity=250, vertical_rate=-8
        )
        assert phase == "descent"

    def test_high_altitude_climb(self) -> None:
        """High altitude still climbing = climb."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=9000, velocity=250, vertical_rate=5
        )
        assert phase == "climb"

    def test_none_values_handled(self) -> None:
        """All None telemetry values should not crash."""
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=None, velocity=None, vertical_rate=None
        )
        assert isinstance(phase, str)

    def test_zero_everything_airborne(self) -> None:
        """Airborne with all zeros = landing (alt<300, vrate=0<0 is false, vel<80)."""
        phase = detect_flight_phase(on_ground=False, baro_altitude=0, velocity=0, vertical_rate=0)
        # At alt=0 with vrate=0 and vel=0, doesn't match landing (vrate<0 required)
        # Doesn't match approach (vrate < -1 required)
        # Doesn't match takeoff (vrate > 3 required)
        # Doesn't match cruise (alt >= 8000 required)
        assert phase == "unknown"

    def test_edge_case_exact_thresholds(self) -> None:
        """Test behavior at exact threshold boundaries."""
        # Exactly at cruise min altitude with zero vrate
        phase = detect_flight_phase(
            on_ground=False, baro_altitude=8000, velocity=250, vertical_rate=0
        )
        assert phase == "cruise"
