"""Tests for the OpenSky producer: schema validation, state vector mapping, and synthetic generation."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.producers.opensky_producer import (
    OpenSkyClient,
    SyntheticGenerator,
    TokenManager,
    validate_event,
)

# ─────────────────────────────────────────────────────────────
# Schema Validation Tests
# ─────────────────────────────────────────────────────────────


class TestSchemaValidation:
    """Test flight event JSON schema validation."""

    def test_valid_event_passes(self, valid_event: dict[str, Any]) -> None:
        """A well-formed event should produce no validation errors."""
        errors = validate_event(valid_event)
        assert errors == [], f"Expected no errors, got: {errors}"

    def test_missing_required_field(self, invalid_event_missing_icao: dict[str, Any]) -> None:
        """Missing required field (icao24) should be caught."""
        errors = validate_event(invalid_event_missing_icao)
        assert len(errors) > 0
        assert any("icao24" in e for e in errors)

    def test_invalid_icao24_pattern(self, invalid_event_bad_icao: dict[str, Any]) -> None:
        """icao24 not matching ^[0-9a-f]{6}$ should fail."""
        errors = validate_event(invalid_event_bad_icao)
        assert len(errors) > 0
        assert any("icao24" in e.lower() or "pattern" in e.lower() for e in errors)

    def test_latitude_out_of_range(self, valid_event: dict[str, Any]) -> None:
        """Latitude outside [-90, 90] should fail."""
        valid_event["latitude"] = 95.0
        errors = validate_event(valid_event)
        assert len(errors) > 0

    def test_longitude_out_of_range(self, valid_event: dict[str, Any]) -> None:
        """Longitude outside [-180, 180] should fail."""
        valid_event["longitude"] = -200.0
        errors = validate_event(valid_event)
        assert len(errors) > 0

    def test_negative_velocity_rejected(self, valid_event: dict[str, Any]) -> None:
        """Negative velocity should fail (minimum: 0)."""
        valid_event["velocity"] = -10.0
        errors = validate_event(valid_event)
        assert len(errors) > 0

    def test_null_optional_fields_ok(self, valid_event: dict[str, Any]) -> None:
        """Nullable fields set to None should pass."""
        valid_event["callsign"] = None
        valid_event["baro_altitude"] = None
        valid_event["velocity"] = None
        valid_event["vertical_rate"] = None
        valid_event["geo_altitude"] = None
        valid_event["squawk"] = None
        valid_event["category"] = None
        errors = validate_event(valid_event)
        assert errors == [], f"Expected no errors, got: {errors}"

    def test_invalid_squawk_pattern(self, valid_event: dict[str, Any]) -> None:
        """Squawk not matching ^[0-7]{4}$ should fail (digit 8 is invalid in octal)."""
        valid_event["squawk"] = "8888"
        errors = validate_event(valid_event)
        assert len(errors) > 0

    def test_invalid_position_source(self, valid_event: dict[str, Any]) -> None:
        """Position source must be 0, 1, 2, or 3."""
        valid_event["position_source"] = 5
        errors = validate_event(valid_event)
        assert len(errors) > 0

    def test_additional_properties_rejected(self, valid_event: dict[str, Any]) -> None:
        """Extra fields should be rejected (additionalProperties: false)."""
        valid_event["unknown_field"] = "surprise"
        errors = validate_event(valid_event)
        assert len(errors) > 0


# ─────────────────────────────────────────────────────────────
# State Vector Mapping Tests
# ─────────────────────────────────────────────────────────────


class TestStateVectorMapping:
    """Test conversion of OpenSky state vector arrays to event dicts."""

    def setup_method(self) -> None:
        """Create a client with a mock token manager."""
        self.token_mgr = MagicMock(spec=TokenManager)
        self.token_mgr.is_configured = False
        self.token_mgr.get_token.return_value = None
        self.client = OpenSkyClient(self.token_mgr)

    def test_valid_state_vector_mapping(self, sample_state_vector: list[Any]) -> None:
        """Full state vector should map to a valid event dict."""
        now_iso = datetime.now(UTC).isoformat()
        event = self.client._state_vector_to_event(sample_state_vector, now_iso)

        assert event is not None
        assert event["icao24"] == "c00001"
        assert event["callsign"] == "RAM123"  # Should be stripped
        assert event["latitude"] == 33.37
        assert event["longitude"] == -7.59
        assert event["baro_altitude"] == 10000.0
        assert event["on_ground"] is False
        assert event["velocity"] == 250.0
        assert event["category"] == 4
        assert event["ingestion_ts"] == now_iso

    def test_callsign_stripped(self, sample_state_vector: list[Any]) -> None:
        """Callsign with trailing spaces should be stripped."""
        sample_state_vector[1] = "RAM123  "
        now_iso = datetime.now(UTC).isoformat()
        event = self.client._state_vector_to_event(sample_state_vector, now_iso)
        assert event["callsign"] == "RAM123"

    def test_empty_callsign_becomes_none(self, sample_state_vector: list[Any]) -> None:
        """Empty/whitespace callsign should become None."""
        sample_state_vector[1] = "        "
        now_iso = datetime.now(UTC).isoformat()
        event = self.client._state_vector_to_event(sample_state_vector, now_iso)
        assert event["callsign"] is None

    def test_none_position_returns_none(self, sample_state_vector: list[Any]) -> None:
        """State vector with null position should be skipped (return None)."""
        sample_state_vector[3] = None  # time_position
        now_iso = datetime.now(UTC).isoformat()
        event = self.client._state_vector_to_event(sample_state_vector, now_iso)
        assert event is None

    def test_none_lat_returns_none(self, sample_state_vector: list[Any]) -> None:
        """State vector with null latitude should be skipped."""
        sample_state_vector[6] = None  # latitude
        now_iso = datetime.now(UTC).isoformat()
        event = self.client._state_vector_to_event(sample_state_vector, now_iso)
        assert event is None

    def test_none_lon_returns_none(self, sample_state_vector: list[Any]) -> None:
        """State vector with null longitude should be skipped."""
        sample_state_vector[5] = None  # longitude
        now_iso = datetime.now(UTC).isoformat()
        event = self.client._state_vector_to_event(sample_state_vector, now_iso)
        assert event is None

    def test_no_category_field(self, sample_state_vector: list[Any]) -> None:
        """State vector without extended category should set category=None."""
        short_sv = sample_state_vector[:17]  # Remove category
        now_iso = datetime.now(UTC).isoformat()
        event = self.client._state_vector_to_event(short_sv, now_iso)
        assert event is not None
        assert event["category"] is None

    def test_event_date_derived(self, sample_state_vector: list[Any]) -> None:
        """event_date should be derived from time_position as YYYY-MM-DD."""
        # Use a known timestamp
        sample_state_vector[3] = 1704067200  # 2024-01-01 00:00:00 UTC
        now_iso = datetime.now(UTC).isoformat()
        event = self.client._state_vector_to_event(sample_state_vector, now_iso)
        assert event["event_date"] == "2024-01-01"

    def test_mapped_event_validates(self, sample_state_vector: list[Any]) -> None:
        """An event mapped from a state vector should pass schema validation."""
        now_iso = datetime.now(UTC).isoformat()
        event = self.client._state_vector_to_event(sample_state_vector, now_iso)
        assert event is not None
        errors = validate_event(event)
        assert errors == [], f"Mapped event failed validation: {errors}"


# ─────────────────────────────────────────────────────────────
# Synthetic Generator Tests
# ─────────────────────────────────────────────────────────────


class TestSyntheticGenerator:
    """Test the synthetic flight data generator."""

    def test_generates_correct_count(self) -> None:
        """Generator should produce between 5 and num_aircraft events."""
        gen = SyntheticGenerator(num_aircraft=20)
        events = gen.generate_batch()
        assert 5 <= len(events) <= 20

    def test_all_events_are_valid(self) -> None:
        """All synthetic events should pass schema validation."""
        gen = SyntheticGenerator(num_aircraft=30)
        events = gen.generate_batch()
        for event in events:
            errors = validate_event(event)
            assert errors == [], (
                f"Synthetic event failed: icao24={event['icao24']}, errors={errors}"
            )

    def test_icao24_is_6_hex_chars(self) -> None:
        """All synthetic icao24 values should be 6 lowercase hex characters."""
        gen = SyntheticGenerator(num_aircraft=15)
        events = gen.generate_batch()
        for event in events:
            assert len(event["icao24"]) == 6
            assert all(c in "0123456789abcdef" for c in event["icao24"])

    def test_positions_in_morocco_bbox(self) -> None:
        """All airborne synthetic positions should be within the Morocco bbox (approx)."""
        gen = SyntheticGenerator(num_aircraft=30)
        for _ in range(5):  # Multiple batches
            events = gen.generate_batch()
            for event in events:
                # Generous bounds (takeoff/approach may overshoot slightly)
                assert -18 < event["longitude"] < 1, f"lon={event['longitude']}"
                assert 20 < event["latitude"] < 37, f"lat={event['latitude']}"

    def test_ground_aircraft_have_low_altitude(self) -> None:
        """Aircraft marked on_ground should have altitude < 100m."""
        gen = SyntheticGenerator(num_aircraft=30)
        for _ in range(10):
            events = gen.generate_batch()
            for event in events:
                if event["on_ground"]:
                    assert event["baro_altitude"] < 100

    def test_multiple_batches_have_variation(self) -> None:
        """Multiple batches should produce varying numbers of events."""
        gen = SyntheticGenerator(num_aircraft=25)
        counts = [len(gen.generate_batch()) for _ in range(20)]
        # Not all the same count
        assert len(set(counts)) > 1, "All batches had the same count — no variation"

    def test_unique_icao24_in_batch(self) -> None:
        """Each batch should have unique icao24 values (no duplicates)."""
        gen = SyntheticGenerator(num_aircraft=25)
        events = gen.generate_batch()
        icaos = [e["icao24"] for e in events]
        assert len(icaos) == len(set(icaos)), "Duplicate icao24 found in batch"


# ─────────────────────────────────────────────────────────────
# Token Manager Tests
# ─────────────────────────────────────────────────────────────


class TestTokenManager:
    """Test OAuth2 token management."""

    def test_not_configured_returns_none(self) -> None:
        """Token manager without credentials should return None."""
        tm = TokenManager("", "", "https://example.com/token")
        assert not tm.is_configured
        assert tm.get_token() is None

    def test_is_configured_with_credentials(self) -> None:
        """Token manager with both client_id and secret should be configured."""
        tm = TokenManager("my_id", "my_secret", "https://example.com/token")
        assert tm.is_configured

    @patch("ingestion.producers.opensky_producer.requests.post")
    def test_token_refresh(self, mock_post: MagicMock) -> None:
        """Token manager should request and cache a token."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "test_token_123", "expires_in": 1800},
        )
        tm = TokenManager("my_id", "my_secret", "https://example.com/token")
        token = tm.get_token()
        assert token == "test_token_123"
        mock_post.assert_called_once()

    @patch("ingestion.producers.opensky_producer.requests.post")
    def test_token_cached_until_expiry(self, mock_post: MagicMock) -> None:
        """Token should be reused until close to expiry."""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "token_1", "expires_in": 1800},
        )
        tm = TokenManager("my_id", "my_secret", "https://example.com/token")
        t1 = tm.get_token()
        t2 = tm.get_token()  # Should use cached
        assert t1 == t2
        assert mock_post.call_count == 1  # Only one HTTP call
