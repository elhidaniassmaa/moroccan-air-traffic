"""
OpenSky Network → Redpanda Producer

Polls the OpenSky /api/states/all endpoint for aircraft in the Moroccan
airspace bounding box, validates messages against the flight_event JSON schema,
and produces them to Redpanda (Kafka-compatible). Invalid messages go to a DLQ.

Supports:
- OAuth2 client credentials auth (basic auth retired March 2026)
- Anonymous access (lower rate limits)
- Synthetic/replay mode for testing without API access
- Exponential backoff on 429 / network errors
- Idempotent producer (acks=all)
- Graceful shutdown on SIGINT/SIGTERM
- Structured JSON logging with counters
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jsonschema
import requests
from confluent_kafka import KafkaError, KafkaException, Producer
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

load_dotenv()

# Morocco bounding box (lat 21–36, lon -17 to -1)
MOROCCO_BBOX = {
    "lamin": 21.0,
    "lamax": 36.0,
    "lomin": -17.0,
    "lomax": -1.0,
}

# Target airports for synthetic data generation
AIRPORTS = {
    "GMMN": {"name": "Casablanca Mohammed V", "lat": 33.3675, "lon": -7.5900},
    "GMMX": {"name": "Marrakech Menara", "lat": 31.6069, "lon": -8.0363},
    "GMME": {"name": "Rabat-Salé", "lat": 34.0515, "lon": -6.7515},
    "GMAD": {"name": "Agadir Al Massira", "lat": 30.3250, "lon": -9.4131},
    "GMTT": {"name": "Tangier Ibn Battouta", "lat": 35.7269, "lon": -5.9169},
}

# OpenSky API configuration
OPENSKY_BASE_URL = "https://opensky-network.org/api"
OPENSKY_TOKEN_URL = os.getenv(
    "OPENSKY_TOKEN_URL",
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
)
OPENSKY_CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID", "")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET", "")

# Kafka / Redpanda
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC_FLIGHTS = os.getenv("KAFKA_TOPIC_FLIGHTS", "flights.raw")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "flights.dlq")

# Producer behavior
POLL_INTERVAL_S = int(os.getenv("PRODUCER_POLL_INTERVAL_S", "15"))
SYNTHETIC_MODE = os.getenv("PRODUCER_SYNTHETIC_MODE", "true").lower() == "true"
LOG_LEVEL = os.getenv("PRODUCER_LOG_LEVEL", "INFO").upper()

# Backoff settings
MAX_BACKOFF_S = 300  # 5 minutes
INITIAL_BACKOFF_S = 5

# Load JSON schema
SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "flight_event.json"

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("opensky_producer")


# ─────────────────────────────────────────────────────────────
# Counters
# ─────────────────────────────────────────────────────────────


class Counters:
    """Thread-safe-ish counters for observability."""

    def __init__(self) -> None:
        self.produced = 0
        self.rejected = 0
        self.errors = 0
        self.api_calls = 0

    def log_summary(self) -> None:
        logger.info(
            "counters: produced=%d rejected=%d errors=%d api_calls=%d",
            self.produced,
            self.rejected,
            self.errors,
            self.api_calls,
        )


counters = Counters()


# ─────────────────────────────────────────────────────────────
# Schema validation
# ─────────────────────────────────────────────────────────────


def load_schema() -> dict[str, Any]:
    """Load and return the flight_event JSON schema."""
    with open(SCHEMA_PATH) as f:
        return json.load(f)


_SCHEMA: dict[str, Any] | None = None


def get_schema() -> dict[str, Any]:
    """Lazy-loaded singleton for the JSON schema."""
    global _SCHEMA  # noqa: PLW0603
    if _SCHEMA is None:
        _SCHEMA = load_schema()
    return _SCHEMA


def validate_event(event: dict[str, Any]) -> list[str]:
    """Validate a flight event against the JSON schema.

    Returns a list of validation error messages (empty if valid).
    """
    errors: list[str] = []
    validator = jsonschema.Draft202012Validator(get_schema())
    for error in validator.iter_errors(event):
        errors.append(f"{error.json_path}: {error.message}")
    return errors


# ─────────────────────────────────────────────────────────────
# OAuth2 Token Manager
# ─────────────────────────────────────────────────────────────


class TokenManager:
    """Manages OAuth2 access tokens with automatic renewal.

    OpenSky tokens expire after 1800s (30 min). We renew 60s before expiry.
    """

    def __init__(self, client_id: str, client_secret: str, token_url: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self._token: str | None = None
        self._expires_at: float = 0.0

    @property
    def is_configured(self) -> bool:
        """Whether OAuth2 credentials are provided."""
        return bool(self.client_id and self.client_secret)

    def get_token(self) -> str | None:
        """Get a valid access token, refreshing if needed.

        Returns None if OAuth2 is not configured (anonymous mode).
        """
        if not self.is_configured:
            return None

        # Renew 60s before expiry
        if self._token and time.time() < (self._expires_at - 60):
            return self._token

        return self._refresh_token()

    def _refresh_token(self) -> str:
        """Request a new token from the OAuth2 endpoint."""
        logger.info("Requesting new OAuth2 token from %s", self.token_url)
        resp = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._expires_at = time.time() + data.get("expires_in", 1800)
        logger.info("OAuth2 token acquired, expires in %ds", data.get("expires_in", 1800))
        return self._token


# ─────────────────────────────────────────────────────────────
# OpenSky API Client
# ─────────────────────────────────────────────────────────────


class OpenSkyClient:
    """Client for the OpenSky Network REST API."""

    # OpenSky state vector indices
    IDX_ICAO24 = 0
    IDX_CALLSIGN = 1
    IDX_ORIGIN_COUNTRY = 2
    IDX_TIME_POSITION = 3
    IDX_LAST_CONTACT = 4
    IDX_LONGITUDE = 5
    IDX_LATITUDE = 6
    IDX_BARO_ALTITUDE = 7
    IDX_ON_GROUND = 8
    IDX_VELOCITY = 9
    IDX_TRUE_TRACK = 10
    IDX_VERTICAL_RATE = 11
    IDX_SENSORS = 12
    IDX_GEO_ALTITUDE = 13
    IDX_SQUAWK = 14
    IDX_SPI = 15
    IDX_POSITION_SOURCE = 16
    IDX_CATEGORY = 17

    def __init__(self, token_manager: TokenManager) -> None:
        self.token_manager = token_manager
        self.session = requests.Session()
        self._backoff_s = INITIAL_BACKOFF_S

    def fetch_states(self) -> list[dict[str, Any]]:
        """Fetch aircraft states in the Morocco bounding box.

        Returns a list of validated flight event dicts.
        Raises on unrecoverable errors; returns [] on empty responses.
        """
        counters.api_calls += 1
        headers: dict[str, str] = {}
        token = self.token_manager.get_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        params = {
            "lamin": MOROCCO_BBOX["lamin"],
            "lamax": MOROCCO_BBOX["lamax"],
            "lomin": MOROCCO_BBOX["lomin"],
            "lomax": MOROCCO_BBOX["lomax"],
            "extended": 1,
        }

        try:
            resp = self.session.get(
                f"{OPENSKY_BASE_URL}/states/all",
                params=params,
                headers=headers,
                timeout=30,
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("X-Rate-Limit-Retry-After-Seconds", "60"))
                logger.warning(
                    "Rate limited (429). Retry after %ds. Remaining credits: %s",
                    retry_after,
                    resp.headers.get("X-Rate-Limit-Remaining", "?"),
                )
                self._backoff_s = min(retry_after, MAX_BACKOFF_S)
                raise RateLimitError(retry_after)

            resp.raise_for_status()
            self._backoff_s = INITIAL_BACKOFF_S  # Reset backoff on success

            data = resp.json()
            states = data.get("states")
            if not states:
                logger.debug("No aircraft in Morocco bbox")
                return []

            now_iso = datetime.now(UTC).isoformat()
            events = []
            for sv in states:
                event = self._state_vector_to_event(sv, now_iso)
                if event is not None:
                    events.append(event)

            logger.info(
                "Fetched %d states, mapped %d events. Credits remaining: %s",
                len(states),
                len(events),
                resp.headers.get("X-Rate-Limit-Remaining", "N/A"),
            )
            return events

        except RateLimitError:
            raise
        except requests.exceptions.RequestException as e:
            counters.errors += 1
            logger.error("OpenSky API request failed: %s", e)
            self._backoff_s = min(self._backoff_s * 2, MAX_BACKOFF_S)
            raise

    def _state_vector_to_event(self, sv: list[Any], ingestion_ts: str) -> dict[str, Any] | None:
        """Convert an OpenSky state vector array to a flight event dict.

        Returns None if mandatory fields are missing (no position).
        """
        # Skip states without position
        time_pos = sv[self.IDX_TIME_POSITION]
        lon = sv[self.IDX_LONGITUDE]
        lat = sv[self.IDX_LATITUDE]
        if time_pos is None or lon is None or lat is None:
            return None

        callsign = sv[self.IDX_CALLSIGN]
        if isinstance(callsign, str):
            callsign = callsign.strip() or None

        # Derive event_date from time_position
        event_date = datetime.fromtimestamp(time_pos, tz=UTC).strftime("%Y-%m-%d")

        event: dict[str, Any] = {
            "icao24": sv[self.IDX_ICAO24],
            "callsign": callsign,
            "origin_country": sv[self.IDX_ORIGIN_COUNTRY],
            "time_position": time_pos,
            "last_contact": sv[self.IDX_LAST_CONTACT],
            "longitude": lon,
            "latitude": lat,
            "baro_altitude": sv[self.IDX_BARO_ALTITUDE],
            "on_ground": sv[self.IDX_ON_GROUND],
            "velocity": sv[self.IDX_VELOCITY],
            "true_track": sv[self.IDX_TRUE_TRACK],
            "vertical_rate": sv[self.IDX_VERTICAL_RATE],
            "geo_altitude": sv[self.IDX_GEO_ALTITUDE],
            "squawk": sv[self.IDX_SQUAWK],
            "spi": sv[self.IDX_SPI],
            "position_source": sv[self.IDX_POSITION_SOURCE],
            "ingestion_ts": ingestion_ts,
            "event_date": event_date,
        }

        # Category is only present with extended=1
        if len(sv) > self.IDX_CATEGORY:
            event["category"] = sv[self.IDX_CATEGORY]
        else:
            event["category"] = None

        return event

    @property
    def backoff_seconds(self) -> float:
        return self._backoff_s


class RateLimitError(Exception):
    """Raised when OpenSky returns HTTP 429."""

    def __init__(self, retry_after: int) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited. Retry after {retry_after}s")


# ─────────────────────────────────────────────────────────────
# Synthetic Data Generator
# ─────────────────────────────────────────────────────────────


class SyntheticGenerator:
    """Generates realistic synthetic flight data for testing.

    Simulates aircraft near the 5 target Moroccan airports with
    realistic positions, altitudes, speeds, and flight phases.
    """

    # Common Moroccan airline callsign prefixes
    CALLSIGN_PREFIXES = ["RAM", "MAC", "JTG", "MAP", "RYR", "EZY", "AFR", "BAW", "IBE"]

    def __init__(self, num_aircraft: int = 25) -> None:
        self.num_aircraft = num_aircraft
        self._aircraft_pool = self._generate_aircraft_pool()
        self._step = 0

    def _generate_aircraft_pool(self) -> list[dict[str, Any]]:
        """Create a pool of synthetic aircraft with stable icao24 addresses."""
        pool = []
        for i in range(self.num_aircraft):
            icao24 = f"{0xC00000 + i:06x}"  # Moroccan ICAO range approximation
            airport_code = random.choice(list(AIRPORTS.keys()))
            airport = AIRPORTS[airport_code]
            prefix = random.choice(self.CALLSIGN_PREFIXES)
            callsign = f"{prefix}{random.randint(100, 999):3d}"

            # Randomize flight phase: 40% airborne near airport, 20% on ground,
            # 20% en route, 20% approaching
            phase = random.choices(
                ["ground", "takeoff", "cruise", "approach"],
                weights=[20, 20, 40, 20],
            )[0]

            pool.append(
                {
                    "icao24": icao24,
                    "callsign": callsign,
                    "airport": airport,
                    "airport_code": airport_code,
                    "phase": phase,
                    "base_heading": random.uniform(0, 360),
                }
            )
        return pool

    def generate_batch(self) -> list[dict[str, Any]]:
        """Generate a batch of synthetic flight events.

        Returns between 10–25 events per call (simulating variable traffic).
        """
        self._step += 1
        now = int(time.time())
        now_iso = datetime.now(UTC).isoformat()
        num_active = random.randint(
            max(5, self.num_aircraft // 3),
            self.num_aircraft,
        )
        active = random.sample(self._aircraft_pool, num_active)

        events = []
        for ac in active:
            event = self._generate_event(ac, now, now_iso)
            events.append(event)
        return events

    def _generate_event(self, ac: dict[str, Any], now: int, now_iso: str) -> dict[str, Any]:
        """Generate a single synthetic flight event."""
        airport = ac["airport"]
        phase = ac["phase"]

        # Position varies by flight phase
        if phase == "ground":
            lat = airport["lat"] + random.uniform(-0.005, 0.005)
            lon = airport["lon"] + random.uniform(-0.005, 0.005)
            baro_alt = random.uniform(0, 50)  # meters
            velocity = random.uniform(0, 5)  # m/s (taxi)
            vertical_rate = 0.0
            on_ground = True
        elif phase == "takeoff":
            offset = random.uniform(0.01, 0.05)
            lat = airport["lat"] + offset * math.cos(math.radians(ac["base_heading"]))
            lon = airport["lon"] + offset * math.sin(math.radians(ac["base_heading"]))
            baro_alt = random.uniform(100, 3000)
            velocity = random.uniform(60, 120)
            vertical_rate = random.uniform(5, 20)
            on_ground = False
        elif phase == "cruise":
            # Anywhere in Morocco bbox
            lat = random.uniform(22, 35)
            lon = random.uniform(-16, -2)
            baro_alt = random.uniform(8000, 12000)
            velocity = random.uniform(200, 280)
            vertical_rate = random.uniform(-2, 2)
            on_ground = False
        else:  # approach
            offset = random.uniform(0.05, 0.3)
            lat = airport["lat"] + offset * math.cos(math.radians(ac["base_heading"] + 180))
            lon = airport["lon"] + offset * math.sin(math.radians(ac["base_heading"] + 180))
            baro_alt = random.uniform(500, 5000)
            velocity = random.uniform(60, 150)
            vertical_rate = random.uniform(-15, -3)
            on_ground = False

        # Evolve phase for next call
        ac["phase"] = random.choices(
            ["ground", "takeoff", "cruise", "approach"],
            weights=[15, 20, 40, 25],
        )[0]

        event_date = datetime.fromtimestamp(now, tz=UTC).strftime("%Y-%m-%d")

        return {
            "icao24": ac["icao24"],
            "callsign": ac["callsign"],
            "origin_country": "Morocco" if ac["callsign"][:3] in ("RAM", "MAC") else "France",
            "time_position": now - random.randint(0, 5),
            "last_contact": now,
            "longitude": round(lon, 4),
            "latitude": round(lat, 4),
            "baro_altitude": round(baro_alt, 1),
            "on_ground": on_ground,
            "velocity": round(velocity, 2),
            "true_track": round(ac["base_heading"] + random.uniform(-5, 5), 2) % 360,
            "vertical_rate": round(vertical_rate, 2),
            "geo_altitude": round(baro_alt + random.uniform(-50, 50), 1),
            "squawk": f"{random.randint(0, 7)}{random.randint(0, 7)}{random.randint(0, 7)}{random.randint(0, 7)}",
            "spi": False,
            "position_source": 0,
            "category": random.choice([0, 2, 3, 4, 6]),
            "ingestion_ts": now_iso,
            "event_date": event_date,
        }


# ─────────────────────────────────────────────────────────────
# Kafka Producer
# ─────────────────────────────────────────────────────────────


def create_kafka_producer() -> Producer:
    """Create a confluent-kafka Producer with idempotent settings."""
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "opensky-producer",
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "retries": 2147483647,  # infinite retries (bounded by delivery.timeout.ms)
        "delivery.timeout.ms": 120000,  # 2 minutes
        "linger.ms": 50,
        "batch.size": 65536,
        "compression.type": "zstd",
    }
    return Producer(config)


def delivery_callback(err: KafkaError | None, msg: Any) -> None:
    """Callback for Kafka produce() to track delivery status."""
    if err is not None:
        counters.errors += 1
        logger.error(
            "Delivery failed for key=%s: %s",
            msg.key().decode() if msg.key() else "N/A",
            err,
        )
    else:
        counters.produced += 1
        logger.debug(
            "Delivered to %s [%d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


# ─────────────────────────────────────────────────────────────
# Main Producer Loop
# ─────────────────────────────────────────────────────────────


class FlightProducer:
    """Orchestrates polling, validation, and producing of flight events."""

    def __init__(self) -> None:
        self._running = True
        self._producer = create_kafka_producer()
        self._token_manager = TokenManager(
            OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET, OPENSKY_TOKEN_URL
        )
        self._opensky_client = OpenSkyClient(self._token_manager)
        self._synthetic = SyntheticGenerator() if SYNTHETIC_MODE else None
        self._schema = get_schema()

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals gracefully."""
        sig_name = signal.Signals(signum).name
        logger.info("Received %s, shutting down gracefully...", sig_name)
        self._running = False

    def run(self) -> None:
        """Main polling loop."""
        mode = "SYNTHETIC" if SYNTHETIC_MODE else "LIVE"
        auth = "OAuth2" if self._token_manager.is_configured else "anonymous"
        logger.info(
            "Starting OpenSky producer — mode=%s auth=%s poll_interval=%ds "
            "topic=%s dlq=%s bootstrap=%s",
            mode,
            auth,
            POLL_INTERVAL_S,
            KAFKA_TOPIC_FLIGHTS,
            KAFKA_TOPIC_DLQ,
            KAFKA_BOOTSTRAP_SERVERS,
        )

        while self._running:
            try:
                events = self._fetch_events()
                self._process_events(events)
                self._producer.poll(0)  # Trigger delivery callbacks
                counters.log_summary()
                self._sleep(POLL_INTERVAL_S)

            except RateLimitError as e:
                logger.warning("Rate limited, sleeping %ds", e.retry_after)
                self._sleep(e.retry_after)

            except requests.exceptions.RequestException:
                backoff = self._opensky_client.backoff_seconds
                logger.warning("API error, backing off %ds", backoff)
                self._sleep(backoff)

            except KafkaException as e:
                counters.errors += 1
                logger.error("Kafka error: %s", e)
                self._sleep(INITIAL_BACKOFF_S)

        self._shutdown()

    def _fetch_events(self) -> list[dict[str, Any]]:
        """Fetch events from OpenSky API or synthetic generator."""
        if self._synthetic:
            events = self._synthetic.generate_batch()
            logger.info("Generated %d synthetic events", len(events))
            return events
        return self._opensky_client.fetch_states()

    def _process_events(self, events: list[dict[str, Any]]) -> None:
        """Validate and produce events to Kafka."""
        for event in events:
            validation_errors = validate_event(event)
            if validation_errors:
                counters.rejected += 1
                logger.warning(
                    "Validation failed for icao24=%s: %s",
                    event.get("icao24", "?"),
                    "; ".join(validation_errors),
                )
                self._send_to_dlq(event, validation_errors)
            else:
                self._produce_event(event)

    def _produce_event(self, event: dict[str, Any]) -> None:
        """Produce a validated event to the flights.raw topic."""
        key = event["icao24"].encode("utf-8")
        value = json.dumps(event, default=str).encode("utf-8")
        try:
            self._producer.produce(
                topic=KAFKA_TOPIC_FLIGHTS,
                key=key,
                value=value,
                callback=delivery_callback,
            )
        except BufferError:
            logger.warning("Producer buffer full, flushing...")
            self._producer.flush(timeout=10)
            self._producer.produce(
                topic=KAFKA_TOPIC_FLIGHTS,
                key=key,
                value=value,
                callback=delivery_callback,
            )

    def _send_to_dlq(self, event: dict[str, Any], errors: list[str]) -> None:
        """Send invalid events to the dead letter queue with error context."""
        dlq_message = {
            "original_event": event,
            "validation_errors": errors,
            "rejected_at": datetime.now(UTC).isoformat(),
        }
        key = event.get("icao24", "unknown").encode("utf-8")
        value = json.dumps(dlq_message, default=str).encode("utf-8")
        try:
            self._producer.produce(
                topic=KAFKA_TOPIC_DLQ,
                key=key,
                value=value,
            )
        except BufferError:
            logger.error("DLQ buffer full, dropping message for %s", event.get("icao24"))

    def _sleep(self, seconds: float) -> None:
        """Interruptible sleep."""
        end = time.time() + seconds
        while self._running and time.time() < end:
            time.sleep(min(1.0, end - time.time()))

    def _shutdown(self) -> None:
        """Flush remaining messages and close the producer."""
        logger.info("Flushing remaining messages...")
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            logger.warning("%d messages were not delivered", remaining)
        counters.log_summary()
        logger.info("Producer shut down cleanly.")


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────


def main() -> None:
    """Entry point for the OpenSky producer."""
    producer = FlightProducer()
    producer.run()


if __name__ == "__main__":
    main()
