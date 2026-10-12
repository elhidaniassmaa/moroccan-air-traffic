"""
Open-Meteo Weather Ingestion — Batch

Fetches hourly weather data for the 5 target Moroccan airports from the
Open-Meteo API (free, no API key needed). Writes partitioned Parquet files
to data/lake/raw/weather/.

Supports:
- Historical backfill (archive endpoint) and forecast (forecast endpoint)
- Idempotent writes (overwrites existing partition files)
- Configurable date ranges
- Retry with exponential backoff on failures
- Structured JSON logging

Open-Meteo docs: https://open-meteo.com/en/docs
- Forecast: https://api.open-meteo.com/v1/forecast
- Archive:  https://archive-api.open-meteo.com/v1/archive
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

OPEN_METEO_FORECAST_URL = (
    os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1") + "/forecast"
)
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

OUTPUT_DIR = Path(os.getenv("WEATHER_OUTPUT_DIR", "data/lake/raw/weather"))

# Target airports with coordinates
AIRPORTS = {
    "GMMN": {"name": "Casablanca Mohammed V", "iata": "CMN", "lat": 33.3675, "lon": -7.5900},
    "GMMX": {"name": "Marrakech Menara", "iata": "RAK", "lat": 31.6069, "lon": -8.0363},
    "GMME": {"name": "Rabat-Salé", "iata": "RBA", "lat": 34.0515, "lon": -6.7515},
    "GMAD": {"name": "Agadir Al Massira", "iata": "AGA", "lat": 30.3250, "lon": -9.4131},
    "GMTT": {"name": "Tangier Ibn Battouta", "iata": "TNG", "lat": 35.7269, "lon": -5.9169},
}

# Weather variables to fetch
HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "wind_speed_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "visibility",
    "cloud_cover",
    "relative_humidity_2m",
    "pressure_msl",
]

LOG_LEVEL = os.getenv("PRODUCER_LOG_LEVEL", "INFO").upper()

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("weather_ingestion")


# ─────────────────────────────────────────────────────────────
# API Client
# ─────────────────────────────────────────────────────────────


def fetch_weather_data(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    *,
    use_archive: bool = False,
    max_retries: int = 3,
) -> dict[str, Any]:
    """Fetch hourly weather data from Open-Meteo.

    Args:
        latitude: Airport latitude (WGS-84).
        longitude: Airport longitude (WGS-84).
        start_date: Start date (YYYY-MM-DD).
        end_date: End date (YYYY-MM-DD).
        use_archive: Use archive endpoint for historical data (ERA5 reanalysis).
        max_retries: Maximum number of retries on failure.

    Returns:
        Raw JSON response from Open-Meteo.

    Note:
        The archive endpoint does NOT support the 'visibility' variable
        (ERA5 reanalysis lacks it). For historical visibility data from 2021+,
        use the Historical Forecast API instead. When use_archive=True, we
        remove 'visibility' from the requested variables.
    """
    url = OPEN_METEO_ARCHIVE_URL if use_archive else OPEN_METEO_FORECAST_URL

    # Archive endpoint doesn't support visibility (ERA5 limitation)
    hourly_vars = [v for v in HOURLY_VARIABLES if not (use_archive and v == "visibility")]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(hourly_vars),
        "timezone": "UTC",
        "wind_speed_unit": "kmh",
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                wait = min(2**attempt * 5, 120)
                logger.warning(
                    "Rate limited (429), waiting %ds (attempt %d/%d)", wait, attempt, max_retries
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()

            data = resp.json()
            if data.get("error"):
                msg = f"Open-Meteo API error: {data.get('reason', 'unknown')}"
                raise ValueError(msg)

            return data

        except requests.exceptions.RequestException as e:
            wait = min(2**attempt * 2, 60)
            logger.warning(
                "Request failed (attempt %d/%d): %s. Retrying in %ds",
                attempt,
                max_retries,
                e,
                wait,
            )
            if attempt == max_retries:
                raise
            time.sleep(wait)

    msg = f"Failed to fetch weather after {max_retries} attempts"
    raise RuntimeError(msg)


# ─────────────────────────────────────────────────────────────
# Data Transformation
# ─────────────────────────────────────────────────────────────


def transform_to_rows(
    raw_data: dict[str, Any],
    icao_code: str,
    iata_code: str,
) -> list[dict[str, Any]]:
    """Transform Open-Meteo hourly response into flat row dicts.

    Each row represents one hour of weather at one airport.
    """
    hourly = raw_data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return []

    rows = []
    for i, ts in enumerate(times):
        row: dict[str, Any] = {
            "airport_icao": icao_code,
            "airport_iata": iata_code,
            "timestamp_utc": ts,
            "date": ts[:10],  # YYYY-MM-DD
            "hour": int(ts[11:13]) if len(ts) >= 13 else 0,
            "latitude": raw_data.get("latitude"),
            "longitude": raw_data.get("longitude"),
            "elevation_m": raw_data.get("elevation"),
        }
        # Add all weather variables
        for var in HOURLY_VARIABLES:
            values = hourly.get(var, [])
            row[var] = values[i] if i < len(values) else None
        rows.append(row)

    return rows


# ─────────────────────────────────────────────────────────────
# Parquet Writer
# ─────────────────────────────────────────────────────────────

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


def write_parquet(rows: list[dict[str, Any]], output_path: Path) -> None:
    """Write rows to a Parquet file.

    Falls back to JSON if pyarrow is not installed.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if HAS_PYARROW:
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, output_path, compression="snappy")
        logger.info("Wrote %d rows to %s (Parquet)", len(rows), output_path)
    else:
        # Fallback: write as JSON lines (for environments without pyarrow)
        json_path = output_path.with_suffix(".jsonl")
        with open(json_path, "w") as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + "\n")
        logger.warning(
            "pyarrow not installed — wrote %d rows to %s (JSONL fallback)",
            len(rows),
            json_path,
        )


# ─────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────


def ingest_weather(
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    use_archive: bool = False,
) -> dict[str, int]:
    """Ingest weather data for all target airports.

    Args:
        start_date: Start date (YYYY-MM-DD). Defaults to yesterday.
        end_date: End date (YYYY-MM-DD). Defaults to today + 7 days (forecast).
        use_archive: Use the archive endpoint for historical data.

    Returns:
        Dict mapping airport ICAO codes to number of rows ingested.
    """
    today = datetime.now(UTC).date()

    if start_date is None:
        start_date = (today - timedelta(days=1)).isoformat()
    if end_date is None:
        if use_archive:
            end_date = (today - timedelta(days=5)).isoformat()  # archive lag ~5 days
        else:
            end_date = (today + timedelta(days=7)).isoformat()

    logger.info(
        "Starting weather ingestion: start=%s end=%s archive=%s airports=%d",
        start_date,
        end_date,
        use_archive,
        len(AIRPORTS),
    )

    results: dict[str, int] = {}

    for icao_code, airport in AIRPORTS.items():
        try:
            raw_data = fetch_weather_data(
                latitude=airport["lat"],
                longitude=airport["lon"],
                start_date=start_date,
                end_date=end_date,
                use_archive=use_archive,
            )

            rows = transform_to_rows(raw_data, icao_code, airport["iata"])

            if rows:
                # Partition by date
                dates_seen: dict[str, list[dict[str, Any]]] = {}
                for row in rows:
                    d = row["date"]
                    if d not in dates_seen:
                        dates_seen[d] = []
                    dates_seen[d].append(row)

                total_rows = 0
                for date_str, date_rows in dates_seen.items():
                    output_path = (
                        OUTPUT_DIR / f"airport={icao_code}" / f"date={date_str}" / "weather.parquet"
                    )
                    write_parquet(date_rows, output_path)
                    total_rows += len(date_rows)

                results[icao_code] = total_rows
                logger.info(
                    "Airport %s (%s): ingested %d rows across %d dates",
                    icao_code,
                    airport["name"],
                    total_rows,
                    len(dates_seen),
                )
            else:
                results[icao_code] = 0
                logger.warning("Airport %s: no weather data returned", icao_code)

        except Exception:
            results[icao_code] = -1
            logger.exception("Failed to ingest weather for %s", icao_code)

    total = sum(v for v in results.values() if v > 0)
    errors = sum(1 for v in results.values() if v < 0)
    logger.info(
        "Weather ingestion complete: total_rows=%d airports=%d errors=%d",
        total,
        len(results),
        errors,
    )

    return results


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────


def main() -> None:
    """CLI entry point for weather ingestion."""
    import argparse

    parser = argparse.ArgumentParser(description="Ingest weather data from Open-Meteo")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--archive",
        action="store_true",
        help="Use archive endpoint (ERA5 reanalysis) for historical data",
    )
    args = parser.parse_args()

    results = ingest_weather(
        start_date=args.start_date,
        end_date=args.end_date,
        use_archive=args.archive,
    )

    # Print summary
    for icao, count in sorted(results.items()):
        status = f"{count} rows" if count >= 0 else "ERROR"
        print(f"  {icao}: {status}")  # noqa: T201


if __name__ == "__main__":
    main()
