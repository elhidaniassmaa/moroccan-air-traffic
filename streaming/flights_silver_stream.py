"""
Spark Structured Streaming — Silver Layer

Reads from the Delta Lake bronze layer, enriches each flight record with:
- Nearest airport (haversine distance)
- Heuristic flight phase detection
- 15-minute windowed distinct aircraft count per airport

Writes the enriched data to the silver Delta Lake table.

Silver layer contract:
- Enriched data with nearest_airport, flight_phase, distance_to_airport
- Partitioned by event_date
- Incremental processing from bronze checkpoint

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.1.0 \\
        streaming/flights_silver_stream.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from streaming.helpers import (
    detect_flight_phase,
    find_nearest_airport,
)

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

DELTA_BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "data/lake/bronze/flights")
DELTA_SILVER_PATH = os.getenv("DELTA_SILVER_PATH", "data/lake/silver/flights")
CHECKPOINT_PATH = os.getenv("SILVER_CHECKPOINT_PATH", "data/checkpoints/silver_flights")
TRIGGER_INTERVAL = os.getenv("SILVER_TRIGGER_INTERVAL", "1 minute")


def create_spark_session() -> SparkSession:
    """Create a Spark session configured for Delta Lake."""
    return (
        SparkSession.builder.appName("FlightsSilverStream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# UDFs
# ─────────────────────────────────────────────────────────────

# Return type for find_nearest_airport UDF
NEAREST_AIRPORT_SCHEMA = T.StructType(
    [
        T.StructField("nearest_airport_icao", T.StringType(), nullable=True),
        T.StructField("nearest_airport_iata", T.StringType(), nullable=True),
        T.StructField("distance_to_airport_km", T.DoubleType(), nullable=True),
    ]
)


@F.udf(returnType=NEAREST_AIRPORT_SCHEMA)
def udf_find_nearest_airport(lat: float, lon: float):
    """Spark UDF wrapper for find_nearest_airport."""
    if lat is None or lon is None:
        return (None, None, None)
    icao, iata, dist = find_nearest_airport(lat, lon)
    return (icao, iata, round(dist, 2) if dist is not None else None)


@F.udf(returnType=T.StringType())
def udf_detect_flight_phase(
    on_ground: bool,
    baro_altitude: float,
    velocity: float,
    vertical_rate: float,
) -> str:
    """Spark UDF wrapper for detect_flight_phase."""
    return detect_flight_phase(
        on_ground=on_ground if on_ground is not None else False,
        baro_altitude=baro_altitude,
        velocity=velocity,
        vertical_rate=vertical_rate,
    )


# ─────────────────────────────────────────────────────────────
# Silver Stream
# ─────────────────────────────────────────────────────────────


def run_silver_stream() -> None:
    """Main entry point for the silver streaming job."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(  # noqa: T201
        f"Starting Silver stream: bronze={DELTA_BRONZE_PATH} silver={DELTA_SILVER_PATH}"
    )

    # ── Read from Bronze Delta Lake (streaming) ──────────────
    bronze_df = spark.readStream.format("delta").load(DELTA_BRONZE_PATH)

    # ── Enrich: nearest airport ──────────────────────────────
    with_airport = (
        bronze_df.withColumn(
            "airport_info",
            udf_find_nearest_airport(F.col("latitude"), F.col("longitude")),
        )
        .select(
            "*",
            F.col("airport_info.nearest_airport_icao").alias("nearest_airport_icao"),
            F.col("airport_info.nearest_airport_iata").alias("nearest_airport_iata"),
            F.col("airport_info.distance_to_airport_km").alias("distance_to_airport_km"),
        )
        .drop("airport_info")
    )

    # ── Enrich: flight phase ─────────────────────────────────
    enriched_df = with_airport.withColumn(
        "flight_phase",
        udf_detect_flight_phase(
            F.col("on_ground"),
            F.col("baro_altitude"),
            F.col("velocity"),
            F.col("vertical_rate"),
        ),
    )

    # ── Derived metrics ──────────────────────────────────────
    enriched_df = enriched_df.withColumn(
        "velocity_kmh",
        F.when(F.col("velocity").isNotNull(), F.col("velocity") * 3.6).otherwise(None),
    ).withColumn(
        "baro_altitude_ft",
        F.when(F.col("baro_altitude").isNotNull(), F.col("baro_altitude") * 3.28084).otherwise(
            None
        ),
    )

    # ── Write to Silver Delta Lake ───────────────────────────
    query = (
        enriched_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(DELTA_SILVER_PATH)
    )

    print(f"Silver stream started. Query ID: {query.id}")  # noqa: T201
    query.awaitTermination()


if __name__ == "__main__":
    run_silver_stream()
