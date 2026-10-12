"""
Spark Structured Streaming — Bronze Layer

Reads raw flight events from the Kafka topic `flights.raw`, parses JSON
with an explicit schema, applies watermark-based deduplication, and writes
to Delta Lake partitioned by event_date.

Bronze layer contract:
- Raw data with minimal transformation (JSON parse + dedup)
- Partitioned by event_date (YYYY-MM-DD)
- Watermark: 10 minutes (late events beyond this are dropped)
- Deduplication on (icao24, time_position) within the watermark window

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \\
        streaming/flights_bronze_stream.py

Version alignment:
    - PySpark 3.5.1
    - delta-spark 3.1.0
    - spark-sql-kafka-0-10_2.12:3.5.1
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from streaming.helpers import FLIGHT_EVENT_SCHEMA

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_FLIGHTS", "flights.raw")
DELTA_BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "data/lake/bronze/flights")
CHECKPOINT_PATH = os.getenv("BRONZE_CHECKPOINT_PATH", "data/checkpoints/bronze_flights")
WATERMARK_DELAY = os.getenv("BRONZE_WATERMARK_DELAY", "10 minutes")
TRIGGER_INTERVAL = os.getenv("BRONZE_TRIGGER_INTERVAL", "30 seconds")


def create_spark_session() -> SparkSession:
    """Create a Spark session configured for Delta Lake."""
    return (
        SparkSession.builder.appName("FlightsBronzeStream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def run_bronze_stream() -> None:
    """Main entry point for the bronze streaming job."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(  # noqa: T201
        f"Starting Bronze stream: "
        f"kafka={KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC} "
        f"output={DELTA_BRONZE_PATH} watermark={WATERMARK_DELAY}"
    )

    # ── Read from Kafka ──────────────────────────────────────
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON with explicit schema ──────────────────────
    parsed_df = (
        raw_df.selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as json_value",
            "timestamp as kafka_timestamp",
            "partition as kafka_partition",
            "offset as kafka_offset",
        )
        .withColumn("parsed", F.from_json(F.col("json_value"), FLIGHT_EVENT_SCHEMA()))
        .select(
            F.col("kafka_key"),
            F.col("kafka_timestamp"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("parsed.*"),
        )
        # Convert time_position to a proper timestamp for watermarking
        .withColumn(
            "event_timestamp",
            F.from_unixtime(F.col("time_position")).cast("timestamp"),
        )
        # Derive event_date if not already set
        .withColumn(
            "event_date",
            F.coalesce(
                F.col("event_date"),
                F.date_format(F.col("event_timestamp"), "yyyy-MM-dd"),
            ),
        )
    )

    # ── Watermark + Deduplication ────────────────────────────
    deduped_df = parsed_df.withWatermark("event_timestamp", WATERMARK_DELAY).dropDuplicates(
        ["icao24", "time_position"]
    )

    # ── Write to Delta Lake ──────────────────────────────────
    query = (
        deduped_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(DELTA_BRONZE_PATH)
    )

    print(f"Bronze stream started. Query ID: {query.id}")  # noqa: T201
    query.awaitTermination()


if __name__ == "__main__":
    run_bronze_stream()
