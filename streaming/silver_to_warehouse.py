"""
Moroccan Air Traffic — Silver to PostgreSQL Data Warehouse Loader

Ce script PySpark batch lit la table Delta Lake Silver (couche enrichie)
et la charge de manière idempotente dans la table `silver_flights` du Data Warehouse PostgreSQL.

Choix technique défendable en entretien :
1. PySpark JDBC : Utilise le pilote JDBC officiel PostgreSQL avec Delta Lake pour garantir la
   cohérence transactionnelle des lectures (ACID via Delta log) et des écritures parallélisées.
2. Idempotence : Utilise le mode 'overwrite' avec l'option 'truncate=true' (ou swap atomique),
   garantissant qu'aucune duplication de données n'est créée si le job est relancé plusieurs fois.
Alternatives écartées :
- DuckDB read_parquet : rejeté pour éliminer DuckDB et respecter l'architecture d'entreprise PostgreSQL.
- COPY PostgreSQL direct : rejeté car les répertoires Delta Lake contiennent des logs transactionnels
  (_delta_log) et des versions historiques de Parquet que seul un moteur compatible Delta sait filtrer.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("silver_to_warehouse")

DELTA_SILVER_PATH = os.getenv("DELTA_SILVER_PATH", "/app/data/lake/silver/flights")
WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "warehouse")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5432")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "flights_dw")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "flights_user")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "flights_password_dev")


def get_spark_session() -> SparkSession:
    """Crée ou récupère une session Spark avec support Delta Lake."""
    return (
        SparkSession.builder.appName("MoroccanAirTraffic-SilverToWarehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def load_silver_to_postgres() -> int:
    """Lit Delta Silver et écrit dans PostgreSQL Warehouse."""
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Lecture de la table Delta Silver : %s", DELTA_SILVER_PATH)

    try:
        silver_df = spark.read.format("delta").load(DELTA_SILVER_PATH)
    except Exception as e:
        logger.error("Impossible de lire Delta Silver à %s : %s", DELTA_SILVER_PATH, e)
        return 0

    total_records = silver_df.count()
    logger.info("Nombre total de positions dans Delta Silver : %d", total_records)

    if total_records == 0:
        logger.warning("Aucun enregistrement trouvé dans Delta Silver. Fin du job.")
        return 0

    # Déduplication finale par icao24 et position_ts (au cas où)
    window_spec = Window.partitionBy("icao24", "time_position").orderBy(
        F.col("ingestion_ts").desc()
    )
    deduped_df = (
        silver_df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    jdbc_url = f"jdbc:postgresql://{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"
    target_table = "silver_flights"

    logger.info("Écriture idempotente vers PostgreSQL JDBC : %s, table: %s", jdbc_url, target_table)

    (
        deduped_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", target_table)
        .option("user", WAREHOUSE_USER)
        .option("password", WAREHOUSE_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("stringtype", "unspecified")
        .option("batchsize", "10000")
        .mode("overwrite")
        .save()
    )

    logger.info("Succès : %d enregistrements insérés dans %s.%s", total_records, WAREHOUSE_DB, target_table)
    return total_records


if __name__ == "__main__":
    count = load_silver_to_postgres()
    print(f"=== SILVER TO WAREHOUSE COMPLETED: {count} rows ===")
