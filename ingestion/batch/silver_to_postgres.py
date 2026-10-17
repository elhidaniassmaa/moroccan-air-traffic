"""
Moroccan Air Traffic — Silver to PostgreSQL Loader (Airflow Task)

Lit les données Silver depuis le Data Lake (/opt/airflow/data/lake/silver/flights)
et les synchronise de manière idempotente dans la table PostgreSQL `silver_flights`.
Peut être appelé directement par les DAGs Airflow via PythonOperator ou en ligne de commande.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("silver_to_postgres")


def sync_silver_to_postgres() -> int:
    """Synchronise la couche Silver vers PostgreSQL Warehouse."""
    import psycopg2
    from psycopg2.extras import execute_batch
    import pyarrow.dataset as ds

    lake_dir = os.getenv("DELTA_SILVER_PATH", "/opt/airflow/data/lake/silver/flights")
    if not os.path.exists(lake_dir):
        # Alternative path si exécuté en local
        lake_dir = os.getenv("DELTA_SILVER_PATH_LOCAL", "data/lake/silver/flights")

    logger.info("Lecture du répertoire Silver : %s", lake_dir)
    if not os.path.exists(lake_dir):
        logger.warning("Répertoire introuvable : %s", lake_dir)
        return 0

    try:
        dataset = ds.dataset(lake_dir, format="parquet", partitioning="hive")
        table = dataset.to_table()
    except Exception as e:
        logger.error("Erreur lors de la lecture Parquet avec pyarrow : %s", e)
        return 0

    total_rows = len(table)
    logger.info("Enregistrements lus depuis le lac Silver : %d", total_rows)
    if total_rows == 0:
        return 0

    pydict = table.to_pydict()
    keys = list(pydict.keys())

    # Connexion PostgreSQL
    host = os.getenv("WAREHOUSE_HOST", "warehouse")
    port = os.getenv("WAREHOUSE_PORT", "5432")
    db = os.getenv("WAREHOUSE_DB", "flights_dw")
    user = os.getenv("WAREHOUSE_USER", "flights_user")
    password = os.getenv("WAREHOUSE_PASSWORD", "flights_password_dev")

    conn = psycopg2.connect(
        host=host,
        port=int(port),
        dbname=db,
        user=user,
        password=password,
        connect_timeout=10,
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights (
            icao24 VARCHAR(20),
            callsign VARCHAR(20),
            origin_country VARCHAR(100),
            time_position BIGINT,
            last_contact BIGINT,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            baro_altitude DOUBLE PRECISION,
            baro_altitude_ft DOUBLE PRECISION,
            on_ground BOOLEAN,
            velocity DOUBLE PRECISION,
            velocity_kmh DOUBLE PRECISION,
            true_track DOUBLE PRECISION,
            vertical_rate DOUBLE PRECISION,
            geo_altitude DOUBLE PRECISION,
            squawk VARCHAR(20),
            spi BOOLEAN,
            position_source INTEGER,
            category INTEGER,
            nearest_airport_icao VARCHAR(10),
            nearest_airport_iata VARCHAR(10),
            distance_to_airport_km DOUBLE PRECISION,
            flight_phase VARCHAR(50),
            event_date DATE,
            ingestion_ts TIMESTAMP
        );
    """)
    conn.commit()

    # Déduplication et insertion par lot
    rows_to_insert = []
    seen = set()
    for i in range(total_rows):
        icao = pydict.get("icao24", [None])[i]
        tp = pydict.get("time_position", [None])[i]
        key = (icao, tp)
        if key in seen or not icao or not tp:
            continue
        seen.add(key)

        row = tuple(pydict[k][i] for k in keys)
        rows_to_insert.append(row)

    cols_str = ", ".join(keys)
    vals_placeholder = ", ".join(["%s"] * len(keys))
    insert_sql = f"INSERT INTO silver_flights ({cols_str}) VALUES ({vals_placeholder})"

    # Chargement idempotent avec troncature préalable
    cur.execute("TRUNCATE TABLE silver_flights;")
    execute_batch(cur, insert_sql, rows_to_insert, page_size=5000)
    conn.commit()

    cur.close()
    conn.close()

    logger.info("Synchronisation terminée : %d lignes insérées dans silver_flights", len(rows_to_insert))
    return len(rows_to_insert)


if __name__ == "__main__":
    count = sync_silver_to_postgres()
    print(f"=== SILVER SYNC COMPLETED: {count} rows ===")
