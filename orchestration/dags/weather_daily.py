"""
DAG: weather_daily

Ingests daily weather forecasts and recent observations from Open-Meteo,
stores them in partitioned Parquet files within the Data Lake, then builds
the corresponding dbt staging and intermediate models.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")


def on_failure_alert(context: dict) -> None:
    """Callback triggered on task failure."""
    dag_id = context.get("task_instance").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    logger.error("AIRFLOW TASK FAILURE: %s.%s at %s", dag_id, task_id, execution_date)


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_alert,
}

with DAG(
    dag_id="weather_daily",
    default_args=default_args,
    description="Ingest weather forecasts and build staging dbt models",
    schedule_interval="0 4 * * *",  # Run daily at 04:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "ingestion", "dbt"],
) as dag:

    def run_weather_ingest(**kwargs) -> None:
        """Call batch weather ingestion."""
        from ingestion.batch.weather import ingest_weather

        # Ingest 1 day back and 7 days forecast
        results = ingest_weather()
        logger.info("Weather ingestion completed: %s", results)

    task_ingest_weather = PythonOperator(
        task_id="ingest_open_meteo_weather",
        python_callable=run_weather_ingest,
    )

    task_dbt_run_weather_staging = BashOperator(
        task_id="dbt_run_weather_staging",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select stg_weather int_weather_hourly --profiles-dir .",
    )

    task_dbt_test_weather = BashOperator(
        task_id="dbt_test_weather",
        bash_command="cd /opt/airflow/dbt_project && dbt test --select stg_weather int_weather_hourly --profiles-dir .",
    )

    task_ingest_weather >> task_dbt_run_weather_staging >> task_dbt_test_weather
