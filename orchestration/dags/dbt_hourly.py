"""
DAG: dbt_hourly

Orchestrates hourly incremental builds of the Kimball analytical marts:
- Updates dimensional models and seeds if modified
- Runs incremental model fct_airport_movements_hourly
- Rebuilds mart_traffic_weather_impact
- Executes dbt test quality suite
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_alert,
}

with DAG(
    dag_id="dbt_hourly",
    default_args=default_args,
    description="Run incremental hourly dbt transformations and data quality tests",
    schedule_interval="15 * * * *",  # Run at minute 15 of every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "incremental", "marts"],
) as dag:
    task_dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt_project && dbt seed --profiles-dir .",
    )

    task_dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select marts --profiles-dir .",
    )

    task_dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command="cd /opt/airflow/dbt_project && dbt test --select marts --profiles-dir .",
    )

    task_dbt_seed >> task_dbt_run_marts >> task_dbt_test_marts
