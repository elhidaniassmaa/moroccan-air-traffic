"""
DAG: dbt_hourly

Orchestre le pipeline analytique horaire :
1. Chargement idempotent de la couche Delta Lake Silver vers PostgreSQL Warehouse (silver_flights)
2. Chargement des référentiels statiques (dbt seed : aéroports, compagnies, événements)
3. Exécution des transformations dbt (staging -> intermediate -> marts)
4. Exécution de la suite complète de tests de qualité des données (dbt test)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")


def on_failure_alert(context: dict) -> None:
    """Callback déclenché en cas d'échec d'une tâche."""
    dag_id = context.get("task_instance").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    logger.error("AIRFLOW TASK FAILURE: %s.%s at %s", dag_id, task_id, execution_date)


def run_silver_to_warehouse(**kwargs) -> None:
    """Charge les données Silver vers la table silver_flights de PostgreSQL."""
    from ingestion.batch.silver_to_postgres import sync_silver_to_postgres

    rows = sync_silver_to_postgres()
    logger.info("Synchronisation Silver -> PostgreSQL réussie (%d lignes)", rows)


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
    description="Synchronise Silver -> PostgreSQL et exécute dbt run / dbt test",
    schedule_interval="15 * * * *",  # Toutes les heures à la 15ème minute
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "incremental", "warehouse", "postgres"],
) as dag:
    task_silver_to_warehouse = PythonOperator(
        task_id="silver_to_warehouse",
        python_callable=run_silver_to_warehouse,
    )

    task_dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt_project && dbt seed --profiles-dir .",
    )

    task_dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir .",
    )

    task_dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir .",
    )

    task_silver_to_warehouse >> task_dbt_seed >> task_dbt_run_marts >> task_dbt_test_marts
