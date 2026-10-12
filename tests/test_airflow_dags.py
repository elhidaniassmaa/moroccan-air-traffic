"""Unit tests validating that Airflow DAGs parse cleanly without syntax or configuration errors."""

from __future__ import annotations

import ast
from pathlib import Path

DAGS_FOLDER = Path(__file__).parent.parent / "orchestration" / "dags"


def test_airflow_dags_syntax_and_structure() -> None:
    """Validate DAG files syntax via AST and verify key DAG attributes."""
    expected_dags = {
        "weather_daily.py": [
            "weather_daily",
            "ingest_open_meteo_weather",
            "dbt_run_weather_staging",
            "dbt_test_weather",
        ],
        "dbt_hourly.py": ["dbt_hourly", "dbt_seed", "dbt_run_marts", "dbt_test_marts"],
    }

    for filename, expected_identifiers in expected_dags.items():
        dag_file = DAGS_FOLDER / filename
        assert dag_file.exists(), f"DAG file missing: {filename}"

        source_code = dag_file.read_text(encoding="utf-8")
        parsed = ast.parse(source_code, filename=str(dag_file))
        assert parsed is not None, f"Failed to parse AST for {filename}"

        # Verify presence of expected tasks / DAG identifiers
        for identifier in expected_identifiers:
            assert identifier in source_code, f"Identifier {identifier} missing in {filename}"

        # Verify retries and error handling are configured
        assert "retries" in source_code
        assert "on_failure_callback" in source_code


def test_airflow_dagbag_if_available() -> None:
    """If Airflow is fully installed with runtime dependencies, run standard DagBag validation."""
    try:
        from airflow.models import DagBag

        dagbag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)
        assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
        assert "weather_daily" in dagbag.dags
        assert "dbt_hourly" in dagbag.dags
    except (ImportError, ModuleNotFoundError):
        # Full Airflow stack runs in Docker container; AST verification above suffices for local host
        pass
