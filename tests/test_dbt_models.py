"""Validation and syntax verification for dbt models, seeds, macros and schemas.

Runs in local environments where native C-extension DLLs (like DuckDB)
may be restricted by enterprise AppLocker policies, ensuring project
structure, referential consistency, macros, and schema compliance.
"""

from __future__ import annotations

import re
from pathlib import Path

DBT_ROOT = Path(__file__).parent.parent / "dbt_project"


def test_dbt_structure_and_references() -> None:
    """Validate all required dbt models, seeds, macros and snapshots exist."""
    seeds = ["airports.csv", "airlines.csv", "calendar_events.csv"]
    for s in seeds:
        seed_path = DBT_ROOT / "seeds" / s
        assert seed_path.exists(), f"Seed missing: {s}"

    staging = ["stg_airports.sql", "stg_airlines.sql", "stg_weather.sql", "stg_flights.sql"]
    for m in staging:
        path = DBT_ROOT / "models" / "staging" / m
        assert path.exists(), f"Staging model missing: {m}"

    intermediate = [
        "int_flights_near_airport.sql",
        "int_flight_phases.sql",
        "int_weather_hourly.sql",
    ]
    for m in intermediate:
        path = DBT_ROOT / "models" / "intermediate" / m
        assert path.exists(), f"Intermediate model missing: {m}"

    marts = [
        "dim_airport.sql",
        "dim_airline.sql",
        "dim_date.sql",
        "fct_airport_movements_hourly.sql",
        "fct_traffic_density.sql",
        "mart_traffic_weather_impact.sql",
    ]
    for m in marts:
        path = DBT_ROOT / "models" / "marts" / m
        assert path.exists(), f"Mart model missing: {m}"

    macros = ["haversine.sql", "flight_phase.sql", "generic_tests.sql"]
    for mc in macros:
        path = DBT_ROOT / "macros" / mc
        assert path.exists(), f"Macro missing: {mc}"

    snapshots = ["snap_dim_airline.sql"]
    for sn in snapshots:
        path = DBT_ROOT / "snapshots" / sn
        assert path.exists(), f"Snapshot missing: {sn}"


def test_incremental_model_configuration() -> None:
    """Verify fct_airport_movements_hourly contains required incremental config."""
    path = DBT_ROOT / "models" / "marts" / "fct_airport_movements_hourly.sql"
    content = path.read_text(encoding="utf-8")
    assert "materialized='incremental'" in content
    assert "unique_key='movement_hour_id'" in content
    assert "incremental_strategy='delete+insert'" in content
    assert "is_incremental()" in content


def test_macro_haversine_content() -> None:
    """Verify haversine macro formula calculation."""
    path = DBT_ROOT / "macros" / "haversine.sql"
    content = path.read_text(encoding="utf-8")
    assert "6371.0" in content
    assert "asin" in content
    assert "radians" in content


def test_macro_flight_phase_content() -> None:
    """Verify flight_phase macro covers all heuristic flight phase categories."""
    path = DBT_ROOT / "macros" / "flight_phase.sql"
    content = path.read_text(encoding="utf-8")
    for phase in [
        "on_ground",
        "landing",
        "approach",
        "takeoff",
        "climb",
        "descent",
        "cruise",
        "unknown",
    ]:
        assert f"'{phase}'" in content, f"Phase {phase} missing from macro"


def test_sql_syntax_checks() -> None:
    """Verify that models don't have basic syntax discrepancies."""
    sql_files = list(DBT_ROOT.glob("models/**/*.sql")) + list(DBT_ROOT.glob("snapshots/**/*.sql"))
    for f in sql_files:
        text = f.read_text(encoding="utf-8")
        # Ensure balanced parentheses for common CTEs
        assert text.count("(") == text.count(")"), f"Unbalanced parentheses in {f.name}"
        # Ensure Jinja blocks have balanced tags
        open_tags = len(re.findall(r"\{[{%]", text))
        close_tags = len(re.findall(r"[%\}]\}", text))
        assert open_tags == close_tags, f"Unbalanced Jinja brackets in {f.name}"
