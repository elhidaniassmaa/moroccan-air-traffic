"""Unit tests for reconciliation, healthcheck, and metric measurement scripts."""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.measure_metrics import measure_producer_throughput
from scripts.reconcile_pipeline import reconcile_counts


def test_reconcile_counts_healthy() -> None:
    """Verify reconciliation succeeds with 0 discrepancy and low DLQ."""
    res = reconcile_counts(produced_count=1000, dlq_count=5, processed_count=995)
    assert res["is_healthy"] is True
    assert res["discrepancy"] == 0
    assert res["dlq_rate_pct"] == 0.5


def test_reconcile_counts_discrepancy() -> None:
    """Verify reconciliation detects discrepancy (data drop)."""
    res = reconcile_counts(produced_count=1000, dlq_count=5, processed_count=900)
    assert res["is_healthy"] is False
    assert res["discrepancy"] == 95


def test_reconcile_counts_excessive_dlq() -> None:
    """Verify reconciliation flags high DLQ rates (> 5%)."""
    res = reconcile_counts(produced_count=100, dlq_count=15, processed_count=85)
    assert res["is_healthy"] is False
    assert res["dlq_rate_pct"] == 15.0


def test_measure_producer_throughput() -> None:
    """Verify metrics measurement generates positive throughput and 100% valid synthetic events."""
    metrics = measure_producer_throughput(samples=50)
    assert metrics["total_measured_events"] >= 50
    assert metrics["validation_success_pct"] == 100.0
    assert metrics["events_per_second"] > 0.0
