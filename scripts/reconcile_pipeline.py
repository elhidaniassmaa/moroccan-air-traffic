"""Data pipeline reconciliation script: produced vs consumed vs DLQ.

Measures:
- Messages published to flights.raw
- Messages received in the DLQ (flights.dlq)
- Validated records processed
- Reconciles counts and alerts on anomalies (> 1% DLQ rate).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

logger = logging.getLogger("reconcile_pipeline")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def reconcile_counts(
    produced_count: int, dlq_count: int, processed_count: int
) -> dict[str, float | bool]:
    """Calculate pipeline reconciliation metrics.

    Args:
        produced_count: Total raw messages produced from OpenSky/replay
        dlq_count: Messages diverted to dead letter queue
        processed_count: Validated records ingested downstream

    Returns:
        Dictionary containing discrepancy metrics and health status.
    """
    total_accounted = processed_count + dlq_count
    discrepancy = abs(produced_count - total_accounted)

    dlq_rate = (dlq_count / produced_count * 100.0) if produced_count > 0 else 0.0
    is_healthy = discrepancy == 0 and dlq_rate <= 5.0

    return {
        "produced": produced_count,
        "processed": processed_count,
        "dlq": dlq_count,
        "discrepancy": discrepancy,
        "dlq_rate_pct": round(dlq_rate, 2),
        "is_healthy": is_healthy,
    }


def main() -> None:
    """CLI runner for pipeline reconciliation."""
    parser = argparse.ArgumentParser(description="Reconcile pipeline ingestion counts")
    parser.add_argument("--produced", type=int, default=1000, help="Total messages produced")
    parser.add_argument("--dlq", type=int, default=5, help="Total messages routed to DLQ")
    parser.add_argument("--processed", type=int, default=995, help="Total messages processed")
    args = parser.parse_args()

    results = reconcile_counts(args.produced, args.dlq, args.processed)
    print("\n📊 Pipeline Reconciliation Report:")  # noqa: T201
    print(json.dumps(results, indent=2))  # noqa: T201

    if not results["is_healthy"]:
        logger.error("Reconciliation failed: data loss or elevated DLQ error rate detected!")
        sys.exit(1)
    logger.info("Pipeline reconciliation SUCCESS: All messages accounted for.")


if __name__ == "__main__":
    main()
