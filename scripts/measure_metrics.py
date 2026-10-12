"""Pipeline metrics measurement script.

Computes actual operational metrics from local runs or test fixtures:
- Message generation throughput (msgs/sec)
- Validation rate (%)
- Latency (seconds)
Outputs real values to avoid fabricated numbers in README.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.producers.opensky_producer import SyntheticGenerator, validate_event


def measure_producer_throughput(samples: int = 200) -> dict[str, float]:
    """Measure actual throughput of synthetic generator and validation engine."""
    gen = SyntheticGenerator(num_aircraft=50)

    start_time = time.perf_counter()
    total_events = 0
    valid_events = 0

    while total_events < samples:
        batch = gen.generate_batch()
        for event in batch:
            errs = validate_event(event)
            if not errs:
                valid_events += 1
            total_events += 1

    elapsed = time.perf_counter() - start_time
    events_per_sec = total_events / elapsed if elapsed > 0 else 0.0

    return {
        "total_measured_events": total_events,
        "valid_events": valid_events,
        "validation_success_pct": round((valid_events / total_events) * 100.0, 2),
        "elapsed_seconds": round(elapsed, 4),
        "events_per_second": round(events_per_sec, 2),
    }


def main() -> None:
    """Run metrics measurement and print verified figures."""
    print("[*] Measuring local pipeline performance...")  # noqa: T201
    metrics = measure_producer_throughput()
    print("\n--- Pipeline Verified Metrics ---")  # noqa: T201
    for k, v in metrics.items():
        print(f"  {k}: {v}")  # noqa: T201


if __name__ == "__main__":
    main()
