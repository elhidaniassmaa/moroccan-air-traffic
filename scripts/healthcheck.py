"""Healthcheck script — verify all services are reachable."""

from __future__ import annotations

import sys

import requests


def check_redpanda(host: str = "localhost", port: int = 19092) -> bool:
    """Check Redpanda is reachable via the admin API."""
    try:
        # Pandaproxy or admin API
        resp = requests.get(f"http://{host}:18082/topics", timeout=5)
        return resp.status_code == 200
    except requests.exceptions.ConnectionError:
        return False


def check_redpanda_console(host: str = "localhost", port: int = 8080) -> bool:
    """Check Redpanda Console UI is reachable."""
    try:
        resp = requests.get(f"http://{host}:{port}/", timeout=5)
        return resp.status_code == 200
    except requests.exceptions.ConnectionError:
        return False


def main() -> None:
    """Run all health checks and report status."""
    checks = {
        "Redpanda (Kafka)": check_redpanda,
        "Redpanda Console": check_redpanda_console,
    }

    all_ok = True
    for name, check_fn in checks.items():
        ok = check_fn()
        status = "✅ UP" if ok else "❌ DOWN"
        print(f"  {status}  {name}")  # noqa: T201
        if not ok:
            all_ok = False

    if all_ok:
        print("\n🟢 All services healthy")  # noqa: T201
    else:
        print("\n🔴 Some services are down")  # noqa: T201
        sys.exit(1)


if __name__ == "__main__":
    main()
