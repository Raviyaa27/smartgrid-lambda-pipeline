"""
Show the alerts Prometheus is evaluating, from the terminal.

    python scripts/alerts.py            # firing and pending alerts
    python scripts/alerts.py --targets  # also each scrape target's health

The same list as http://localhost:9090/alerts and the Grafana table, for
when a browser is not at hand.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

PROMETHEUS = "http://localhost:9090"


def get(path: str) -> dict:
    with urllib.request.urlopen(PROMETHEUS + path, timeout=5) as response:
        return json.load(response)["data"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--targets", action="store_true", help="also show scrape targets")
    args = parser.parse_args()
    try:
        alerts = get("/api/v1/alerts")["alerts"]
        targets = get("/api/v1/targets")["activeTargets"] if args.targets else []
    except (urllib.error.URLError, OSError):
        print(f"Prometheus is not reachable at {PROMETHEUS}. Is the stack up?")
        return 1

    order = {"firing": 0, "pending": 1}
    alerts.sort(key=lambda a: (order.get(a["state"], 2), a["labels"]["alertname"]))
    if not alerts:
        print("\n  No alerts firing or pending.\n")
    else:
        print(f"\n  {'state':<9}{'severity':<10}{'alert':<20}{'where':<20}summary")
        for alert in alerts:
            labels = alert["labels"]
            where = labels.get("zone") or labels.get("job", "")
            print(
                f"  {alert['state']:<9}{labels.get('severity', ''):<10}"
                f"{labels['alertname']:<20}{where:<20}{alert['annotations'].get('summary', '')}"
            )
        print()
    for target in targets:
        print(f"  target {target['labels']['job']:<20} {target['health']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
