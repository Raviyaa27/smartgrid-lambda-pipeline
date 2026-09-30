"""
Generate the Grafana operations dashboard: infra/grafana/dashboards/smartgrid-operations.json

    python scripts/build_grafana_dashboard.py

The dashboard is code: panels are declared here and the JSON is generated,
so a change is a reviewable diff of a few lines, not of hand-edited JSON.
Grafana loads the generated file through provisioning (read-only in the UI).

Chart rules, the same as the business dashboard's:
- one axis per panel; different units are different panels;
- colour follows identity in a fixed slot order (zones A-F, reasons, statuses),
  from the validated reference palette, never Grafana's cycling defaults;
- status colours (good / warning / critical) only where a value MEANS good or
  bad, and always beside a number and a title, never colour alone;
- thresholds drawn as dashed lines, gridlines left as they are.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parents[1] / "infra/grafana/dashboards/smartgrid-operations.json"
DS = {"type": "prometheus", "uid": "prometheus"}

# Reference categorical palette, light steps, fixed order (validated CVD-safe
# on adjacent pairs; slots 3-5 are below 3:1 contrast, so every multi-series
# panel carries a legend table with values -- the relief rule).
SLOTS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
GOOD, WARNING, CRITICAL = "#0ca30c", "#fab219", "#d03b3b"

ZONES = [f"ZONE-{c}" for c in "ABCDEF"]
REASONS = [
    "malformed_json",
    "missing_field",
    "wrong_type",
    "out_of_range",
    "unknown_household",
    "future_timestamp",
]

_ids = iter(range(1, 1000))


def fixed(colour: str) -> dict:
    return {"mode": "fixed", "fixedColor": colour}


def by_name(names: list[str]) -> list[dict]:
    """Overrides pinning each series name to its slot: colour follows identity."""
    return [
        {
            "matcher": {"id": "byName", "options": name},
            "properties": [{"id": "color", "value": fixed(SLOTS[i])}],
        }
        for i, name in enumerate(names)
    ]


def target(expr: str, legend: str = "", *, instant: bool = False, ref: str = "A") -> dict:
    return {
        "refId": ref,
        "datasource": DS,
        "expr": expr,
        "legendFormat": legend or "__auto",
        "instant": instant,
        "range": not instant,
    }


def steps(*pairs: tuple[float | None, str]) -> dict:
    return {"mode": "absolute", "steps": [{"value": v, "color": c} for v, c in pairs]}


def stat(title: str, expr: str, x: int, *, unit: str, thresholds: dict, description: str) -> dict:
    return {
        "id": next(_ids),
        "type": "stat",
        "title": title,
        "description": description,
        "datasource": DS,
        "gridPos": {"x": x, "y": 0, "w": 4, "h": 4},
        "targets": [target(expr, instant=True)],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "decimals": 0 if unit in ("none", "short") else 1,
                "color": {"mode": "thresholds"},
                "thresholds": thresholds,
                "noValue": "no data",
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "background",
            "graphMode": "none",
            "textMode": "value",
            "justifyMode": "center",
            "orientation": "auto",
            "showPercentChange": False,
        },
    }


def timeseries(
    title: str,
    targets: list[dict],
    pos: tuple[int, int, int, int],
    *,
    unit: str,
    description: str,
    names: list[str] | None = None,
    threshold: float | None = None,
    single: bool = False,
    step: bool = False,
    minimum: float | None = 0,
) -> dict:
    x, y, w, h = pos
    custom: dict[str, Any] = {
        "drawStyle": "line",
        "lineWidth": 2,
        "fillOpacity": 10 if single else 0,  # a wash for one series, never blocks
        "lineInterpolation": "stepAfter" if step else "linear",
        "showPoints": "never",
        "spanNulls": False,
        "axisPlacement": "left",
        "axisSoftMin": minimum,
        "thresholdsStyle": {"mode": "dashed" if threshold is not None else "off"},
    }
    defaults: dict[str, Any] = {"unit": unit, "custom": custom}
    if single:
        defaults["color"] = fixed(SLOTS[0])
    if threshold is not None:
        defaults["thresholds"] = steps((None, "transparent"), (threshold, "#898781"))
    return {
        "id": next(_ids),
        "type": "timeseries",
        "title": title,
        "description": description,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": by_name(names or [])},
        "options": {
            "legend": {
                "showLegend": not single,
                "displayMode": "table",
                "placement": "right",
                "calcs": ["lastNotNull"],
            },
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }


def row(title: str, y: int) -> dict:
    return {
        "id": next(_ids),
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }


def alerts_table(y: int) -> dict:
    return {
        "id": next(_ids),
        "type": "table",
        "title": "Firing alerts",
        "description": "Prometheus alert rules currently firing (infra/prometheus/rules).",
        "datasource": DS,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 4},
        "targets": [{**target('ALERTS{alertstate="firing"}', instant=True), "format": "table"}],
        "transformations": [
            {
                "id": "organize",
                "options": {
                    "excludeByName": {
                        "Time": True,
                        "Value": True,
                        "__name__": True,
                        "alertstate": True,
                        "project": True,
                    },
                    "indexByName": {"alertname": 0, "severity": 1, "zone": 2, "job": 3},
                    "renameByName": {
                        "alertname": "Alert",
                        "severity": "Severity",
                        "zone": "Zone",
                        "job": "Component",
                        "instance": "Instance",
                    },
                },
            }
        ],
        "fieldConfig": {"defaults": {"noValue": "No alerts firing"}, "overrides": []},
        "options": {"showHeader": True, "cellHeight": "sm"},
    }


def build() -> dict:
    panels = [
        # -- Is the pipeline healthy right now? ----------------------------------
        stat(
            "Alerts firing",
            'count(ALERTS{alertstate="firing"}) or vector(0)',
            0,
            unit="none",
            thresholds=steps((None, GOOD), (1, CRITICAL)),
            description="Alert rules currently firing. The table below names them.",
        ),
        stat(
            "Real-time lag",
            "smartgrid_speed_layer_lag_real_seconds",
            4,
            unit="s",
            thresholds=steps((None, GOOD), (60, CRITICAL)),
            description="How far the archive trails the simulated clock. Target: 60 s (R1).",
        ),
        stat(
            "Days unsettled",
            "smartgrid_days_unsettled",
            8,
            unit="none",
            thresholds=steps((None, GOOD), (1, WARNING)),
            description="Simulated days that have ended and are not settled yet.",
        ),
        stat(
            "Days blocked",
            "smartgrid_days_refused",
            12,
            unit="none",
            thresholds=steps((None, GOOD), (1, CRITICAL)),
            description="Unsettled days whose daily drop the quality gate refused.",
        ),
        stat(
            "Readings rejected",
            'sum(rate(smartgrid_speed_records_total{outcome!="valid"}[2m]))'
            " / sum(rate(smartgrid_speed_records_total[2m]))",
            16,
            unit="percentunit",
            thresholds=steps((None, GOOD), (0.05, WARNING)),
            description="Share of readings sent to the dead-letter topic. Baseline 0.7 %.",
        ),
        stat(
            "Components down",
            "count(up == 0) or vector(0)",
            20,
            unit="none",
            thresholds=steps((None, GOOD), (1, CRITICAL)),
            description="Scrape targets down: API, speed layer, the two sources, Prometheus.",
        ),
        alerts_table(4),
        # -- Ingestion and the speed layer ----------------------------------------
        row("Ingestion and the speed layer", 8),
        timeseries(
            "Readings per second: produced and processed",
            [
                target("sum(rate(smartgrid_producer_messages_total[1m]))", "Produced", ref="A"),
                target("sum(rate(smartgrid_speed_records_total[1m]))", "Processed", ref="B"),
            ],
            (0, 9, 12, 8),
            unit="reqps",
            names=["Produced", "Processed"],
            description="Meter simulator output against speed-layer throughput. Apart = backlog.",
        ),
        timeseries(
            "Rejected readings per second, by reason",
            [
                target(
                    'sum by (outcome) (rate(smartgrid_speed_records_total{outcome!="valid"}[2m]))',
                    "{{outcome}}",
                )
            ],
            (12, 9, 12, 8),
            unit="reqps",
            names=REASONS,
            description="Why readings went to the dead-letter topic.",
        ),
        timeseries(
            "Real-time view behind the simulated clock",
            [target("smartgrid_speed_layer_lag_real_seconds", "Lag")],
            (0, 17, 12, 8),
            unit="s",
            single=True,
            threshold=60,
            description="Dashed line: the 60 s freshness target (R1).",
        ),
        timeseries(
            "Micro-batch duration",
            [target("smartgrid_speed_batch_duration_seconds", "{{query}}")],
            (12, 17, 12, 8),
            unit="s",
            names=["ingest", "zone_metrics"],
            threshold=5,
            description="Per streaming query. Dashed line: the 5 s trigger interval.",
        ),
        # -- The grid -----------------------------------------------------------------
        row("The grid (latest complete window per zone)", 25),
        timeseries(
            "Solar share of consumption, by zone",
            [target("smartgrid_zone_renewable_share_ratio", "{{zone}}")],
            (0, 26, 12, 9),
            unit="percentunit",
            names=ZONES,
            threshold=0.30,
            description="Dashed line: the alert floor, applied 10:00-14:00 simulated.",
        ),
        timeseries(
            "Grid load, by zone",
            [target("smartgrid_zone_grid_load_kw", "{{zone}}")],
            (12, 26, 12, 9),
            unit="kwatt",
            names=ZONES,
            description="Average draw over each zone's latest complete 15-minute window.",
        ),
        # -- Batch and serving -----------------------------------------------------------
        row("Batch layer and serving", 35),
        timeseries(
            "Settlement backlog",
            [
                target("smartgrid_days_unsettled", "Awaiting settlement", ref="A"),
                target("smartgrid_days_refused", "Blocked by a refused drop", ref="B"),
            ],
            (0, 36, 8, 8),
            unit="none",
            step=True,
            names=["Awaiting settlement", "Blocked by a refused drop"],
            description="Ended days without a settlement. Normally 0, briefly 1.",
        ),
        timeseries(
            "API requests per second, by status",
            [target("sum by (status) (rate(smartgrid_api_requests_total[1m]))", "{{status}}")],
            (8, 36, 8, 8),
            unit="reqps",
            names=["200", "404", "422", "503"],
            description="404s are expected: bills for unsettled days are refused by design.",
        ),
        timeseries(
            "API latency, 95th percentile",
            [
                target(
                    "histogram_quantile(0.95, "
                    "sum by (le) (rate(smartgrid_api_request_seconds_bucket[2m])))",
                    "p95",
                )
            ],
            (16, 36, 8, 8),
            unit="s",
            single=True,
            description="Across all routes.",
        ),
    ]
    return {
        "uid": "smartgrid-operations",
        "title": "Smart grid: pipeline operations",
        "description": "Health of every stage, the grid's state and the settlement backlog.",
        "tags": ["smartgrid"],
        "timezone": "browser",
        "editable": False,
        "graphTooltip": 1,  # shared crosshair across panels
        "refresh": "10s",
        "time": {"from": "now-15m", "to": "now"},
        "schemaVersion": 41,
        "version": 1,
        "panels": panels,
    }


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
