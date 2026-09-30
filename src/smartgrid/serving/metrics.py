"""
Pipeline and business state as Prometheus metrics, computed at scrape time.

Each component exports its own machinery: messages sent, micro-batch
durations, requests served. What none of them can export is the state only
the serving store knows:

- how old each zone's latest figure is;
- whether a zone's solar share is below the alert floor while the sun is high;
- how many simulated days have ended without being settled;
- whether a day is blocked because its drop was refused.

This collector reads that state through the same ServingStore the API uses,
each time Prometheus scrapes /metrics. The alert rules and the dashboard
therefore judge the same figures, and the thresholds come from the same
Settings (the alert floor is exported, so the rule reads it).

Values are in REAL seconds wherever a rule compares them to a wall-clock
threshold, and labelled so.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta
from typing import Any

from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from smartgrid.common.clock import day_end, in_hours
from smartgrid.common.config import Settings
from smartgrid.serving.repository import ServingStore


def _gauge(name: str, doc: str, value: float, labels: dict[str, str] | None = None):
    family = GaugeMetricFamily(name, doc, labels=list(labels or {}))
    family.add_metric(list((labels or {}).values()), value)
    return family


class PipelineCollector(Collector):
    def __init__(self, store: ServingStore, settings: Settings) -> None:
        self.store = store
        self.settings = settings

    def collect(self) -> Iterator[GaugeMetricFamily]:
        try:
            families = self._families()
        except Exception:  # store down or mid-restart: say so, never fail the scrape
            yield _gauge("smartgrid_store_up", "1 if the serving store answered.", 0)
            return
        yield _gauge("smartgrid_store_up", "1 if the serving store answered.", 1)
        yield from families

    def _families(self) -> list[GaugeMetricFamily]:
        settings = self.settings
        clock = self.store.clock()
        out = [
            _gauge(
                "smartgrid_simulation_running",
                "1 if a simulation is running (the shared clock exists).",
                0 if clock is None else 1,
            ),
            _gauge(
                "smartgrid_renewable_alert_floor_ratio",
                "Solar share of consumption below which a zone is flagged in alert hours.",
                settings.renewable_alert_floor,
            ),
        ]
        if clock is None:
            return out
        now = clock.now()
        compression = clock.compression

        def real_seconds(delta: timedelta) -> float:
            return delta.total_seconds() / compression

        out.append(
            _gauge(
                "smartgrid_simulated_time_seconds", "Simulated now, Unix seconds.", now.timestamp()
            )
        )

        # -- Speed layer ------------------------------------------------------------
        archived = self.store.stream_progress().get("ingest")
        if archived is not None:
            out.append(
                _gauge(
                    "smartgrid_speed_layer_lag_real_seconds",
                    "How far the archive trails the simulated clock, in real seconds.",
                    real_seconds(now - archived),
                )
            )

        # -- Zones: each zone's latest complete window --------------------------------
        load = GaugeMetricFamily(
            "smartgrid_zone_grid_load_kw", "Average load, latest complete window.", labels=["zone"]
        )
        solar = GaugeMetricFamily(
            "smartgrid_zone_solar_kw", "Solar output, latest complete window.", labels=["zone"]
        )
        share = GaugeMetricFamily(
            "smartgrid_zone_renewable_share_ratio",
            "Solar / consumption, latest complete window.",
            labels=["zone"],
        )
        age = GaugeMetricFamily(
            "smartgrid_zone_data_age_real_seconds",
            "Age of the zone's latest complete window, in real seconds.",
            labels=["zone"],
        )
        hours = GaugeMetricFamily(
            "smartgrid_zone_in_alert_hours",
            "1 if the zone's latest window falls in the renewable alert hours.",
            labels=["zone"],
        )
        for row in self.store.latest_complete_windows(settings.speed_window_minutes):
            zone = [row["grid_zone"]]
            load.add_metric(zone, row["grid_load_kw"])
            solar.add_metric(zone, row["solar_kw"])
            if row["renewable_share"] is not None:
                share.add_metric(zone, row["renewable_share"])
            age.add_metric(zone, real_seconds(now - row["window_end"]))
            daylight = in_hours(
                row["window_start"],
                settings.renewable_alert_start_hour,
                settings.renewable_alert_end_hour,
            )
            hours.add_metric(zone, 1 if daylight else 0)
        out += [load, solar, share, age, hours]

        # -- Settlement ------------------------------------------------------------------
        start, today = clock.start.date(), clock.sim_date()
        ended = []
        day = start
        while day < today and day_end(day) <= now:
            ended.append(day)
            day += timedelta(days=1)
        settled = self.store.settled_runs(start, today)
        unsettled = [d for d in ended if d not in settled]
        verdicts: dict[Any, bool] = self.store.latest_gate_verdicts(int(clock.real_start))
        refused = [d for d in unsettled if verdicts.get(d) is False]
        oldest = real_seconds(now - day_end(unsettled[0])) if unsettled else 0.0
        out += [
            _gauge(
                "smartgrid_days_ended", "Simulated days ended and due for settlement.", len(ended)
            ),
            _gauge("smartgrid_days_settled", "Days with a successful settlement.", len(settled)),
            _gauge("smartgrid_days_unsettled", "Days ended but not settled.", len(unsettled)),
            _gauge(
                "smartgrid_days_refused",
                "Unsettled days whose latest drop the quality gate refused.",
                len(refused),
            ),
            _gauge(
                "smartgrid_oldest_unsettled_day_age_real_seconds",
                "Real seconds since the oldest unsettled day became due; 0 if none.",
                oldest,
            ),
        ]
        return out
