"""
The business and pipeline state the API exports for Prometheus.

These are the values the alert rules compare against thresholds, so they are
pinned here: shares, ages in REAL seconds, the alert-hours flag, and the
settlement backlog. Same fake store as the API tests: today is day 3 at noon.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from smartgrid.common.config import get_settings
from smartgrid.serving.api import create_app
from smartgrid.serving.metrics import PipelineCollector
from tests.unit.test_serving_api import DAY2, FLEET, FakeStore


def scrape(store: FakeStore) -> dict[tuple[str, tuple], float]:
    """{(metric name, sorted label pairs): value} for one collection."""
    values = {}
    for family in PipelineCollector(store, get_settings()).collect():
        for sample in family.samples:
            values[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return values


def value(values, name: str, **labels) -> float:
    return values[(name, tuple(sorted(labels.items())))]


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


def test_zone_state_is_exported_per_zone(store):
    values = scrape(store)
    assert value(values, "smartgrid_zone_renewable_share_ratio", zone="ZONE-A") == 1.0 / 8.0
    assert value(values, "smartgrid_zone_renewable_share_ratio", zone="ZONE-B") == 0.75
    assert value(values, "smartgrid_zone_grid_load_kw", zone="ZONE-A") == 32.0
    # both windows start at 11:00-11:15, inside the 10:00-14:00 alert hours
    assert value(values, "smartgrid_zone_in_alert_hours", zone="ZONE-A") == 1


def test_ages_are_in_real_seconds_because_the_thresholds_are(store):
    values = scrape(store)
    # ZONE-A's window ended 11:30; simulated now is ~12:00: 30 min / 288 = 6.25 s
    assert value(values, "smartgrid_zone_data_age_real_seconds", zone="ZONE-A") == pytest.approx(
        6.25, abs=0.3
    )
    # archive at 11:45: 15 simulated minutes = 3.1 real seconds behind
    assert value(values, "smartgrid_speed_layer_lag_real_seconds") == pytest.approx(3.1, abs=0.3)


def test_the_alert_floor_is_exported_so_the_rule_and_the_dashboard_share_it(store):
    values = scrape(store)
    assert (
        value(values, "smartgrid_renewable_alert_floor_ratio")
        == get_settings().renewable_alert_floor
    )


def test_a_healthy_backlog_is_zero(store):
    values = scrape(store)
    assert value(values, "smartgrid_days_ended") == 2  # days 1 and 2; day 3 is today
    assert value(values, "smartgrid_days_unsettled") == 0
    assert value(values, "smartgrid_oldest_unsettled_day_age_real_seconds") == 0


def test_an_unsettled_day_with_a_refused_drop_is_counted_and_aged(store):
    del store.settled[DAY2]
    store.verdicts[DAY2] = False
    values = scrape(store)
    assert value(values, "smartgrid_days_unsettled") == 1
    assert value(values, "smartgrid_days_refused") == 1
    # day 2 became due at day 3 00:45; now is day 3 12:00: 11.25 h / 288 = 140.6 s
    assert value(values, "smartgrid_oldest_unsettled_day_age_real_seconds") == pytest.approx(
        140.6, abs=0.5
    )


def test_an_unsettled_day_awaiting_its_drop_is_not_counted_as_refused(store):
    del store.settled[DAY2]
    del store.verdicts[DAY2]  # the gate has not run yet
    values = scrape(store)
    assert value(values, "smartgrid_days_unsettled") == 1
    assert value(values, "smartgrid_days_refused") == 0


def test_a_store_outage_is_reported_not_raised(store):
    store.up = False
    assert scrape(store) == {("smartgrid_store_up", ()): 0}


def test_no_simulation_exports_only_what_is_known(store):
    store._clock = None
    values = scrape(store)
    assert value(values, "smartgrid_simulation_running") == 0
    assert not any(name.startswith("smartgrid_zone_") for name, _ in values)


def test_the_api_serves_them_on_metrics(store):
    client = TestClient(create_app(store=store, settings=get_settings(), fleet=FLEET))
    body = client.get("/metrics").text
    assert 'smartgrid_zone_renewable_share_ratio{zone="ZONE-A"}' in body
    assert "smartgrid_days_unsettled" in body
