"""
The serving API's HTTP contract, against an in-memory store.

The fake returns rows shaped exactly like PostgresStore's queries, so these
tests pin down the merge rule as the API applies it: which layer each figure
comes from, how it is labelled, and what is refused.
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest
from fastapi.testclient import TestClient

from smartgrid.common.clock import SimulatedClock
from smartgrid.common.config import get_settings
from smartgrid.common.domain import build_fleet
from smartgrid.serving.api import create_app

FLEET = build_fleet(num_households=20, num_zones=3, seed=5)
DAY1, DAY2, TODAY = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
HOUSEHOLD = "HH-00001"


def at(day: date, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


def clock_at(moment: datetime) -> SimulatedClock:
    """A clock that reads `moment` now (and for the next few real seconds)."""
    start = at(DAY1)
    elapsed_real = (moment - start).total_seconds() / 288.0
    return SimulatedClock(start=start, day_seconds=300.0, real_start=time.time() - elapsed_real)


def window(zone: str, start: datetime, consumption: float, generation: float = 1.0) -> dict:
    return {
        "grid_zone": zone,
        "window_start": start,
        "window_end": start + timedelta(minutes=15),
        "consumption_kwh": consumption,
        "generation_kwh": generation,
        "net_kwh": consumption - generation,
        "grid_load_kw": consumption * 4,
        "solar_kw": generation * 4,
        "renewable_share": generation / consumption,
        "readings": 10,
        "meters_reporting": 7,
    }


def bill(day: date, run_id: int, total: str, *, trigger="scheduled", reason=None, **extra) -> dict:
    return {
        "run_id": run_id,
        "business_date": day,
        "household_id": HOUSEHOLD,
        "tariff_tier": "DOMESTIC_STD",
        "readings": 150,
        "gross_consumption_kwh": Decimal("12.345"),
        "solar_generation_kwh": Decimal("2.000"),
        "net_import_kwh": Decimal("10.345"),
        "net_export_kwh": Decimal("0.000"),
        "energy_charge": Decimal("124.14"),
        "fixed_charge": Decimal("10.00"),
        "export_credit": Decimal("0.00"),
        "subsidy_amount": Decimal("0.00"),
        "total_payable": Decimal(total),
        "subsidy_applied": False,
        "currency": "LKR",
        "trigger": trigger,
        "drop_version": 2 if trigger == "restatement" else 1,
        "reason": reason,
        "finished_at": at(day + timedelta(days=1), 1),
        "restated": False,
        **extra,
    }


class FakeStore:
    """Days 1 and 2 settled (runs 36 and 37); today is day 3, at noon."""

    def __init__(self, now: datetime | None = None) -> None:
        self.up = True
        self._clock = clock_at(now or at(TODAY, 12))
        self.settled = {DAY1: 36, DAY2: 37}
        self.batch = {
            36: [window("ZONE-A", at(DAY1, 0), 5.0), window("ZONE-A", at(DAY1, 0, 15), 5.0)],
            37: [window("ZONE-A", at(DAY2, 0), 6.0)],
        }
        self.speed = [
            window("ZONE-A", at(DAY1, 0), 4.9),  # superseded by settlement: must never appear
            window("ZONE-A", at(TODAY, 11), 7.0),
            window("ZONE-A", at(TODAY, 11, 15), 8.0),
            window("ZONE-B", at(TODAY, 11), 2.0, 1.5),
        ]
        self.live = [
            window("ZONE-A", at(TODAY, 11, 15), 8.0),
            window("ZONE-B", at(TODAY, 11), 2.0, 1.5),
        ]
        self.progress = {"ingest": at(TODAY, 11, 45)}

    def ping(self) -> bool:
        return self.up

    def clock(self) -> SimulatedClock | None:
        if not self.up:
            raise psycopg.OperationalError("connection refused")
        return self._clock

    def stream_progress(self):
        return self.progress

    def settled_runs(self, start, end):
        return {d: r for d, r in self.settled.items() if start <= d <= end}

    def speed_windows(self, zone, start, end):
        return [
            w for w in self.speed if w["grid_zone"] == zone and start <= w["window_start"] < end
        ]

    def batch_windows(self, zone, run_ids):
        return [w for r in run_ids for w in self.batch.get(r, []) if w["grid_zone"] == zone]

    def latest_complete_windows(self, window_minutes):
        return self.live

    def speed_daily(self, start, end):
        rows = [w for w in self.speed if start <= w["window_start"] < end]
        zones = sorted({w["grid_zone"] for w in rows})
        return [
            {
                "grid_zone": z,
                "consumption_kwh": sum(w["consumption_kwh"] for w in rows if w["grid_zone"] == z),
                "generation_kwh": sum(w["generation_kwh"] for w in rows if w["grid_zone"] == z),
                "peak_load_kw": max(w["grid_load_kw"] for w in rows if w["grid_zone"] == z),
                "readings": 10,
                "meters_reporting": 7,
                "windows": 1,
            }
            for z in zones
        ]

    def batch_daily(self, run_id):
        return [
            {
                "grid_zone": "ZONE-A",
                "consumption_kwh": 991.4,
                "generation_kwh": 242.2,
                "peak_load_kw": 63.3,
                "readings": 4700,
                "meters_reporting": 70,
                "forecast_irradiance": 0.58,
                "speed_vs_batch_pct": -1.12,
                "late_readings_recovered": 110,
            }
        ]

    def household_bills(self, household_id, start, end):
        rows = [
            bill(DAY1, 36, "286.11", trigger="restatement", restated=True),
            bill(DAY2, 37, "254.47"),
        ]
        return [
            r
            for r in rows
            if r["household_id"] == household_id and start <= r["business_date"] <= end
        ]

    def bill_history(self, household_id, day):
        return [
            bill(DAY1, 35, "254.47", current=False),
            bill(
                DAY1, 36, "286.11", trigger="restatement", reason="Regulator revision", current=True
            ),
        ]

    def day_bills(self, day, tier, limit, offset):
        return [bill(day, self.settled[day], "254.47")][offset : offset + limit]

    def day_bill_totals(self, day):
        return [
            {
                "tariff_tier": "DOMESTIC_STD",
                "households": 86,
                "net_import_kwh": Decimal("766.400"),
                "net_export_kwh": Decimal("12.000"),
                "total_payable": Decimal("24605.74"),
                "in_credit": 3,
            }
        ]

    def settlement_runs(self, day, limit):
        return []

    def current_report(self, day):
        if day not in (None, DAY1):
            return None
        return {
            "run_id": 36,
            "business_date": DAY1,
            "object_key": "reports/dt=2026-01-01/run=36/daily_report.html",
            "created_at": at(DAY2, 1),
        }

    def report_html(self, object_key):
        return b"<html>settled</html>"


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def client(store) -> TestClient:
    return TestClient(create_app(store=store, settings=get_settings(), fleet=FLEET))


# -- The merge rule, as served ------------------------------------------------------


def test_a_range_is_merged_settled_days_from_batch_today_from_speed(client):
    body = client.get("/api/v1/zones/ZONE-A/windows?from=2026-01-01&to=2026-01-03").json()
    assert [(d["date"], d["status"], d["layer"]) for d in body["days"]] == [
        ("2026-01-01", "SETTLED", "batch"),
        ("2026-01-02", "SETTLED", "batch"),
        ("2026-01-03", "PROVISIONAL", "speed"),
    ]
    served = [(w["window_start"][:16], w["layer"], w["consumption_kwh"]) for w in body["windows"]]
    assert served == [
        ("2026-01-01T00:00", "batch", 5.0),
        ("2026-01-01T00:15", "batch", 5.0),
        ("2026-01-02T00:00", "batch", 6.0),
        ("2026-01-03T11:00", "speed", 7.0),
        ("2026-01-03T11:15", "speed", 8.0),
    ]


def test_the_batch_view_wins_the_speed_figure_for_a_settled_day_is_never_served(client):
    body = client.get("/api/v1/zones/ZONE-A/windows?from=2026-01-01&to=2026-01-01").json()
    assert 4.9 not in [w["consumption_kwh"] for w in body["windows"]]
    assert {w["status"] for w in body["windows"]} == {"SETTLED"}


def test_an_unsettled_past_day_is_served_provisionally(store, client):
    del store.settled[DAY2]
    day = client.get("/api/v1/zones/daily?date=2026-01-02").json()["source"]
    assert (day["status"], day["layer"], day["reason"]) == (
        "PROVISIONAL",
        "speed",
        "ended, awaiting settlement",
    )


def test_daily_figures_for_a_settled_day_carry_the_reconciliation(client):
    body = client.get("/api/v1/zones/daily?date=2026-01-01").json()
    assert body["source"]["status"] == "SETTLED"
    assert body["source"]["settlement_run_id"] == 36
    zone = body["zones"][0]
    assert zone["speed_vs_batch_pct"] == -1.12
    assert zone["late_readings_recovered"] == 110
    assert zone["renewable_share"] == pytest.approx(242.2 / 991.4)


def test_todays_daily_figures_are_provisional_and_computed_from_the_speed_layer(client):
    body = client.get("/api/v1/zones/daily").json()
    assert body["source"] == {
        "date": "2026-01-03",
        "status": "PROVISIONAL",
        "layer": "speed",
        "reason": "today: a day is settled only after it ends",
        "settlement_run_id": None,
    }
    zone_a = next(z for z in body["zones"] if z["grid_zone"] == "ZONE-A")
    assert zone_a["consumption_kwh"] == 15.0
    assert zone_a["speed_vs_batch_pct"] is None  # there is nothing to reconcile yet


# -- Live view ------------------------------------------------------------------------


def test_the_live_view_is_provisional_with_its_age_stated(client):
    body = client.get("/api/v1/zones/live").json()
    assert body["status"] == "PROVISIONAL"
    assert {z["layer"] for z in body["zones"]} == {"speed"}
    zone_a = next(z for z in body["zones"] if z["grid_zone"] == "ZONE-A")
    # window ended 11:30, simulated now is ~12:00: 30 simulated minutes, 6.25 real seconds
    assert zone_a["data_age_simulated_minutes"] == pytest.approx(30, abs=1)
    assert zone_a["data_age_real_seconds"] == pytest.approx(6.25, abs=0.3)
    assert body["stale"] is False
    assert body["total_grid_load_kw"] == 32.0 + 8.0
    assert body["renewable_share"] == pytest.approx(2.5 / 10.0)


def test_the_live_view_is_flagged_stale_beyond_the_freshness_target():
    store = FakeStore(now=at(TODAY, 18))  # six simulated hours after the last window
    client = TestClient(create_app(store=store, settings=get_settings(), fleet=FLEET))
    assert client.get("/api/v1/zones/live").json()["stale"] is True


# -- Billing ------------------------------------------------------------------------


def test_a_household_sees_only_settled_bills_and_why_other_days_have_none(client):
    body = client.get(f"/api/v1/households/{HOUSEHOLD}/bills").json()
    assert [(b["business_date"], b["status"]) for b in body["bills"]] == [
        ("2026-01-01", "SETTLED"),
        ("2026-01-02", "SETTLED"),
    ]
    assert body["bills"][0]["restated"] is True
    assert body["unbilled"] == [
        {"date": "2026-01-03", "reason": "day in progress: bills are issued after settlement"}
    ]


def test_money_is_exact_decimal_strings_never_floats(client):
    body = client.get(f"/api/v1/households/{HOUSEHOLD}/bills").json()
    assert body["bills"][0]["total_payable"] == "286.11"
    assert body["total_payable"] == "540.58"


def test_there_is_no_provisional_bill_for_an_unsettled_day(store, client):
    del store.settled[DAY2]
    response = client.get("/api/v1/bills?date=2026-01-02")
    assert response.status_code == 404
    assert "no provisional bill" in response.json()["detail"]


def test_a_settled_days_bills_come_with_tier_totals(client):
    body = client.get("/api/v1/bills?date=2026-01-01").json()
    assert (body["status"], body["settlement_run_id"], body["total_billed"]) == (
        "SETTLED",
        36,
        "24605.74",
    )
    assert body["tiers"][0]["households"] == 86


def test_bill_history_shows_the_original_and_the_restatement(client):
    body = client.get(f"/api/v1/households/{HOUSEHOLD}/bills/2026-01-01/history").json()
    revisions = [
        (r["settlement_run_id"], r["total_payable"], r["current"], r["restated"])
        for r in body["revisions"]
    ]
    assert revisions == [(35, "254.47", False, False), (36, "286.11", True, True)]
    assert body["revisions"][1]["reason"] == "Regulator revision"


# -- Reports ------------------------------------------------------------------------


def test_the_latest_report_and_its_html(client):
    meta = client.get("/api/v1/reports/latest").json()
    assert meta["html_url"] == "/api/v1/reports/2026-01-01/html"
    html = client.get(meta["html_url"])
    assert html.status_code == 200
    assert html.headers["content-type"].startswith("text/html")
    assert client.get("/api/v1/reports/2026-01-02").status_code == 404


# -- What is refused ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "status"),
    [
        ("/api/v1/zones/ZONE-Z/windows", 404),
        ("/api/v1/households/HH-99999/bills", 404),
        ("/api/v1/zones/daily?date=2026-01-04", 422),  # the simulated future
        ("/api/v1/zones/daily?date=2025-12-31", 422),  # before the simulation
        ("/api/v1/zones/ZONE-A/windows?from=2026-01-03&to=2026-01-01", 422),
        ("/api/v1/zones/daily?date=not-a-date", 422),
    ],
)
def test_bad_requests_are_refused_with_a_reason(client, url, status):
    response = client.get(url)
    assert response.status_code == status
    assert response.json()["detail"]


def test_no_simulation_means_503_not_an_empty_answer(store, client):
    store._clock = None
    response = client.get("/api/v1/zones/live")
    assert response.status_code == 503
    assert "no simulation" in response.json()["detail"]


def test_a_store_outage_is_a_503(store, client):
    store.up = False
    assert client.get("/api/v1/zones/live").status_code == 503
    assert client.get("/health").json() == {"status": "down", "checks": {"postgres": "unreachable"}}


# -- Observability --------------------------------------------------------------------


def test_health_reports_speed_layer_freshness(store, client):
    assert client.get("/health").json()["status"] == "ok"
    store.progress = {"ingest": at(TODAY, 3)}  # nine simulated hours behind
    body = client.get("/health").json()
    assert body["status"] == "degraded"
    assert body["checks"]["speed_layer"]["fresh"] is False


def test_metrics_are_labelled_by_route_template_not_raw_path(client):
    client.get("/api/v1/zones/ZONE-A/windows")
    client.get("/api/v1/zones/ZONE-B/windows")
    metrics = client.get("/metrics").text
    assert 'route="/api/v1/zones/{zone}/windows"' in metrics
    assert "ZONE-A" not in metrics


def test_every_response_carries_a_request_id(client):
    assert client.get("/api/v1/zones").headers["X-Request-ID"]
    echoed = client.get("/api/v1/zones", headers={"X-Request-ID": "trace-123"})
    assert echoed.headers["X-Request-ID"] == "trace-123"
