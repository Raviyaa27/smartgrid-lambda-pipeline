"""
Every query the serving API runs, against the real PostgreSQL schema.

The API's unit tests use an in-memory store, so they cannot catch a wrong
column name or a type error in SQL. These run each query for real. The
data depends on what the pipeline has produced, so they assert shapes and
invariants, not values. Read-only: nothing here writes.
"""

from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest

from smartgrid.common.config import get_settings
from smartgrid.common.db import ensure_schema

pytestmark = pytest.mark.integration

DAY = date(2026, 1, 1)
START = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def store():
    settings = get_settings()
    try:
        psycopg.connect(settings.postgres_dsn, connect_timeout=3).close()
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL is not reachable; run `docker compose up -d` first")
    ensure_schema(settings.postgres_dsn)  # idempotent: tables exist on a fresh stack too

    from smartgrid.serving.repository import PostgresStore

    store = PostgresStore(settings)
    store.pool.open(wait=True, timeout=10)
    yield store
    store.close()


def test_the_store_answers(store):
    assert store.ping() is True


def test_zone_queries_run_and_return_window_rows(store):
    window_keys = {"grid_zone", "window_start", "window_end", "grid_load_kw", "renewable_share"}
    for rows in (
        store.speed_windows("ZONE-A", START, START + timedelta(days=1)),
        store.latest_complete_windows(15),
        store.batch_windows("ZONE-A", list(store.settled_runs(DAY, DAY).values())),
    ):
        assert all(window_keys <= row.keys() for row in rows)
    assert all(
        row["consumption_kwh"] >= 0 for row in store.speed_daily(START, START + timedelta(days=1))
    )


def test_settled_runs_map_days_to_runs(store):
    runs = store.settled_runs(DAY, DAY + timedelta(days=30))
    assert all(isinstance(d, date) and isinstance(r, int) for d, r in runs.items())
    for run_id in runs.values():
        assert all("speed_vs_batch_pct" in row for row in store.batch_daily(run_id))


def test_bill_queries_run(store):
    for row in store.household_bills("HH-00001", DAY, DAY + timedelta(days=30)):
        assert row["household_id"] == "HH-00001"
    for row in store.bill_history("HH-00001", DAY):
        assert isinstance(row["current"], bool)
    assert isinstance(store.day_bills(DAY, None, 5, 0), list)
    assert isinstance(store.day_bills(DAY, "DOMESTIC_STD", 5, 0), list)
    assert isinstance(store.day_bill_totals(DAY), list)


def test_settlement_and_report_queries_run(store):
    for run in store.settlement_runs(None, 5):
        assert run["status"] in {"running", "succeeded", "failed"}
    assert isinstance(store.settlement_runs(DAY, 5), list)
    report = store.current_report(None)
    assert report is None or report["object_key"].startswith("reports/")


def test_progress_and_clock_are_readable(store):
    assert isinstance(store.stream_progress(), dict)
    clock = store.clock()
    assert clock is None or clock.day_seconds > 0
