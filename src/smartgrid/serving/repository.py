"""
Everything the serving API reads, as named queries.

`ServingStore` is the interface the API depends on; `PostgresStore` is the
real one, reading the three serving schemas (ADR-0005) and MinIO for report
files. The API's tests substitute an in-memory store, so they exercise the
merge rule and the HTTP contract without a database.

Reads only: the API never writes, never creates the simulated clock, and
never touches the master dataset.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Protocol

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from smartgrid.common import storage
from smartgrid.common.clock import SimulatedClock
from smartgrid.common.clock_store import read_clock
from smartgrid.common.config import Settings

Row = dict[str, Any]

_WINDOW_COLUMNS = (
    "grid_zone, window_start, window_end, consumption_kwh, generation_kwh, net_kwh, "
    "grid_load_kw, solar_kw, renewable_share, readings, meters_reporting"
)

_BILL_COLUMNS = (
    "b.run_id, b.business_date, b.household_id, b.tariff_tier, b.readings, "
    "b.gross_consumption_kwh, b.solar_generation_kwh, b.net_import_kwh, b.net_export_kwh, "
    "b.energy_charge, b.fixed_charge, b.export_credit, b.subsidy_amount, b.total_payable, "
    "b.subsidy_applied, b.currency, r.trigger, r.drop_version, r.reason, r.finished_at"
)


class ServingStore(Protocol):
    def ping(self) -> bool: ...
    def clock(self) -> SimulatedClock | None: ...
    def stream_progress(self) -> dict[str, datetime | None]: ...
    def settled_runs(self, start: date, end: date) -> dict[date, int]: ...
    def speed_windows(self, zone: str, start: datetime, end: datetime) -> list[Row]: ...
    def batch_windows(self, zone: str, run_ids: list[int]) -> list[Row]: ...
    def latest_complete_windows(self, window_minutes: int) -> list[Row]: ...
    def speed_daily(self, start: datetime, end: datetime) -> list[Row]: ...
    def batch_daily(self, run_id: int) -> list[Row]: ...
    def household_bills(self, household_id: str, start: date, end: date) -> list[Row]: ...
    def bill_history(self, household_id: str, day: date) -> list[Row]: ...
    def day_bills(self, day: date, tier: str | None, limit: int, offset: int) -> list[Row]: ...
    def day_bill_totals(self, day: date) -> list[Row]: ...
    def settlement_runs(self, day: date | None, limit: int) -> list[Row]: ...
    def current_report(self, day: date | None) -> Row | None: ...
    def report_html(self, object_key: str) -> bytes | None: ...
    def latest_gate_verdicts(self, sim_id: int) -> dict[date, bool]: ...


class PostgresStore:
    """Reads the serving store through a small connection pool."""

    def __init__(self, settings: Settings, *, max_connections: int = 5) -> None:
        self.settings = settings
        self._s3: Any = None
        # open=False: the API starts even if PostgreSQL is not up yet; requests
        # then fail with 503 and /health says why, instead of a crash loop.
        self.pool = ConnectionPool(
            settings.postgres_dsn,
            min_size=1,
            max_size=max_connections,
            kwargs={"row_factory": dict_row, "autocommit": True, "connect_timeout": 5},
            timeout=5,
            open=False,
        )

    def open(self) -> None:
        self.pool.open(wait=False)

    def close(self) -> None:
        self.pool.close()

    def _all(self, sql: str, params: tuple = ()) -> list[Row]:
        with self.pool.connection() as conn:
            return conn.execute(sql, params).fetchall()

    def _one(self, sql: str, params: tuple = ()) -> Row | None:
        with self.pool.connection() as conn:
            return conn.execute(sql, params).fetchone()

    # -- health and time ------------------------------------------------------

    def ping(self) -> bool:
        try:
            return self._one("SELECT 1 AS ok") is not None
        except Exception:  # connection refused, pool timeout, auth failure: all mean "down"
            return False

    def clock(self) -> SimulatedClock | None:
        with self.pool.connection() as conn:
            return read_clock(conn)

    def stream_progress(self) -> dict[str, datetime | None]:
        rows = self._all("SELECT query_name, max_event_time FROM ops.stream_progress")
        return {r["query_name"]: r["max_event_time"] for r in rows}

    # -- zones ----------------------------------------------------------------

    def settled_runs(self, start: date, end: date) -> dict[date, int]:
        rows = self._all(
            "SELECT business_date, run_id FROM batch.current_runs "
            "WHERE business_date BETWEEN %s AND %s",
            (start, end),
        )
        return {r["business_date"]: r["run_id"] for r in rows}

    def speed_windows(self, zone: str, start: datetime, end: datetime) -> list[Row]:
        return self._all(
            f"SELECT {_WINDOW_COLUMNS} FROM speed.zone_metrics "
            "WHERE grid_zone = %s AND window_start >= %s AND window_start < %s "
            "ORDER BY window_start",
            (zone, start, end),
        )

    def batch_windows(self, zone: str, run_ids: list[int]) -> list[Row]:
        if not run_ids:
            return []
        return self._all(
            f"SELECT {_WINDOW_COLUMNS} FROM batch.zone_metrics "
            "WHERE grid_zone = %s AND run_id = ANY(%s) ORDER BY window_start",
            (zone, run_ids),
        )

    def latest_complete_windows(self, window_minutes: int) -> list[Row]:
        # The zone_metrics query records the end of the newest window it has
        # touched -- a window still filling. One window-length before that,
        # every earlier window has seen all its on-time readings.
        return self._all(
            f"SELECT DISTINCT ON (grid_zone) {_WINDOW_COLUMNS} FROM speed.zone_metrics "
            "WHERE window_end <= (SELECT max_event_time FROM ops.stream_progress "
            "                     WHERE query_name = 'zone_metrics') - make_interval(mins => %s) "
            "ORDER BY grid_zone, window_start DESC",
            (window_minutes,),
        )

    def speed_daily(self, start: datetime, end: datetime) -> list[Row]:
        return self._all(
            "SELECT grid_zone, sum(consumption_kwh) AS consumption_kwh, "
            "sum(generation_kwh) AS generation_kwh, max(grid_load_kw) AS peak_load_kw, "
            "sum(readings)::int AS readings, max(meters_reporting) AS meters_reporting, "
            "count(*)::int AS windows "
            "FROM speed.zone_metrics WHERE window_start >= %s AND window_start < %s "
            "GROUP BY grid_zone ORDER BY grid_zone",
            (start, end),
        )

    def batch_daily(self, run_id: int) -> list[Row]:
        return self._all(
            "SELECT z.grid_zone, z.consumption_kwh, z.generation_kwh, z.peak_load_kw, "
            "z.readings, z.meters_reporting, z.forecast_irradiance, "
            "r.delta_pct AS speed_vs_batch_pct, r.missed_by_speed AS late_readings_recovered "
            "FROM batch.zone_daily z LEFT JOIN ops.reconciliation r "
            "  ON r.run_id = z.run_id AND r.grid_zone = z.grid_zone "
            "WHERE z.run_id = %s ORDER BY z.grid_zone",
            (run_id,),
        )

    # -- bills ------------------------------------------------------------------

    def household_bills(self, household_id: str, start: date, end: date) -> list[Row]:
        return self._all(
            f"SELECT {_BILL_COLUMNS}, "
            "EXISTS (SELECT 1 FROM ops.settlement_runs e WHERE e.business_date = b.business_date "
            "        AND e.status = 'succeeded' AND e.run_id < b.run_id) AS restated "
            "FROM batch.current_bills b JOIN ops.settlement_runs r USING (run_id) "
            "WHERE b.household_id = %s AND b.business_date BETWEEN %s AND %s "
            "ORDER BY b.business_date",
            (household_id, start, end),
        )

    def bill_history(self, household_id: str, day: date) -> list[Row]:
        # Succeeded runs only: a run that failed or was voided never issued a bill.
        return self._all(
            f"SELECT {_BILL_COLUMNS}, COALESCE(c.run_id = b.run_id, false) AS current "
            "FROM batch.bills b JOIN ops.settlement_runs r USING (run_id) "
            "LEFT JOIN batch.current_runs c ON c.business_date = b.business_date "
            "WHERE b.household_id = %s AND b.business_date = %s AND r.status = 'succeeded' "
            "ORDER BY b.run_id",
            (household_id, day),
        )

    def day_bills(self, day: date, tier: str | None, limit: int, offset: int) -> list[Row]:
        return self._all(
            f"SELECT {_BILL_COLUMNS}, false AS restated "
            "FROM batch.current_bills b JOIN ops.settlement_runs r USING (run_id) "
            "WHERE b.business_date = %s AND (%s::text IS NULL OR b.tariff_tier = %s) "
            "ORDER BY b.household_id LIMIT %s OFFSET %s",
            (day, tier, tier, limit, offset),
        )

    def day_bill_totals(self, day: date) -> list[Row]:
        return self._all(
            "SELECT tariff_tier, count(*)::int AS households, "
            "sum(net_import_kwh) AS net_import_kwh, sum(net_export_kwh) AS net_export_kwh, "
            "sum(total_payable) AS total_payable, "
            "count(*) FILTER (WHERE total_payable < 0)::int AS in_credit "
            "FROM batch.current_bills WHERE business_date = %s "
            "GROUP BY tariff_tier ORDER BY tariff_tier",
            (day,),
        )

    # -- settlements and reports ---------------------------------------------------

    def settlement_runs(self, day: date | None, limit: int) -> list[Row]:
        return self._all(
            "SELECT r.run_id, r.business_date, r.status, r.trigger, r.reason, r.drop_version, "
            "r.airflow_run_id, r.readings_archived, r.readings_settled, r.duplicates_removed, "
            "r.readings_rejected, r.households_billed, r.total_billed, r.started_at, "
            "r.finished_at, r.error, COALESCE(c.run_id = r.run_id, false) AS current, "
            "(SELECT sum(missed_by_speed)::int FROM ops.reconciliation x "
            " WHERE x.run_id = r.run_id) AS late_readings_recovered, "
            "(SELECT avg(abs(delta_pct)) FROM ops.reconciliation x "
            " WHERE x.run_id = r.run_id) AS mean_abs_speed_gap_pct, "
            "d.object_key AS report_key "
            "FROM ops.settlement_runs r "
            "LEFT JOIN batch.current_runs c USING (business_date) "
            "LEFT JOIN ops.daily_reports d ON d.run_id = r.run_id "
            "WHERE (%s::date IS NULL OR r.business_date = %s) "
            "ORDER BY r.business_date DESC, r.run_id DESC LIMIT %s",
            (day, day, limit),
        )

    def current_report(self, day: date | None) -> Row | None:
        return self._one(
            "SELECT d.run_id, d.business_date, d.object_key, d.created_at "
            "FROM ops.daily_reports d JOIN batch.current_runs c ON c.run_id = d.run_id "
            "WHERE (%s::date IS NULL OR d.business_date = %s) "
            "ORDER BY d.business_date DESC LIMIT 1",
            (day, day),
        )

    def latest_gate_verdicts(self, sim_id: int) -> dict[date, bool]:
        """Each day's most recent quality-gate verdict in this simulation: passed or not."""
        rows = self._all(
            "SELECT DISTINCT ON (business_date) business_date, passed "
            "FROM ops.quality_gate_results WHERE sim_id = %s "
            "ORDER BY business_date, id DESC",
            (sim_id,),
        )
        return {r["business_date"]: r["passed"] for r in rows}

    def report_html(self, object_key: str) -> bytes | None:
        if self._s3 is None:
            self._s3 = storage.s3_client(self.settings)
        return storage.get_bytes(self._s3, self.settings.minio_bucket_lake, object_key)
