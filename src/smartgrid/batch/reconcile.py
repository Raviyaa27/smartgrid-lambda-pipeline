"""
Speed vs batch: the speed layer's approximation error, MEASURED (ADR-0001).

For each zone on a settled day, compares what the real-time view reported
(speed.zone_metrics) with what settlement found (batch.zone_daily):

    delta_pct        how far the provisional figure was from the settled one
    missed_by_speed  readings only the batch layer saw -- late arrivals the
                     speed layer's watermark had to drop

This turns "the speed layer is approximate" from an assertion into a number
per zone per day, stored in ops.reconciliation for the dashboard, the daily
report and the drift alert.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta

import psycopg


@dataclass(frozen=True)
class ZoneTotals:
    consumption_kwh: float
    readings: int


@dataclass(frozen=True)
class Reconciliation:
    grid_zone: str
    speed_kwh: float | None
    batch_kwh: float
    delta_kwh: float | None
    delta_pct: float | None
    speed_readings: int | None
    batch_readings: int
    missed_by_speed: int | None


def compare(
    batch: Mapping[str, ZoneTotals], speed: Mapping[str, ZoneTotals]
) -> list[Reconciliation]:
    """
    One row per zone the batch layer settled. A zone the speed layer never
    reported gets None for its speed figures -- absent, not zero, because
    "the real-time view showed nothing" and "it showed 0 kWh" are different.
    """
    rows = []
    for zone in sorted(batch):
        settled = batch[zone]
        live = speed.get(zone)
        delta = None if live is None else live.consumption_kwh - settled.consumption_kwh
        rows.append(
            Reconciliation(
                grid_zone=zone,
                speed_kwh=None if live is None else live.consumption_kwh,
                batch_kwh=settled.consumption_kwh,
                delta_kwh=delta,
                delta_pct=(
                    None
                    if delta is None or settled.consumption_kwh == 0
                    else 100.0 * delta / settled.consumption_kwh
                ),
                speed_readings=None if live is None else live.readings,
                batch_readings=settled.readings,
                missed_by_speed=None if live is None else settled.readings - live.readings,
            )
        )
    return rows


def reconcile_run(dsn: str, run_id: int) -> list[Reconciliation]:
    """Reconcile one settlement run and store the result."""
    with psycopg.connect(dsn) as conn:
        (day,) = conn.execute(
            "SELECT business_date FROM ops.settlement_runs WHERE run_id = %s", (run_id,)
        ).fetchone()
        start = datetime.combine(day, time(0, 0), tzinfo=UTC)
        batch = {
            zone: ZoneTotals(kwh, readings)
            for zone, kwh, readings in conn.execute(
                "SELECT grid_zone, consumption_kwh, readings FROM batch.zone_daily "
                "WHERE run_id = %s",
                (run_id,),
            ).fetchall()
        }
        speed = {
            zone: ZoneTotals(kwh, int(readings))
            for zone, kwh, readings in conn.execute(
                "SELECT grid_zone, sum(consumption_kwh), sum(readings) FROM speed.zone_metrics "
                "WHERE window_start >= %s AND window_start < %s GROUP BY grid_zone",
                (start, start + timedelta(days=1)),
            ).fetchall()
        }
        rows = compare(batch, speed)
        conn.execute("DELETE FROM ops.reconciliation WHERE run_id = %s", (run_id,))
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO ops.reconciliation (run_id, business_date, grid_zone, speed_kwh, "
                "batch_kwh, delta_kwh, delta_pct, speed_readings, batch_readings, missed_by_speed) "
                "VALUES (%(run_id)s, %(business_date)s, %(grid_zone)s, %(speed_kwh)s, "
                "%(batch_kwh)s, "
                "%(delta_kwh)s, %(delta_pct)s, %(speed_readings)s, %(batch_readings)s, "
                "%(missed_by_speed)s)",
                [{**asdict(r), "run_id": run_id, "business_date": day} for r in rows],
            )
    return rows


def business_date_of(dsn: str, run_id: int) -> date:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT business_date FROM ops.settlement_runs WHERE run_id = %s", (run_id,)
        ).fetchone()[0]
