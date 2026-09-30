"""
What the Airflow tasks actually do, as plain functions (ADR-0006).

The DAG files in airflow/dags/ only wire these together. Keeping the logic
here -- with no Airflow import -- means it is unit-tested without Airflow,
and the DAG files stay short enough to read at a glance.

Simulated days drive the schedule, not Airflow's calendar (ADR-0007). A
`sim_clock_tick` DAG runs every real minute, works out which simulated days
have ended and have not been settled, and triggers `daily_settlement` once
for each, passing the date as a parameter. Restating a day is triggering
that DAG again for the same date.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections import deque
from collections.abc import Iterable
from dataclasses import asdict
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import psycopg

from smartgrid.common import drops, storage
from smartgrid.common.clock_store import shared_clock
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.db import ensure_schema
from smartgrid.common.domain import build_fleet_from_settings
from smartgrid.common.drop_quality import check_latest
from smartgrid.common.logging import get_logger

log = get_logger(__name__)

SETTLEMENT_DAG = "daily_settlement"

# A day is settled once this much simulated time has passed after its
# midnight: enough for the speed layer's watermark (30 simulated minutes) to
# close the day's last windows and for the archive to catch up.
SETTLEMENT_GRACE = timedelta(minutes=45)

# How many days one tick may trigger. After a long outage the backlog is
# worked off a few days per minute rather than in one burst.
MAX_TRIGGERS_PER_TICK = 4


class QualityGateFailed(RuntimeError):
    """The day's drop is unusable. Settlement must not run (ADR-0006: fail closed)."""


class StaleSimulation(RuntimeError):
    """The run was triggered before a simulation reset. It must not touch the new one."""


class SettlementRefused(RuntimeError):
    """The settlement job refused to run. A retry would get the same answer."""


def check_simulation(expected: str | int | None, current: int) -> None:
    """
    A run carries the simulation it was triggered for. Airflow resumes
    interrupted runs after a restart -- including runs from BEFORE a
    simulation reset, which would otherwise settle the new simulation's
    half-finished day under the old run's name. Blank means "whichever
    simulation is current", for runs triggered by hand.
    """
    if expected in (None, "", "None"):
        return
    if int(expected) != current:
        raise StaleSimulation(
            f"this run belongs to simulation {int(expected)}, but the current simulation is "
            f"{current}: the simulation was reset after the run was triggered"
        )


# -- The clock tick -------------------------------------------------------------


def due_business_dates(
    sim_now: datetime,
    first_day: date,
    already: Iterable[date],
    *,
    grace: timedelta = SETTLEMENT_GRACE,
    limit: int = MAX_TRIGGERS_PER_TICK,
) -> list[date]:
    """Days that have ended (plus grace) and have not been triggered, oldest first."""
    done = set(already)
    due = []
    day = first_day
    while datetime.combine(day + timedelta(days=1), time(0, 0), tzinfo=UTC) + grace <= sim_now:
        if day not in done:
            due.append(day)
            if len(due) == limit:
                break
        day += timedelta(days=1)
    return due


def settlement_run_id(sim_id: int, day: date) -> str:
    """Deterministic, so triggering the same day twice is a no-op in Airflow."""
    return f"settle__{sim_id}__{day.isoformat()}"


def due_trigger_specs(settings: Settings | None = None) -> list[dict[str, Any]]:
    """Keyword arguments for one TriggerDagRunOperator per due day."""
    settings = settings or get_settings()
    ensure_schema(settings.postgres_dsn)
    clock = shared_clock(settings)
    sim_id = int(clock.real_start)
    with psycopg.connect(settings.postgres_dsn) as conn:
        already = [
            row[0]
            for row in conn.execute(
                "SELECT business_date FROM ops.settlement_triggers WHERE sim_id = %s", (sim_id,)
            ).fetchall()
        ]
    due = due_business_dates(clock.now(), clock.start.date(), already)
    if due:
        log.info(
            "settlements due",
            extra={
                "sim_id": sim_id,
                "dates": [d.isoformat() for d in due],
                "simulated_now": clock.now().isoformat(timespec="minutes"),
            },
        )
    return [
        {
            "trigger_run_id": settlement_run_id(sim_id, day),
            "conf": {
                "business_date": day.isoformat(),
                "trigger": "scheduled",
                "reason": "",
                "sim_id": sim_id,
            },
        }
        for day in due
    ]


def record_triggers(specs: list[dict[str, Any]], settings: Settings | None = None) -> int:
    """Remember what was triggered, AFTER the trigger succeeded."""
    settings = settings or get_settings()
    sim_id = int(shared_clock(settings).real_start)
    with psycopg.connect(settings.postgres_dsn) as conn:
        for spec in specs:
            conn.execute(
                "INSERT INTO ops.settlement_triggers (sim_id, business_date, airflow_run_id) "
                "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (sim_id, date.fromisoformat(spec["conf"]["business_date"]), spec["trigger_run_id"]),
            )
    return len(specs)


# -- The settlement DAG's tasks --------------------------------------------------


def drop_is_complete(business_date: str, settings: Settings | None = None) -> bool:
    """Sensor: the day's drop has a manifest (ADR-0008)."""
    settings = settings or get_settings()
    day = date.fromisoformat(business_date)
    version = drops.latest_complete_version(
        storage.s3_client(settings), settings.minio_bucket_raw, day
    )
    log.info(
        "waiting for drop" if version is None else "drop present",
        extra={"business_date": business_date, "version": version},
    )
    return version is not None


def archive_has_passed(business_date: str, settings: Settings | None = None) -> bool:
    """
    Sensor: the speed layer has archived readings past the end of the day,
    by at least the grace period -- so the archive holds the whole day as
    far as the stream has delivered it.
    """
    settings = settings or get_settings()
    day = date.fromisoformat(business_date)
    needed = datetime.combine(day + timedelta(days=1), time(0, 0), tzinfo=UTC) + SETTLEMENT_GRACE
    with psycopg.connect(settings.postgres_dsn) as conn:
        row = conn.execute(
            "SELECT max_event_time FROM ops.stream_progress WHERE query_name = 'ingest'"
        ).fetchone()
    reached = row[0] if row else None
    ok = reached is not None and reached >= needed
    log.info(
        "archive progress",
        extra={
            "business_date": business_date,
            "needed": needed.isoformat(),
            "reached": reached.isoformat() if reached else None,
            "ready": ok,
        },
    )
    return ok


def run_quality_gate(
    business_date: str,
    airflow_run_id: str | None = None,
    settings: Settings | None = None,
    *,
    sim_id: str | int | None = None,
) -> int:
    """Gate the latest complete drop; record the verdict; return the version to settle."""
    settings = settings or get_settings()
    current = int(shared_clock(settings).real_start)
    check_simulation(sim_id, current)
    day = date.fromisoformat(business_date)
    report = check_latest(
        storage.s3_client(settings),
        settings.minio_bucket_raw,
        day,
        build_fleet_from_settings(settings),
    )
    with psycopg.connect(settings.postgres_dsn) as conn:
        conn.execute(
            "INSERT INTO ops.quality_gate_results (sim_id, business_date, drop_version, passed, "
            "findings, airflow_run_id) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                current,
                day,
                report.version,
                report.passed,
                json.dumps(report.to_dict()["findings"]),
                airflow_run_id,
            ),
        )
    if not report.passed:
        detail = "; ".join(f"{f.check.value} x{f.count}: {f.detail}" for f in report.findings)
        log.error(
            "quality gate FAILED",
            extra={"business_date": business_date, "version": report.version, "findings": detail},
        )
        raise QualityGateFailed(f"drop {business_date} v{report.version}: {detail}")
    log.info(
        "quality gate passed",
        extra={
            "business_date": business_date,
            "version": report.version,
            "stats": dict(report.stats),
        },
    )
    return report.version


def run_settlement(
    business_date: str,
    drop_version: int,
    trigger: str,
    reason: str | None,
    airflow_run_id: str | None,
    *,
    sim_id: str | int | None = None,
) -> int:
    """
    Run the Spark settlement as a CHILD PROCESS, not inside the Airflow task.
    The JVM then starts clean and is gone when the job ends, rather than
    lingering in a long-lived worker. Arguments are passed as a list -- never
    through a shell -- so a reason containing quotes cannot become a command.

    The child's output is copied into the task log line by line AS IT RUNS,
    so a slow or stuck settlement can be watched rather than guessed at.
    """
    from smartgrid.batch.settlement import REFUSAL_EXIT_CODES, RUN_ID_MARKER

    command = [
        sys.executable,
        "-m",
        "smartgrid.batch.settlement",
        "--date",
        business_date,
        "--drop-version",
        str(drop_version),
        "--trigger",
        trigger or "manual",
        "--airflow-run-id",
        airflow_run_id or "",
    ]
    if reason:
        command += ["--reason", reason]
    if sim_id not in (None, "", "None"):
        command += ["--sim-id", str(sim_id)]
    return _run_and_find_run_id(command, RUN_ID_MARKER, refused=REFUSAL_EXIT_CODES)


def _run_and_find_run_id(
    command: list[str], marker: str, *, refused: frozenset[int] = frozenset()
) -> int:
    """Run `command`, echoing its output; return the run id it prints after `marker`."""
    tail: deque[str] = deque(maxlen=25)
    run_id: int | None = None
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            line = line.rstrip("\n")
            print(line, flush=True)  # into the Airflow task log
            tail.append(line)
            if line.startswith(marker):
                run_id = int(line[len(marker) :])
    if process.returncode in refused:
        raise SettlementRefused(
            f"settlement refused to run (exit {process.returncode}):\n" + "\n".join(tail)
        )
    if process.returncode != 0:
        raise RuntimeError(f"settlement exited with {process.returncode}:\n" + "\n".join(tail))
    if run_id is None:
        raise RuntimeError("settlement succeeded but reported no run id")
    return run_id


def reconcile(run_id: int, settings: Settings | None = None) -> list[dict[str, Any]]:
    from smartgrid.batch.reconcile import reconcile_run

    settings = settings or get_settings()
    rows = [asdict(r) for r in reconcile_run(settings.postgres_dsn, int(run_id))]
    log.info("reconciled", extra={"run_id": int(run_id), "zones": rows})
    return rows


def publish_report(run_id: int, settings: Settings | None = None) -> str:
    from smartgrid.batch.report import publish

    settings = settings or get_settings()
    key = publish(settings, int(run_id))
    log.info("daily report published", extra={"run_id": int(run_id), "object_key": key})
    return key
