"""
Batch layer: settle one business date from the immutable archive (ADR-0001).

    python -m smartgrid.batch.settlement --date 2026-01-01

Runs in the Airflow container on the same Spark engine as the speed layer.
For one simulated day it:

  1. refuses to run unless the day's drop passes the quality gate -- checked
     again here, not only by Airflow, so no caller can bill from a bad drop;
  2. reads the day's partition of the Parquet master dataset;
  3. RE-VALIDATES it with the shared validator under today's rules -- which
     is what makes restatement meaningful: fix a rule, re-run a day, and the
     fix applies to history;
  4. deduplicates by event_id over the WHOLE day, with no watermark, so the
     late readings the speed layer had to drop are included;
  5. computes zone metrics with the same function the speed layer uses, and
     household totals, then bills every household in the drop in Decimal;
  6. writes a snapshot of exactly the readings it settled, so any bill can be
     traced to its inputs;
  7. commits bills and zone figures in ONE transaction, as a new run. Nothing
     from an earlier run is overwritten; `batch.current_*` views pick the
     latest successful run, so a restated day sits beside its original.

On success it prints `SETTLEMENT_RUN_ID=<id>`, which Airflow passes to the
next task.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from smartgrid.batch.bills import HouseholdDay, parse_drop, settle_households, unbilled
from smartgrid.batch.orchestration import StaleSimulation, check_simulation
from smartgrid.common import drops, storage
from smartgrid.common.clock import SimulatedClock
from smartgrid.common.clock_store import shared_clock
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.db import ensure_schema
from smartgrid.common.domain import build_fleet_from_settings
from smartgrid.common.drop_quality import check_drop
from smartgrid.common.logging import PipelineStage, configure_logging, get_logger
from smartgrid.common.transformations import validate_frame
from smartgrid.streaming.spark_session import build_spark
from smartgrid.streaming.transforms import zone_window_metrics

SERVICE = "settlement"
log = get_logger("smartgrid.batch.settlement")

# Marks the line carrying the run id, so the caller finds it among Spark's
# own output without depending on line order.
RUN_ID_MARKER = "SETTLEMENT_RUN_ID="

# Exit codes for a REFUSAL -- the job decided not to run. Retrying cannot
# change the answer, so the caller fails the task instead of retrying it.
EXIT_DROP_REJECTED = 2
EXIT_STALE_SIMULATION = 3
REFUSAL_EXIT_CODES = frozenset({EXIT_DROP_REJECTED, EXIT_STALE_SIMULATION})


class DropRejected(RuntimeError):
    """The day's drop failed the quality gate: settlement must not run."""


@dataclass(frozen=True)
class SettlementResult:
    run_id: int
    business_date: date
    readings_archived: int
    readings_settled: int
    duplicates_removed: int
    readings_rejected: int
    households_billed: int
    total_billed: Decimal
    snapshot_path: str


# -- Reading the day -----------------------------------------------------------


def read_archive_day(spark: SparkSession, settings: Settings, day: date) -> DataFrame | None:
    """The day's partition of the master dataset, or None if nothing was archived."""
    prefix = f"readings/dt={day.isoformat()}/"
    client = storage.s3_client(settings)
    if not storage.list_keys(client, settings.minio_bucket_lake, prefix):
        return None
    base = f"s3a://{settings.minio_bucket_lake}/readings"
    return spark.read.option("basePath", base).parquet(f"{base}/dt={day.isoformat()}")


def revalidate(
    archived: DataFrame, *, known_household_ids: frozenset[str], clock: SimulatedClock
) -> DataFrame:
    """
    Apply TODAY'S rules to archived readings with the shared validator
    (ADR-0004). Adds `is_valid` and `quarantine_reason`.
    """
    schema = StructType(
        list(archived.schema.fields)
        + [
            StructField("quarantine_reason", StringType(), True),
            StructField("is_valid", BooleanType(), False),
        ]
    )

    def check(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for batch in batches:
            yield validate_frame(batch, known_household_ids=known_household_ids, now=clock.now())

    return archived.mapInPandas(check, schema=schema)


# -- Bookkeeping -----------------------------------------------------------------


def _open_run(
    dsn: str,
    sim_id: int,
    day: date,
    drop_version: int,
    trigger: str,
    reason: str | None,
    airflow_run_id: str | None,
) -> int:
    with psycopg.connect(dsn) as conn:
        (run_id,) = conn.execute(
            "INSERT INTO ops.settlement_runs (sim_id, business_date, status, drop_version, "
            "trigger, reason, airflow_run_id) VALUES (%s, %s, 'running', %s, %s, %s, %s) "
            "RETURNING run_id",
            (sim_id, day, drop_version, trigger, reason, airflow_run_id),
        ).fetchone()
        # A run still 'running' for this day died without closing itself (its
        # process or container was killed). Airflow runs one settlement at a
        # time, so it cannot still be working: record it as failed.
        conn.execute(
            "UPDATE ops.settlement_runs SET status = 'failed', finished_at = now(), "
            "error = 'abandoned: did not finish; superseded by run ' || %s "
            "WHERE sim_id = %s AND business_date = %s AND status = 'running' AND run_id < %s",
            (str(run_id), sim_id, day, run_id),
        )
    return run_id


def _fail_run(dsn: str, run_id: int, error: str) -> None:
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "UPDATE ops.settlement_runs SET status = 'failed', error = %s, finished_at = now() "
            "WHERE run_id = %s",
            (error[:2000], run_id),
        )


def _utc(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


# -- The run -----------------------------------------------------------------------


def settle(
    settings: Settings,
    day: date,
    *,
    drop_version: int | None = None,
    trigger: str = "manual",
    reason: str | None = None,
    airflow_run_id: str | None = None,
    expected_sim_id: int | None = None,
) -> SettlementResult:
    clock = shared_clock(settings)
    sim_id = int(clock.real_start)
    # 0. The run must belong to THIS simulation (see check_simulation).
    check_simulation(expected_sim_id, sim_id)
    fleet = build_fleet_from_settings(settings)
    ensure_schema(settings.postgres_dsn)
    client = storage.s3_client(settings)

    # 1. The gate, again. Fail closed.
    version = drop_version or drops.latest_complete_version(client, settings.minio_bucket_raw, day)
    if version is None:
        raise DropRejected(f"no complete drop for {day}")
    loaded = drops.load_drop(client, settings.minio_bucket_raw, day, version)
    report = check_drop(loaded, fleet)
    if not report.passed:
        raise DropRejected(
            f"drop {day} v{version} failed the quality gate: "
            + "; ".join(f"{f.check.value}: {f.detail}" for f in report.findings)
        )
    contents = parse_drop(loaded)

    run_id = _open_run(settings.postgres_dsn, sim_id, day, version, trigger, reason, airflow_run_id)
    log.info(
        "settlement started",
        extra={
            "run_id": run_id,
            "business_date": day.isoformat(),
            "drop_version": version,
            "trigger": trigger,
        },
    )
    spark = build_spark("smartgrid-settlement", settings)
    spark.sparkContext.setLogLevel("WARN")
    try:
        with PipelineStage(log, "settle", run_id=run_id, business_date=day.isoformat()) as stage:
            result = _settle(spark, settings, day, run_id, version, contents, clock, fleet)
            stage.records_in = result.readings_archived
            stage.records_out = result.readings_settled
            stage.records_quarantined = result.readings_rejected
            stage.context.update(
                duplicates_removed=result.duplicates_removed,
                households_billed=result.households_billed,
                total_billed=str(result.total_billed),
            )
        return result
    except Exception as exc:
        _fail_run(settings.postgres_dsn, run_id, f"{type(exc).__name__}: {exc}")
        raise
    finally:
        spark.stop()


def _settle(
    spark: SparkSession,
    settings: Settings,
    day: date,
    run_id: int,
    drop_version: int,
    contents: Any,
    clock: SimulatedClock,
    fleet: Any,
) -> SettlementResult:
    interval_hours = settings.sim_emit_interval_seconds * clock.compression / 3600.0
    # Seconds spent in each step, logged at the end: where a slow run went.
    timings: dict[str, float] = {}
    mark = time.monotonic()

    def lap(step: str) -> None:
        nonlocal mark
        now = time.monotonic()
        timings[step] = round(now - mark, 2)
        mark = now

    archived = read_archive_day(spark, settings, day)

    usage: dict[str, HouseholdDay] = {}
    windows: list[dict[str, Any]] = []
    zones: list[dict[str, Any]] = []
    archived_n = settled_n = rejected_n = 0
    snapshot = ""

    if archived is not None:
        archive_files = len(archived.inputFiles())
        lap("list_archive")
        checked = revalidate(archived, known_household_ids=fleet.household_ids, clock=clock).cache()
        archived_n = checked.count()
        rejected_n = checked.filter(~F.col("is_valid")).count()
        lap("read_and_revalidate")
        # The whole day, no watermark: late readings are kept, retransmissions removed.
        settled = checked.filter(F.col("is_valid")).dropDuplicates(["event_id"]).cache()
        settled_n = settled.count()
        lap("deduplicate")

        for row in (
            settled.groupBy("household_id")
            .agg(
                F.count(F.lit(1)).alias("readings"),
                F.sum("power_consumption_kwh").alias("consumption"),
                F.sum("solar_generation_kwh").alias("generation"),
            )
            .collect()
        ):
            usage[row["household_id"]] = HouseholdDay(
                row["household_id"], row["readings"], row["consumption"], row["generation"]
            )

        # The SAME window arithmetic as the speed layer, so reconciliation
        # compares like with like.
        windows = [
            row.asDict()
            for row in zone_window_metrics(
                settled,
                window=f"{settings.speed_window_minutes} minutes",
                reading_interval_hours=interval_hours,
            ).collect()
        ]
        zones = [
            row.asDict()
            for row in settled.groupBy("grid_zone")
            .agg(
                F.sum("power_consumption_kwh").alias("consumption_kwh"),
                F.sum("solar_generation_kwh").alias("generation_kwh"),
                F.count(F.lit(1)).alias("readings"),
                F.countDistinct("meter_id").alias("meters_reporting"),
            )
            .collect()
        ]
        lap("aggregate")

        snapshot = f"s3a://{settings.minio_bucket_lake}/settled/dt={day.isoformat()}/run={run_id}"
        settled.coalesce(1).write.mode("overwrite").parquet(snapshot)
        checked.unpersist()
        settled.unpersist()
        lap("write_snapshot")
        log.info(
            "archive read",
            extra={"run_id": run_id, "archive_files": archive_files, "readings": archived_n},
        )

    missing = unbilled(usage, contents)
    if missing:
        log.warning(
            "readings for households absent from the drop were not billed",
            extra={"run_id": run_id, "households": missing[:10], "count": len(missing)},
        )

    bills = settle_households(day, usage, contents)
    total = sum((s.bill.total_payable for s in bills), Decimal("0.00"))
    peak = {}
    for w in windows:
        peak[w["grid_zone"]] = max(peak.get(w["grid_zone"], 0.0), w["grid_load_kw"])

    # One transaction: the run's bills and zone figures commit together or not at all.
    with psycopg.connect(settings.postgres_dsn) as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO batch.bills (run_id, business_date, household_id, tariff_tier, "
            "readings, gross_consumption_kwh, solar_generation_kwh, net_import_kwh, "
            "net_export_kwh, energy_charge, fixed_charge, export_credit, subsidy_amount, "
            "total_payable, subsidy_applied, currency) VALUES "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    day,
                    b.household_id,
                    b.tariff_tier,
                    s.readings,
                    b.gross_consumption_kwh,
                    b.solar_generation_kwh,
                    b.net_import_kwh,
                    b.net_export_kwh,
                    b.energy_charge,
                    b.fixed_charge,
                    b.export_credit,
                    b.subsidy_amount,
                    b.total_payable,
                    b.subsidy_applied,
                    b.currency,
                )
                for s in bills
                for b in [s.bill]
            ],
        )
        cur.executemany(
            "INSERT INTO batch.zone_metrics (run_id, grid_zone, window_start, window_end, "
            "consumption_kwh, generation_kwh, net_kwh, grid_load_kw, solar_kw, "
            "renewable_share, readings, meters_reporting) VALUES "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    w["grid_zone"],
                    _utc(w["window_start"]),
                    _utc(w["window_end"]),
                    w["consumption_kwh"],
                    w["generation_kwh"],
                    w["net_kwh"],
                    w["grid_load_kw"],
                    w["solar_kw"],
                    w["renewable_share"],
                    w["readings"],
                    w["meters_reporting"],
                )
                for w in windows
            ],
        )
        cur.executemany(
            "INSERT INTO batch.zone_daily (run_id, business_date, grid_zone, consumption_kwh, "
            "generation_kwh, renewable_share, peak_load_kw, readings, meters_reporting, "
            "forecast_irradiance) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    day,
                    z["grid_zone"],
                    z["consumption_kwh"],
                    z["generation_kwh"],
                    (z["generation_kwh"] / z["consumption_kwh"]) if z["consumption_kwh"] else None,
                    peak.get(z["grid_zone"], 0.0),
                    z["readings"],
                    z["meters_reporting"],
                    (contents.forecast.get(z["grid_zone"]) or {}).get("irradiance_index"),
                )
                for z in zones
            ],
        )
        cur.execute(
            "UPDATE ops.settlement_runs SET status = 'succeeded', readings_archived = %s, "
            "readings_settled = %s, duplicates_removed = %s, readings_rejected = %s, "
            "households_billed = %s, total_billed = %s, snapshot_path = %s, "
            "finished_at = now() WHERE run_id = %s",
            (
                archived_n,
                settled_n,
                archived_n - rejected_n - settled_n,
                rejected_n,
                len(bills),
                total,
                snapshot or None,
                run_id,
            ),
        )
    lap("bill_and_commit")
    log.info("settlement timings", extra={"run_id": run_id, "seconds": timings})

    return SettlementResult(
        run_id=run_id,
        business_date=day,
        readings_archived=archived_n,
        readings_settled=settled_n,
        duplicates_removed=archived_n - rejected_n - settled_n,
        readings_rejected=rejected_n,
        households_billed=len(bills),
        total_billed=total,
        snapshot_path=snapshot,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Settle one business date.")
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--drop-version", type=int, default=None)
    parser.add_argument(
        "--trigger", default="manual", choices=("scheduled", "restatement", "manual")
    )
    parser.add_argument("--reason", default=None)
    parser.add_argument("--airflow-run-id", default=None)
    parser.add_argument(
        "--sim-id", type=int, default=None, help="refuse unless this is the current simulation"
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(service=SERVICE, level=settings.log_level)
    try:
        result = settle(
            settings,
            args.date,
            drop_version=args.drop_version,
            trigger=args.trigger,
            reason=args.reason,
            airflow_run_id=args.airflow_run_id,
            expected_sim_id=args.sim_id,
        )
    except (DropRejected, StaleSimulation) as exc:
        log.error(
            "settlement refused", extra={"business_date": args.date.isoformat(), "error": str(exc)}
        )
        return EXIT_DROP_REJECTED if isinstance(exc, DropRejected) else EXIT_STALE_SIMULATION
    print(f"{RUN_ID_MARKER}{result.run_id}", flush=True)  # for Airflow's next task
    return 0


if __name__ == "__main__":
    sys.exit(main())
