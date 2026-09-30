"""
Daily-batch source: one reference drop per simulated day.

Each drop is the reference data the batch layer needs to settle a day:

    tariff_schedule.json      the tariff in force (blocks, fixed charges,
                              export credit, subsidy) -- pricing as DATA
    household_tariffs.jsonl   each household's tier and subsidy flag, with
                              its headline rate
    weather_forecast.jsonl    the day-ahead forecast for every grid zone

Publication follows the shared simulated clock. A day's drop normally lands
at 00:30 simulated time on that day, so it is in place long before the day
ends and settlement begins.

Faults are drawn per day, deterministically from (seed, date), so a demo
run is reproducible:

    late     the drop lands 24-30 simulated hours late -- after the day it
             describes has ended, so settlement must wait for it
    missing  the drop never arrives; settlement must not run
    corrupt  the drop arrives damaged in one of six ways, and the quality
             gate (common.drop_quality) must refuse it

As with the stream, the pipeline never learns which days were faulted: the
ground truth is written under `_ground_truth/`, which only the measurement
script `scripts/inspect_drops.py` reads.

Published drops are never overwritten (ADR-0008). `publish` on a date that
already has a drop adds a new version; `revise` publishes a new version
with changed rates -- a retroactive tariff revision, the scenario that
forces the batch layer to restate a past day.

    python -m smartgrid.producers.daily_batch_source follow
    python -m smartgrid.producers.daily_batch_source publish --date 2026-01-03
    python -m smartgrid.producers.daily_batch_source publish --date 2026-01-03 \\
        --corrupt unknown_tier
    python -m smartgrid.producers.daily_batch_source revise --date 2026-01-03 \\
        --rate-change 10 --tier DOMESTIC_STD --reason "Regulator backdated revision"
"""

from __future__ import annotations

import argparse
import contextlib
import json
import random
import signal
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from decimal import Decimal
from enum import StrEnum
from typing import Any

import prometheus_client as prom

from smartgrid.common import drops, weather
from smartgrid.common.billing import DEFAULT_SCHEDULE, TariffSchedule
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.domain import Fleet, build_fleet_from_settings
from smartgrid.common.drop_quality import DropCheck
from smartgrid.common.logging import configure_logging, get_logger
from smartgrid.common.schemas import (
    TARIFF_RECORD_FIELDS,
    WEATHER_RECORD_FIELDS,
    TariffRecord,
    WeatherRecord,
)
from smartgrid.common.transformations import validate_record

SOURCE_NAME = "daily-batch-source"

# When a day's drop normally lands, as an offset from simulated midnight.
PUBLISH_OFFSET = timedelta(minutes=30)
# A LATE drop lands this many simulated hours after its normal time, i.e.
# during the NEXT day, after settlement of its own day has begun waiting.
LATE_DELAY_HOURS = (24.5, 29.5)

log = get_logger(__name__)

# -- Metrics ---------------------------------------------------------------
DROPS = prom.Counter(
    "smartgrid_batch_source_drops_total",
    "Daily drops handled, by outcome (published, late, corrupt, missing, revision).",
    ["outcome"],
)
LAST_BUSINESS_DATE = prom.Gauge(
    "smartgrid_batch_source_last_business_date_seconds",
    "Business date of the most recent drop published, as Unix seconds.",
)
PUBLISH_ERRORS = prom.Counter(
    "smartgrid_batch_source_publish_errors_total",
    "Attempts to publish a drop that failed with an error.",
)


# -- Faults ----------------------------------------------------------------


class DropFault(StrEnum):
    LATE = "late"
    MISSING = "missing"
    CORRUPT = "corrupt"


class Corruption(StrEnum):
    NEGATIVE_RATE = "negative_rate"  # a schedule block priced below zero
    UNKNOWN_TIER = "unknown_tier"  # households on a tier the schedule lacks
    MISSING_HOUSEHOLDS = "missing_households"  # records silently dropped at source
    DUPLICATE_HOUSEHOLD = "duplicate_household"  # the same household listed twice
    INVALID_RECORD = "invalid_record"  # a negative rate on a household record
    TRUNCATED_FILE = "truncated_file"  # upload cut short in transit


# The gate check each corruption must trigger. The gate may find more --
# one fault often has several symptoms -- but it must find this one.
EXPECTED_CHECK: Mapping[Corruption, DropCheck] = {
    Corruption.NEGATIVE_RATE: DropCheck.SCHEDULE_INVALID,
    Corruption.UNKNOWN_TIER: DropCheck.UNKNOWN_TIER,
    Corruption.MISSING_HOUSEHOLDS: DropCheck.HOUSEHOLD_MISSING,
    Corruption.DUPLICATE_HOUSEHOLD: DropCheck.HOUSEHOLD_DUPLICATED,
    Corruption.INVALID_RECORD: DropCheck.RECORD_INVALID,
    Corruption.TRUNCATED_FILE: DropCheck.CHECKSUM_MISMATCH,
}


@dataclass(frozen=True)
class DropFaultProfile:
    """Per-day probabilities of each drop fault; at most one fault per day."""

    name: str
    late: float = 0.0
    missing: float = 0.0
    corrupt: float = 0.0

    def __post_init__(self) -> None:
        rates = (self.late, self.missing, self.corrupt)
        if any(r < 0 for r in rates) or sum(rates) > 1:
            raise ValueError("drop fault rates must be non-negative and sum to at most 1")


# Named to match the stream's profiles, so one setting drives both sources.
DROP_PROFILES: dict[str, DropFaultProfile] = {
    "none": DropFaultProfile("none"),
    "realistic": DropFaultProfile("realistic", late=0.10, missing=0.02, corrupt=0.05),
    "chaos": DropFaultProfile("chaos", late=0.25, missing=0.10, corrupt=0.25),
}


def get_drop_profile(name: str) -> DropFaultProfile:
    try:
        return DROP_PROFILES[name]
    except KeyError:
        raise ValueError(
            f"unknown fault profile {name!r}; choose from {sorted(DROP_PROFILES)}"
        ) from None


@dataclass(frozen=True)
class DayPlan:
    """What happens to one business date's drop, decided in advance."""

    business_date: date
    publish_at: datetime  # simulated
    fault: DropFault | None = None
    corruption: Corruption | None = None
    late_by_hours: float | None = None


def plan_day(day: date, seed: int, profile: DropFaultProfile) -> DayPlan:
    """Deterministic in (seed, day, profile): a re-run faults the same days."""
    rng = random.Random(f"drop-plan:{seed}:{day.isoformat()}")
    normal = datetime.combine(day, clock_time(0, 0), tzinfo=UTC) + PUBLISH_OFFSET
    draw = rng.random()
    corruption_choice = rng.choice(list(Corruption))
    delay = rng.uniform(*LATE_DELAY_HOURS)

    if draw < profile.missing:
        return DayPlan(day, normal, DropFault.MISSING)
    if draw < profile.missing + profile.corrupt:
        return DayPlan(day, normal, DropFault.CORRUPT, corruption=corruption_choice)
    if draw < profile.missing + profile.corrupt + profile.late:
        return DayPlan(
            day, normal + timedelta(hours=delay), DropFault.LATE, late_by_hours=round(delay, 2)
        )
    return DayPlan(day, normal)


# -- Building the drop ------------------------------------------------------


def _midnight(day: date) -> datetime:
    return datetime.combine(day, clock_time(0, 0), tzinfo=UTC)


def build_household_tariffs(
    fleet: Fleet, day: date, schedule: TariffSchedule
) -> list[dict[str, Any]]:
    """One tariff record per household, each validated against the shared schema."""
    records = []
    for household in fleet:
        tier = household.tariff_tier
        record = TariffRecord(
            household_id=household.household_id,
            tariff_rate=float(schedule.headline_rate(tier)),
            billing_tier=tier,
            subsidy_flag=household.subsidy_eligible,
            fixed_charge=float(schedule.tier(tier).fixed_charge),
            effective_date=_midnight(day),
        ).to_dict()
        # Validate, but SERIALISE the builder's output. The validator returns
        # parsed Python objects (datetimes) that json.dumps cannot encode.
        result = validate_record(record, TARIFF_RECORD_FIELDS)
        if not result.ok:
            raise ValueError(f"generated an invalid tariff record: {result.detail}")
        records.append(record)
    return records


def build_weather_forecast(fleet: Fleet, day: date, seed: int) -> list[dict[str, Any]]:
    """One forecast per zone: the same forecast the meters' ACTUAL weather departs from."""
    records = []
    for zone in fleet.zones:
        predicted = weather.forecast(zone, day, seed)
        record = WeatherRecord(
            grid_zone=zone,
            forecast_date=_midnight(day),
            cloud_cover_pct=predicted.cloud_cover_pct,
            temperature_c=predicted.temperature_c,
            irradiance_index=predicted.irradiance_index,
        ).to_dict()
        result = validate_record(record, WEATHER_RECORD_FIELDS)
        if not result.ok:
            raise ValueError(f"generated an invalid weather record: {result.detail}")
        records.append(record)
    return records


def render_drop(
    fleet: Fleet, day: date, seed: int, schedule: TariffSchedule = DEFAULT_SCHEDULE
) -> dict[str, bytes]:
    """The three data files as bytes. Deterministic: same inputs, same bytes."""
    return {
        drops.SCHEDULE_FILE: drops.to_json(
            {"business_date": day.isoformat(), **schedule.to_dict()}
        ),
        drops.HOUSEHOLDS_FILE: drops.to_jsonl(build_household_tariffs(fleet, day, schedule)),
        drops.WEATHER_FILE: drops.to_jsonl(build_weather_forecast(fleet, day, seed)),
    }


def _rewrite_lines(body: bytes, edit) -> bytes:
    records = [json.loads(line) for line in body.decode("utf-8").splitlines() if line.strip()]
    return drops.to_jsonl(edit(records))


def corrupt_files(
    files: Mapping[str, bytes], corruption: Corruption, rng: random.Random
) -> tuple[dict[str, bytes], dict[str, bytes]]:
    """
    Damage a drop. Returns (what the manifest describes, what is uploaded).

    Source faults damage the data BEFORE the manifest is built -- the
    publisher faithfully ships bad data, so checksums match and only content
    validation can catch it. A transit fault damages only the upload, so the
    manifest still describes the intact file and the checksum exposes it.
    """
    described = dict(files)

    def pick(records: list[dict], low: int, high: int) -> list[int]:
        return rng.sample(range(len(records)), k=min(len(records), rng.randint(low, high)))

    match corruption:
        case Corruption.NEGATIVE_RATE:
            schedule = json.loads(described[drops.SCHEDULE_FILE])
            tier = rng.choice(sorted(schedule["tiers"]))
            blocks = schedule["tiers"][tier]["blocks"]
            block = blocks[rng.randrange(len(blocks))]
            block["rate"] = str(-abs(Decimal(block["rate"])))
            described[drops.SCHEDULE_FILE] = drops.to_json(schedule)

        case Corruption.UNKNOWN_TIER:

            def edit(records):
                for i in pick(records, 1, 5):
                    records[i]["billing_tier"] = "DOMESTIC_PREMIUM"
                return records

            described[drops.HOUSEHOLDS_FILE] = _rewrite_lines(
                described[drops.HOUSEHOLDS_FILE], edit
            )

        case Corruption.MISSING_HOUSEHOLDS:

            def edit(records):
                gone = set(pick(records, 3, 15))
                return [r for i, r in enumerate(records) if i not in gone]

            described[drops.HOUSEHOLDS_FILE] = _rewrite_lines(
                described[drops.HOUSEHOLDS_FILE], edit
            )

        case Corruption.DUPLICATE_HOUSEHOLD:

            def edit(records):
                return records + [dict(records[i]) for i in pick(records, 1, 5)]

            described[drops.HOUSEHOLDS_FILE] = _rewrite_lines(
                described[drops.HOUSEHOLDS_FILE], edit
            )

        case Corruption.INVALID_RECORD:

            def edit(records):
                for i in pick(records, 1, 5):
                    records[i]["tariff_rate"] = -abs(records[i]["tariff_rate"])
                return records

            described[drops.HOUSEHOLDS_FILE] = _rewrite_lines(
                described[drops.HOUSEHOLDS_FILE], edit
            )

        case Corruption.TRUNCATED_FILE:
            uploaded = dict(described)
            body = described[drops.HOUSEHOLDS_FILE]
            uploaded[drops.HOUSEHOLDS_FILE] = body[: int(len(body) * 0.6)]
            return described, uploaded

    return described, described


# -- Publishing -------------------------------------------------------------


def publish(
    client: Any,
    bucket: str,
    fleet: Fleet,
    day: date,
    *,
    seed: int,
    sim_now: datetime,
    schedule: TariffSchedule = DEFAULT_SCHEDULE,
    corruption: Corruption | None = None,
    fault: DropFault | None = None,
    late_by_hours: float | None = None,
    revision: Mapping[str, Any] | None = None,
    root: str = "",
) -> drops.DropManifest:
    """
    Publish the next version of a day's drop. Never overwrites: if v1 exists
    this writes v2, recording which complete version it supersedes.
    """
    existing = drops.versions(client, bucket, day, root)
    version = existing[-1] + 1 if existing else 1
    supersedes = drops.latest_complete_version(client, bucket, day, root)

    files = render_drop(fleet, day, seed, schedule)
    described, uploaded = (
        corrupt_files(files, corruption, random.Random(f"drop-corrupt:{seed}:{day}:{version}"))
        if corruption is not None
        else (files, files)
    )
    manifest = drops.build_manifest(
        day,
        version,
        described,
        published_at_sim=sim_now.isoformat(timespec="seconds"),
        supersedes=supersedes,
        revision_reason=None if revision is None else revision.get("reason"),
    )
    drops.upload_drop(client, bucket, manifest, uploaded, root)
    drops.write_ground_truth(
        client,
        bucket,
        day,
        version,
        {
            "business_date": day.isoformat(),
            "version": version,
            "fault": None if fault is None else fault.value,
            "corruption": None if corruption is None else corruption.value,
            "expected_check": None if corruption is None else EXPECTED_CHECK[corruption].value,
            "late_by_sim_hours": late_by_hours,
            "revision": None if revision is None else dict(revision),
        },
        root,
    )
    return manifest


def withhold(client: Any, bucket: str, day: date, root: str = "") -> None:
    """Record that a day's drop was deliberately never published."""
    drops.write_ground_truth(
        client,
        bucket,
        day,
        1,
        {
            "business_date": day.isoformat(),
            "version": 1,
            "fault": DropFault.MISSING.value,
            "corruption": None,
            "expected_check": DropCheck.MANIFEST_MISSING.value,
            "late_by_sim_hours": None,
            "revision": None,
        },
        root,
    )


def execute_plan(
    client: Any, bucket: str, fleet: Fleet, plan: DayPlan, *, seed: int, sim_now: datetime
) -> None:
    day = plan.business_date
    if plan.fault is DropFault.MISSING:
        withhold(client, bucket, day)
        DROPS.labels("missing").inc()
        log.warning(
            "drop withheld (injected fault)",
            extra={"business_date": day.isoformat(), "fault": "missing"},
        )
        return

    manifest = publish(
        client,
        bucket,
        fleet,
        day,
        seed=seed,
        sim_now=sim_now,
        corruption=plan.corruption,
        fault=plan.fault,
        late_by_hours=plan.late_by_hours,
    )
    outcome = plan.fault.value if plan.fault else "published"
    DROPS.labels(outcome).inc()
    LAST_BUSINESS_DATE.set(_midnight(day).timestamp())
    log.info(
        "drop published",
        extra={
            "business_date": day.isoformat(),
            "version": manifest.version,
            "records": {name: meta["records"] for name, meta in manifest.files.items()},
            "fault": plan.fault.value if plan.fault else None,
            "corruption": plan.corruption.value if plan.corruption else None,
            "late_by_sim_hours": plan.late_by_hours,
            "simulated_now": sim_now.isoformat(timespec="minutes"),
        },
    )


def _already_handled(client: Any, bucket: str, day: date) -> bool:
    """True if a previous run already published or withheld this day."""
    return bool(drops.versions(client, bucket, day)) or (
        drops.read_ground_truth(client, bucket, day, 1) is not None
    )


def _raise_interrupt(signum, frame) -> None:
    raise KeyboardInterrupt


def follow(
    settings: Settings,
    *,
    profile: DropFaultProfile,
    from_date: date | None = None,
    duration: float | None = None,
    poll_seconds: float = 1.0,
    metrics_port: int = 0,
) -> None:
    """Publish each day's drop as the shared clock reaches its publish time."""
    from smartgrid.common.clock_store import shared_clock
    from smartgrid.common.storage import s3_client

    clock = shared_clock(settings)
    client = s3_client(settings)
    fleet = build_fleet_from_settings(settings)
    bucket = settings.minio_bucket_raw
    start = from_date or clock.sim_date()

    if metrics_port:
        prom.start_http_server(metrics_port)
    with contextlib.suppress(ValueError, AttributeError):
        signal.signal(signal.SIGTERM, _raise_interrupt)

    log.info(
        "daily batch source starting",
        extra={
            "clock": clock.describe(),
            "simulated_now": clock.now().isoformat(timespec="minutes"),
            "from_date": start.isoformat(),
            "fault_profile": profile.name,
            "bucket": bucket,
            "publish_offset_minutes": PUBLISH_OFFSET.total_seconds() / 60,
            "metrics_port": metrics_port or None,
        },
    )

    handled: set[date] = set()
    started = time.monotonic()
    try:
        while duration is None or time.monotonic() - started < duration:
            sim_now = clock.now()
            day = start
            while day <= sim_now.date():
                if day not in handled:
                    plan = plan_day(day, settings.sim_seed, profile)
                    if _already_handled(client, bucket, day):
                        handled.add(day)
                        log.info(
                            "drop already handled by an earlier run; skipping",
                            extra={"business_date": day.isoformat()},
                        )
                    elif sim_now >= plan.publish_at:
                        try:
                            execute_plan(
                                client, bucket, fleet, plan, seed=settings.sim_seed, sim_now=sim_now
                            )
                            handled.add(day)
                        except Exception:  # noqa: BLE001 - log, count, retry next poll
                            PUBLISH_ERRORS.inc()
                            log.exception(
                                "drop publication failed; will retry",
                                extra={"business_date": day.isoformat()},
                            )
                day += timedelta(days=1)
            while start in handled:  # never re-scan fully handled history
                start += timedelta(days=1)
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        log.info("stop requested")
    log.info("daily batch source stopped", extra={"days_handled": len(handled)})


def reset_drops(settings: Settings) -> int:
    """
    Delete every drop and its ground truth, to begin a NEW simulation.

    This is a development tool, not part of the pipeline. Within one
    simulation a published drop is never modified (ADR-0008); a reset
    discards the whole simulated world, typically alongside
    `clock_store reset`. Without it, `follow` would treat the previous
    simulation's drops as already published and skip those dates.
    """
    from smartgrid.common.storage import delete_keys, list_keys, s3_client

    client = s3_client(settings)
    bucket = settings.minio_bucket_raw
    keys = list_keys(client, bucket, f"{drops.DATASET}/") + list_keys(
        client, bucket, f"{drops.GROUND_TRUTH_PREFIX}/"
    )
    delete_keys(client, bucket, keys)
    return len(keys)


def revise(
    settings: Settings,
    day: date,
    *,
    factor: Decimal,
    tiers: Iterable[str] | None,
    reason: str,
) -> drops.DropManifest:
    """
    Publish a retroactive tariff revision for a past day: a new version whose
    schedule is the latest complete version's, with rates scaled by `factor`.
    """
    from smartgrid.common.clock_store import shared_clock
    from smartgrid.common.storage import s3_client

    client = s3_client(settings)
    bucket = settings.minio_bucket_raw
    fleet = build_fleet_from_settings(settings)

    latest = drops.latest_complete_version(client, bucket, day)
    if latest is None:
        raise SystemExit(f"no complete drop for {day} to revise; publish one first")
    loaded = drops.load_drop(client, bucket, day, latest)
    try:
        base = TariffSchedule.from_dict(json.loads(loaded.files[drops.SCHEDULE_FILE]))
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(
            f"{day} v{latest} has no usable schedule ({exc}); "
            "republish it with `publish` before revising"
        ) from exc
    if base.problems():
        raise SystemExit(
            f"{day} v{latest} schedule is invalid ({base.problems()[0]}); "
            "republish it with `publish` before revising"
        )

    tier_list = None if tiers is None else sorted(tiers)
    revised = base.with_rate_change(factor, tier_list)
    manifest = publish(
        client,
        bucket,
        fleet,
        day,
        seed=settings.sim_seed,
        sim_now=shared_clock(settings).now(),
        schedule=revised,
        revision={
            "reason": reason,
            "rate_factor": str(factor),
            "tiers": tier_list or sorted(base.tiers),
            "supersedes": latest,
        },
    )
    DROPS.labels("revision").inc()
    return manifest


# -- CLI --------------------------------------------------------------------


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {value!r}") from None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily reference drops for the batch layer.")
    commands = parser.add_subparsers(dest="command")

    f = commands.add_parser("follow", help="publish each day's drop on the shared clock (default)")
    f.add_argument(
        "--faults",
        choices=sorted(DROP_PROFILES),
        default=None,
        help="fault profile (default: SIM_FAULT_PROFILE)",
    )
    f.add_argument(
        "--from-date",
        type=_parse_date,
        default=None,
        help="also publish every day since this date (default: today, simulated)",
    )
    f.add_argument("--duration", type=float, default=None, help="stop after N real seconds")
    f.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help="Prometheus port (default: BATCH_SOURCE_METRICS_PORT; 0 disables)",
    )

    p = commands.add_parser("publish", help="publish (or republish) one day's drop now")
    p.add_argument("--date", type=_parse_date, required=True)
    p.add_argument(
        "--corrupt",
        choices=[c.value for c in Corruption],
        default=None,
        help="publish it damaged, to demonstrate the quality gate",
    )

    x = commands.add_parser("reset", help="delete ALL drops, to begin a new simulation")
    x.add_argument("--yes", action="store_true", help="confirm deleting every drop")

    r = commands.add_parser("revise", help="publish a retroactive tariff revision for a day")
    r.add_argument("--date", type=_parse_date, required=True)
    r.add_argument(
        "--rate-change",
        type=Decimal,
        required=True,
        help="percentage change to every block rate, e.g. 10 or -5",
    )
    r.add_argument(
        "--tier",
        action="append",
        default=None,
        help="tier to revise; repeatable (default: all tiers)",
    )
    r.add_argument("--reason", required=True, help="why the tariff was revised")

    args = parser.parse_args(argv)
    if args.command is None:
        args = parser.parse_args(["follow", *(argv or [])])
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = get_settings()
    configure_logging(service=SOURCE_NAME, level=settings.log_level)

    if args.command == "follow":
        follow(
            settings,
            profile=get_drop_profile(args.faults or settings.sim_fault_profile),
            from_date=args.from_date,
            duration=args.duration,
            metrics_port=(
                settings.batch_source_metrics_port
                if args.metrics_port is None
                else args.metrics_port
            ),
        )
        return

    from smartgrid.common.clock_store import shared_clock
    from smartgrid.common.storage import s3_client

    if args.command == "publish":
        corruption = None if args.corrupt is None else Corruption(args.corrupt)
        manifest = publish(
            s3_client(settings),
            settings.minio_bucket_raw,
            build_fleet_from_settings(settings),
            args.date,
            seed=settings.sim_seed,
            sim_now=shared_clock(settings).now(),
            corruption=corruption,
            fault=None if corruption is None else DropFault.CORRUPT,
        )
        log.info(
            "drop published",
            extra={
                "business_date": args.date.isoformat(),
                "version": manifest.version,
                "supersedes": manifest.supersedes,
                "corruption": args.corrupt,
            },
        )
    elif args.command == "reset":
        if not args.yes:
            raise SystemExit("reset deletes every published drop; re-run with --yes to confirm")
        deleted = reset_drops(settings)
        log.warning("all drops deleted for a new simulation", extra={"objects_deleted": deleted})
    elif args.command == "revise":
        manifest = revise(
            settings,
            args.date,
            factor=Decimal("1") + args.rate_change / Decimal("100"),
            tiers=args.tier,
            reason=args.reason,
        )
        log.info(
            "tariff revision published",
            extra={
                "business_date": args.date.isoformat(),
                "version": manifest.version,
                "supersedes": manifest.supersedes,
                "rate_change_pct": str(args.rate_change),
                "tiers": args.tier or "all",
                "reason": args.reason,
            },
        )


if __name__ == "__main__":
    main()
