"""
Quality gate for the daily reference drop.

The batch layer runs this before settling a day, and settles only if it
passes. It FAILS CLOSED: a drop that cannot be shown to be correct produces
no bills, because a missing bill can be issued later but a wrong one has to
be refunded, explained and audited.

Two layers, checked in order:

  Integrity -- did the bytes arrive as the publisher sent them? The manifest
  must exist and list every file; each file's SHA-256 and record count must
  match. If integrity fails the gate stops there: judging the content of
  bytes that are known not to be what was sent would only report symptoms
  of the same fault.

  Validity -- is the content right? The tariff schedule must be internally
  consistent; every household record must pass the shared schema, name a
  tier the schedule defines and quote that tier's rate; every household in
  the fleet must appear exactly once; every grid zone must have a forecast;
  and everything must be for the right business date.

Findings are aggregated per check -- 200 households with one systematic
problem are one finding with a count of 200, not 200 findings.

Pure functions over bytes. `check_drop` does no I/O, so it is unit-tested
directly; `check_latest` adds the object-store lookup.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from smartgrid.common import drops
from smartgrid.common.billing import TariffSchedule
from smartgrid.common.domain import Fleet
from smartgrid.common.schemas import TARIFF_RECORD_FIELDS, WEATHER_RECORD_FIELDS
from smartgrid.common.transformations import validate_record

# A household record's quoted rate may differ from the schedule's by less
# than half a cent (JSON numbers travel as floats); anything more is a fault.
_RATE_TOLERANCE = Decimal("0.005")


class DropCheck(StrEnum):
    # Integrity
    MANIFEST_MISSING = "manifest_missing"
    MANIFEST_INVALID = "manifest_invalid"
    FILE_MISSING = "file_missing"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    RECORD_COUNT_MISMATCH = "record_count_mismatch"
    # Validity
    WRONG_DATE = "wrong_date"
    SCHEDULE_INVALID = "schedule_invalid"
    RECORD_INVALID = "record_invalid"
    UNKNOWN_TIER = "unknown_tier"
    HEADLINE_RATE_MISMATCH = "headline_rate_mismatch"
    HOUSEHOLD_MISSING = "household_missing"
    HOUSEHOLD_DUPLICATED = "household_duplicated"
    UNKNOWN_HOUSEHOLD = "unknown_household"
    ZONE_MISSING = "zone_missing"


@dataclass(frozen=True)
class Finding:
    check: DropCheck
    detail: str  # the first example, plus how many more
    count: int = 1


@dataclass(frozen=True)
class QualityReport:
    business_date: date
    version: int | None
    findings: tuple[Finding, ...]
    stats: Mapping[str, int] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return not self.findings

    @property
    def checks_failed(self) -> frozenset[DropCheck]:
        return frozenset(f.check for f in self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "business_date": self.business_date.isoformat(),
            "version": self.version,
            "passed": self.passed,
            "findings": [
                {"check": f.check.value, "count": f.count, "detail": f.detail}
                for f in self.findings
            ],
            "stats": dict(self.stats),
        }


class _Findings:
    """Collects findings, grouping repeats of the same check."""

    def __init__(self) -> None:
        self._details: dict[DropCheck, list[str]] = {}
        self._counts: Counter[DropCheck] = Counter()

    def add(self, check: DropCheck, detail: str, count: int = 1) -> None:
        self._details.setdefault(check, []).append(detail)
        self._counts[check] += count

    def __bool__(self) -> bool:
        return bool(self._details)

    def freeze(self) -> tuple[Finding, ...]:
        frozen = []
        for check, details in self._details.items():
            more = len(details) - 1
            detail = details[0] + (f" (and {more} more)" if more else "")
            frozen.append(Finding(check=check, detail=detail, count=self._counts[check]))
        return tuple(frozen)


def _count(n: int, noun: str) -> str:
    return f"{n} {noun}" + ("" if n == 1 else "s")


def _lines(body: bytes) -> list[str]:
    return [line for line in body.decode("utf-8", errors="replace").splitlines() if line.strip()]


def check_drop(drop: drops.LoadedDrop, fleet: Fleet) -> QualityReport:
    """Run every check on one loaded drop."""
    found = _Findings()
    stats: dict[str, int] = {}

    def report() -> QualityReport:
        return QualityReport(drop.business_date, drop.version, found.freeze(), stats)

    # -- Integrity ---------------------------------------------------------
    if drop.manifest is None:
        found.add(
            DropCheck.MANIFEST_MISSING,
            "no _MANIFEST.json: the drop is incomplete or was never published",
        )
        return report()
    try:
        manifest = drops.DropManifest.from_dict(drop.manifest)
    except ValueError as exc:
        found.add(DropCheck.MANIFEST_INVALID, str(exc))
        return report()

    if manifest.business_date != drop.business_date:
        found.add(
            DropCheck.WRONG_DATE,
            f"manifest is for {manifest.business_date}, filed under {drop.business_date}",
        )

    for name in drops.DATA_FILES:
        meta = manifest.files.get(name)
        body = drop.files.get(name)
        if meta is None:
            found.add(DropCheck.MANIFEST_INVALID, f"manifest does not list {name}")
        elif body is None:
            found.add(DropCheck.FILE_MISSING, f"{name} is listed in the manifest but absent")
        else:
            if drops.sha256(body) != meta.get("sha256"):
                found.add(
                    DropCheck.CHECKSUM_MISMATCH,
                    f"{name}: received {len(body)} bytes that do not match the manifest "
                    f"checksum ({meta.get('bytes')} bytes were sent)",
                )
            if drops.count_records(name, body) != meta.get("records"):
                found.add(
                    DropCheck.RECORD_COUNT_MISMATCH,
                    f"{name}: {drops.count_records(name, body)} records received, "
                    f"{meta.get('records')} sent",
                )
    if found:
        return report()

    # -- Tariff schedule ---------------------------------------------------
    schedule: TariffSchedule | None = None
    try:
        schedule = TariffSchedule.from_dict(json.loads(drop.files[drops.SCHEDULE_FILE]))
    except (ValueError, json.JSONDecodeError) as exc:
        found.add(DropCheck.SCHEDULE_INVALID, str(exc))
    if schedule is not None:
        for problem in schedule.problems():
            found.add(DropCheck.SCHEDULE_INVALID, problem)
        stats["schedule_tiers"] = len(schedule.tiers)

    # -- Household tariff records -----------------------------------------
    households: Counter[str] = Counter()
    household_lines = _lines(drop.files[drops.HOUSEHOLDS_FILE])
    for line_no, line in enumerate(household_lines, start=1):
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            found.add(DropCheck.RECORD_INVALID, f"household_tariffs line {line_no} is not JSON")
            continue
        result = validate_record(raw, TARIFF_RECORD_FIELDS)
        if not result.ok:
            found.add(
                DropCheck.RECORD_INVALID,
                f"household_tariffs line {line_no}: {result.reason} ({result.detail})",
            )
            continue
        record = result.record
        households[record["household_id"]] += 1

        if record["effective_date"].date() != drop.business_date:
            found.add(
                DropCheck.WRONG_DATE,
                f"{record['household_id']}: effective {record['effective_date'].date()}",
            )

        tier = record["billing_tier"]
        if schedule is not None:
            if tier not in schedule.tiers:
                found.add(
                    DropCheck.UNKNOWN_TIER,
                    f"{record['household_id']}: tier {tier!r} is not in the schedule",
                )
            else:
                try:
                    quoted = Decimal(str(record["tariff_rate"]))
                except InvalidOperation:
                    quoted = Decimal("-1")
                expected = schedule.headline_rate(tier)
                if abs(quoted - expected) > _RATE_TOLERANCE:
                    found.add(
                        DropCheck.HEADLINE_RATE_MISMATCH,
                        f"{record['household_id']}: quotes {quoted}, schedule says "
                        f"{expected} for {tier}",
                    )

    missing = fleet.household_ids - households.keys()
    if missing:
        found.add(
            DropCheck.HOUSEHOLD_MISSING,
            f"{_count(len(missing), 'household')} without a tariff record, "
            f"e.g. {sorted(missing)[:3]}",
            count=len(missing),
        )
    duplicated = sorted(h for h, n in households.items() if n > 1)
    if duplicated:
        found.add(
            DropCheck.HOUSEHOLD_DUPLICATED,
            f"{_count(len(duplicated), 'household')} listed more than once, e.g. {duplicated[:3]}",
            count=len(duplicated),
        )
    unknown = sorted(households.keys() - fleet.household_ids)
    if unknown:
        found.add(
            DropCheck.UNKNOWN_HOUSEHOLD,
            f"{_count(len(unknown), 'record')} for households not in the fleet, e.g. {unknown[:3]}",
            count=len(unknown),
        )
    stats["household_records"] = len(household_lines)
    stats["households_covered"] = len(households.keys() & fleet.household_ids)

    # -- Weather forecast --------------------------------------------------
    zones: set[str] = set()
    for line_no, line in enumerate(_lines(drop.files[drops.WEATHER_FILE]), start=1):
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            found.add(DropCheck.RECORD_INVALID, f"weather_forecast line {line_no} is not JSON")
            continue
        result = validate_record(raw, WEATHER_RECORD_FIELDS)
        if not result.ok:
            found.add(
                DropCheck.RECORD_INVALID,
                f"weather_forecast line {line_no}: {result.reason} ({result.detail})",
            )
            continue
        if result.record["forecast_date"].date() != drop.business_date:
            found.add(
                DropCheck.WRONG_DATE,
                f"forecast for {result.record['grid_zone']} is dated "
                f"{result.record['forecast_date'].date()}",
            )
        zones.add(result.record["grid_zone"])

    missing_zones = sorted(set(fleet.zones) - zones)
    if missing_zones:
        found.add(
            DropCheck.ZONE_MISSING,
            f"no forecast for {missing_zones}",
            count=len(missing_zones),
        )
    stats["zones_forecast"] = len(zones & set(fleet.zones))

    return report()


def check_latest(
    client: Any, bucket: str, day: date, fleet: Fleet, root: str = ""
) -> QualityReport:
    """Gate the latest complete version of a day's drop -- what settlement would read."""
    version = drops.latest_complete_version(client, bucket, day, root)
    if version is None:
        # Nothing complete to read. Load the newest partial version, if any,
        # so the report says whether files arrived without a manifest.
        partial = drops.versions(client, bucket, day, root)
        version_to_report = partial[-1] if partial else None
        return QualityReport(
            business_date=day,
            version=version_to_report,
            findings=(
                Finding(
                    DropCheck.MANIFEST_MISSING,
                    "no complete drop: files may be arriving, delayed, or never published",
                ),
            ),
        )
    return check_drop(drops.load_drop(client, bucket, day, version, root), fleet)
