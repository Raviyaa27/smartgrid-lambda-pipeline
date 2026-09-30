"""
Validation, normalisation and enrichment -- shared by BOTH Lambda layers.

The speed layer (Spark Structured Streaming, Section 6) and the batch layer
(Spark batch under Airflow, Section 7) both import this module. Neither
implements its own copy of these rules.

That is deliberate. The strongest argument against Lambda is Jay Kreps'
objection that maintaining two code paths guarantees they eventually
disagree. This module is the mitigation: the rules are declared once in
schemas.py and applied by thin adapters here -- `validate_record` for
row-at-a-time Python, `validate_frame` / `validate_json_frame` for
vectorised pandas inside Spark. Both adapters share the same coercion
function and the same definition of "missing", so they agree by
construction, and a fuzz test holds them to it.

Accepted cost: the Spark path uses pandas UDFs rather than native Catalyst
expressions, which is slower. We trade throughput for a guarantee of zero
logic drift. At production scale you would generate both the Python
predicates and the Spark expressions from the same spec -- noted as future
work in the report's limitations section.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import pandas as pd

from smartgrid.common.domain import Fleet
from smartgrid.common.schemas import METER_READING_FIELDS, FieldSpec

# A reading timestamped further ahead than this is a clock fault upstream.
FUTURE_TOLERANCE = timedelta(minutes=5)


class QuarantineReason(StrEnum):
    """Why a record was sent to the DLQ instead of the pipeline."""

    MALFORMED_JSON = "malformed_json"
    MISSING_FIELD = "missing_field"
    WRONG_TYPE = "wrong_type"
    OUT_OF_RANGE = "out_of_range"
    UNKNOWN_HOUSEHOLD = "unknown_household"
    FUTURE_TIMESTAMP = "future_timestamp"


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    record: dict[str, Any] | None = None
    reason: QuarantineReason | None = None
    detail: str | None = None

    @classmethod
    def accept(cls, record: dict[str, Any]) -> ValidationResult:
        return cls(ok=True, record=record)

    @classmethod
    def reject(cls, reason: QuarantineReason, detail: str) -> ValidationResult:
        return cls(ok=False, reason=reason, detail=detail)


# -- Type coercion -------------------------------------------------------


def _is_missing(value: Any) -> bool:
    """
    The single definition of "no value", shared by both validators: None, an
    empty string, or NaN/NaT. NaN is included because pandas uses it for an
    absent value -- a key missing from one JSON record becomes NaN once
    records share a DataFrame -- and a NaN measurement carries no measurement.
    """
    if value is None or value is pd.NaT:
        return True
    if isinstance(value, str):
        return value == ""
    return isinstance(value, float) and value != value


def _parse_timestamp(value: Any) -> datetime:
    """Accept ISO-8601 (with or without trailing Z) or epoch seconds."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        try:
            parsed = datetime.fromtimestamp(float(value), tz=UTC)
        except (OverflowError, OSError, ValueError) as exc:
            # inf, 1e20 or a large negative number. These must be REJECTED,
            # not raised: an uncaught error here fails the whole Spark
            # micro-batch, and the restarted query re-reads the same message
            # -- a poison pill that stalls the stream for good.
            raise ValueError(f"timestamp {value!r} is out of range") from exc
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"cannot read {type(value).__name__} as a timestamp")
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce(value: Any, spec: FieldSpec) -> Any:
    """Coerce to the declared type, raising ValueError/TypeError on failure."""
    match spec.dtype:
        case "string":
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                raise TypeError(f"{spec.name}: expected string, got {type(value).__name__}")
            return str(value).strip()
        case "double":
            if isinstance(value, bool):
                raise TypeError(f"{spec.name}: bool is not a number")
            coerced = float(value)
            if coerced != coerced:  # NaN is never a valid measurement
                raise ValueError(f"{spec.name}: NaN")
            return coerced
        case "integer":
            if isinstance(value, bool):
                raise TypeError(f"{spec.name}: bool is not an integer")
            return int(value)
        case "boolean":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"true", "t", "1", "yes"}:
                    return True
                if lowered in {"false", "f", "0", "no"}:
                    return False
            raise TypeError(f"{spec.name}: cannot read {value!r} as boolean")
        case "timestamp":
            return _parse_timestamp(value)
    raise TypeError(f"{spec.name}: unsupported dtype {spec.dtype}")


# -- Row-at-a-time validation --------------------------------------------


def validate_record(
    raw: dict[str, Any],
    specs: tuple[FieldSpec, ...] = METER_READING_FIELDS,
    *,
    known_household_ids: frozenset[str] | None = None,
    now: datetime | None = None,
) -> ValidationResult:
    """
    Validate and normalise one raw record.

    Returns an accepted result carrying the cleaned record, or a rejection
    naming the reason -- which becomes the DLQ partition key, so the
    dashboard can break quarantined volume down by failure mode.
    """
    if not isinstance(raw, dict):
        return ValidationResult.reject(
            QuarantineReason.MALFORMED_JSON, f"expected object, got {type(raw).__name__}"
        )

    clean: dict[str, Any] = {}

    for spec in specs:
        value = raw.get(spec.name)

        if _is_missing(value):
            if spec.required:
                return ValidationResult.reject(
                    QuarantineReason.MISSING_FIELD, f"'{spec.name}' is required"
                )
            clean[spec.name] = None
            continue

        try:
            coerced = _coerce(value, spec)
        except (TypeError, ValueError) as exc:
            return ValidationResult.reject(QuarantineReason.WRONG_TYPE, str(exc))

        if spec.min_value is not None and coerced < spec.min_value:
            return ValidationResult.reject(
                QuarantineReason.OUT_OF_RANGE,
                f"{spec.name}={coerced} below minimum {spec.min_value}",
            )
        if spec.max_value is not None and coerced > spec.max_value:
            return ValidationResult.reject(
                QuarantineReason.OUT_OF_RANGE,
                f"{spec.name}={coerced} above maximum {spec.max_value}",
            )

        clean[spec.name] = coerced

    # Referential integrity: a reading for a household we do not bill is
    # unjoinable, so it cannot reach the settlement layer.
    if known_household_ids is not None:
        household_id = clean.get("household_id")
        if household_id not in known_household_ids:
            return ValidationResult.reject(
                QuarantineReason.UNKNOWN_HOUSEHOLD, f"no such household: {household_id!r}"
            )

    event_time = clean.get("event_time")
    if isinstance(event_time, datetime):
        reference = now or datetime.now(UTC)
        if event_time > reference + FUTURE_TOLERANCE:
            return ValidationResult.reject(
                QuarantineReason.FUTURE_TIMESTAMP,
                f"event_time {event_time.isoformat()} is ahead of {reference.isoformat()}",
            )

    return ValidationResult.accept(clean)


# -- Vectorised validation, for Spark ------------------------------------


def validate_frame(
    frame: pd.DataFrame,
    specs: tuple[FieldSpec, ...] = METER_READING_FIELDS,
    *,
    known_household_ids: frozenset[str] | None = None,
    now: datetime | None = None,
) -> pd.DataFrame:
    """
    Vectorised sibling of `validate_record`, driven by the same FieldSpec
    table. Adds two columns and mutates nothing else:

        is_valid           bool
        quarantine_reason  str | None

    Used from a Spark pandas UDF so the speed layer applies byte-identical
    rules to the batch layer.
    """
    result = frame.copy()
    # Python objects, not numpy scalars: a numpy bool_ is not a Python bool,
    # and would slip past the check that rejects booleans as numbers.
    columns = {name: result[name].astype(object) for name in result.columns}
    reason, _ = _frame_verdicts(columns, result.index, specs, known_household_ids, now)
    result["quarantine_reason"] = reason
    result["is_valid"] = reason.isna()
    return result


# Marks a value that failed type coercion.
_BAD = object()


def _coerce_or_bad(value: Any, spec: FieldSpec) -> Any:
    if _is_missing(value):
        return None
    try:
        return _coerce(value, spec)
    except (TypeError, ValueError):
        return _BAD


def _frame_verdicts(
    columns: dict[str, pd.Series],
    index: pd.Index,
    specs: tuple[FieldSpec, ...],
    known_household_ids: frozenset[str] | None,
    now: datetime | None,
) -> tuple[pd.Series, dict[str, pd.Series]]:
    """
    The vectorised validation, returning each row's quarantine reason (None
    if valid) and the coerced value of every field.

    It reaches the same verdict as `validate_record` BY CONSTRUCTION, not by
    parallel implementation: type coercion calls the very same `_coerce`,
    missing values use the same `_is_missing`, and checks run in the same
    order with the first failure winning. Only the range and cross-field
    checks are vectorised. `tests/unit/test_validator_parity.py` fuzzes the
    two against each other.
    """
    reason = pd.Series([None] * len(index), index=index, dtype="object")
    coerced_columns: dict[str, pd.Series] = {}

    def fail(mask: pd.Series, why: QuarantineReason) -> None:
        nonlocal reason
        reason = reason.mask(mask.astype(bool) & reason.isna(), why.value)

    for spec in specs:
        column = columns.get(spec.name)
        if column is None:
            if spec.required:
                reason = reason.fillna(QuarantineReason.MISSING_FIELD.value)
            coerced_columns[spec.name] = pd.Series([None] * len(index), index=index, dtype=object)
            continue

        missing = column.map(_is_missing).astype(bool)
        if spec.required:
            fail(missing, QuarantineReason.MISSING_FIELD)

        coerced = column.map(lambda value, s=spec: _coerce_or_bad(value, s))
        bad = coerced.map(lambda value: value is _BAD).astype(bool)
        fail(bad, QuarantineReason.WRONG_TYPE)

        usable = ~missing & ~bad
        if spec.min_value is not None:
            fail(
                usable
                & coerced.map(
                    lambda v, lo=spec.min_value: v is not _BAD and v is not None and v < lo
                ),
                QuarantineReason.OUT_OF_RANGE,
            )
        if spec.max_value is not None:
            fail(
                usable
                & coerced.map(
                    lambda v, hi=spec.max_value: v is not _BAD and v is not None and v > hi
                ),
                QuarantineReason.OUT_OF_RANGE,
            )

        coerced_columns[spec.name] = coerced.map(lambda value: None if value is _BAD else value)

    if known_household_ids is not None and "household_id" in coerced_columns:
        households = coerced_columns["household_id"]
        fail(
            ~households.map(lambda h: h in known_household_ids), QuarantineReason.UNKNOWN_HOUSEHOLD
        )

    if "event_time" in coerced_columns:
        limit = (now or datetime.now(UTC)) + FUTURE_TOLERANCE
        future = coerced_columns["event_time"].map(lambda t: isinstance(t, datetime) and t > limit)
        fail(future, QuarantineReason.FUTURE_TIMESTAMP)

    return reason, coerced_columns


def validate_json_frame(
    raw_values: pd.Series,
    specs: tuple[FieldSpec, ...] = METER_READING_FIELDS,
    *,
    known_household_ids: frozenset[str] | None = None,
    now: datetime | None = None,
) -> pd.DataFrame:
    """
    Parse and validate a batch of raw JSON messages, as they come off Kafka.

    This is the speed layer's entry point: it sees bytes, some of which are
    not JSON at all. Returns one row per message with every field of `specs`
    in its TYPED form (float, UTC timestamp, string) plus `is_valid` and
    `quarantine_reason`. Malformed JSON -- or JSON that is not an object --
    is quarantined as `malformed_json`, exactly as `validate_record` does.
    """
    index = raw_values.index
    records: list[dict[str, Any]] = []
    malformed: list[bool] = []
    for raw in raw_values:
        try:
            parsed = json.loads(raw) if raw is not None else None
        except (TypeError, ValueError):
            parsed = None
        is_object = isinstance(parsed, dict)
        records.append(parsed if is_object else {})
        malformed.append(not is_object)

    columns = {
        spec.name: pd.Series(
            [record.get(spec.name) for record in records], index=index, dtype=object
        )
        for spec in specs
    }
    reason, coerced = _frame_verdicts(columns, index, specs, known_household_ids, now)
    reason = reason.mask(pd.Series(malformed, index=index), QuarantineReason.MALFORMED_JSON.value)

    typed: dict[str, pd.Series] = {}
    for spec in specs:
        values = coerced[spec.name]
        if spec.dtype in ("double", "integer"):
            typed[spec.name] = pd.to_numeric(values, errors="coerce").astype("float64")
        elif spec.dtype == "timestamp":
            typed[spec.name] = pd.to_datetime(values, utc=True, errors="coerce")
        else:
            typed[spec.name] = values.astype(object)
    out = pd.DataFrame(typed, index=index)
    out["quarantine_reason"] = reason
    out["is_valid"] = reason.isna()
    return out


# -- Enrichment ----------------------------------------------------------


def enrich_reading(record: dict[str, Any], fleet: Fleet) -> dict[str, Any]:
    """
    Join a validated reading to household reference data and derive the
    fields the serving layer actually reports on.

    `net_kwh` is the number that matters: positive means the household drew
    from the grid, negative means it exported. Computing it once here means
    the real-time dashboard and the settlement run can never disagree on the
    sign convention.
    """
    household = fleet.by_household_id.get(record["household_id"])
    consumption = float(record["power_consumption_kwh"])
    generation = float(record["solar_generation_kwh"])

    enriched = dict(record)
    enriched.update(
        {
            "tariff_tier": household.tariff_tier if household else None,
            "has_solar": household.has_solar if household else None,
            "solar_capacity_kw": household.solar_capacity_kw if household else None,
            "net_kwh": round(consumption - generation, 6),
            "is_exporting": generation > consumption,
            "renewable_share": (
                round(min(generation / consumption, 1.0), 6) if consumption > 0 else None
            ),
        }
    )
    return enriched
