"""
Validation, normalisation and enrichment -- shared by BOTH Lambda layers.

The speed layer (Spark Structured Streaming, Section 6) and the batch layer
(Spark batch under Airflow, Section 7) both import this module. Neither
implements its own copy of these rules.

That is deliberate. The strongest argument against Lambda is Jay Kreps'
objection that maintaining two code paths guarantees they eventually
disagree. This module is the mitigation: the rules are declared once in
schemas.py and applied by thin adapters here -- `validate_record` for
row-at-a-time Python, `validate_frame` for vectorised pandas inside Spark.
Both walk the same FieldSpec table, so a rule change lands in both layers
simultaneously or in neither.

Accepted cost: the Spark path uses pandas UDFs rather than native Catalyst
expressions, which is slower. We trade throughput for a guarantee of zero
logic drift. At production scale you would generate both the Python
predicates and the Spark expressions from the same spec -- noted as future
work in the report's limitations section.
"""

from __future__ import annotations

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


def _parse_timestamp(value: Any) -> datetime:
    """Accept ISO-8601 (with or without trailing Z) or epoch seconds."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(float(value), tz=UTC)
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

        if value is None or value == "":
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
    reason = pd.Series([None] * len(result), index=result.index, dtype="object")

    for spec in specs:
        if spec.name not in result.columns:
            if spec.required:
                reason = reason.fillna(QuarantineReason.MISSING_FIELD.value)
            continue

        column = result[spec.name]

        if spec.required:
            reason = reason.mask(
                column.isna() & reason.isna(), QuarantineReason.MISSING_FIELD.value
            )

        if spec.dtype == "double":
            numeric = pd.to_numeric(column, errors="coerce")
            bad_type = numeric.isna() & column.notna()
            reason = reason.mask(bad_type & reason.isna(), QuarantineReason.WRONG_TYPE.value)

            if spec.min_value is not None:
                below = (numeric < spec.min_value).fillna(False)
                reason = reason.mask(below & reason.isna(), QuarantineReason.OUT_OF_RANGE.value)
            if spec.max_value is not None:
                above = (numeric > spec.max_value).fillna(False)
                reason = reason.mask(above & reason.isna(), QuarantineReason.OUT_OF_RANGE.value)

    if known_household_ids is not None and "household_id" in result.columns:
        unknown = ~result["household_id"].isin(known_household_ids)
        reason = reason.mask(unknown & reason.isna(), QuarantineReason.UNKNOWN_HOUSEHOLD.value)

    if "event_time" in result.columns:
        reference = now or datetime.now(UTC)
        times = pd.to_datetime(result["event_time"], errors="coerce", utc=True)
        future = (times > (reference + FUTURE_TOLERANCE)).fillna(False)
        reason = reason.mask(future & reason.isna(), QuarantineReason.FUTURE_TIMESTAMP.value)

    result["quarantine_reason"] = reason
    result["is_valid"] = reason.isna()
    return result


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
