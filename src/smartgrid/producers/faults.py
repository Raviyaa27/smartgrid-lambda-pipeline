"""
Fault injection for the meter simulator.

A source that only ever emits clean data proves nothing about the pipeline
downstream of it. Every defence we build -- validation, the dead-letter
queue, deduplication, watermarks, late-data reconciliation, the
no-data alert -- needs a fault to defend against. This module supplies them,
at controlled rates, and records the ground truth.

Two families:

  Content faults change what a reading SAYS. Validation must reject them
  and route them to the DLQ with the right reason.

  Delivery faults change WHEN or HOW OFTEN a valid reading arrives. The
  reading is correct and must be kept; the pipeline must handle it by
  deduplication (DUPLICATE) or by event-time processing (LATE, BACKFILL).

Every injected message carries an `injected_fault` Kafka header. The
pipeline never reads it -- validation must reach its verdict from the
payload alone -- but `scripts/inspect_stream.py` compares the header
against the pipeline's verdict to measure detection accuracy.
"""

from __future__ import annotations

import json
import random
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from smartgrid.common.transformations import QuarantineReason


class FaultKind(StrEnum):
    # Content faults
    MISSING_FIELD = "missing_field"
    NEGATIVE_VALUE = "negative_value"
    SPIKE = "spike"
    MALFORMED_JSON = "malformed_json"
    UNKNOWN_HOUSEHOLD = "unknown_household"
    # Delivery faults
    DUPLICATE = "duplicate"
    LATE = "late"
    BACKFILL = "backfill"  # a meter reconnecting and uploading readings it missed


# The verdict the pipeline must reach for each fault. None means the record
# is VALID and must be accepted; the fault is handled downstream rather than
# by validation.
EXPECTED_QUARANTINE: Mapping[FaultKind, QuarantineReason | None] = {
    FaultKind.MISSING_FIELD: QuarantineReason.MISSING_FIELD,
    FaultKind.NEGATIVE_VALUE: QuarantineReason.OUT_OF_RANGE,
    FaultKind.SPIKE: QuarantineReason.OUT_OF_RANGE,
    FaultKind.MALFORMED_JSON: QuarantineReason.MALFORMED_JSON,
    FaultKind.UNKNOWN_HOUSEHOLD: QuarantineReason.UNKNOWN_HOUSEHOLD,
    FaultKind.DUPLICATE: None,
    FaultKind.LATE: None,
    FaultKind.BACKFILL: None,
}

# Faults chosen per message. BACKFILL is not: it arises from meter outages.
MESSAGE_FAULTS: tuple[FaultKind, ...] = (
    FaultKind.MISSING_FIELD,
    FaultKind.NEGATIVE_VALUE,
    FaultKind.SPIKE,
    FaultKind.MALFORMED_JSON,
    FaultKind.UNKNOWN_HOUSEHOLD,
    FaultKind.DUPLICATE,
    FaultKind.LATE,
)

# Fields a faulty meter firmware might omit. All are required by the schema.
_DROPPABLE_FIELDS: tuple[str, ...] = (
    "meter_id",
    "household_id",
    "grid_zone",
    "power_consumption_kwh",
    "event_time",
)


@dataclass(frozen=True)
class FaultProfile:
    """
    Fault rates for one run.

    `rates` are per-message probabilities; at most one fault is applied to
    any message, so they must sum to at most 1. `outage_start` is the chance
    per meter per round that a meter drops offline; `outage_backfill` is the
    chance that it uploads the missed readings when it reconnects.
    """

    name: str
    rates: Mapping[FaultKind, float] = field(default_factory=dict)
    outage_start: float = 0.0
    outage_backfill: float = 0.0

    def __post_init__(self) -> None:
        unknown = set(self.rates) - set(MESSAGE_FAULTS)
        if unknown:
            raise ValueError(f"not message-level faults: {sorted(unknown)}")
        if any(rate < 0 for rate in self.rates.values()):
            raise ValueError("fault rates cannot be negative")
        if sum(self.rates.values()) > 1.0:
            raise ValueError("message fault rates must sum to at most 1")
        if not 0.0 <= self.outage_start <= 1.0 or not 0.0 <= self.outage_backfill <= 1.0:
            raise ValueError("outage probabilities must lie in [0, 1]")

    @property
    def message_fault_rate(self) -> float:
        return sum(self.rates.values())


PROFILES: dict[str, FaultProfile] = {
    "none": FaultProfile(name="none"),
    # ~2.7% of messages faulty, and a handful of meter outages per simulated
    # day -- enough to exercise every defence without dominating the data.
    "realistic": FaultProfile(
        name="realistic",
        rates={
            FaultKind.MISSING_FIELD: 0.002,
            FaultKind.NEGATIVE_VALUE: 0.002,
            FaultKind.SPIKE: 0.001,
            FaultKind.MALFORMED_JSON: 0.001,
            FaultKind.UNKNOWN_HOUSEHOLD: 0.001,
            FaultKind.DUPLICATE: 0.010,
            FaultKind.LATE: 0.010,
        },
        outage_start=0.0003,
        outage_backfill=0.85,
    ),
    # Ten times the realistic rates: for demonstrating alerts and the DLQ.
    "chaos": FaultProfile(
        name="chaos",
        rates={
            FaultKind.MISSING_FIELD: 0.02,
            FaultKind.NEGATIVE_VALUE: 0.02,
            FaultKind.SPIKE: 0.01,
            FaultKind.MALFORMED_JSON: 0.01,
            FaultKind.UNKNOWN_HOUSEHOLD: 0.01,
            FaultKind.DUPLICATE: 0.05,
            FaultKind.LATE: 0.05,
        },
        outage_start=0.003,
        outage_backfill=0.85,
    ),
}


def get_profile(name: str) -> FaultProfile:
    try:
        return PROFILES[name]
    except KeyError:
        raise ValueError(
            f"unknown fault profile {name!r}; choose from {sorted(PROFILES)}"
        ) from None


def choose_fault(profile: FaultProfile, rng: random.Random) -> FaultKind | None:
    """Draw at most one fault for a message."""
    draw = rng.random()
    cumulative = 0.0
    for kind in MESSAGE_FAULTS:
        cumulative += profile.rates.get(kind, 0.0)
        if draw < cumulative:
            return kind
    return None


def corrupt(
    payload: dict[str, Any], kind: FaultKind | None, rng: random.Random
) -> dict[str, Any] | bytes:
    """
    Apply a content fault. Returns a dict to be serialised as JSON, or raw
    bytes for MALFORMED_JSON. Delivery faults leave the content untouched.
    """
    match kind:
        case FaultKind.MISSING_FIELD:
            damaged = dict(payload)
            del damaged[rng.choice(_DROPPABLE_FIELDS)]
            return damaged
        case FaultKind.NEGATIVE_VALUE:
            # A sign error in firmware: plausible magnitude, impossible sign.
            return {**payload, "power_consumption_kwh": -round(rng.uniform(0.01, 5.0), 6)}
        case FaultKind.SPIKE:
            # A sensor glitch: far beyond what one household can draw in one
            # interval, so it must never reach a bill.
            return {**payload, "power_consumption_kwh": round(rng.uniform(200.0, 5000.0), 3)}
        case FaultKind.MALFORMED_JSON:
            # A truncated transmission.
            text = json.dumps(payload, default=str)
            return text[: len(text) // 2].encode("utf-8")
        case FaultKind.UNKNOWN_HOUSEHOLD:
            # A meter installed but never registered for billing.
            return {**payload, "household_id": f"HH-{rng.randint(90000, 99999)}"}
        case _:
            return payload
