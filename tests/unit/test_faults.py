"""
Every injected fault must reach its designed verdict in the shared validator.

This ties the streaming source to the validation both Lambda layers use: if
someone changes a fault or a rule so they no longer agree, this fails.
"""

import json
import random
from datetime import UTC, datetime

import pytest

from smartgrid.common.domain import build_fleet
from smartgrid.common.transformations import QuarantineReason, validate_record
from smartgrid.producers.faults import (
    EXPECTED_QUARANTINE,
    MESSAGE_FAULTS,
    PROFILES,
    FaultKind,
    FaultProfile,
    choose_fault,
    corrupt,
    get_profile,
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
FLEET = build_fleet(num_households=10, num_zones=2, seed=1)


def clean_payload() -> dict:
    h = FLEET.households[0]
    return {
        "event_id": f"{h.meter_id}:20260101T120000",
        "meter_id": h.meter_id,
        "household_id": h.household_id,
        "grid_zone": h.grid_zone,
        "power_consumption_kwh": 0.12,
        "solar_generation_kwh": 0.04,
        "voltage_v": 231.0,
        "event_time": NOW.isoformat(),
        "correlation_id": "abc123",
        "schema_version": "1.0.0",
    }


def verdict(sent) -> QuarantineReason | None:
    if isinstance(sent, bytes):
        try:
            sent = json.loads(sent)
        except json.JSONDecodeError:
            return QuarantineReason.MALFORMED_JSON
    result = validate_record(sent, known_household_ids=FLEET.household_ids, now=NOW)
    return None if result.ok else result.reason


@pytest.mark.parametrize("kind", list(FaultKind))
def test_every_fault_reaches_its_designed_verdict(kind):
    for seed in range(40):  # cover every random branch
        sent = corrupt(clean_payload(), kind, random.Random(seed))
        assert verdict(sent) is EXPECTED_QUARANTINE[kind], f"{kind} seed={seed}"


def test_a_clean_payload_is_valid():
    assert verdict(corrupt(clean_payload(), None, random.Random(0))) is None


def test_every_fault_has_a_designed_outcome():
    assert set(EXPECTED_QUARANTINE) == set(FaultKind)


def test_none_profile_never_injects():
    rng = random.Random(0)
    assert all(choose_fault(PROFILES["none"], rng) is None for _ in range(10_000))


def test_realistic_rates_are_honoured():
    profile = get_profile("realistic")
    rng = random.Random(42)
    draws = [choose_fault(profile, rng) for _ in range(200_000)]
    observed = sum(1 for d in draws if d is not None) / len(draws)
    assert observed == pytest.approx(profile.message_fault_rate, rel=0.05)


def test_chaos_is_harsher_than_realistic():
    assert get_profile("chaos").message_fault_rate > 5 * get_profile("realistic").message_fault_rate


def test_backfill_is_not_a_per_message_fault():
    assert FaultKind.BACKFILL not in MESSAGE_FAULTS


def test_invalid_profiles_are_rejected():
    with pytest.raises(ValueError):
        FaultProfile("bad", rates={FaultKind.DUPLICATE: 0.7, FaultKind.LATE: 0.7})
    with pytest.raises(ValueError):
        FaultProfile("bad", rates={FaultKind.BACKFILL: 0.1})
    with pytest.raises(ValueError):
        get_profile("nonexistent")
