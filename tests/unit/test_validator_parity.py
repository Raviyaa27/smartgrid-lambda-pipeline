"""
The row validator and the frame validators must reach the SAME verdict.

This is the claim ADR-0004 makes about Lambda's two code paths, so it is
tested adversarially: thousands of randomly damaged readings -- missing,
empty, NaN, wrong-typed, out-of-range, future-dated, unregistered, not JSON,
not an object -- go through `validate_record` (row by row, as the batch
layer and the inspection tools use it) and through `validate_json_frame` /
`validate_frame` (vectorised, as the speed layer uses it). Every verdict
must match.
"""

import json
import math
import random
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from smartgrid.common.domain import build_fleet
from smartgrid.common.schemas import METER_READING_FIELDS
from smartgrid.common.transformations import (
    validate_frame,
    validate_json_frame,
    validate_record,
)
from smartgrid.producers.faults import FaultKind, corrupt

NOW = datetime(2026, 1, 5, 12, 0, tzinfo=UTC)
FLEET = build_fleet(num_households=30, num_zones=3, seed=9)
KNOWN = FLEET.household_ids
FIELDS = [spec.name for spec in METER_READING_FIELDS]

_JUNK = [
    None,
    "",
    "   ",
    float("nan"),
    True,
    False,
    [],
    {},
    [1, 2],
    {"a": 1},
    "abc",
    "12.5",
    12,
    -3.2,
    0,
    1e9,
    -0.0,
    float("inf"),
    "2026-01-05T11:00:00+00:00",
    "2026-01-05T11:00:00Z",
    "2026-13-45",
    "yesterday",
    1767614400,
    (NOW + timedelta(hours=2)).isoformat(),
    "HH-99999",
]


def valid_record(rng: random.Random) -> dict:
    household = rng.choice(FLEET.households)
    when = NOW - timedelta(minutes=rng.randint(0, 600))
    return {
        "event_id": f"{household.meter_id}:{when:%Y%m%dT%H%M%S}",
        "meter_id": household.meter_id,
        "household_id": household.household_id,
        "grid_zone": household.grid_zone,
        "power_consumption_kwh": round(rng.uniform(0, 3), 6),
        "solar_generation_kwh": round(rng.uniform(0, 2), 6),
        "voltage_v": round(rng.uniform(200, 260), 1),
        "event_time": when.isoformat(),
        "ingest_time": when.isoformat(),
        "correlation_id": f"{rng.getrandbits(48):012x}",
        "schema_version": "1.0.0",
    }


def damaged_record(rng: random.Random) -> dict:
    record = valid_record(rng)
    for _ in range(rng.choice([0, 1, 1, 2, 3])):
        field = rng.choice(FIELDS)
        if rng.random() < 0.25:
            record.pop(field, None)
        else:
            record[field] = rng.choice(_JUNK)
    return record


def to_raw(rng: random.Random, record: dict) -> str:
    roll = rng.random()
    if roll < 0.03:
        return json.dumps(record)[: rng.randint(1, 40)]  # truncated
    if roll < 0.05:
        return json.dumps(rng.choice([[1, 2], 42, "text", None]))  # JSON, not an object
    return json.dumps(record, allow_nan=True)


def row_verdict(raw: str) -> str:
    try:
        parsed = json.loads(raw)
    except ValueError:
        return "malformed_json"
    result = validate_record(parsed, known_household_ids=KNOWN, now=NOW)
    return "valid" if result.ok else result.reason.value


def frame_verdicts(raws: list[str]) -> list[str]:
    frame = validate_json_frame(pd.Series(raws), known_household_ids=KNOWN, now=NOW)
    return [r if isinstance(r, str) else "valid" for r in frame["quarantine_reason"]]


@pytest.mark.parametrize("seed", range(8))
def test_row_and_frame_validators_agree_on_random_damage(seed):
    rng = random.Random(seed)
    raws = [to_raw(rng, damaged_record(rng)) for _ in range(600)]
    rows = [row_verdict(raw) for raw in raws]
    frame = frame_verdicts(raws)
    disagreements = [(raw, r, f) for raw, r, f in zip(raws, rows, frame, strict=True) if r != f]
    assert not disagreements, disagreements[:5]
    # The fuzz must actually exercise the rules, not just valid records.
    assert len(set(rows)) >= 6, set(rows)


@pytest.mark.parametrize("kind", list(FaultKind))
def test_validators_agree_on_every_injected_fault(kind):
    rng = random.Random(1)
    raws = []
    for _ in range(50):
        sent = corrupt(valid_record(rng), kind, rng)
        raws.append(sent.decode() if isinstance(sent, bytes) else json.dumps(sent))
    assert [row_verdict(r) for r in raws] == frame_verdicts(raws)


def test_validate_frame_agrees_on_dataframes_pandas_builds_itself():
    """pandas turns absent keys into NaN and may infer numpy dtypes."""
    rng = random.Random(3)
    records = [damaged_record(rng) for _ in range(400)]
    frame = validate_frame(pd.DataFrame(records), known_household_ids=KNOWN, now=NOW)
    for record, (_, row) in zip(records, frame.iterrows(), strict=True):
        clean = {k: v for k, v in record.items()}
        result = validate_record(clean, known_household_ids=KNOWN, now=NOW)
        expected = None if result.ok else result.reason.value
        got = row["quarantine_reason"] if isinstance(row["quarantine_reason"], str) else None
        assert got == expected, (record, got, expected)


def test_an_all_boolean_column_is_still_rejected_as_a_number():
    records = [{**valid_record(random.Random(i)), "power_consumption_kwh": True} for i in range(5)]
    frame = validate_frame(pd.DataFrame(records), known_household_ids=KNOWN, now=NOW)
    assert set(frame["quarantine_reason"]) == {"wrong_type"}


def test_valid_rows_come_back_typed_for_spark():
    rng = random.Random(4)
    raws = [json.dumps(valid_record(rng)) for _ in range(20)]
    frame = validate_json_frame(pd.Series(raws), known_household_ids=KNOWN, now=NOW)
    assert frame["is_valid"].all()
    assert frame["power_consumption_kwh"].dtype == "float64"
    assert str(frame["event_time"].dtype) == "datetime64[ns, UTC]"
    assert not math.isnan(frame["power_consumption_kwh"].iloc[0])


def test_a_nan_measurement_counts_as_missing_in_both():
    record = {**valid_record(random.Random(0)), "power_consumption_kwh": float("nan")}
    raw = json.dumps(record, allow_nan=True)
    assert row_verdict(raw) == "missing_field"
    assert frame_verdicts([raw]) == ["missing_field"]


@pytest.mark.parametrize("value", [float("inf"), 1e20, -1e20])
def test_an_out_of_range_timestamp_is_rejected_not_raised(value):
    """
    Regression: `datetime.fromtimestamp(inf)` raised OverflowError, which
    escaped both validators. In Spark that fails the whole micro-batch, and
    the restarted query re-reads the same message -- a poison pill.
    """
    record = {**valid_record(random.Random(0)), "event_time": value}
    raw = json.dumps(record, allow_nan=True)
    assert row_verdict(raw) == "wrong_type"
    assert frame_verdicts([raw]) == ["wrong_type"]
