from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from smartgrid.common.domain import build_fleet
from smartgrid.common.transformations import (
    QuarantineReason,
    enrich_reading,
    validate_frame,
    validate_record,
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def fleet():
    return build_fleet(num_households=20, num_zones=3, seed=42)


def good_reading(**overrides):
    record = {
        "event_id": "evt-0001",
        "meter_id": "MTR-00001",
        "household_id": "HH-00001",
        "grid_zone": "ZONE-B",
        "power_consumption_kwh": 0.42,
        "solar_generation_kwh": 0.10,
        "event_time": NOW.isoformat(),
    }
    record.update(overrides)
    return record


def test_valid_reading_is_accepted_and_coerced(fleet):
    result = validate_record(good_reading(), known_household_ids=fleet.household_ids, now=NOW)
    assert result.ok
    assert isinstance(result.record["power_consumption_kwh"], float)
    assert isinstance(result.record["event_time"], datetime)


def test_missing_required_field_is_quarantined(fleet):
    record = good_reading()
    del record["meter_id"]
    result = validate_record(record, known_household_ids=fleet.household_ids, now=NOW)
    assert result.reason is QuarantineReason.MISSING_FIELD


def test_negative_consumption_is_out_of_range(fleet):
    result = validate_record(
        good_reading(power_consumption_kwh=-1.0),
        known_household_ids=fleet.household_ids,
        now=NOW,
    )
    assert result.reason is QuarantineReason.OUT_OF_RANGE


def test_absurd_consumption_is_out_of_range(fleet):
    result = validate_record(
        good_reading(power_consumption_kwh=9_999.0),
        known_household_ids=fleet.household_ids,
        now=NOW,
    )
    assert result.reason is QuarantineReason.OUT_OF_RANGE


def test_non_numeric_consumption_is_wrong_type(fleet):
    result = validate_record(
        good_reading(power_consumption_kwh="not-a-number"),
        known_household_ids=fleet.household_ids,
        now=NOW,
    )
    assert result.reason is QuarantineReason.WRONG_TYPE


def test_unknown_household_is_rejected(fleet):
    result = validate_record(
        good_reading(household_id="HH-99999"),
        known_household_ids=fleet.household_ids,
        now=NOW,
    )
    assert result.reason is QuarantineReason.UNKNOWN_HOUSEHOLD


def test_future_timestamp_is_rejected(fleet):
    future = (NOW + timedelta(hours=1)).isoformat()
    result = validate_record(
        good_reading(event_time=future), known_household_ids=fleet.household_ids, now=NOW
    )
    assert result.reason is QuarantineReason.FUTURE_TIMESTAMP


def test_small_clock_skew_is_tolerated(fleet):
    """Meters drift by seconds; that is not a fault worth quarantining."""
    slightly_ahead = (NOW + timedelta(minutes=2)).isoformat()
    result = validate_record(
        good_reading(event_time=slightly_ahead),
        known_household_ids=fleet.household_ids,
        now=NOW,
    )
    assert result.ok


def test_optional_field_may_be_absent(fleet):
    result = validate_record(good_reading(), known_household_ids=fleet.household_ids, now=NOW)
    assert result.ok
    assert result.record["voltage_v"] is None


def test_non_dict_payload_is_malformed(fleet):
    result = validate_record(["not", "an", "object"], known_household_ids=fleet.household_ids)
    assert result.reason is QuarantineReason.MALFORMED_JSON


def test_vectorised_validation_agrees_with_row_validation(fleet):
    """
    The guarantee the whole Lambda argument rests on: both layers apply the
    same rules to the same data and reach the same verdict.
    """
    records = [
        good_reading(event_id="a"),
        good_reading(event_id="b", power_consumption_kwh=-5.0),
        good_reading(event_id="c", household_id="HH-99999"),
        good_reading(event_id="d", power_consumption_kwh=9_999.0),
    ]

    row_verdicts = [
        validate_record(r, known_household_ids=fleet.household_ids, now=NOW).ok for r in records
    ]
    frame_verdicts = validate_frame(
        pd.DataFrame(records), known_household_ids=fleet.household_ids, now=NOW
    )["is_valid"].tolist()

    assert row_verdicts == frame_verdicts == [True, False, False, False]


def test_vectorised_validation_names_the_same_reason(fleet):
    frame = validate_frame(
        pd.DataFrame([good_reading(power_consumption_kwh=-5.0)]),
        known_household_ids=fleet.household_ids,
        now=NOW,
    )
    assert frame["quarantine_reason"].iloc[0] == QuarantineReason.OUT_OF_RANGE.value


def test_enrichment_derives_net_position(fleet):
    accepted = validate_record(
        good_reading(power_consumption_kwh=1.0, solar_generation_kwh=0.25),
        known_household_ids=fleet.household_ids,
        now=NOW,
    )
    enriched = enrich_reading(accepted.record, fleet)
    assert enriched["net_kwh"] == pytest.approx(0.75)
    assert enriched["is_exporting"] is False
    assert enriched["renewable_share"] == pytest.approx(0.25)


def test_enrichment_flags_an_exporting_household(fleet):
    accepted = validate_record(
        good_reading(power_consumption_kwh=0.20, solar_generation_kwh=1.50),
        known_household_ids=fleet.household_ids,
        now=NOW,
    )
    enriched = enrich_reading(accepted.record, fleet)
    assert enriched["net_kwh"] == pytest.approx(-1.30)
    assert enriched["is_exporting"] is True


def test_enrichment_attaches_reference_data(fleet):
    accepted = validate_record(
        good_reading(), known_household_ids=fleet.household_ids, now=NOW
    )
    enriched = enrich_reading(accepted.record, fleet)
    assert enriched["tariff_tier"] == fleet.by_household_id["HH-00001"].tariff_tier
