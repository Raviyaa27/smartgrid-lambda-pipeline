from __future__ import annotations

from datetime import UTC, date, datetime

from smartgrid.common.config import get_settings
from smartgrid.common.domain import build_fleet_from_settings
from smartgrid.common.schemas import (
    TARIFF_RECORD_FIELDS,
    WEATHER_RECORD_FIELDS,
)
from smartgrid.common.transformations import validate_record
from smartgrid.producers.daily_batch_source import (
    build_tariff_records,
    build_weather_records,
)
from smartgrid.producers.stream_producer import build_reading_payload


def test_stream_reading_is_valid() -> None:
    settings = get_settings()
    fleet = build_fleet_from_settings(settings)
    household = fleet.households[0]

    reading = build_reading_payload(
        household=household,
        sim_now=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
        event_index=1,
    )

    assert reading["household_id"] == household.household_id
    assert reading["meter_id"] == household.meter_id
    assert reading["grid_zone"] == household.grid_zone
    assert reading["power_consumption_kwh"] >= 0
    assert reading["solar_generation_kwh"] >= 0

    result = validate_record(
        reading,
        now=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )

    assert result.ok is True
    assert result.record is not None


def test_daily_tariff_source_creates_one_record_per_household() -> None:
    settings = get_settings()
    fleet = build_fleet_from_settings(settings)

    records = build_tariff_records(
        fleet=fleet,
        billing_date=date(2026, 1, 1),
    )

    assert len(records) == len(fleet.households)

    for record in records:
        result = validate_record(
            record,
            specs=TARIFF_RECORD_FIELDS,
        )
        assert result.ok is True


def test_daily_weather_source_creates_one_record_per_zone() -> None:
    settings = get_settings()
    fleet = build_fleet_from_settings(settings)

    records = build_weather_records(
        fleet=fleet,
        forecast_date=date(2026, 1, 1),
        seed=settings.sim_seed,
    )

    assert len(records) == len(fleet.zones)

    for record in records:
        result = validate_record(
            record,
            specs=WEATHER_RECORD_FIELDS,
        )
        assert result.ok is True


def test_weather_generation_is_deterministic() -> None:
    settings = get_settings()
    fleet = build_fleet_from_settings(settings)

    first = build_weather_records(
        fleet=fleet,
        forecast_date=date(2026, 1, 1),
        seed=settings.sim_seed,
    )

    second = build_weather_records(
        fleet=fleet,
        forecast_date=date(2026, 1, 1),
        seed=settings.sim_seed,
    )

    assert first == second