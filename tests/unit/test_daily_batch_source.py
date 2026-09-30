import json
import random
from datetime import date, datetime, timedelta

import pytest

from smartgrid.common import drops, weather
from smartgrid.common.billing import DEFAULT_SCHEDULE
from smartgrid.common.domain import build_fleet
from smartgrid.producers.daily_batch_source import (
    DROP_PROFILES,
    PUBLISH_OFFSET,
    Corruption,
    DropFault,
    DropFaultProfile,
    build_household_tariffs,
    build_weather_forecast,
    corrupt_files,
    get_drop_profile,
    plan_day,
    render_drop,
)

DAY = date(2026, 1, 3)
SEED = 20260101
FLEET = build_fleet(num_households=25, num_zones=3, seed=5)


def test_one_tariff_record_per_household():
    records = build_household_tariffs(FLEET, DAY, DEFAULT_SCHEDULE)
    assert [r["household_id"] for r in records] == [h.household_id for h in FLEET]


def test_household_records_quote_their_tiers_headline_rate():
    for record in build_household_tariffs(FLEET, DAY, DEFAULT_SCHEDULE):
        assert record["tariff_rate"] == float(
            DEFAULT_SCHEDULE.headline_rate(record["billing_tier"])
        )


def test_one_forecast_per_zone_matching_the_shared_weather_model():
    records = build_weather_forecast(FLEET, DAY, SEED)
    assert [r["grid_zone"] for r in records] == list(FLEET.zones)
    for record in records:
        assert (
            record["cloud_cover_pct"]
            == weather.forecast(record["grid_zone"], DAY, SEED).cloud_cover_pct
        )


def test_the_drop_serialises_to_json():
    """Regression: serialising VALIDATED records crashed on datetime objects."""
    files = render_drop(FLEET, DAY, SEED)
    json.loads(files[drops.SCHEDULE_FILE])
    for name in (drops.HOUSEHOLDS_FILE, drops.WEATHER_FILE):
        for line in files[name].decode().splitlines():
            json.loads(line)


def test_rendering_is_byte_for_byte_reproducible():
    """What makes a restated day comparable with its original."""
    assert render_drop(FLEET, DAY, SEED) == render_drop(FLEET, DAY, SEED)


def test_different_days_differ():
    assert render_drop(FLEET, DAY, SEED) != render_drop(FLEET, DAY + timedelta(days=1), SEED)


def test_a_transit_fault_leaves_the_described_file_intact():
    files = render_drop(FLEET, DAY, SEED)
    described, uploaded = corrupt_files(files, Corruption.TRUNCATED_FILE, random.Random(0))
    assert described == files
    assert len(uploaded[drops.HOUSEHOLDS_FILE]) < len(files[drops.HOUSEHOLDS_FILE])


@pytest.mark.parametrize(
    "corruption", [c for c in Corruption if c is not Corruption.TRUNCATED_FILE]
)
def test_a_source_fault_is_shipped_faithfully(corruption):
    """The publisher sends bad data intact: only content checks can catch it."""
    described, uploaded = corrupt_files(render_drop(FLEET, DAY, SEED), corruption, random.Random(0))
    assert described == uploaded
    assert described != render_drop(FLEET, DAY, SEED)


def test_day_plans_are_deterministic():
    profile = get_drop_profile("chaos")
    assert plan_day(DAY, SEED, profile) == plan_day(DAY, SEED, profile)


def test_a_clean_drop_lands_early_on_its_own_day():
    plan = plan_day(DAY, SEED, get_drop_profile("none"))
    assert plan.fault is None
    assert (
        plan.publish_at
        == datetime.combine(DAY, datetime.min.time()).replace(tzinfo=plan.publish_at.tzinfo)
        + PUBLISH_OFFSET
    )


def test_a_late_drop_lands_after_its_day_has_ended():
    always_late = DropFaultProfile("late", late=1.0)
    for offset in range(30):
        day = DAY + timedelta(days=offset)
        plan = plan_day(day, SEED, always_late)
        assert plan.fault is DropFault.LATE
        assert plan.publish_at.date() == day + timedelta(days=1)


def test_realistic_fault_rates_are_honoured_over_many_days():
    profile = get_drop_profile("realistic")
    plans = [plan_day(DAY + timedelta(days=d), SEED, profile) for d in range(4000)]
    for fault, rate in (
        (DropFault.LATE, profile.late),
        (DropFault.CORRUPT, profile.corrupt),
        (DropFault.MISSING, profile.missing),
    ):
        observed = sum(1 for p in plans if p.fault is fault) / len(plans)
        assert observed == pytest.approx(rate, abs=0.015), fault


def test_every_corrupt_plan_names_a_corruption():
    profile = DropFaultProfile("corrupt", corrupt=1.0)
    kinds = {plan_day(DAY + timedelta(days=d), SEED, profile).corruption for d in range(300)}
    assert kinds == set(Corruption)


def test_profiles_match_the_stream_profiles():
    from smartgrid.producers.faults import PROFILES

    assert set(DROP_PROFILES) == set(PROFILES)


def test_invalid_drop_profiles_are_rejected():
    with pytest.raises(ValueError):
        DropFaultProfile("bad", late=0.6, corrupt=0.6)
    with pytest.raises(ValueError):
        get_drop_profile("nonexistent")
