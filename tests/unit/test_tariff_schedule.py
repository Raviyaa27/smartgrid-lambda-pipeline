"""The tariff as data: what makes a retroactive revision possible."""

import json
from datetime import date
from decimal import Decimal

import pytest

from smartgrid.common.billing import (
    DEFAULT_SCHEDULE,
    EXPORT_CREDIT_RATE,
    FIXED_CHARGES,
    SUBSIDY_FRACTION,
    TIER_BLOCKS,
    TariffBlock,
    TariffSchedule,
    TierTariff,
    calculate_bill,
)

DAY = date(2026, 1, 3)


def bill(schedule=DEFAULT_SCHEDULE, tier="DOMESTIC_STD", kwh=6.0):
    return calculate_bill(
        household_id="HH-00001",
        billing_date=DAY,
        tariff_tier=tier,
        consumption_kwh=kwh,
        generation_kwh=0.0,
        subsidy_flag=False,
        schedule=schedule,
    )


def test_default_schedule_is_built_from_the_published_constants():
    for tier, blocks in TIER_BLOCKS.items():
        assert DEFAULT_SCHEDULE.tier(tier).blocks == blocks
        assert DEFAULT_SCHEDULE.tier(tier).fixed_charge == FIXED_CHARGES[tier]
    assert DEFAULT_SCHEDULE.export_credit_rate == EXPORT_CREDIT_RATE
    assert DEFAULT_SCHEDULE.subsidy_fraction == SUBSIDY_FRACTION


def test_default_schedule_is_valid():
    assert DEFAULT_SCHEDULE.problems() == []


def test_schedule_survives_a_round_trip_through_json():
    restored = TariffSchedule.from_dict(json.loads(json.dumps(DEFAULT_SCHEDULE.to_dict())))
    assert restored == DEFAULT_SCHEDULE


def test_amounts_are_strings_in_json_never_floats():
    doc = DEFAULT_SCHEDULE.to_dict()
    assert isinstance(doc["export_credit_rate"], str)
    for tier in doc["tiers"].values():
        assert all(isinstance(block["rate"], str) for block in tier["blocks"])


def test_a_revision_reprices_the_same_consumption():
    revised = DEFAULT_SCHEDULE.with_rate_change(Decimal("1.10"), ["DOMESTIC_STD"])
    before, after = bill(), bill(revised)
    assert after.energy_charge > before.energy_charge
    assert after.fixed_charge == before.fixed_charge


def test_a_revision_touches_only_the_chosen_tiers():
    revised = DEFAULT_SCHEDULE.with_rate_change(Decimal("1.10"), ["DOMESTIC_STD"])
    assert revised.tier("DOMESTIC_LOW") == DEFAULT_SCHEDULE.tier("DOMESTIC_LOW")
    assert revised.headline_rate("DOMESTIC_STD") == Decimal("13.20")


def test_revised_rates_are_rounded_to_the_cent():
    revised = DEFAULT_SCHEDULE.with_rate_change(Decimal("1.0333"))
    for tariff in revised.tiers.values():
        for block in tariff.blocks:
            assert block.rate.as_tuple().exponent == -2


def test_revising_an_unknown_tier_is_rejected():
    with pytest.raises(ValueError):
        DEFAULT_SCHEDULE.with_rate_change(Decimal("1.1"), ["NOT_A_TIER"])


def test_calculate_bill_uses_the_schedule_it_is_given():
    doubled = DEFAULT_SCHEDULE.with_rate_change(Decimal("2"))
    assert bill(doubled).energy_charge == bill().energy_charge * 2


def _schedule(blocks, fixed="100", export="10", subsidy="0.25"):
    return TariffSchedule(
        tiers={"T": TierTariff(tuple(blocks), Decimal(fixed))},
        export_credit_rate=Decimal(export),
        subsidy_fraction=Decimal(subsidy),
    )


@pytest.mark.parametrize(
    "schedule, fragment",
    [
        (_schedule([TariffBlock(None, Decimal("-5"))]), "not positive"),
        (_schedule([TariffBlock(Decimal("10"), Decimal("5"))]), "must be unbounded"),
        (
            _schedule([TariffBlock(None, Decimal("5")), TariffBlock(None, Decimal("6"))]),
            "only the last block",
        ),
        (
            _schedule(
                [
                    TariffBlock(Decimal("20"), Decimal("5")),
                    TariffBlock(Decimal("10"), Decimal("6")),
                    TariffBlock(None, Decimal("7")),
                ]
            ),
            "does not exceed",
        ),
        (_schedule([TariffBlock(None, Decimal("5"))], fixed="-1"), "fixed charge"),
        (_schedule([TariffBlock(None, Decimal("5"))], subsidy="1.5"), "subsidy fraction"),
        (_schedule([TariffBlock(None, Decimal("5"))], export="-1"), "export credit"),
    ],
)
def test_problems_are_reported_not_raised(schedule, fragment):
    problems = schedule.problems()
    assert any(fragment in p for p in problems), problems


def test_a_structurally_broken_document_raises():
    with pytest.raises(ValueError):
        TariffSchedule.from_dict({"tiers": {"T": {"blocks": [{"rate": "abc"}]}}})
    with pytest.raises(ValueError):
        TariffSchedule.from_dict({"no": "tiers"})
