from datetime import date
from decimal import Decimal

import pytest

from smartgrid.common.billing import (
    DAYS_PER_BILLING_MONTH,
    EXPORT_CREDIT_RATE,
    TIER_BLOCKS,
    block_energy_charge,
    calculate_bill,
    prorate_blocks,
)
from smartgrid.common.domain import build_fleet

BILLING_DATE = date(2026, 1, 1)
MONTH = DAYS_PER_BILLING_MONTH


def test_consumption_inside_the_first_block_uses_only_that_rate():
    total, lines = block_energy_charge(Decimal("20"), TIER_BLOCKS["DOMESTIC_LOW"])
    assert total == Decimal("160.00")          # 20 * 8.00
    assert len(lines) == 1


def test_crossing_a_boundary_prices_each_block_marginally():
    """45 kWh = 30 @ 8.00 + 15 @ 10.00. NOT 45 @ 10.00 -- that is the whole point."""
    total, lines = block_energy_charge(Decimal("45"), TIER_BLOCKS["DOMESTIC_LOW"])
    assert total == Decimal("390.00")
    assert [line.amount for line in lines] == [Decimal("240.00"), Decimal("150.00")]


def test_the_unbounded_final_block_absorbs_the_remainder():
    total, lines = block_energy_charge(Decimal("200"), TIER_BLOCKS["DOMESTIC_LOW"])
    # 30*8 + 30*10 + 30*16 + 110*50 = 240 + 300 + 480 + 5500
    assert total == Decimal("6520.00")
    assert lines[-1].units_kwh == Decimal("110.000")


def test_zero_consumption_costs_nothing():
    total, lines = block_energy_charge(Decimal("0"), TIER_BLOCKS["DOMESTIC_STD"])
    assert total == Decimal("0.00")
    assert lines == ()


def test_negative_consumption_is_rejected():
    with pytest.raises(ValueError):
        block_energy_charge(Decimal("-1"), TIER_BLOCKS["DOMESTIC_STD"])


def test_proration_scales_caps_but_never_rates():
    daily = prorate_blocks(TIER_BLOCKS["DOMESTIC_LOW"], Decimal("1"))
    assert daily[0].upper_kwh == Decimal("30") / MONTH
    assert daily[0].rate == Decimal("8.00")
    assert daily[-1].upper_kwh is None


def test_daily_block_caps_are_exact():
    """
    Regression: dividing before multiplying left the caps at 0.9999...9, one
    ulp below their intended value.
    """
    daily = prorate_blocks(TIER_BLOCKS["DOMESTIC_LOW"], Decimal("1"))
    assert daily[0].upper_kwh == Decimal("1")
    assert daily[1].upper_kwh == Decimal("2")
    assert daily[2].upper_kwh == Decimal("3")


def test_consumption_exactly_on_a_cap_stays_in_the_lower_block():
    """The boundary case the precision bug would have mispriced."""
    blocks = prorate_blocks(TIER_BLOCKS["DOMESTIC_LOW"], Decimal("1"))
    total, lines = block_energy_charge(Decimal("1"), blocks)
    assert total == Decimal("8.00")
    assert len(lines) == 1


def test_bill_totals_reconcile():
    bill = calculate_bill(
        household_id="HH-00001",
        billing_date=BILLING_DATE,
        tariff_tier="DOMESTIC_STD",
        consumption_kwh=12.0,
        generation_kwh=4.0,
        subsidy_flag=False,
        settlement_days=MONTH,
    )
    assert bill.net_import_kwh == Decimal("8.000")
    assert bill.net_export_kwh == Decimal("0.000")
    expected = bill.energy_charge - bill.subsidy_amount + bill.fixed_charge - bill.export_credit
    assert bill.total_payable == expected


def test_subsidy_discounts_energy_but_not_the_fixed_charge():
    kwargs = dict(
        household_id="HH-00002",
        billing_date=BILLING_DATE,
        tariff_tier="DOMESTIC_LOW",
        consumption_kwh=50.0,
        generation_kwh=0.0,
        settlement_days=MONTH,
    )
    plain = calculate_bill(**kwargs, subsidy_flag=False)
    subsidised = calculate_bill(**kwargs, subsidy_flag=True)

    assert subsidised.subsidy_amount == plain.energy_charge * Decimal("0.25")
    assert subsidised.fixed_charge == plain.fixed_charge
    assert subsidised.total_payable < plain.total_payable


def test_net_metering_bills_only_the_net_position():
    bill = calculate_bill(
        household_id="HH-00003",
        billing_date=BILLING_DATE,
        tariff_tier="DOMESTIC_STD",
        consumption_kwh=10.0,
        generation_kwh=25.0,
        subsidy_flag=False,
        settlement_days=MONTH,
    )
    assert bill.net_import_kwh == Decimal("0.000")
    assert bill.net_export_kwh == Decimal("15.000")
    assert bill.energy_charge == Decimal("0.00")
    assert bill.export_credit == Decimal("15") * EXPORT_CREDIT_RATE


def test_a_heavy_exporter_can_finish_in_credit():
    bill = calculate_bill(
        household_id="HH-00004",
        billing_date=BILLING_DATE,
        tariff_tier="DOMESTIC_LOW",
        consumption_kwh=1.0,
        generation_kwh=80.0,
        subsidy_flag=False,
        settlement_days=MONTH,
    )
    assert bill.total_payable < Decimal("0.00")


def test_daily_settlement_prorates_the_fixed_charge():
    daily = calculate_bill(
        household_id="HH-00007",
        billing_date=BILLING_DATE,
        tariff_tier="DOMESTIC_STD",
        consumption_kwh=2.0,
        generation_kwh=0.0,
        subsidy_flag=False,
    )
    assert daily.fixed_charge == Decimal("10.00")   # 300.00 / 30


def test_money_is_never_float():
    bill = calculate_bill(
        household_id="HH-00005",
        billing_date=BILLING_DATE,
        tariff_tier="DOMESTIC_HIGH",
        consumption_kwh=7.3,
        generation_kwh=1.1,
        subsidy_flag=True,
    )
    for value in (bill.energy_charge, bill.fixed_charge, bill.total_payable, bill.export_credit):
        assert isinstance(value, Decimal)
    assert bill.total_payable.as_tuple().exponent == -2


def test_unknown_tier_is_rejected():
    with pytest.raises(ValueError, match="unknown tariff tier"):
        calculate_bill(
            household_id="HH-00006",
            billing_date=BILLING_DATE,
            tariff_tier="NOT_A_TIER",
            consumption_kwh=1.0,
            generation_kwh=0.0,
            subsidy_flag=False,
        )


def test_bill_serialises_without_floats():
    bill = calculate_bill(
        household_id="HH-00008",
        billing_date=BILLING_DATE,
        tariff_tier="DOMESTIC_STD",
        consumption_kwh=5.0,
        generation_kwh=1.0,
        subsidy_flag=False,
    )
    row = bill.to_dict()
    assert not any(isinstance(v, float) for v in row.values())
    assert row["billing_date"] == "2026-01-01"


def test_fleet_generation_is_deterministic():
    a = build_fleet(num_households=50, num_zones=4, seed=7)
    b = build_fleet(num_households=50, num_zones=4, seed=7)
    assert a.households == b.households


def test_different_seeds_produce_different_fleets():
    a = build_fleet(num_households=50, num_zones=4, seed=7)
    b = build_fleet(num_households=50, num_zones=4, seed=8)
    assert a.households != b.households
