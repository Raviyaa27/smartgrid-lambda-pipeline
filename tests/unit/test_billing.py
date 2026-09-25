from smartgrid.batch.billing import calculate_bill
from smartgrid.common.models import DailyTariff


def test_calculate_bill_without_subsidy() -> None:
    tariff = DailyTariff(
        household_id="HH-0001",
        simulated_date="2026-01-01",
        tariff_rate=0.25,
        billing_tier="standard",
        subsidy_flag=False,
        weather_factor=1.0,
    )

    bill = calculate_bill(
        household_id="HH-0001",
        simulated_date="2026-01-01",
        total_consumption_kwh=10.0,
        total_solar_generation_kwh=2.0,
        tariff=tariff,
    )

    assert bill.final_bill == 2.0
    assert bill.total_consumption_kwh == 10.0
    assert bill.total_solar_generation_kwh == 2.0


def test_calculate_bill_with_subsidy() -> None:
    tariff = DailyTariff(
        household_id="HH-0001",
        simulated_date="2026-01-01",
        tariff_rate=0.25,
        billing_tier="standard",
        subsidy_flag=True,
        weather_factor=1.0,
    )

    bill = calculate_bill(
        household_id="HH-0001",
        simulated_date="2026-01-01",
        total_consumption_kwh=10.0,
        total_solar_generation_kwh=2.0,
        tariff=tariff,
    )

    assert bill.final_bill == 1.60


def test_solar_generation_cannot_create_negative_bill() -> None:
    tariff = DailyTariff(
        household_id="HH-0001",
        simulated_date="2026-01-01",
        tariff_rate=0.25,
        billing_tier="standard",
        subsidy_flag=False,
        weather_factor=1.0,
    )

    bill = calculate_bill(
        household_id="HH-0001",
        simulated_date="2026-01-01",
        total_consumption_kwh=2.0,
        total_solar_generation_kwh=5.0,
        tariff=tariff,
    )

    assert bill.final_bill == 0.0