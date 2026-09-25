"""
Batch-layer billing reconciliation.
 
Joins a household's daily aggregated meter readings (from the speed/lake
layer) against the daily tariff file to compute a final bill.
 
Rule (derived from spec + fixed test cases):
    net_kwh    = max(0, consumption - solar_generation * weather_factor)
    raw_bill   = net_kwh * tariff_rate
    final_bill = raw_bill * (1 - SUBSIDY_DISCOUNT) if subsidy_flag else raw_bill
 
Solar generation can only offset consumption down to zero -- a household
that generates more than it consumes is not paid for the surplus here.
"""
 
from __future__ import annotations
 
from smartgrid.common.models import DailyBill, DailyTariff
 
SUBSIDY_DISCOUNT = 0.20  # 20% off the final bill when subsidy_flag is set
 
 
def calculate_bill(
    *,
    household_id: str,
    simulated_date: str,
    total_consumption_kwh: float,
    total_solar_generation_kwh: float,
    tariff: DailyTariff,
) -> DailyBill:
    """Compute a household's final bill for one simulated day.
 
    Args:
        household_id: must match tariff.household_id (checked below).
        simulated_date: the day these aggregates cover.
        total_consumption_kwh: sum of power_consumption_kwh for the day.
        total_solar_generation_kwh: sum of solar_generation_kwh for the day.
        tariff: the matching DailyTariff row for this household/day.
 
    Raises:
        ValueError: if the aggregates don't belong to this tariff row, or
            either input is negative.
    """
    if household_id != tariff.household_id:
        raise ValueError(
            f"household mismatch: aggregates are for {household_id!r}, "
            f"tariff is for {tariff.household_id!r}"
        )
    if total_consumption_kwh < 0 or total_solar_generation_kwh < 0:
        raise ValueError("consumption and solar generation must be non-negative")
 
    effective_solar_kwh = total_solar_generation_kwh * tariff.weather_factor
    net_kwh = max(0.0, total_consumption_kwh - effective_solar_kwh)
 
    raw_bill = net_kwh * tariff.tariff_rate
    final_bill = raw_bill * (1 - SUBSIDY_DISCOUNT) if tariff.subsidy_flag else raw_bill
 
    return DailyBill(
        household_id=household_id,
        simulated_date=simulated_date,
        total_consumption_kwh=total_consumption_kwh,
        total_solar_generation_kwh=total_solar_generation_kwh,
        tariff_rate=tariff.tariff_rate,
        subsidy_flag=tariff.subsidy_flag,
        final_bill=round(final_bill, 2),
    )
 