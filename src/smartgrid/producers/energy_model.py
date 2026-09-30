"""
The physical model behind the simulated meter readings.

Two quantities per household per interval: energy drawn (load) and energy
generated (rooftop solar). Both start as instantaneous POWER in kW and are
converted to ENERGY in kWh over the interval the reading covers:

    energy_kwh = power_kw * interval_hours

The interval is SIMULATED time. A meter emitting every 2 real seconds at
288x compression covers 576 simulated seconds -- 9.6 minutes -- per reading.
Computing energy over the 2 real seconds instead would undercount every
household's daily consumption by roughly 288x, so the tiered tariff would
never leave its first block.

Load profile. `Household.base_load_kw` is the household's AVERAGE draw. The
diurnal shape is normalised so its mean over a day is exactly 1.0, which
keeps that meaning intact: a 0.5 kW household uses 0.5 x 24 = 12 kWh per
day on average, whatever the shape.

Solar profile. Zero outside daylight, a sine arc peaking at solar noon,
scaled by the day's irradiance and a performance ratio for real-world
losses (inverter, temperature, soiling, wiring). Output can never exceed
rated capacity. A clear day yields about 5.7 kWh per kW of capacity, in line
with tropical rooftop yields.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import datetime
from functools import cache

SUNRISE_HOUR = 6.0
SUNSET_HOUR = 18.0
PERFORMANCE_RATIO = 0.75


def hour_of_day(moment: datetime) -> float:
    """Fractional hour, 0.0 <= h < 24.0."""
    return moment.hour + moment.minute / 60.0 + moment.second / 3600.0


# -- Solar ---------------------------------------------------------------


def solar_power_kw(capacity_kw: float, hour: float, irradiance: float) -> float:
    """Instantaneous PV output. Zero at night; never above rated capacity."""
    if capacity_kw <= 0.0 or not SUNRISE_HOUR < hour < SUNSET_HOUR:
        return 0.0
    position = (hour - SUNRISE_HOUR) / (SUNSET_HOUR - SUNRISE_HOUR)
    clear_sky = math.sin(math.pi * position)  # 0 at sunrise, 1 at noon
    return capacity_kw * PERFORMANCE_RATIO * clear_sky * max(0.0, min(1.0, irradiance))


# -- Load ----------------------------------------------------------------


def _bump(hour: float, centre: float, width: float) -> float:
    """Gaussian bump on a 24-hour circle, so an evening peak wraps past midnight."""
    distance = abs(hour - centre)
    distance = min(distance, 24.0 - distance)
    return math.exp(-0.5 * (distance / width) ** 2)


def _domestic_shape(hour: float) -> float:
    # Overnight baseline, a morning peak, a small midday bump, and the
    # evening peak -- the largest, when lighting, cooking and appliances
    # overlap.
    return (
        0.55
        + 0.55 * _bump(hour, 7.5, 1.2)
        + 0.20 * _bump(hour, 13.0, 2.5)
        + 1.25 * _bump(hour, 19.5, 1.8)
    )


def _industrial_shape(hour: float) -> float:
    # A working-hours plateau from 08:00 to 18:00 with soft edges, over a
    # standing overnight load.
    rise = 1.0 / (1.0 + math.exp(-(hour - 8.0) / 0.75))
    fall = 1.0 / (1.0 + math.exp((hour - 18.0) / 0.75))
    return 0.45 + rise * fall


_SHAPES: dict[str, Callable[[float], float]] = {
    "DOMESTIC_LOW": _domestic_shape,
    "DOMESTIC_STD": _domestic_shape,
    "DOMESTIC_HIGH": _domestic_shape,
    "INDUSTRIAL": _industrial_shape,
}


@cache
def _daily_mean(tier: str) -> float:
    """Mean of a tier's raw shape over one day, sampled per minute."""
    shape = _SHAPES[tier]
    return sum(shape(minute / 60.0) for minute in range(1440)) / 1440.0


def load_multiplier(tier: str, hour: float) -> float:
    """Diurnal load factor, normalised so its daily mean is exactly 1.0."""
    if tier not in _SHAPES:
        raise ValueError(f"unknown tariff tier {tier!r}")
    return _SHAPES[tier](hour) / _daily_mean(tier)


def load_power_kw(base_load_kw: float, tier: str, hour: float) -> float:
    return base_load_kw * load_multiplier(tier, hour)


# -- Energy --------------------------------------------------------------


def energy_kwh(power_kw: float, interval_seconds: float) -> float:
    """Energy over an interval. `interval_seconds` must be SIMULATED seconds."""
    return max(0.0, power_kw) * interval_seconds / 3600.0
