"""Physical plausibility of the simulated readings."""

from datetime import UTC, datetime

import pytest

from smartgrid.producers import energy_model as em

TIERS = ("DOMESTIC_LOW", "DOMESTIC_STD", "DOMESTIC_HIGH", "INDUSTRIAL")


@pytest.mark.parametrize("hour", [0.0, 2.0, 4.5, 5.99, 18.0, 20.0, 23.5])
def test_no_solar_output_at_night(hour):
    assert em.solar_power_kw(5.0, hour, irradiance=1.0) == 0.0


def test_solar_peaks_at_noon():
    outputs = {h: em.solar_power_kw(4.0, h, 1.0) for h in (8.0, 10.0, 12.0, 14.0, 16.0)}
    assert max(outputs, key=outputs.get) == 12.0


@pytest.mark.parametrize("hour", [h / 4 for h in range(96)])
def test_solar_never_exceeds_rated_capacity(hour):
    """Regression: a previous model produced 1.42x rated capacity at 09:00."""
    assert em.solar_power_kw(6.0, hour, irradiance=1.0) <= 6.0


def test_cloud_reduces_solar_output():
    assert em.solar_power_kw(4.0, 12.0, 0.3) < em.solar_power_kw(4.0, 12.0, 1.0)


def test_clear_day_yield_is_realistic_for_the_tropics():
    """About 5.7 kWh per kW of panel on a clear day."""
    per_minute = [em.solar_power_kw(1.0, m / 60, 1.0) for m in range(1440)]
    daily_kwh = sum(per_minute) / 60
    assert 5.0 < daily_kwh < 6.5


@pytest.mark.parametrize("tier", TIERS)
def test_load_multiplier_averages_to_one(tier):
    """Keeps base_load_kw meaning 'average draw' whatever the curve shape."""
    mean = sum(em.load_multiplier(tier, m / 60) for m in range(1440)) / 1440
    assert mean == pytest.approx(1.0, abs=1e-9)


def test_domestic_evening_peak_exceeds_the_night():
    assert em.load_multiplier("DOMESTIC_STD", 19.5) > 2 * em.load_multiplier("DOMESTIC_STD", 3.0)


def test_industrial_load_follows_working_hours():
    assert em.load_multiplier("INDUSTRIAL", 12.0) > 2 * em.load_multiplier("INDUSTRIAL", 2.0)


def test_unknown_tier_is_rejected():
    with pytest.raises(ValueError):
        em.load_multiplier("NOT_A_TIER", 12.0)


def test_energy_converts_power_over_the_interval():
    assert em.energy_kwh(3.0, 1800) == pytest.approx(1.5)  # 3 kW for 30 min
    assert em.energy_kwh(-1.0, 3600) == 0.0


def test_daily_consumption_matches_average_draw():
    """
    Regression: computing each reading over REAL seconds instead of the
    simulated interval undercounted daily consumption by ~189x. At 576
    simulated seconds per reading, a day of readings must sum to base load
    x 24 hours.
    """
    base_load_kw = 0.5
    interval = 576.0
    readings = int(86_400 / interval)
    total = sum(
        em.energy_kwh(
            em.load_power_kw(base_load_kw, "DOMESTIC_STD", (i * interval / 3600) % 24), interval
        )
        for i in range(readings)
    )
    assert total == pytest.approx(base_load_kw * 24, rel=0.02)


def test_hour_of_day_is_fractional():
    assert em.hour_of_day(datetime(2026, 1, 1, 18, 45, 0, tzinfo=UTC)) == 18.75
