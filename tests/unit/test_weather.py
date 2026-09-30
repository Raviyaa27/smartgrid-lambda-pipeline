from datetime import date, timedelta

from smartgrid.common import weather

SEED = 20260101
DAY = date(2026, 1, 1)


def test_weather_is_deterministic():
    assert weather.forecast("ZONE-A", DAY, SEED) == weather.forecast("ZONE-A", DAY, SEED)
    assert weather.actual("ZONE-A", DAY, SEED) == weather.actual("ZONE-A", DAY, SEED)


def test_irradiance_stays_within_physical_bounds():
    for offset in range(120):
        for zone in ("ZONE-A", "ZONE-B", "ZONE-C"):
            w = weather.actual(zone, DAY + timedelta(days=offset), SEED)
            assert 0.25 <= w.irradiance_index <= 1.0
            assert 0.0 <= w.cloud_cover_pct <= 100.0


def test_actual_weather_differs_from_the_forecast():
    """Forecast error is what makes forecast-vs-delivered solar worth comparing."""
    differences = [
        abs(
            weather.actual("ZONE-A", DAY + timedelta(days=d), SEED).cloud_cover_pct
            - weather.forecast("ZONE-A", DAY + timedelta(days=d), SEED).cloud_cover_pct
        )
        for d in range(60)
    ]
    assert sum(1 for d in differences if d > 0.5) > 30


def test_overcast_days_hit_every_zone_together():
    """A weather system is drawn per DAY, so bad days are grid-wide."""
    zones = [f"ZONE-{c}" for c in "ABCDEF"]
    for offset in range(200):
        day = DAY + timedelta(days=offset)
        clouds = [weather.forecast(z, day, SEED).cloud_cover_pct for z in zones]
        if min(clouds) >= 65:  # an overcast regime day
            return
    raise AssertionError("expected at least one grid-wide overcast day in 200")


def test_overcast_days_occur_at_roughly_the_configured_rate():
    days = [DAY + timedelta(days=d) for d in range(1000)]
    overcast = sum(1 for d in days if weather.forecast("ZONE-A", d, SEED).cloud_cover_pct >= 65)
    assert 0.08 < overcast / len(days) < 0.30


def test_different_seeds_give_different_weather():
    assert weather.forecast("ZONE-A", DAY, 1) != weather.forecast("ZONE-A", DAY, 2)


def test_full_cloud_still_leaves_diffuse_light():
    assert weather.irradiance_from_cloud(100.0) == 0.25
    assert weather.irradiance_from_cloud(0.0) == 1.0
