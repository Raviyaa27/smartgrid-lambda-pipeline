"""
Deterministic daily weather per grid zone.

Two views of the same day, deliberately different:

    forecast(...)  what the daily batch file publishes (the batch source)
    actual(...)    what the sky really did -- drives simulated solar output

The gap between them is a realistic forecast error. It is also why the
batch layer can later compare expected against delivered solar yield per
zone, instead of the two being identical by construction.

Both are pure functions of (seed, zone, date). The streaming source and the
batch source therefore agree on a day's weather without either reading the
other's output, and a re-run reproduces the same weather exactly.

`random.Random` is seeded with a string, which Python hashes with SHA-512.
That is stable across processes and machines -- unlike the built-in
`hash()`, which is randomised per process and would make every run
different.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date

# Share of days on which a weather system covers the whole grid. It is drawn
# per DAY, not per zone, so a bad day hits every zone at once -- which is
# what makes the "renewable contribution low" alert fire across zones
# together, as it would in reality.
OVERCAST_PROBABILITY = 0.15

# Standard deviation of forecast error, in percentage points of cloud cover.
FORECAST_ERROR_SD = 8.0


@dataclass(frozen=True)
class ZoneWeather:
    grid_zone: str
    day: date
    cloud_cover_pct: float
    temperature_c: float
    irradiance_index: float  # clear-sky fraction: 1.0 cloudless, 0.25 fully overcast


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def irradiance_from_cloud(cloud_cover_pct: float) -> float:
    """
    Fraction of clear-sky irradiance reaching the panels. Floors at 0.25
    rather than 0: even under full cloud, diffuse light still produces some
    output, and a model that drops solar to zero on cloudy days would
    overstate how often the renewable alert should fire.
    """
    return round(1.0 - 0.75 * _clamp(cloud_cover_pct, 0.0, 100.0) / 100.0, 4)


def _zone_climate_bias(seed: int, zone: str) -> float:
    """Persistent per-zone offset: some zones are simply cloudier than others."""
    return random.Random(f"zone-climate:{seed}:{zone}").uniform(-10.0, 15.0)


def forecast(zone: str, day: date, seed: int) -> ZoneWeather:
    """The day-ahead forecast published in the daily batch file."""
    overcast = random.Random(f"regime:{seed}:{day.isoformat()}").random() < OVERCAST_PROBABILITY
    rng = random.Random(f"forecast:{seed}:{zone}:{day.isoformat()}")

    cloud = rng.uniform(75.0, 95.0) if overcast else rng.uniform(5.0, 55.0)
    cloud = _clamp(cloud + _zone_climate_bias(seed, zone), 0.0, 100.0)
    temperature = rng.uniform(27.0, 33.0) - 4.0 * cloud / 100.0

    return ZoneWeather(
        grid_zone=zone,
        day=day,
        cloud_cover_pct=round(cloud, 1),
        temperature_c=round(temperature, 1),
        irradiance_index=irradiance_from_cloud(cloud),
    )


def actual(zone: str, day: date, seed: int) -> ZoneWeather:
    """What the day's weather turned out to be: the forecast plus its error."""
    predicted = forecast(zone, day, seed)
    rng = random.Random(f"actual:{seed}:{zone}:{day.isoformat()}")
    cloud = _clamp(predicted.cloud_cover_pct + rng.gauss(0.0, FORECAST_ERROR_SD), 0.0, 100.0)

    return ZoneWeather(
        grid_zone=zone,
        day=day,
        cloud_cover_pct=round(cloud, 1),
        temperature_c=predicted.temperature_c,
        irradiance_index=irradiance_from_cloud(cloud),
    )
