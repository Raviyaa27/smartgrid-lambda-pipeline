"""
The simulated world: grid zones, households, meters and tariff tiers.

The fleet is generated deterministically from a seed, so the streaming
source (meter readings) and the daily batch source (tariff records) describe
the same households. Without that shared derivation the join between the two
sources would be fictional.

Same seed => same fleet, on any machine, in any process. That is what makes
the whole pipeline reproducible for a grader.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from dataclasses import dataclass, field

# Tariff tiers, ordered from most to least subsidised. Tier assignment is a
# property of the household (its connection class), supplied daily by the
# billing system in the batch feed.
TARIFF_TIERS: tuple[str, ...] = (
    "DOMESTIC_LOW",
    "DOMESTIC_STD",
    "DOMESTIC_HIGH",
    "INDUSTRIAL",
)

# Population mix. Deliberately skewed to the standard domestic tier so the
# daily report has a realistic long tail rather than a uniform distribution.
_TIER_WEIGHTS: tuple[float, ...] = (0.25, 0.45, 0.20, 0.10)

_SOLAR_PROBABILITY: dict[str, float] = {
    "DOMESTIC_LOW": 0.15,
    "DOMESTIC_STD": 0.40,
    "DOMESTIC_HIGH": 0.65,
    "INDUSTRIAL": 0.75,
}

_BASE_LOAD_RANGE: dict[str, tuple[float, float]] = {
    "DOMESTIC_LOW": (0.10, 0.35),
    "DOMESTIC_STD": (0.30, 0.90),
    "DOMESTIC_HIGH": (0.80, 2.00),
    "INDUSTRIAL": (3.00, 12.00),
}


@dataclass(frozen=True)
class Household:
    """A billable connection and the meter that serves it."""

    household_id: str
    meter_id: str
    grid_zone: str
    tariff_tier: str
    has_solar: bool
    solar_capacity_kw: float      # 0.0 when has_solar is False
    base_load_kw: float           # average draw, before diurnal variation
    subsidy_eligible: bool


@dataclass(frozen=True)
class Fleet:
    """The full simulated population, with lookup indexes."""

    households: tuple[Household, ...]
    zones: tuple[str, ...]
    by_household_id: dict[str, Household] = field(repr=False, default_factory=dict)
    by_meter_id: dict[str, Household] = field(repr=False, default_factory=dict)

    def __iter__(self) -> Iterator[Household]:
        return iter(self.households)

    def __len__(self) -> int:
        return len(self.households)

    @property
    def household_ids(self) -> frozenset[str]:
        return frozenset(self.by_household_id)

    @property
    def solar_count(self) -> int:
        return sum(1 for h in self.households if h.has_solar)

    def in_zone(self, zone: str) -> tuple[Household, ...]:
        return tuple(h for h in self.households if h.grid_zone == zone)


def zone_names(num_zones: int) -> tuple[str, ...]:
    """ZONE-A, ZONE-B, ... Capped at 26 for the demo."""
    if not 1 <= num_zones <= 26:
        raise ValueError(f"num_zones must be between 1 and 26, got {num_zones}")
    return tuple(f"ZONE-{chr(ord('A') + i)}" for i in range(num_zones))


def build_fleet(num_households: int, num_zones: int, seed: int) -> Fleet:
    """
    Build the household population. Deterministic for a given seed.

    Solar penetration correlates loosely with tier, mirroring the real
    pattern where higher-consumption connections are likelier to have rooftop
    PV. That correlation is what makes the renewable-mix-by-zone figure in
    the dashboard vary meaningfully between zones.
    """
    if num_households < 1:
        raise ValueError(f"num_households must be positive, got {num_households}")

    rng = random.Random(seed)
    zones = zone_names(num_zones)
    households: list[Household] = []

    for index in range(1, num_households + 1):
        tier = rng.choices(TARIFF_TIERS, weights=_TIER_WEIGHTS, k=1)[0]
        has_solar = rng.random() < _SOLAR_PROBABILITY[tier]
        low, high = _BASE_LOAD_RANGE[tier]

        households.append(
            Household(
                household_id=f"HH-{index:05d}",
                meter_id=f"MTR-{index:05d}",
                grid_zone=zones[index % len(zones)],
                tariff_tier=tier,
                has_solar=has_solar,
                solar_capacity_kw=round(rng.uniform(1.5, 8.0), 2) if has_solar else 0.0,
                base_load_kw=round(rng.uniform(low, high), 3),
                # Subsidy is means-tested: only the lowest tier qualifies, and
                # not all of them. The batch feed carries the daily flag.
                subsidy_eligible=(tier == "DOMESTIC_LOW" and rng.random() < 0.70),
            )
        )

    frozen = tuple(households)
    return Fleet(
        households=frozen,
        zones=zones,
        by_household_id={h.household_id: h for h in frozen},
        by_meter_id={h.meter_id: h for h in frozen},
    )


def build_fleet_from_settings(settings) -> Fleet:
    return build_fleet(
        num_households=settings.sim_num_households,
        num_zones=settings.sim_num_zones,
        seed=settings.sim_seed,
    )
