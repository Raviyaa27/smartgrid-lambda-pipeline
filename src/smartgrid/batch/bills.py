"""
Bills for one business date: household totals x the day's drop.

Pure Python, no Spark: the Spark job aggregates a day of readings down to one
row per household (a few hundred rows) and hands them here. That keeps the
monetary logic -- the part a customer can dispute -- in plain, unit-tested
code using the shared Decimal billing (`common.billing`).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from smartgrid.common import drops
from smartgrid.common.billing import Bill, TariffSchedule, calculate_bill


@dataclass(frozen=True)
class HouseholdDay:
    """One household's settled totals for the day."""

    household_id: str
    readings: int
    consumption_kwh: float
    generation_kwh: float


@dataclass(frozen=True)
class DropContents:
    schedule: TariffSchedule
    tariffs: Mapping[str, Mapping[str, Any]]  # household_id -> tariff record
    forecast: Mapping[str, Mapping[str, Any]]  # grid_zone -> forecast record


def parse_drop(loaded: drops.LoadedDrop) -> DropContents:
    """Read a drop that has already passed the quality gate."""

    def lines(name: str) -> list[dict[str, Any]]:
        return [
            json.loads(line) for line in loaded.files[name].decode().splitlines() if line.strip()
        ]

    return DropContents(
        schedule=TariffSchedule.from_dict(json.loads(loaded.files[drops.SCHEDULE_FILE])),
        tariffs={r["household_id"]: r for r in lines(drops.HOUSEHOLDS_FILE)},
        forecast={r["grid_zone"]: r for r in lines(drops.WEATHER_FILE)},
    )


@dataclass(frozen=True)
class SettledBill:
    bill: Bill
    readings: int


def settle_households(
    business_date: date,
    usage: Mapping[str, HouseholdDay],
    contents: DropContents,
) -> list[SettledBill]:
    """
    One bill per household IN THE DROP, not per household that happened to
    send readings. A meter silent all day still owes its fixed charge, and a
    billing run that silently skipped it would under-bill without any error.
    """
    settled = []
    for household_id, tariff in sorted(contents.tariffs.items()):
        day = usage.get(household_id, HouseholdDay(household_id, 0, 0.0, 0.0))
        bill = calculate_bill(
            household_id=household_id,
            billing_date=business_date,
            tariff_tier=tariff["billing_tier"],
            consumption_kwh=day.consumption_kwh,
            generation_kwh=day.generation_kwh,
            subsidy_flag=bool(tariff["subsidy_flag"]),
            fixed_charge=tariff["fixed_charge"],
            schedule=contents.schedule,
        )
        settled.append(SettledBill(bill=bill, readings=day.readings))
    return settled


def unbilled(usage: Mapping[str, HouseholdDay], contents: DropContents) -> list[str]:
    """Households with readings but no tariff record. The quality gate makes this empty."""
    return sorted(set(usage) - set(contents.tariffs))
