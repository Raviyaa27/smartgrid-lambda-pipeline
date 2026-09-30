"""
The Lambda merge rule (ADR-0001), as plain functions.

Every figure the serving layer returns comes from exactly one layer, chosen
here and nowhere else:

    zone metrics for day D:
        D == today (simulated)   -> speed layer   PROVISIONAL
        D settled                -> batch layer   SETTLED     (the batch view always wins)
        D ended, not yet settled -> speed layer   PROVISIONAL (awaiting settlement)

    a household's bill for day D:
        D settled                -> batch layer   SETTLED
        otherwise                -> no bill. A provisional bill is never published.

The batch view is never blended with the speed view: once a day is settled
the speed layer's figures for it are ignored, not averaged in. No database
or web framework here, so the rule is tested on its own.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum


class Status(StrEnum):
    SETTLED = "SETTLED"  # from the batch layer: the system of record
    PROVISIONAL = "PROVISIONAL"  # from the speed layer: fresh, approximate


class Layer(StrEnum):
    BATCH = "batch"
    SPEED = "speed"


class FutureDate(ValueError):
    """A simulated date that has not happened yet."""


@dataclass(frozen=True)
class DaySource:
    """Where one day's zone figures come from, and why."""

    day: date
    status: Status
    layer: Layer
    reason: str
    settlement_run_id: int | None = None


def zone_source(day: date, today: date, settled_runs: Mapping[date, int]) -> DaySource:
    """Which layer serves zone figures for `day`. `settled_runs` maps day -> current run."""
    if day > today:
        raise FutureDate(f"{day} is in the simulated future (today is {today})")
    if day == today:
        return DaySource(
            day, Status.PROVISIONAL, Layer.SPEED, "today: a day is settled only after it ends"
        )
    if day in settled_runs:
        return DaySource(day, Status.SETTLED, Layer.BATCH, "settled", settled_runs[day])
    return DaySource(day, Status.PROVISIONAL, Layer.SPEED, "ended, awaiting settlement")


@dataclass(frozen=True)
class BillAvailability:
    day: date
    billed: bool
    reason: str
    settlement_run_id: int | None = None


def bill_availability(day: date, today: date, settled_runs: Mapping[date, int]) -> BillAvailability:
    """Whether `day` has bills. There is no provisional answer: settled, or no bill."""
    if day > today:
        raise FutureDate(f"{day} is in the simulated future (today is {today})")
    if day in settled_runs:
        return BillAvailability(day, True, "settled", settled_runs[day])
    if day == today:
        return BillAvailability(day, False, "day in progress: bills are issued after settlement")
    return BillAvailability(day, False, "ended, awaiting settlement: no provisional bill is issued")


def date_range(start: date, end: date, *, max_days: int) -> list[date]:
    """Inclusive list of days, refusing reversed or oversized ranges."""
    if end < start:
        raise ValueError(f"range ends ({end}) before it starts ({start})")
    days = (end - start).days + 1
    if days > max_days:
        raise ValueError(f"range of {days} days exceeds the maximum of {max_days}")
    return [start + timedelta(days=i) for i in range(days)]
