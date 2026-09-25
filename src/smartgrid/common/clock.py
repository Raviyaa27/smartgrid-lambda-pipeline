"""
Simulated clock: compresses one simulated day into a few real seconds.

Default compression is 1 simulated day = 300 real seconds, i.e. 288x faster
than wall clock, so a full week of grid operation and seven daily settlement
runs fit inside a 35-minute demo.

Every component derives simulated time from this one class. Nothing calls
datetime.now() to decide what "day" it is -- that is what makes the pipeline
reproducible and the batch layer replayable.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

SECONDS_PER_DAY = 86_400.0


@dataclass(frozen=True)
class SimulatedClock:
    """
    Maps real elapsed time onto a compressed simulated timeline.

    `real_start` is captured once at construction; every later call derives
    simulated time from the real time that has passed since. Passing
    `real_now` explicitly makes the class fully deterministic under test.
    """

    start: datetime           # simulated instant corresponding to real_start
    day_seconds: float        # real seconds per simulated day
    real_start: float         # time.time() at construction

    @classmethod
    def from_settings(cls, settings, real_start: float | None = None) -> SimulatedClock:
        start = datetime.combine(
            settings.sim_start_date, datetime.min.time(), tzinfo=UTC
        )
        return cls(
            start=start,
            day_seconds=settings.sim_day_seconds,
            real_start=real_start if real_start is not None else time.time(),
        )

    @property
    def compression(self) -> float:
        """Simulated seconds elapsed per real second."""
        return SECONDS_PER_DAY / self.day_seconds

    def now(self, real_now: float | None = None) -> datetime:
        """Current simulated instant (UTC)."""
        real_now = time.time() if real_now is None else real_now
        elapsed_real = max(0.0, real_now - self.real_start)
        return self.start + timedelta(seconds=elapsed_real * self.compression)

    def sim_date(self, real_now: float | None = None) -> date:
        """Current simulated calendar date -- the batch layer's partition key."""
        return self.now(real_now).date()

    def elapsed_sim_days(self, real_now: float | None = None) -> float:
        real_now = time.time() if real_now is None else real_now
        return max(0.0, real_now - self.real_start) / self.day_seconds

    def day_fraction(self, real_now: float | None = None) -> float:
        """
        Position within the current simulated day, 0.0 at midnight to 1.0 at
        the next midnight. Drives the diurnal solar curve in Section 4.
        """
        moment = self.now(real_now)
        midnight = moment.replace(hour=0, minute=0, second=0, microsecond=0)
        return (moment - midnight).total_seconds() / SECONDS_PER_DAY

    def real_seconds_until_sim_date(self, target: date, real_now: float | None = None) -> float:
        """Real seconds to wait until the simulated clock reaches `target` midnight."""
        target_dt = datetime.combine(target, datetime.min.time(), tzinfo=UTC)
        sim_delta = (target_dt - self.now(real_now)).total_seconds()
        return max(0.0, sim_delta / self.compression)

    def describe(self) -> str:
        """One-line summary for logs and the report."""
        return (
            f"1 simulated day = {self.day_seconds:g}s real "
            f"({self.compression:.0f}x), starting {self.start.date().isoformat()}"
        )
