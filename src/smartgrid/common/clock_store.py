"""
Shared anchor for the simulated clock.

`SimulatedClock` maps real elapsed time onto simulated time from an anchor,
`real_start`. If every process took its own `time.time()` as that anchor, a
producer started 30 real seconds after the batch source would run 2.4
simulated hours ahead of it. The two sources would disagree about which day
it is, and the join between them would silently break at every day boundary.

So the anchor is stored once, in `ops.sim_clock`, and every component reads
it at startup. The first component to start creates it; `reset` rewrites it
to begin a fresh simulation from SIM_START_DATE. Components read the anchor
once, so after a reset every running component must be restarted.

    python -m smartgrid.common.clock_store show
    python -m smartgrid.common.clock_store reset
"""

from __future__ import annotations

import argparse
import time
from datetime import UTC, date, datetime

import psycopg

from smartgrid.common.clock import SimulatedClock
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.logging import configure_logging, get_logger

log = get_logger(__name__)

# A single-row table: the CHECK constraint makes a second anchor impossible.
_DDL = """
CREATE TABLE IF NOT EXISTS ops.sim_clock (
    id          SMALLINT         PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    real_epoch  DOUBLE PRECISION NOT NULL,
    sim_start   DATE             NOT NULL,
    day_seconds DOUBLE PRECISION NOT NULL,
    anchored_at TIMESTAMPTZ      NOT NULL DEFAULT now()
)
"""

_SELECT = "SELECT real_epoch, sim_start, day_seconds FROM ops.sim_clock WHERE id = 1"


def _to_clock(real_epoch: float, sim_start: date, day_seconds: float) -> SimulatedClock:
    return SimulatedClock(
        start=datetime.combine(sim_start, datetime.min.time(), tzinfo=UTC),
        day_seconds=float(day_seconds),
        real_start=float(real_epoch),
    )


def shared_clock(settings: Settings | None = None) -> SimulatedClock:
    """
    The clock every component must use. Creates the anchor on first call,
    otherwise returns the existing one -- the INSERT is a no-op if a row is
    already there, so concurrent first starts still agree on one anchor.
    """
    settings = settings or get_settings()
    with psycopg.connect(settings.postgres_dsn, connect_timeout=10) as conn:
        conn.execute(_DDL)
        conn.execute(
            "INSERT INTO ops.sim_clock (id, real_epoch, sim_start, day_seconds) "
            "VALUES (1, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (time.time(), settings.sim_start_date, settings.sim_day_seconds),
        )
        real_epoch, sim_start, day_seconds = conn.execute(_SELECT).fetchone()

    # The anchor wins over .env: two components must never run on different
    # clocks just because one of them was started with an edited .env.
    if sim_start != settings.sim_start_date or float(day_seconds) != settings.sim_day_seconds:
        log.warning(
            "shared clock anchor differs from .env; the anchor is used. "
            "Run `python -m smartgrid.common.clock_store reset` to apply .env.",
            extra={
                "anchor_sim_start": sim_start,
                "anchor_day_seconds": day_seconds,
                "env_sim_start": settings.sim_start_date,
                "env_day_seconds": settings.sim_day_seconds,
            },
        )
    return _to_clock(real_epoch, sim_start, day_seconds)


def reset_clock(
    settings: Settings | None = None, real_epoch: float | None = None
) -> SimulatedClock:
    """Restart the simulation: simulated time returns to SIM_START_DATE now."""
    settings = settings or get_settings()
    real_epoch = time.time() if real_epoch is None else real_epoch
    with psycopg.connect(settings.postgres_dsn, connect_timeout=10) as conn:
        conn.execute(_DDL)
        conn.execute(
            "INSERT INTO ops.sim_clock (id, real_epoch, sim_start, day_seconds) "
            "VALUES (1, %s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET real_epoch = EXCLUDED.real_epoch, "
            "sim_start = EXCLUDED.sim_start, day_seconds = EXCLUDED.day_seconds, "
            "anchored_at = now()",
            (real_epoch, settings.sim_start_date, settings.sim_day_seconds),
        )
    return _to_clock(real_epoch, settings.sim_start_date, settings.sim_day_seconds)


def _describe(clock: SimulatedClock) -> str:
    now = clock.now()
    anchor = datetime.fromtimestamp(clock.real_start, UTC).isoformat(timespec="seconds")
    return (
        f"  anchor         : {anchor} (real)\n"
        f"  compression    : {clock.describe()}\n"
        f"  simulated now  : {now.isoformat(timespec='seconds')}\n"
        f"  simulated day  : {clock.sim_date()}  "
        f"(day {int(clock.elapsed_sim_days()) + 1} of the simulation, "
        f"{clock.day_fraction():.0%} through it)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect or reset the shared simulated clock.")
    parser.add_argument("action", choices=("show", "reset"))
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(service="clock-store", level=settings.log_level)

    if args.action == "reset":
        clock = reset_clock(settings)
        print("\nSimulated clock RESET. Restart any running pipeline components.\n")
    else:
        clock = shared_clock(settings)
        print("\nShared simulated clock\n")
    print(_describe(clock) + "\n")


if __name__ == "__main__":
    main()
