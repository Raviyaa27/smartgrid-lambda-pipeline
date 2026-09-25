"""The simulated clock must be deterministic; every test injects real time."""

from datetime import UTC, date, datetime

from smartgrid.common.clock import SimulatedClock

START = datetime(2026, 1, 1, tzinfo=UTC)


def make_clock(day_seconds: float = 300.0) -> SimulatedClock:
    return SimulatedClock(start=START, day_seconds=day_seconds, real_start=1_000.0)


def test_compression_factor():
    assert make_clock(300.0).compression == 288.0


def test_one_real_day_period_advances_one_simulated_day():
    clock = make_clock(300.0)
    assert clock.now(real_now=1_300.0) == datetime(2026, 1, 2, tzinfo=UTC)


def test_sim_date_is_the_batch_partition_key():
    clock = make_clock(300.0)
    assert clock.sim_date(real_now=1_000.0) == date(2026, 1, 1)
    assert clock.sim_date(real_now=1_450.0) == date(2026, 1, 2)
    assert clock.sim_date(real_now=1_900.0) == date(2026, 1, 4)


def test_day_fraction_spans_zero_to_one():
    clock = make_clock(300.0)
    assert clock.day_fraction(real_now=1_000.0) == 0.0
    assert abs(clock.day_fraction(real_now=1_150.0) - 0.5) < 1e-9


def test_clock_never_runs_backwards_before_start():
    clock = make_clock()
    assert clock.now(real_now=500.0) == START


def test_waiting_for_a_future_simulated_date():
    clock = make_clock(300.0)
    assert clock.real_seconds_until_sim_date(date(2026, 1, 3), real_now=1_000.0) == 600.0


def test_elapsed_sim_days_tracks_real_elapsed_periods():
    clock = make_clock(300.0)
    assert clock.elapsed_sim_days(real_now=1_000.0) == 0.0
    assert clock.elapsed_sim_days(real_now=1_600.0) == 2.0


def test_describe_states_the_compression_for_the_report():
    assert "288x" in make_clock(300.0).describe()
