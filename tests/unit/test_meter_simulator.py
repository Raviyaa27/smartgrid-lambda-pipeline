"""
Behaviour of the streaming source, with no Kafka involved.

Real time is injected, so every test is deterministic: `real_start` is 0
and each round advances real time by the 2-second emit interval.
"""

import json
import random
from datetime import UTC, datetime

import pytest

from smartgrid.common.clock import SimulatedClock
from smartgrid.common.domain import build_fleet
from smartgrid.common.transformations import validate_record
from smartgrid.producers.faults import FaultKind, FaultProfile
from smartgrid.producers.meter_simulator import MeterSimulator, SilenceWindow

INTERVAL = 2.0
FLEET = build_fleet(num_households=12, num_zones=3, seed=7)
NONE = FaultProfile("none")


def make_clock() -> SimulatedClock:
    return SimulatedClock(start=datetime(2026, 1, 1, tzinfo=UTC), day_seconds=300.0, real_start=0.0)


def make_sim(profile: FaultProfile = NONE, *, silences=(), seed: int = 1) -> MeterSimulator:
    return MeterSimulator(
        FLEET,
        make_clock(),
        profile,
        emit_interval_real_seconds=INTERVAL,
        weather_seed=99,
        rng=random.Random(seed),
        silences=silences,
    )


def decode(message, clock=None) -> dict:
    return json.loads(message.encode((clock or make_clock()).now(0.0)))


def test_one_reading_per_household_per_round():
    messages = make_sim().tick(real_now=10.0)
    assert len(messages) == len(FLEET)
    assert {m.key.decode() for m in messages} == {h.meter_id for h in FLEET}


def test_readings_are_keyed_by_meter_as_adr_0003_specifies():
    for message in make_sim().tick(real_now=10.0):
        assert message.key.decode() == decode(message)["meter_id"]


def test_clean_readings_pass_the_shared_validator():
    sim = make_sim()
    now = sim.clock.now(10.0)
    for message in sim.tick(real_now=10.0):
        result = validate_record(decode(message), known_household_ids=FLEET.household_ids, now=now)
        assert result.ok, result.detail


def test_headers_carry_trace_and_schema_but_no_fault_when_clean():
    message = make_sim().tick(real_now=10.0)[0]
    headers = dict(message.headers())
    assert headers["correlation_id"] == message.correlation_id.encode()
    assert headers["schema_version"] == b"1.0.0"
    assert "injected_fault" not in headers
    assert decode(message)["correlation_id"] == message.correlation_id


def test_reading_interval_is_in_simulated_time():
    """Regression: 2 real seconds at 288x must cover 576 simulated seconds."""
    assert make_sim().interval_sim_seconds == pytest.approx(576.0)


def test_a_simulated_day_of_readings_has_plausible_consumption():
    """Regression for the ~189x undercount: a day's readings sum to ~24h of load."""
    sim = make_sim()
    totals: dict[str, float] = {}
    rounds_per_day = int(300 / INTERVAL)
    for r in range(rounds_per_day):
        for message in sim.tick(real_now=r * INTERVAL):
            body = decode(message)
            totals[body["household_id"]] = (
                totals.get(body["household_id"], 0.0) + body["power_consumption_kwh"]
            )
    for household in FLEET:
        expected = household.base_load_kw * 24
        assert totals[household.household_id] == pytest.approx(expected, rel=0.15)


def test_ingest_time_is_simulated_and_follows_event_time():
    sim = make_sim()
    message = sim.tick(real_now=10.0)[0]
    body = json.loads(message.encode(sim.clock.now(12.0)))
    assert datetime.fromisoformat(body["ingest_time"]) >= datetime.fromisoformat(body["event_time"])


def test_duplicates_are_byte_identical_retransmissions():
    sim = make_sim(FaultProfile("dup", rates={FaultKind.DUPLICATE: 1.0}))
    messages = sim.tick(real_now=10.0)
    assert len(messages) == 2 * len(FLEET)
    sim_now = sim.clock.now(10.0)
    for original, copy in zip(messages[::2], messages[1::2], strict=True):
        assert original.encode(sim_now) == copy.encode(sim_now)
        assert original.fault is None and copy.fault is FaultKind.DUPLICATE


def test_late_readings_are_withheld_then_released_with_original_timestamps():
    sim = make_sim(FaultProfile("late", rates={FaultKind.LATE: 1.0}))
    assert sim.tick(real_now=10.0) == []
    assert sim.late_buffer_size == len(FLEET)
    sim.profile = NONE  # only the first round is late

    released = []
    for step in range(1, 40):  # the longest delay is ~50 real s
        released.extend(sim.tick(real_now=10.0 + step * INTERVAL))
        if sim.late_buffer_size == 0:
            break
    assert sim.late_buffer_size == 0
    late = [m for m in released if m.fault is FaultKind.LATE]
    assert len(late) == len(FLEET)
    assert all(m.event_time <= sim.clock.now(10.0) for m in late)


def test_an_outage_stops_readings_then_backfills_the_gap():
    sim = make_sim(FaultProfile("outage", outage_start=1.0, outage_backfill=1.0))
    assert sim.tick(real_now=10.0) == []  # everyone drops offline
    assert sim.meters_offline == len(FLEET)
    sim.profile = NONE  # no NEW outages once they reconnect

    backfilled = []
    for step in range(1, 60):  # the longest outage is ~75 real s
        backfilled.extend(
            m for m in sim.tick(real_now=10.0 + step * INTERVAL) if m.fault is FaultKind.BACKFILL
        )
        if sim.meters_offline == 0:
            break
    assert sim.meters_offline == 0
    # Every meter uploads the interval it went down in, plus each one it missed.
    assert {m.key for m in backfilled} == {h.meter_id.encode() for h in FLEET}
    assert min(m.event_time for m in backfilled) <= sim.clock.now(10.0)


def test_a_silenced_zone_goes_quiet_and_recovers_its_readings():
    zone = FLEET.zones[1]
    window = SilenceWindow(zone=zone, start_real=10.0, end_real=20.0, backfill=True)
    sim = make_sim(silences=[window])

    during = sim.tick(real_now=12.0)
    assert zone not in {m.grid_zone for m in during}
    assert len({m.grid_zone for m in during}) == len(FLEET.zones) - 1

    after = sim.tick(real_now=22.0)
    backfill = [m for m in after if m.grid_zone == zone and m.fault is FaultKind.BACKFILL]
    assert len(backfill) == len(FLEET.in_zone(zone))


def test_silence_without_backfill_loses_the_readings():
    zone = FLEET.zones[0]
    sim = make_sim(silences=[SilenceWindow(zone, 10.0, 20.0, backfill=False)])
    sim.tick(real_now=12.0)
    after = sim.tick(real_now=22.0)
    assert not any(m.fault is FaultKind.BACKFILL for m in after)


def test_event_ids_are_unique_per_meter_and_interval():
    sim = make_sim()
    ids = [decode(m)["event_id"] for r in range(20) for m in sim.tick(real_now=r * INTERVAL)]
    assert len(ids) == len(set(ids))


def test_no_solar_is_reported_at_night():
    sim = make_sim()
    # real 0 s = 00:00 simulated; every meter's reading is from the small hours
    for message in sim.tick(real_now=0.0):
        assert decode(message)["solar_generation_kwh"] == 0.0


def test_some_meters_omit_the_optional_voltage_field():
    fleet = build_fleet(num_households=30, num_zones=3, seed=7)
    sim = MeterSimulator(
        fleet,
        make_clock(),
        NONE,
        emit_interval_real_seconds=INTERVAL,
        weather_seed=99,
        rng=random.Random(1),
    )
    voltages = [decode(m)["voltage_v"] for m in sim.tick(real_now=10.0)]
    assert any(v is None for v in voltages) and any(v is not None for v in voltages)


def test_emitted_counts_label_every_message():
    sim = make_sim(FaultProfile("dup", rates={FaultKind.DUPLICATE: 1.0}))
    sim.tick(real_now=10.0)
    assert sim.emitted == {"none": len(FLEET), "duplicate": len(FLEET)}
