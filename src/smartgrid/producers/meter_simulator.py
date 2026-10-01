"""
Streaming source: simulated smart meters publishing readings to Kafka.

Every household in the fleet reports once per round. With the defaults --
one round every 2 real seconds at 288x compression -- each reading covers
9.6 simulated minutes, and the fleet of 200 publishes about 100 readings per
real second.

Readings follow a physical model (`energy_model`): a diurnal load curve
around each household's average draw, and rooftop solar that follows the
sun and the zone's ACTUAL weather for the day (`common.weather`). The daily
batch file will publish the FORECAST for the same day; the gap between them
is real forecast error, not an artefact.

Faults are injected at the rates of a named profile (`faults.PROFILES`):
corrupted content for validation to catch, duplicates for deduplication,
late readings and outage backfills for event-time processing, and whole-zone
silences for the no-data alert. The pipeline never sees which messages were
faulted -- but an `injected_fault` Kafka header records it, so detection can
be measured by `scripts/inspect_stream.py`.

Time: `event_time` and `ingest_time` are both SIMULATED time from the
shared clock anchor (`clock_store`), so `ingest_time - event_time` is a
meaningful delivery lag. Kafka's own message timestamp carries real time,
for measuring machine latency.

    python -m smartgrid.producers.meter_simulator
    python -m smartgrid.producers.meter_simulator --faults chaos
    python -m smartgrid.producers.meter_simulator --silence-zone ZONE-C \\
        --silence-after 60 --silence-for 90
"""

from __future__ import annotations

import argparse
import contextlib
import heapq
import itertools
import json
import random
import signal
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from typing import Any

import prometheus_client as prom

from smartgrid.common import weather
from smartgrid.common.clock import SimulatedClock
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.domain import Fleet, Household, build_fleet_from_settings
from smartgrid.common.logging import configure_logging, get_logger, new_correlation_id
from smartgrid.common.schemas import SCHEMA_VERSION, MeterReading
from smartgrid.producers import energy_model
from smartgrid.producers.faults import (
    PROFILES,
    FaultKind,
    FaultProfile,
    choose_fault,
    corrupt,
    get_profile,
)

SOURCE_NAME = "meter-simulator"

# How long a LATE reading is held back, in simulated minutes. The range
# straddles the speed layer's watermark on purpose: some late readings still
# make their window, others miss it and are recovered only by the batch layer.
LATE_DELAY_SIM_MINUTES = (15.0, 240.0)

# How long a meter outage lasts, in simulated hours.
OUTAGE_SIM_HOURS = (1.0, 6.0)

# Relative noise per reading. Real meters never trace a perfect curve.
LOAD_NOISE_SD = 0.10
SOLAR_NOISE_SD = 0.05

# About one meter in ten runs older firmware that does not report voltage,
# which exercises the schema's optional-field path.
_NO_VOLTAGE_EVERY = 10

# -- Metrics -------------------------------------------------------------
# Scraped by Prometheus; the same numbers appear in the structured logs.
MESSAGES = prom.Counter(
    "smartgrid_producer_messages_total",
    "Readings handed to Kafka, by injected fault ('none' for clean readings).",
    ["fault"],
)
DELIVERY_ERRORS = prom.Counter(
    "smartgrid_producer_delivery_errors_total",
    "Messages the Kafka broker failed to acknowledge.",
)
LATE_BUFFER = prom.Gauge(
    "smartgrid_producer_late_buffer",
    "Readings currently held back to be delivered late.",
)
METERS_OFFLINE = prom.Gauge(
    "smartgrid_producer_meters_offline",
    "Meters currently in a simulated outage.",
)
SIM_TIME = prom.Gauge(
    "smartgrid_producer_simulated_time_seconds",
    "Current simulated time, as Unix seconds.",
)
ROUND_SECONDS = prom.Histogram(
    "smartgrid_producer_round_seconds",
    "Real time taken to build and send one round of readings.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0),
)


@dataclass(frozen=True)
class OutboundMessage:
    """One message ready for Kafka, plus the ground truth about it."""

    key: bytes
    payload: dict[str, Any] | bytes
    correlation_id: str
    grid_zone: str
    event_time: datetime
    fault: FaultKind | None = None

    def headers(self) -> list[tuple[str, bytes]]:
        headers = [
            ("correlation_id", self.correlation_id.encode()),
            ("schema_version", SCHEMA_VERSION.encode()),
            ("source", SOURCE_NAME.encode()),
        ]
        if self.fault is not None:
            headers.append(("injected_fault", self.fault.value.encode()))
        return headers

    def encode(self, sim_now: datetime) -> bytes:
        """
        Serialise at SEND time, stamping `ingest_time`. A late reading is
        encoded when it is finally released, so its ingest_time records how
        late it really was.
        """
        if isinstance(self.payload, bytes):
            return self.payload
        body = dict(self.payload)
        body["ingest_time"] = sim_now.isoformat()
        return json.dumps(body, separators=(",", ":"), default=str).encode()


@dataclass(frozen=True)
class SilenceWindow:
    """
    Take a whole zone offline for a period of REAL time. This is the demo
    lever for the no-data alert: silence a zone, watch the alert fire, then
    watch the backfill arrive when it returns.
    """

    zone: str
    start_real: float
    end_real: float | None = None  # None: silent until the process stops
    backfill: bool = True

    def active(self, real_now: float) -> bool:
        return real_now >= self.start_real and (self.end_real is None or real_now < self.end_real)


@dataclass
class _MeterState:
    offline_until: datetime | None = None
    backfill_on_return: bool = False
    missed: list[datetime] = field(default_factory=list)


class MeterSimulator:
    """
    Produces each round's messages. Holds no Kafka connection, so every
    behaviour -- physics, faults, outages, late delivery -- is unit-testable.
    """

    def __init__(
        self,
        fleet: Fleet,
        clock: SimulatedClock,
        profile: FaultProfile,
        *,
        emit_interval_real_seconds: float,
        weather_seed: int,
        rng: random.Random | None = None,
        silences: Iterable[SilenceWindow] = (),
    ) -> None:
        self.fleet = fleet
        self.clock = clock
        self.profile = profile
        # The interval each reading covers, in SIMULATED seconds.
        self.interval_sim_seconds = emit_interval_real_seconds * clock.compression
        self.weather_seed = weather_seed
        self.rng = rng or random.Random()
        self.silences = tuple(silences)

        self._state = {h.meter_id: _MeterState() for h in fleet}
        self._late: list[tuple[float, int, OutboundMessage]] = []
        self._sequence = itertools.count()
        self._irradiance: dict[tuple[str, date], float] = {}
        self.emitted: Counter[str] = Counter()

    @property
    def late_buffer_size(self) -> int:
        return len(self._late)

    @property
    def meters_offline(self) -> int:
        return sum(1 for s in self._state.values() if s.offline_until is not None)

    def tick(self, real_now: float) -> list[OutboundMessage]:
        """Every message due to be sent at `real_now`."""
        sim_now = self.clock.now(real_now)
        outbound: list[OutboundMessage] = []
        for index, household in enumerate(self.fleet):
            outbound.extend(self._meter_tick(household, index, sim_now, real_now))
        outbound.extend(self._release_late(real_now))
        for message in outbound:
            self.emitted[message.fault.value if message.fault else "none"] += 1
        return outbound

    # -- One meter, one round --------------------------------------------

    def _meter_tick(
        self, household: Household, index: int, sim_now: datetime, real_now: float
    ) -> list[OutboundMessage]:
        state = self._state[household.meter_id]
        # Each reading covers the interval ENDING at event_time. A small
        # per-meter offset stops the whole fleet stamping the same instant.
        event_time = sim_now - timedelta(seconds=index % 30)

        silence = self._active_silence(household.grid_zone, real_now)
        if silence is not None:
            if silence.backfill:
                state.missed.append(event_time)
            return []

        if state.offline_until is not None:
            if event_time < state.offline_until:
                if state.backfill_on_return:
                    state.missed.append(event_time)
                return []
            state.offline_until = None

        # Back online: upload whatever was missed, with its ORIGINAL timestamps.
        messages = [self._build(household, missed, FaultKind.BACKFILL) for missed in state.missed]
        state.missed.clear()

        if self.rng.random() < self.profile.outage_start:
            state.offline_until = event_time + timedelta(hours=self.rng.uniform(*OUTAGE_SIM_HOURS))
            state.backfill_on_return = self.rng.random() < self.profile.outage_backfill
            if state.backfill_on_return:
                state.missed.append(event_time)
            return messages

        fault = choose_fault(self.profile, self.rng)
        message = self._build(household, event_time, fault)

        if fault is FaultKind.DUPLICATE:
            # A retransmission: byte-identical value, same event_id. Only the
            # second copy is labelled, because only it should be dropped.
            return [*messages, replace(message, fault=None), message]

        if fault is FaultKind.LATE:
            delay_sim_seconds = self.rng.uniform(*LATE_DELAY_SIM_MINUTES) * 60.0
            release_at = real_now + delay_sim_seconds / self.clock.compression
            heapq.heappush(self._late, (release_at, next(self._sequence), message))
            return messages

        return [*messages, message]

    def _build(
        self, household: Household, event_time: datetime, fault: FaultKind | None
    ) -> OutboundMessage:
        hour = energy_model.hour_of_day(event_time)
        irradiance = self._actual_irradiance(household.grid_zone, event_time.date())

        load_kw = energy_model.load_power_kw(household.base_load_kw, household.tariff_tier, hour)
        solar_kw = energy_model.solar_power_kw(household.solar_capacity_kw, hour, irradiance)
        load_kw *= max(0.0, self.rng.gauss(1.0, LOAD_NOISE_SD))
        solar_kw *= max(0.0, self.rng.gauss(1.0, SOLAR_NOISE_SD))

        reports_voltage = int(household.meter_id[-5:]) % _NO_VOLTAGE_EVERY != 0
        correlation_id = new_correlation_id()

        reading = MeterReading(
            # Identity is (meter, interval). A retransmission therefore
            # carries the same event_id, which is what makes it dedupable.
            event_id=f"{household.meter_id}:{event_time:%Y%m%dT%H%M%S}",
            meter_id=household.meter_id,
            household_id=household.household_id,
            grid_zone=household.grid_zone,
            power_consumption_kwh=round(
                energy_model.energy_kwh(load_kw, self.interval_sim_seconds), 6
            ),
            solar_generation_kwh=round(
                energy_model.energy_kwh(solar_kw, self.interval_sim_seconds), 6
            ),
            event_time=event_time,
            voltage_v=(
                round(min(260.0, max(200.0, self.rng.gauss(230.0, 3.0))), 1)
                if reports_voltage
                else None
            ),
            correlation_id=correlation_id,
        )
        payload = reading.to_dict()
        payload.pop("ingest_time")  # stamped at send time, see encode()

        return OutboundMessage(
            key=household.meter_id.encode(),  # ADR-0003: keyed by meter
            payload=corrupt(payload, fault, self.rng),
            correlation_id=correlation_id,
            grid_zone=household.grid_zone,
            event_time=event_time,
            fault=fault,
        )

    # -- Helpers ---------------------------------------------------------

    def _release_late(self, real_now: float) -> list[OutboundMessage]:
        due: list[OutboundMessage] = []
        while self._late and self._late[0][0] <= real_now:
            due.append(heapq.heappop(self._late)[2])
        return due

    def _active_silence(self, zone: str, real_now: float) -> SilenceWindow | None:
        for window in self.silences:
            if window.zone == zone and window.active(real_now):
                return window
        return None

    def _actual_irradiance(self, zone: str, day: date) -> float:
        key = (zone, day)
        if key not in self._irradiance:
            self._irradiance[key] = weather.actual(zone, day, self.weather_seed).irradiance_index
        return self._irradiance[key]


# -- Kafka runner --------------------------------------------------------


def build_producer(settings: Settings) -> Any:
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": SOURCE_NAME,
            # Wait for the broker to persist each message before counting it
            # delivered, and let the client dedupe its own retries. Our
            # injected duplicates are separate messages and are unaffected.
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            # Route the C client's own log lines through our JSON logger, so
            # they are structured like everything else instead of raw stderr.
            "logger": get_logger("kafka.producer"),
        }
    )


def _send(
    producer: Any, topic: str, message: OutboundMessage, sim_now: datetime, on_delivery
) -> None:
    while True:
        try:
            producer.produce(
                topic=topic,
                key=message.key,
                value=message.encode(sim_now),
                headers=message.headers(),
                on_delivery=on_delivery,
            )
            return
        except BufferError:
            # The client's local queue is full: let deliveries drain, then retry.
            producer.poll(0.5)


def _raise_interrupt(signum, frame) -> None:
    raise KeyboardInterrupt


def run(
    settings: Settings,
    *,
    profile: FaultProfile,
    duration: float | None = None,
    silences: Iterable[SilenceWindow] = (),
    metrics_port: int = 0,
    log_every: int = 15,
) -> Counter[str]:
    """Publish until stopped (Ctrl+C / SIGTERM) or for `duration` real seconds."""
    from smartgrid.common.clock_store import shared_clock

    log = get_logger(__name__)
    clock = shared_clock(settings)
    fleet = build_fleet_from_settings(settings)
    silences = tuple(silences)

    unknown = {w.zone for w in silences} - set(fleet.zones)
    if unknown:
        raise SystemExit(
            f"unknown zone(s) {sorted(unknown)}; valid zones: {', '.join(fleet.zones)}"
        )

    simulator = MeterSimulator(
        fleet,
        clock,
        profile,
        emit_interval_real_seconds=settings.sim_emit_interval_seconds,
        weather_seed=settings.sim_seed,
        silences=silences,
    )
    producer = build_producer(settings)
    topic = settings.kafka_topic_readings

    if metrics_port:
        prom.start_http_server(metrics_port)

    log.info(
        "meter simulator starting",
        extra={
            "clock": clock.describe(),
            "simulated_now": clock.now().isoformat(timespec="seconds"),
            "households": len(fleet),
            "zones": list(fleet.zones),
            "fault_profile": profile.name,
            "message_fault_rate": profile.message_fault_rate,
            "topic": topic,
            "bootstrap": settings.kafka_bootstrap,
            "reading_interval_sim_minutes": round(simulator.interval_sim_seconds / 60, 2),
            "silences": [
                {
                    "zone": w.zone,
                    "starts_in_s": round(w.start_real - time.time(), 1),
                    "lasts_s": None if w.end_real is None else round(w.end_real - w.start_real, 1),
                    "backfill": w.backfill,
                }
                for w in silences
            ],
            "metrics_port": metrics_port or None,
        },
    )

    def on_delivery(err, msg) -> None:
        if err is not None:
            DELIVERY_ERRORS.inc()
            log.error("kafka delivery failed", extra={"error": str(err), "topic": msg.topic()})

    # `docker stop` sends SIGTERM. Registration fails off the main thread,
    # which only happens in tests; the process then stops on Ctrl+C alone.
    with contextlib.suppress(ValueError, AttributeError):
        signal.signal(signal.SIGTERM, _raise_interrupt)

    interval = settings.sim_emit_interval_seconds
    started = time.monotonic()
    next_round = started
    rounds = 0
    window: Counter[str] = Counter()

    try:
        while True:
            real_now = time.time()
            sim_now = clock.now(real_now)
            with ROUND_SECONDS.time():
                messages = simulator.tick(real_now)
                for message in messages:
                    _send(producer, topic, message, sim_now, on_delivery)
                    label = message.fault.value if message.fault else "none"
                    MESSAGES.labels(label).inc()
                    window[label] += 1
                producer.poll(0)

            rounds += 1
            LATE_BUFFER.set(simulator.late_buffer_size)
            METERS_OFFLINE.set(simulator.meters_offline)
            SIM_TIME.set(sim_now.timestamp())

            if rounds % log_every == 0:
                log.info(
                    "round summary",
                    extra={
                        "rounds": rounds,
                        "simulated_now": sim_now.isoformat(timespec="minutes"),
                        "simulated_day": sim_now.date().isoformat(),
                        "messages": sum(window.values()),
                        "faults": {k: v for k, v in window.items() if k != "none"},
                        "late_buffer": simulator.late_buffer_size,
                        "meters_offline": simulator.meters_offline,
                        "zones_silenced": sorted(w.zone for w in silences if w.active(real_now)),
                    },
                )
                window.clear()

            if duration is not None and time.monotonic() - started >= duration:
                break

            # Schedule against a deadline rather than sleeping a fixed amount,
            # so time spent producing does not stretch the reading interval.
            next_round += interval
            delay = next_round - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:
                next_round = time.monotonic()  # fell behind: do not burst to catch up
    except KeyboardInterrupt:
        log.info("stop requested")
    finally:
        unflushed = producer.flush(10)
        log.info(
            "meter simulator stopped",
            extra={
                "rounds": rounds,
                "totals": dict(simulator.emitted),
                "late_readings_never_sent": simulator.late_buffer_size,
                "unflushed_messages": unflushed,
            },
        )
    return simulator.emitted


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulated smart meters publishing to Kafka.")
    parser.add_argument(
        "--faults",
        choices=sorted(PROFILES),
        default=None,
        help="fault profile (default: SIM_FAULT_PROFILE from .env)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="stop after this many real seconds (default: run until Ctrl+C)",
    )
    parser.add_argument(
        "--silence-zone",
        action="append",
        default=[],
        metavar="ZONE",
        help="take a whole zone offline; repeatable (e.g. ZONE-C)",
    )
    parser.add_argument(
        "--silence-after",
        type=float,
        default=30.0,
        help="real seconds before the silence begins (default 30)",
    )
    parser.add_argument(
        "--silence-for",
        type=float,
        default=0.0,
        help="real seconds the silence lasts; 0 = until stopped",
    )
    parser.add_argument(
        "--no-backfill",
        action="store_true",
        help="a silenced zone does NOT upload its missed readings on return",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help="Prometheus port (default: PRODUCER_METRICS_PORT; 0 disables)",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=15,
        help="log a summary every N rounds (default 15, i.e. every 30 s)",
    )
    return parser.parse_args(argv)


def parse_silence_spec(spec: str, now: float, *, backfill: bool = True) -> list[SilenceWindow]:
    """
    "ZONE-C:120:90" -> ZONE-C offline from now+120 s for 90 s (0 = until stopped).
    Several are separated by commas. Raises ValueError on a malformed entry.
    """
    windows = []
    for entry in filter(None, (part.strip() for part in spec.split(","))):
        try:
            zone, after, duration = entry.split(":")
            after_s, for_s = float(after), float(duration)
        except ValueError as exc:
            raise ValueError(f"bad silence {entry!r}: expected ZONE:after_s:for_s") from exc
        windows.append(
            SilenceWindow(
                zone=zone,
                start_real=now + after_s,
                end_real=now + after_s + for_s if for_s > 0 else None,
                backfill=backfill,
            )
        )
    return windows


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = get_settings()
    configure_logging(service=SOURCE_NAME, level=settings.log_level)

    now = time.time()
    silences = [
        SilenceWindow(
            zone=zone,
            start_real=now + args.silence_after,
            end_real=(now + args.silence_after + args.silence_for)
            if args.silence_for > 0
            else None,
            backfill=not args.no_backfill,
        )
        for zone in args.silence_zone
    ]
    if not silences and settings.sim_silence:  # flags win; SIM_SILENCE otherwise
        silences = parse_silence_spec(settings.sim_silence, now, backfill=not args.no_backfill)

    run(
        settings,
        profile=get_profile(args.faults or settings.sim_fault_profile),
        duration=args.duration,
        silences=silences,
        metrics_port=settings.producer_metrics_port
        if args.metrics_port is None
        else args.metrics_port,
        log_every=max(1, args.log_every),
    )


if __name__ == "__main__":
    main()
