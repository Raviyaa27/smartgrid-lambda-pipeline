from __future__ import annotations

import json
import math
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Producer

from smartgrid.common.clock import SimulatedClock
from smartgrid.common.config import get_settings
from smartgrid.common.domain import Household, build_fleet_from_settings
from smartgrid.common.transformations import validate_record


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


def _diurnal_multiplier(sim_now: datetime) -> float:
    """
    Smooth day/night shape for the synthetic meter readings.
    Keeps the signal realistic without depending on external weather data.
    """
    hour = sim_now.hour + sim_now.minute / 60.0 + sim_now.second / 3600.0
    phase = ((hour - 6.0) / 24.0) * 2.0 * math.pi
    return 0.35 + max(0.0, math.sin(phase) + 1.0) / 2.0


def build_reading_payload(
    household: Household,
    sim_now: datetime,
    event_index: int,
    interval_seconds: float = 2.0,
) -> dict[str, Any]:
    """
    Build one valid meter-reading event using the shared domain model
    and the shared validation rules.
    """
    diurnal = _diurnal_multiplier(sim_now)

    # Use instantaneous kW shape, then convert to kWh over this emission interval.
    load_kw = household.base_load_kw * (0.55 + diurnal * 1.15)
    generation_kw = 0.0

    if household.has_solar:
        solar_phase = (
            ((sim_now.hour + sim_now.minute / 60.0 + sim_now.second / 3600.0) / 24.0)
            * 2.0
            * math.pi
            - 0.9
        )
        solar_curve = max(0.0, math.sin(solar_phase) + 0.5)
        generation_kw = household.solar_capacity_kw * (0.15 + 0.85 * solar_curve)

    power_consumption_kwh = max(0.0, load_kw * (interval_seconds / 3600.0))
    solar_generation_kwh = max(0.0, generation_kw * (interval_seconds / 3600.0))

    raw = {
        "event_id": (
            f"{household.meter_id}-{sim_now.date().isoformat()}-"
            f"{event_index}-{uuid.uuid4().hex[:8]}"
        ),
        "meter_id": household.meter_id,
        "household_id": household.household_id,
        "grid_zone": household.grid_zone,
        "power_consumption_kwh": round(power_consumption_kwh, 6),
        "solar_generation_kwh": round(solar_generation_kwh, 6),
        "event_time": sim_now.isoformat(),
        "ingest_time": datetime.now(UTC).isoformat(),
        "correlation_id": f"sim-{event_index}",
        "schema_version": "1.0.0",
    }

    validation = validate_record(
        raw,
        known_household_ids=frozenset({household.household_id}),
        now=sim_now,
    )

    if not validation.ok:
        raise ValueError(
            f"Generated invalid reading for {household.household_id}: "
            f"{validation.reason} / {validation.detail}"
        )

    return validation.record


def emit_stream(
    settings: Any,
    producer: Producer,
    fleet: Any,
    clock: SimulatedClock,
) -> None:
    """
    Produce readings continuously until the process is stopped.
    """
    event_index = 0

    while True:
        sim_now = clock.now()
        for household in fleet.households:
            payload = build_reading_payload(
                household=household,
                sim_now=sim_now,
                event_index=event_index,
            )

            producer.produce(
                topic=settings.kafka_topic_readings,
                key=household.household_id.encode("utf-8"),
                value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                callback=delivery_report,
            )
            event_index += 1

        producer.flush(timeout=5.0)
        time.sleep(settings.sim_emit_interval_seconds)


def main() -> None:
    settings = get_settings()
    fleet = build_fleet_from_settings(settings)
    clock = SimulatedClock.from_settings(settings)

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": "smartgrid-producer",
        }
    )

    print(f"Starting producer on {settings.kafka_bootstrap}")
    print(f"Fleet size: {len(fleet)} households")
    print(f"Sim clock: {clock.describe()}")

    emit_stream(settings, producer, fleet, clock)


if __name__ == "__main__":
    main()