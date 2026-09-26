from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaException

from smartgrid.common.config import get_settings
from smartgrid.common.transformations import validate_record


def _build_consumer_config() -> dict[str, Any]:
    settings = get_settings()
    return {
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": "smartgrid-speed-layer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "smartgrid-speed-consumer",
    }


def _read_message_value(message: Any) -> dict[str, Any]:
    if message is None or message.value() is None:
        raise ValueError("empty kafka message")

    payload = message.value()
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")

    return json.loads(payload)


def _insert_zone_snapshot(zone: str, snapshot: dict[str, Any]) -> None:
    settings = get_settings()

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO speed.zone_snapshot (
                    zone_name,
                    snapshot_ts,
                    total_consumption_kwh,
                    total_generation_kwh,
                    net_kwh,
                    renewable_share,
                    active_meters
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    zone,
                    snapshot["snapshot_ts"],
                    snapshot["total_consumption_kwh"],
                    snapshot["total_generation_kwh"],
                    snapshot["net_kwh"],
                    snapshot["renewable_share"],
                    snapshot["active_meters"],
                ),
            )
        conn.commit()


def _window_snapshot(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_consumption = sum(float(r["power_consumption_kwh"]) for r in rows)
    total_generation = sum(float(r["solar_generation_kwh"]) for r in rows)
    net_kwh = total_consumption - total_generation

    renewable_share = (
        total_generation / total_consumption if total_consumption > 0 else 0.0
    )

    snapshot_ts = max(datetime.fromisoformat(r["event_time"]) for r in rows)

    return {
        "snapshot_ts": snapshot_ts.replace(tzinfo=UTC).isoformat(),
        "total_consumption_kwh": round(total_consumption, 6),
        "total_generation_kwh": round(total_generation, 6),
        "net_kwh": round(net_kwh, 6),
        "renewable_share": round(renewable_share, 6),
        "active_meters": len(rows),
    }


def _ensure_speed_table() -> None:
    settings = get_settings()

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS speed.zone_snapshot (
                    zone_name TEXT NOT NULL,
                    snapshot_ts TIMESTAMPTZ NOT NULL,
                    total_consumption_kwh DOUBLE PRECISION NOT NULL,
                    total_generation_kwh DOUBLE PRECISION NOT NULL,
                    net_kwh DOUBLE PRECISION NOT NULL,
                    renewable_share DOUBLE PRECISION NOT NULL,
                    active_meters INTEGER NOT NULL,
                    PRIMARY KEY (zone_name, snapshot_ts)
                )
                """
            )
        conn.commit()


def consume_speed_stream() -> None:
    settings = get_settings()
    consumer = Consumer(_build_consumer_config())
    consumer.subscribe([settings.kafka_topic_readings])

    _ensure_speed_table()

    window: dict[str, list[dict[str, Any]]] = defaultdict(list)
    last_flush = time.time()

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                if time.time() - last_flush >= 5.0 and window:
                    for zone, rows in list(window.items()):
                        snapshot = _window_snapshot(rows)
                        _insert_zone_snapshot(zone, snapshot)
                        window[zone] = []
                    last_flush = time.time()
                continue

            if message.error():
                raise KafkaException(message.error())

            try:
                raw = _read_message_value(message)
                result = validate_record(
                    raw,
                    known_household_ids=None,
                    now=datetime.now(UTC),
                )

                if not result.ok:
                    print(f"Rejected reading: {result.reason} | {result.detail}")
                    continue

                record = result.record
                zone = record["grid_zone"]
                window[zone].append(record)

                if len(window[zone]) >= 10:
                    snapshot = _window_snapshot(window[zone])
                    _insert_zone_snapshot(zone, snapshot)
                    window[zone] = []

                consumer.commit(asynchronous=False)

            except Exception as exc:
                print(f"Error processing record: {exc}")
                continue

    finally:
        consumer.close()


def main() -> None:
    print("Starting speed-layer consumer...")
    consume_speed_stream()


if __name__ == "__main__":
    main()