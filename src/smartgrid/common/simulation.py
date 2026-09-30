"""
Start a completely new simulation.

Simulated time restarts at SIM_START_DATE, so everything the previous
simulation left behind would collide with the new one: readings for the same
dates, drops for the same dates, windows for the same hours. A reset clears
all of it:

    clock       new anchor in ops.sim_clock (day 1 starts now)
    Kafka       the readings and dead-letter topics are deleted and recreated
                with the SAME partitions and retention they had -- read from
                the broker, so docker-compose.yml stays the only definition
    MinIO       every daily drop and its ground truth; the Parquet archive
    Postgres    every table that holds per-simulation rows

It is a development tool, not part of the pipeline. Within one simulation
nothing is overwritten (ADR-0008); a reset discards the whole simulated world.

Stop the producers and the speed layer first -- `scripts/dev.ps1 sim-reset`
does this -- because they hold the old clock anchor and would write into the
new simulation with it.

    python -m smartgrid.common.simulation reset --yes
"""

from __future__ import annotations

import argparse
import time
from typing import Any

import psycopg

from smartgrid.common import drops, storage
from smartgrid.common.clock_store import reset_clock
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.db import SIMULATION_TABLES, ensure_schema
from smartgrid.common.logging import configure_logging, get_logger

log = get_logger(__name__)

LAKE_PREFIXES = ("readings/",)


def _recreate_topics(settings: Settings, names: list[str]) -> dict[str, dict[str, Any]]:
    """Delete and recreate topics exactly as they were configured."""
    from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    metadata = admin.list_topics(timeout=15)

    specs: dict[str, dict[str, Any]] = {}
    for name in names:
        topic = metadata.topics.get(name)
        if topic is None:
            continue
        resource = ConfigResource(ResourceType.TOPIC, name)
        config = admin.describe_configs([resource])[resource].result(timeout=15)
        specs[name] = {
            "partitions": len(topic.partitions),
            "replication": len(next(iter(topic.partitions.values())).replicas),
            "retention_ms": config["retention.ms"].value,
        }

    if specs:
        for future in admin.delete_topics(list(specs), operation_timeout=30).values():
            future.result()
        # Deletion is asynchronous on the broker: wait until the topics are gone.
        deadline = time.monotonic() + 60
        while set(specs) & set(admin.list_topics(timeout=10).topics):
            if time.monotonic() > deadline:
                raise TimeoutError(f"topics {sorted(specs)} were not deleted within 60 s")
            time.sleep(1)

    for attempt in range(30):
        pending = [n for n in specs if n not in admin.list_topics(timeout=10).topics]
        if not pending:
            break
        futures = admin.create_topics(
            [
                NewTopic(
                    name,
                    num_partitions=specs[name]["partitions"],
                    replication_factor=specs[name]["replication"],
                    config={"retention.ms": specs[name]["retention_ms"]},
                )
                for name in pending
            ]
        )
        for future in futures.values():
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001 - "already exists" while deletion completes
                log.info(
                    "topic not yet recreatable, retrying",
                    extra={"error": str(exc), "attempt": attempt},
                )
        time.sleep(1)
    return specs


def reset_simulation(settings: Settings) -> dict[str, Any]:
    summary: dict[str, Any] = {}

    clock = reset_clock(settings)
    summary["clock"] = clock.describe()

    client = storage.s3_client(settings)
    raw_keys = storage.list_keys(client, settings.minio_bucket_raw, f"{drops.DATASET}/")
    raw_keys += storage.list_keys(
        client, settings.minio_bucket_raw, f"{drops.GROUND_TRUTH_PREFIX}/"
    )
    storage.delete_keys(client, settings.minio_bucket_raw, raw_keys)
    lake_keys = [
        key
        for prefix in LAKE_PREFIXES
        for key in storage.list_keys(client, settings.minio_bucket_lake, prefix)
    ]
    storage.delete_keys(client, settings.minio_bucket_lake, lake_keys)
    summary["drop_objects_deleted"] = len(raw_keys)
    summary["archive_objects_deleted"] = len(lake_keys)

    ensure_schema(settings.postgres_dsn)
    with psycopg.connect(settings.postgres_dsn) as conn:
        conn.execute(f"TRUNCATE {', '.join(SIMULATION_TABLES)}")
    summary["tables_cleared"] = list(SIMULATION_TABLES)

    summary["topics_recreated"] = _recreate_topics(
        settings, [settings.kafka_topic_readings, settings.kafka_topic_dlq]
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Start a completely new simulation.")
    parser.add_argument("action", choices=("reset",))
    parser.add_argument(
        "--yes", action="store_true", help="confirm discarding the current simulation"
    )
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(service="simulation", level=settings.log_level)
    if not args.yes:
        raise SystemExit(
            "reset discards the whole current simulation; re-run with --yes to confirm"
        )

    summary = reset_simulation(settings)
    log.warning("new simulation started", extra=summary)


if __name__ == "__main__":
    main()
