"""
Reconcile the speed layer: nothing lost, nothing double-counted.

Counts the same readings at every stage they pass through and checks that
the stages agree:

    Kafka readings topic  -- messages published
    ops.ingest_batches    -- messages the speed layer processed, split into
                             valid and quarantined (exactly-once per batch)
    Kafka dead-letter     -- quarantined messages actually dead-lettered
    Parquet archive       -- valid readings actually archived
    speed.zone_metrics    -- what the real-time view currently shows

The archive and the dead-letter topic are written at-least-once, so they may
hold MORE rows than processed if a micro-batch was retried -- never fewer.
Run it after the producer has stopped and the speed layer has caught up.

    python scripts/inspect_speed_layer.py
"""

from __future__ import annotations

import sys
import uuid

import psycopg
import pyarrow.dataset as ds
from confluent_kafka import Consumer, TopicPartition
from pyarrow import fs

from smartgrid.common.clock_store import shared_clock
from smartgrid.common.config import get_settings

RULE = "-" * 78


def topic_messages(settings, topic: str) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"inspect-{uuid.uuid4().hex[:8]}",
        }
    )
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        total = 0
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            total += high - low
        return total
    finally:
        consumer.close()


def archive(settings):
    endpoint = settings.s3_endpoint.split("://", 1)
    s3 = fs.S3FileSystem(
        access_key=settings.minio_root_user,
        secret_key=settings.minio_root_password,
        endpoint_override=endpoint[1],
        scheme=endpoint[0],
    )
    return ds.dataset(
        f"{settings.minio_bucket_lake}/readings",
        filesystem=s3,
        format="parquet",
        partitioning="hive",
    )


def main() -> int:
    settings = get_settings()
    clock = shared_clock(settings)

    with psycopg.connect(settings.postgres_dsn) as conn:
        batches, records_in, valid, quarantined = conn.execute(
            "SELECT count(*), coalesce(sum(records_in),0), coalesce(sum(records_valid),0), "
            "coalesce(sum(records_quarantined),0) FROM ops.ingest_batches "
            "WHERE sim_id = %s",
            (int(clock.real_start),),
        ).fetchone()
        reasons = conn.execute(
            "SELECT reason, sum(records) FROM ops.quarantine_counts WHERE sim_id = %s "
            "GROUP BY reason ORDER BY 2 DESC",
            (int(clock.real_start),),
        ).fetchall()
        progress = dict(
            conn.execute("SELECT query_name, max_event_time FROM ops.stream_progress").fetchall()
        )
        zones = conn.execute(
            "SELECT grid_zone, count(*), min(window_start), max(window_end), "
            "round(avg(grid_load_kw)::numeric, 1), round(max(renewable_share)::numeric, 2) "
            "FROM speed.zone_metrics GROUP BY grid_zone ORDER BY grid_zone"
        ).fetchall()

    published = topic_messages(settings, settings.kafka_topic_readings)
    dead_lettered = topic_messages(settings, settings.kafka_topic_dlq)

    table = archive(settings).to_table(columns=["event_id", "dt"])
    archived = table.num_rows
    frame = table.to_pandas()
    duplicate_rows = int(frame["event_id"].duplicated().sum())
    per_day = frame.groupby("dt").size().to_dict()

    print(
        f"\n{RULE}\n  Speed layer reconciliation   (simulation {int(clock.real_start)}, "
        f"simulated now {clock.now():%Y-%m-%d %H:%M})\n{RULE}"
    )
    print(f"  published to Kafka          {published:>8,}")
    print(f"  processed by the speed layer {records_in:>7,}   in {batches} micro-batches")
    print(f"    valid                     {valid:>8,}")
    print(
        f"    quarantined               {quarantined:>8,}   "
        + ", ".join(f"{r} {n}" for r, n in reasons)
    )
    print(f"  dead-lettered               {dead_lettered:>8,}")
    print(
        f"  archived to Parquet         {archived:>8,}   "
        + " | ".join(f"dt={d} {n:,}" for d, n in sorted(per_day.items()))
    )
    print(
        f"    of which retransmissions  {duplicate_rows:>8,}   "
        "(kept by design; the batch layer dedupes)"
    )

    print("\n  real-time view (speed.zone_metrics)")
    for zone, windows, first, last, load, peak_share in zones:
        print(
            f"    {zone}  {windows:>3} windows  {first:%m-%d %H:%M} .. {last:%m-%d %H:%M}  "
            f"avg load {load} kW  peak renewable share {peak_share}"
        )
    for query, when in sorted(progress.items()):
        lag = (clock.now() - when).total_seconds() / 60 if when else float("nan")
        print(
            f"  {query:<13} has reached {when:%m-%d %H:%M}  ({lag:,.0f} simulated minutes behind)"
        )

    checks = [
        ("every published message was processed", records_in == published),
        ("processed = valid + quarantined", records_in == valid + quarantined),
        ("every quarantined message reached the dead-letter topic", dead_lettered >= quarantined),
        ("every valid reading reached the archive", archived >= valid),
        ("nothing was dead-lettered twice", dead_lettered == quarantined),
        ("nothing was archived twice", archived == valid),
    ]
    print(f"\n{RULE}")
    failed = 0
    for name, ok in checks:
        failed += 0 if ok else 1
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(RULE + "\n")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
