"""
Speed layer: Spark Structured Streaming from Kafka (ADR-0001, ADR-0002).

Two streaming queries read the same topic, each with its own checkpoint.

  ingest -- every micro-batch, stateless:
      * validates every message with the SHARED validator (ADR-0004);
      * appends every VALID reading to the Parquet master dataset,
        s3a://lake/readings/dt=<simulated date>/grid_zone=<zone>/;
      * sends every INVALID one to the dead-letter topic, with its reason and
        the Kafka offset it came from;
      * records, exactly once per micro-batch, how many of each it saw and how
        far through simulated time the archive has got.

  zone_metrics -- stateful, event-time:
      * deduplicates retransmissions by event_id within the watermark;
      * aggregates load and renewable mix per zone per 15-minute window;
      * upserts every changed window into speed.zone_metrics (PROVISIONAL).

Why the archive keeps duplicates. A watermark-bounded deduplication would
also DISCARD readings that arrive after the watermark -- and late readings
are exactly what the batch layer exists to recover. So the archive keeps
every valid reading, retransmissions included, and deduplication happens
where it is safe: here for the real-time view, and in the batch computation
(Section 7), which reads the whole day and deduplicates by event_id.

Delivery guarantees. The metrics upsert overwrites each window with its
current aggregate, so replaying a micro-batch converges instead of double
counting. The archive is at-least-once -- a retried micro-batch may append
the same rows twice -- which the batch layer's deduplication absorbs. The
batch bookkeeping in ops.* is keyed by (simulation, batch id), so it is
exactly-once, and so are the Prometheus counters derived from it.

    python -m smartgrid.streaming.speed_layer      (inside the Spark container)
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import prometheus_client as prom
import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from smartgrid.common.clock import SimulatedClock
from smartgrid.common.clock_store import shared_clock
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.db import ensure_schema
from smartgrid.common.domain import build_fleet_from_settings
from smartgrid.common.logging import PipelineStage, configure_logging, get_logger
from smartgrid.streaming.spark_session import build_spark
from smartgrid.streaming.transforms import (
    archive_frame,
    dlq_frame,
    kafka_rows,
    validated_readings,
    zone_window_metrics,
)

SERVICE = "speed-layer"
log = get_logger("smartgrid.streaming.speed_layer")

# -- Metrics -----------------------------------------------------------------
RECORDS = prom.Counter(
    "smartgrid_speed_records_total",
    "Readings processed by the speed layer, by outcome ('valid' or the quarantine reason).",
    ["outcome"],
)
LATE_DROPPED = prom.Counter(
    "smartgrid_speed_late_rows_dropped_total",
    "Readings too late for the real-time view (behind the watermark). Still archived.",
)
INPUT_RATE = prom.Gauge(
    "smartgrid_speed_input_rows_per_second", "Kafka input rate, per query.", ["query"]
)
PROCESS_RATE = prom.Gauge(
    "smartgrid_speed_processed_rows_per_second", "Processing rate, per query.", ["query"]
)
BATCH_SECONDS = prom.Gauge(
    "smartgrid_speed_batch_duration_seconds", "Duration of the last micro-batch.", ["query"]
)
LAST_BATCH = prom.Gauge(
    "smartgrid_speed_last_batch_timestamp_seconds",
    "Real time the last micro-batch finished, per query. Staleness means the stream stalled.",
    ["query"],
)
WATERMARK = prom.Gauge(
    "smartgrid_speed_watermark_seconds",
    "Event-time watermark of the metrics query, as simulated Unix seconds.",
)
ARCHIVED_UP_TO = prom.Gauge(
    "smartgrid_speed_archived_event_time_seconds",
    "Latest simulated event time written to the master dataset.",
)


# -- Bookkeeping -------------------------------------------------------------


def _utc(moment: datetime | None) -> datetime | None:
    """
    PySpark's collect() returns naive datetimes in the Python process's local
    zone. The container runs in UTC, but event time must not depend on that.
    """
    if moment is None or moment.tzinfo is not None:
        return moment
    return moment.replace(tzinfo=UTC)


def record_ingest(
    dsn: str,
    sim_id: int,
    batch_id: int,
    counts: dict[str, int],
    max_event_time: datetime | None,
) -> bool:
    """
    Record one ingest micro-batch, exactly once. Returns False if this batch
    was already recorded -- Spark re-running a micro-batch after a failure --
    so callers can avoid counting it twice.
    """
    valid = counts.get("valid", 0)
    total = sum(counts.values())
    with psycopg.connect(dsn) as conn:
        inserted = conn.execute(
            "INSERT INTO ops.ingest_batches (sim_id, batch_id, records_in, records_valid, "
            "records_quarantined, max_event_time) VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (sim_id, batch_id) DO NOTHING",
            (sim_id, batch_id, total, valid, total - valid, max_event_time),
        ).rowcount
        if not inserted:
            return False
        for reason, records in counts.items():
            if reason != "valid":
                conn.execute(
                    "INSERT INTO ops.quarantine_counts (sim_id, batch_id, reason, records) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (sim_id, batch_id, reason, records),
                )
        _record_progress(conn, "ingest", sim_id, batch_id, max_event_time)
    return True


def _record_progress(
    conn: Any, query: str, sim_id: int, batch_id: int, max_event_time: datetime | None
) -> None:
    conn.execute(
        "INSERT INTO ops.stream_progress (query_name, sim_id, batch_id, max_event_time) "
        "VALUES (%s, %s, %s, %s) ON CONFLICT (query_name) DO UPDATE SET "
        "sim_id = EXCLUDED.sim_id, batch_id = EXCLUDED.batch_id, "
        "max_event_time = GREATEST(ops.stream_progress.max_event_time, EXCLUDED.max_event_time), "
        "updated_at = now()",
        (query, sim_id, batch_id, max_event_time),
    )


# -- Sinks -------------------------------------------------------------------


class IngestSink:
    """foreachBatch handler: archive the valid, dead-letter the invalid."""

    def __init__(self, settings: Settings, sim_id: int) -> None:
        self.settings = settings
        self.sim_id = sim_id
        self.archive_path = f"s3a://{settings.minio_bucket_lake}/readings"

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        with PipelineStage(log, "ingest", batch_id=batch_id) as stage:
            batch.persist()
            try:
                summary = (
                    batch.groupBy(
                        F.when(F.col("is_valid"), F.lit("valid"))
                        .otherwise(F.col("quarantine_reason"))
                        .alias("outcome")
                    )
                    .agg(F.count(F.lit(1)).alias("n"), F.max("event_time").alias("latest"))
                    .collect()
                )
                counts = {row["outcome"]: row["n"] for row in summary}
                latest = _utc(
                    max(
                        (r["latest"] for r in summary if r["outcome"] == "valid" and r["latest"]),
                        default=None,
                    )
                )
                valid = counts.get("valid", 0)
                quarantined = sum(counts.values()) - valid

                if valid:
                    (
                        archive_frame(batch.filter(F.col("is_valid")))
                        .repartition("dt", "grid_zone")  # one file per partition per batch
                        .write.mode("append")
                        .partitionBy("dt", "grid_zone")
                        .parquet(self.archive_path)
                    )
                if quarantined:
                    (
                        dlq_frame(batch.filter(~F.col("is_valid")))
                        .write.format("kafka")
                        .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap)
                        .option("topic", self.settings.kafka_topic_dlq)
                        .save()
                    )

                if counts and record_ingest(
                    self.settings.postgres_dsn, self.sim_id, batch_id, counts, latest
                ):
                    for outcome, n in counts.items():
                        RECORDS.labels(outcome).inc(n)
                    if latest is not None:
                        ARCHIVED_UP_TO.set(latest.timestamp())

                stage.records_in = sum(counts.values())
                stage.records_out = valid
                stage.records_quarantined = quarantined
                stage.context["quarantined_by_reason"] = {
                    k: v for k, v in counts.items() if k != "valid"
                }
                stage.context["archived_up_to"] = latest.isoformat() if latest else None
            finally:
                batch.unpersist()


class ZoneMetricsSink:
    """foreachBatch handler: upsert every window whose aggregate changed."""

    _UPSERT = (
        "INSERT INTO speed.zone_metrics (grid_zone, window_start, window_end, consumption_kwh, "
        "generation_kwh, net_kwh, grid_load_kw, solar_kw, renewable_share, readings, "
        "meters_reporting) VALUES (%(grid_zone)s, %(window_start)s, %(window_end)s, "
        "%(consumption_kwh)s, %(generation_kwh)s, %(net_kwh)s, %(grid_load_kw)s, %(solar_kw)s, "
        "%(renewable_share)s, %(readings)s, %(meters_reporting)s) "
        "ON CONFLICT (grid_zone, window_start) DO UPDATE SET "
        "window_end = EXCLUDED.window_end, consumption_kwh = EXCLUDED.consumption_kwh, "
        "generation_kwh = EXCLUDED.generation_kwh, net_kwh = EXCLUDED.net_kwh, "
        "grid_load_kw = EXCLUDED.grid_load_kw, solar_kw = EXCLUDED.solar_kw, "
        "renewable_share = EXCLUDED.renewable_share, readings = EXCLUDED.readings, "
        "meters_reporting = EXCLUDED.meters_reporting, updated_at = now()"
    )

    def __init__(self, settings: Settings, sim_id: int) -> None:
        self.settings = settings
        self.sim_id = sim_id

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        with PipelineStage(log, "zone_metrics", batch_id=batch_id) as stage:
            rows = [row.asDict() for row in batch.collect()]  # a few windows x 6 zones
            for row in rows:
                row["window_start"] = _utc(row["window_start"])
                row["window_end"] = _utc(row["window_end"])
            stage.records_in = stage.records_out = len(rows)
            if not rows:
                return
            with psycopg.connect(self.settings.postgres_dsn) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(self._UPSERT, rows)
                _record_progress(
                    conn,
                    "zone_metrics",
                    self.sim_id,
                    batch_id,
                    max(row["window_end"] for row in rows),
                )
            stage.context["windows"] = sorted(
                {row["window_start"].strftime("%Y-%m-%d %H:%M") for row in rows}
            )


# -- Observability -----------------------------------------------------------


class ProgressListener(StreamingQueryListener):
    """Turns Spark's per-batch progress reports into metrics and JSON logs."""

    def onQueryStarted(self, event) -> None:
        log.info("streaming query started", extra={"query": event.name, "run_id": str(event.runId)})

    def onQueryProgress(self, event) -> None:
        progress = event.progress
        name = progress.name or "unnamed"
        INPUT_RATE.labels(name).set(progress.inputRowsPerSecond or 0.0)
        PROCESS_RATE.labels(name).set(progress.processedRowsPerSecond or 0.0)
        BATCH_SECONDS.labels(name).set((progress.batchDuration or 0) / 1000.0)
        LAST_BATCH.labels(name).set(time.time())

        dropped = sum(op.numRowsDroppedByWatermark for op in progress.stateOperators or [])
        if dropped:
            LATE_DROPPED.inc(dropped)
        watermark = (progress.eventTime or {}).get("watermark")
        if watermark:
            WATERMARK.set(datetime.fromisoformat(watermark.replace("Z", "+00:00")).timestamp())

        log.info(
            "micro-batch progress",
            extra={
                "query": name,
                "batch_id": progress.batchId,
                "input_rows": progress.numInputRows,
                "input_rows_per_second": round(progress.inputRowsPerSecond or 0.0, 1),
                "processed_rows_per_second": round(progress.processedRowsPerSecond or 0.0, 1),
                "batch_ms": progress.batchDuration,
                "watermark": watermark,
                "late_rows_dropped": dropped,
                "state_rows": sum(op.numRowsTotal for op in progress.stateOperators or []),
            },
        )

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        log.error(
            "streaming query terminated",
            extra={"run_id": str(event.runId), "exception": event.exception},
        )


# -- Wiring ------------------------------------------------------------------


def start_queries(
    spark: SparkSession,
    settings: Settings,
    clock: SimulatedClock,
    known_household_ids: frozenset[str],
) -> list:
    sim_id = int(clock.real_start)
    # Checkpoints are per SIMULATION: after a reset, a new anchor means a new
    # checkpoint directory, so the job starts cleanly on the recreated topic.
    checkpoints = f"{settings.spark_checkpoint_root}/sim-{sim_id}"
    trigger = f"{settings.speed_trigger_seconds} seconds"

    def source() -> DataFrame:
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", settings.kafka_topic_readings)
            .option("startingOffsets", "earliest")
            .option("includeHeaders", "true")
            .option("maxOffsetsPerTrigger", settings.speed_max_offsets_per_trigger)
            # A simulation reset deletes and recreates the topic.
            .option("failOnDataLoss", "false")
            .load()
        )

    def readings() -> DataFrame:
        return validated_readings(
            kafka_rows(source()), known_household_ids=known_household_ids, clock=clock
        )

    ingest = (
        readings()
        .writeStream.queryName("ingest")
        .foreachBatch(IngestSink(settings, sim_id))
        .option("checkpointLocation", f"{checkpoints}/ingest")
        .trigger(processingTime=trigger)
        .start()
    )

    windowed = zone_window_metrics(
        readings()
        .filter(F.col("is_valid"))
        .withWatermark("event_time", f"{settings.speed_watermark_minutes} minutes")
        .dropDuplicatesWithinWatermark(["event_id"]),
        window=f"{settings.speed_window_minutes} minutes",
        reading_interval_hours=settings.sim_emit_interval_seconds * clock.compression / 3600.0,
    )
    zone_metrics = (
        windowed.writeStream.queryName("zone_metrics")
        .outputMode("update")
        .foreachBatch(ZoneMetricsSink(settings, sim_id))
        .option("checkpointLocation", f"{checkpoints}/zone_metrics")
        .trigger(processingTime=trigger)
        .start()
    )
    return [ingest, zone_metrics]


def main() -> None:
    settings = get_settings()
    configure_logging(service=SERVICE, level=settings.log_level)

    clock = shared_clock(settings)
    fleet = build_fleet_from_settings(settings)
    ensure_schema(settings.postgres_dsn)

    spark = build_spark("smartgrid-speed-layer", settings, streaming=True)
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressListener())
    prom.start_http_server(settings.speed_layer_metrics_port)

    queries = start_queries(spark, settings, clock, fleet.household_ids)
    log.info(
        "speed layer running",
        extra={
            "clock": clock.describe(),
            "sim_id": int(clock.real_start),
            "topic": settings.kafka_topic_readings,
            "dlq": settings.kafka_topic_dlq,
            "archive": f"s3a://{settings.minio_bucket_lake}/readings",
            "window_minutes": settings.speed_window_minutes,
            "watermark_minutes": settings.speed_watermark_minutes,
            "trigger_seconds": settings.speed_trigger_seconds,
            "queries": [q.name for q in queries],
            "metrics_port": settings.speed_layer_metrics_port,
        },
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
