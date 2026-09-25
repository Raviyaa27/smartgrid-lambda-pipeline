"""
Speed layer: real-time per-zone grid load / renewable-mix aggregation.

Reads MeterReading events from Kafka, validates them, routes anything
malformed to the DLQ topic, and writes tumbling-window aggregates
(total consumption, total solar, renewable %) per zone into the
`speed` Postgres schema. This is the "speed layer" half of the Lambda
architecture -- fast, approximate, and continuously updated.

Run (from repo root, venv active, infra up, spark-submit available):
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        -m smartgrid.streaming.speed_layer

Env vars used (see common/config.py):
    KAFKA_BOOTSTRAP_HOST, KAFKA_TOPIC_READINGS, KAFKA_TOPIC_DLQ,
    POSTGRES_HOST/PORT/USER/PASSWORD/POSTGRES_SERVING_DB,
    SPEED_WINDOW_SECONDS, SPEED_WATERMARK_SECONDS

Before running, apply infra/speed_layer_ddl.sql to create the
speed.zone_load table.
"""

from __future__ import annotations

import json
import logging

import psycopg
from confluent_kafka import Producer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from smartgrid.common.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("speed-layer")

# Mirrors MeterReading in common/models.py. Kept separate (not imported)
# because Spark schemas are declarative, not Pydantic models.
READING_SCHEMA = StructType(
    [
        StructField("meter_id", StringType(), nullable=True),
        StructField("household_id", StringType(), nullable=True),
        StructField("zone", StringType(), nullable=True),
        StructField("power_consumption_kwh", DoubleType(), nullable=True),
        StructField("solar_generation_kwh", DoubleType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
    ]
)

_dlq_producer: Producer | None = None


def _get_dlq_producer() -> Producer:
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    return _dlq_producer


def _send_to_dlq(raw_value: str, reason: str) -> None:
    producer = _get_dlq_producer()
    envelope = {"reason": reason, "original_value": raw_value}
    producer.produce(settings.dlq_topic, value=json.dumps(envelope))
    producer.poll(0)


def read_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.readings_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_split(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse JSON, then split into (valid, invalid) DataFrames.

    Invalid = failed JSON parsing (all target columns null) OR failed
    basic business validation (negative usage, missing keys).
    """
    parsed = raw_df.select(
        raw_df.value.cast("string").alias("raw_value"),
        F.from_json(raw_df.value.cast("string"), READING_SCHEMA).alias("data"),
    )

    is_valid = (
        parsed.data.meter_id.isNotNull()
        & parsed.data.household_id.isNotNull()
        & parsed.data.zone.isNotNull()
        & parsed.data.timestamp.isNotNull()
        & (parsed.data.power_consumption_kwh >= 0)
        & (parsed.data.solar_generation_kwh >= 0)
    )

    valid = parsed.filter(is_valid).select("data.*")
    invalid = parsed.filter(~is_valid).select("raw_value")
    return valid, invalid


def aggregate_by_zone(valid_df: DataFrame) -> DataFrame:
    watermarked = valid_df.withWatermark("timestamp", f"{settings.speed_watermark_seconds} seconds")
    window = F.window("timestamp", f"{settings.speed_window_seconds} seconds")

    aggregated = watermarked.groupBy("zone", window).agg(
        F.sum("power_consumption_kwh").alias("total_consumption_kwh"),
        F.sum("solar_generation_kwh").alias("total_solar_kwh"),
        F.count("*").alias("reading_count"),
    )

    return aggregated.select(
        "zone",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "total_consumption_kwh",
        "total_solar_kwh",
        F.when(
            F.col("total_consumption_kwh") > 0,
            F.least(F.col("total_solar_kwh") / F.col("total_consumption_kwh") * 100, F.lit(100.0)),
        )
        .otherwise(F.lit(0.0))
        .alias("renewable_pct"),
        "reading_count",
    )


def _pg_dsn() -> str:
    return (
        f"host={settings.postgres_host} port={settings.postgres_port} "
        f"user={settings.postgres_user} password={settings.postgres_password} "
        f"dbname={settings.postgres_serving_db}"
    )


def write_zone_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    with psycopg.connect(_pg_dsn()) as conn, conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO speed.zone_load
                    (zone, window_start, window_end, total_consumption_kwh,
                     total_solar_kwh, renewable_pct, reading_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (zone, window_start) DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                    total_solar_kwh = EXCLUDED.total_solar_kwh,
                    renewable_pct = EXCLUDED.renewable_pct,
                    reading_count = EXCLUDED.reading_count,
                    updated_at = now()
                """,
                (
                    row.zone,
                    row.window_start,
                    row.window_end,
                    row.total_consumption_kwh,
                    row.total_solar_kwh,
                    row.renewable_pct,
                    row.reading_count,
                ),
            )
        conn.commit()

    logger.info("zone_batch_written batch_id=%s rows=%s", batch_id, len(rows))


def write_dlq_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return
    for row in rows:
        _send_to_dlq(row.raw_value, reason="schema_or_validation_failure")
    _get_dlq_producer().flush(timeout=10)
    logger.warning("dlq_batch_sent batch_id=%s rows=%s", batch_id, len(rows))


def run() -> None:
    spark = SparkSession.builder.appName("smartgrid-speed-layer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "speed_layer_starting topic=%s window=%ss watermark=%ss",
        settings.readings_topic,
        settings.speed_window_seconds,
        settings.speed_watermark_seconds,
    )

    raw = read_stream(spark)
    valid, invalid = parse_and_split(raw)
    zone_aggregates = aggregate_by_zone(valid)

    zone_query = (
        zone_aggregates.writeStream.outputMode("update")
        .foreachBatch(write_zone_batch)
        .option("checkpointLocation", "/tmp/smartgrid/checkpoints/zone_load")
        .start()
    )

    dlq_query = (
        invalid.writeStream.outputMode("append")
        .foreachBatch(write_dlq_batch)
        .option("checkpointLocation", "/tmp/smartgrid/checkpoints/dlq")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()