"""
Spark transformations shared by the Lambda layers.

Each function takes and returns a DataFrame and works identically on a
streaming or a static one. That is what lets them be unit-tested on small
static frames (tests/spark/), and what lets the batch layer reuse
`zone_window_metrics` so the speed-vs-batch reconciliation compares figures
computed by the same arithmetic.
"""

from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    TimestampType,
)

from smartgrid.common.clock import SimulatedClock
from smartgrid.common.schemas import METER_READING_FIELDS, field_names
from smartgrid.common.transformations import validate_json_frame
from smartgrid.streaming.spark_schema import struct_for

# Kafka metadata carried alongside every reading, for tracing and audit.
KAFKA_FIELDS = (
    StructField("kafka_key", StringType(), True),
    StructField("raw_value", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),  # real time, set by the producer client
    StructField("trace_id", StringType(), True),  # correlation id from the header
)

VALIDATED_SCHEMA = struct_for(
    METER_READING_FIELDS,
    extra=(
        *KAFKA_FIELDS,
        StructField("quarantine_reason", StringType(), True),
        StructField("is_valid", BooleanType(), False),
    ),
)

READING_COLUMNS = field_names(METER_READING_FIELDS)

# What the master dataset keeps for every valid reading: the reading itself
# plus where it came from, so any archived row can be traced back to its
# Kafka offset and its correlation id.
ARCHIVE_COLUMNS = (
    *READING_COLUMNS,
    "trace_id",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
)


def _header(name: str) -> Column:
    """A Kafka header's value as a string, or null if absent."""
    return F.expr(f"filter(headers, h -> h.key = '{name}')")[0]["value"].cast("string")


def kafka_rows(kafka_df: DataFrame) -> DataFrame:
    """Flatten the Kafka source's columns into the ones the pipeline uses."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        _header("correlation_id").alias("trace_id"),
    )


def validated_readings(
    rows: DataFrame, *, known_household_ids: frozenset[str], clock: SimulatedClock
) -> DataFrame:
    """
    Parse and validate every message with the SHARED validator
    (`common.transformations.validate_json_frame`) -- the same rules, the same
    coercion, the same verdicts as the batch layer and the inspection tools
    (ADR-0004). Adds typed reading columns, `is_valid` and `quarantine_reason`.

    `clock` travels to the executors with the function, so "now" for the
    future-timestamp check is the shared simulated clock, not the wall clock.
    """

    def validate(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for batch in batches:
            checked = validate_json_frame(
                batch["raw_value"], known_household_ids=known_household_ids, now=clock.now()
            )
            yield pd.concat([batch, checked], axis=1)[VALIDATED_SCHEMA.fieldNames()]

    return rows.mapInPandas(validate, schema=VALIDATED_SCHEMA)


def archive_frame(valid: DataFrame) -> DataFrame:
    """
    Rows for the Parquet master dataset, with `dt` -- the SIMULATED date the
    reading was taken, in UTC -- as the partition key (ADR-0005, ADR-0007).
    """
    return valid.select(*ARCHIVE_COLUMNS, F.to_date("event_time").alias("dt"))


def dlq_frame(invalid: DataFrame) -> DataFrame:
    """
    Dead-letter records: the original bytes, why they were rejected, and
    exactly where in Kafka they came from, so a rejected reading can be
    investigated, fixed at source, and replayed.
    """
    envelope = F.struct(
        F.col("quarantine_reason"),
        F.col("raw_value"),
        F.col("trace_id").alias("correlation_id"),
        F.col("kafka_partition").alias("source_partition"),
        F.col("kafka_offset").alias("source_offset"),
        F.col("kafka_timestamp").alias("source_timestamp"),
        F.current_timestamp().alias("quarantined_at"),
    )
    return invalid.select(F.col("kafka_key").alias("key"), F.to_json(envelope).alias("value"))


def zone_window_metrics(
    readings: DataFrame, *, window: str, reading_interval_hours: float
) -> DataFrame:
    """
    Per-zone load and renewable mix per event-time window.

    `grid_load_kw` is the zone's average draw across the window. Each reading
    is the energy used over one reading interval, so its average power is
    kWh / interval; the mean of that over the window's readings, times the
    number of meters reporting, is the zone's load.

    That is exact when every meter reports the same number of times in the
    window, which holds here because the fleet reports in rounds; a meter
    straddling a window boundary makes it a close approximation, which is
    acceptable for a provisional view. The obvious alternative -- summing raw
    kWh over the window -- is far worse: a 15-minute window holds one round of
    9.6-minute readings or two, so the load would jump by 2x between windows.
    """
    per_reading = F.lit(1.0 / reading_interval_hours)
    grouped = readings.groupBy(F.window("event_time", window).alias("w"), "grid_zone").agg(
        F.sum("power_consumption_kwh").alias("consumption_kwh"),
        F.sum("solar_generation_kwh").alias("generation_kwh"),
        F.avg("power_consumption_kwh").alias("_avg_kwh"),
        F.avg("solar_generation_kwh").alias("_avg_solar_kwh"),
        F.count(F.lit(1)).alias("readings"),
        F.approx_count_distinct("meter_id", rsd=0.01).alias("meters_reporting"),
    )
    return grouped.select(
        "grid_zone",
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "consumption_kwh",
        "generation_kwh",
        (F.col("consumption_kwh") - F.col("generation_kwh")).alias("net_kwh"),
        (F.col("_avg_kwh") * per_reading * F.col("meters_reporting")).alias("grid_load_kw"),
        (F.col("_avg_solar_kwh") * per_reading * F.col("meters_reporting")).alias("solar_kw"),
        F.when(
            F.col("consumption_kwh") > 0, F.col("generation_kwh") / F.col("consumption_kwh")
        ).alias("renewable_share"),
        "readings",
        "meters_reporting",
    )
