"""
The speed layer's Spark transformations, on small static DataFrames.

These need a JVM, so they run inside the Spark image:

    docker compose run --rm --no-deps speed-layer python -m pytest tests/spark -p no:cacheprovider

and are skipped automatically anywhere pyspark is not installed.
"""

import json
import random
import time
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402 - after the importorskip guard
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from smartgrid.common.clock import SimulatedClock  # noqa: E402
from smartgrid.common.domain import build_fleet  # noqa: E402
from smartgrid.common.schemas import METER_READING_FIELDS  # noqa: E402
from smartgrid.common.transformations import validate_record  # noqa: E402
from smartgrid.producers.faults import FaultKind, corrupt  # noqa: E402
from smartgrid.streaming.spark_schema import struct_for  # noqa: E402
from smartgrid.streaming.transforms import (  # noqa: E402
    archive_frame,
    dlq_frame,
    kafka_rows,
    validated_readings,
    zone_window_metrics,
)

pytestmark = pytest.mark.spark

NOW = datetime(2026, 1, 5, 12, 0, tzinfo=UTC)
FLEET = build_fleet(num_households=12, num_zones=2, seed=4)


def make_clock() -> SimulatedClock:
    """
    Anchored NOW, at 1x: on the executors, "now" is the test's NOW plus the
    few real seconds the test takes. An anchor at Unix time 0 would put "now"
    56 years x 288 into the future -- past the year 9999.
    """
    return SimulatedClock(start=NOW, day_seconds=86_400.0, real_start=time.time())

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
        StructField(
            "headers",
            ArrayType(
                StructType([StructField("key", StringType()), StructField("value", BinaryType())])
            ),
        ),
    ]
)


def reading(household, when, kwh=0.2, solar=0.05):
    return {
        "event_id": f"{household.meter_id}:{when:%Y%m%dT%H%M%S}",
        "meter_id": household.meter_id,
        "household_id": household.household_id,
        "grid_zone": household.grid_zone,
        "power_consumption_kwh": kwh,
        "solar_generation_kwh": solar,
        "event_time": when.isoformat(),
        "correlation_id": "c0ffee",
    }


def kafka_frame(spark, values):
    rows = [
        (
            b"k",
            value.encode(),
            "meter.readings.v1",
            0,
            i,
            datetime(2026, 9, 30, 12, 0),
            0,
            [("correlation_id", f"trace-{i}".encode())],
        )
        for i, value in enumerate(values)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def validated(spark, values):
    return validated_readings(
        kafka_rows(kafka_frame(spark, values)),
        known_household_ids=FLEET.household_ids,
        clock=make_clock(),
    )


def test_spark_schema_is_generated_from_the_field_specs():
    schema = struct_for(METER_READING_FIELDS)
    assert schema.fieldNames() == [spec.name for spec in METER_READING_FIELDS]


def test_spark_reaches_the_same_verdict_as_the_row_validator(spark):
    rng = random.Random(0)
    values = []
    for kind in [None, *FaultKind]:
        for _ in range(5):
            record = reading(
                rng.choice(FLEET.households), NOW - timedelta(minutes=rng.randint(1, 90))
            )
            sent = corrupt(record, kind, rng)
            values.append(sent.decode() if isinstance(sent, bytes) else json.dumps(sent))

    now = make_clock().now()
    expected = []
    for value in values:
        try:
            result = validate_record(
                json.loads(value), known_household_ids=FLEET.household_ids, now=now
            )
            expected.append(None if result.ok else result.reason.value)
        except ValueError:
            expected.append("malformed_json")

    got = [
        row["quarantine_reason"]
        for row in validated(spark, values).orderBy("kafka_offset").collect()
    ]
    assert got == expected


def test_valid_rows_are_typed_and_carry_their_trace(spark):
    household = FLEET.households[0]
    rows = validated(spark, [json.dumps(reading(household, NOW - timedelta(minutes=5)))]).collect()
    row = rows[0]
    assert row["is_valid"] is True
    assert isinstance(row["power_consumption_kwh"], float)
    assert row["trace_id"] == "trace-0"
    assert row["kafka_offset"] == 0


def test_archive_partitions_by_the_simulated_utc_date(spark):
    household = FLEET.households[0]
    # The evening BEFORE the test's 'now' (12:00 on the 5th): a reading at
    # 23:55 on the 5th would be in the future, and rightly quarantined.
    late_evening = datetime(2026, 1, 4, 23, 55, tzinfo=UTC)
    frame = validated(spark, [json.dumps(reading(household, late_evening))])
    row = archive_frame(frame.filter("is_valid")).collect()[0]
    assert row["dt"].isoformat() == "2026-01-04"
    assert row["trace_id"] == "trace-0"


def test_dead_letters_carry_reason_payload_and_origin(spark):
    frame = validated(spark, ['{"truncated": '])
    envelope = json.loads(dlq_frame(frame.filter("NOT is_valid")).collect()[0]["value"])
    assert envelope["quarantine_reason"] == "malformed_json"
    assert envelope["raw_value"] == '{"truncated": '
    assert envelope["source_offset"] == 0
    assert envelope["correlation_id"] == "trace-0"


def test_zone_load_is_average_power_times_meters(spark):
    """
    Three meters, two readings each in one window, at 1, 2 and 0.5 kW over
    a 0.16 h reading interval. Zone load must be 3.5 kW -- not the raw kWh sum.
    """
    interval_h = 0.16
    zone = FLEET.zones[0]
    meters = FLEET.in_zone(zone)[:3]
    powers = [1.0, 2.0, 0.5]
    base = datetime(2026, 1, 5, 10, 0, tzinfo=UTC)
    values = []
    for household, kw in zip(meters, powers, strict=True):
        for step in (1, 8):
            values.append(
                json.dumps(
                    reading(
                        household, base + timedelta(minutes=step), kwh=kw * interval_h, solar=0.0
                    )
                )
            )
    readings = validated(spark, values).filter("is_valid")
    row = zone_window_metrics(
        readings, window="15 minutes", reading_interval_hours=interval_h
    ).collect()[0]

    assert row["meters_reporting"] == 3
    assert row["readings"] == 6
    assert row["grid_load_kw"] == pytest.approx(3.5)
    assert row["consumption_kwh"] == pytest.approx(2 * 3.5 * interval_h)
    assert row["renewable_share"] == 0.0
    assert row["window_end"] - row["window_start"] == timedelta(minutes=15)
