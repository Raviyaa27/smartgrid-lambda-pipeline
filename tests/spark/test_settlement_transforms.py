"""
The settlement job's Spark steps, on a small static archive.

    docker compose run --rm --no-deps speed-layer python -m pytest tests/spark -p no:cacheprovider
"""

import time
from datetime import UTC, date, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from smartgrid.batch.settlement import revalidate  # noqa: E402
from smartgrid.common.clock import SimulatedClock  # noqa: E402
from smartgrid.common.domain import build_fleet  # noqa: E402

pytestmark = pytest.mark.spark

NOW = datetime(2026, 1, 6, 1, 0, tzinfo=UTC)
FLEET = build_fleet(num_households=6, num_zones=2, seed=2)


def make_clock() -> SimulatedClock:
    return SimulatedClock(start=NOW, day_seconds=86_400.0, real_start=time.time())


def archive_rows(spark, rows):
    """Rows shaped like the Parquet master dataset."""
    columns = [
        "event_id",
        "meter_id",
        "household_id",
        "grid_zone",
        "power_consumption_kwh",
        "solar_generation_kwh",
        "voltage_v",
        "event_time",
        "ingest_time",
        "correlation_id",
        "schema_version",
        "trace_id",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "dt",
    ]
    return spark.createDataFrame(rows, columns)


def row(household, minutes, kwh=0.2, offset=0):
    when = datetime(2026, 1, 5, 12, 0, tzinfo=UTC) + timedelta(minutes=minutes)
    return (
        f"{household.meter_id}:{when:%Y%m%dT%H%M%S}",
        household.meter_id,
        household.household_id,
        household.grid_zone,
        kwh,
        0.0,
        230.0,
        when,
        when,
        "c",
        "1.0.0",
        "t",
        0,
        offset,
        datetime(2026, 9, 30, 12, 0),
        date(2026, 1, 5),
    )


def test_resettlement_applies_todays_rules_to_archived_readings(spark):
    """
    A reading archived under an older, looser rule (60 kWh in one interval,
    above today's 50 kWh maximum) must be REJECTED when the day is settled
    again -- that is what makes restatement meaningful.
    """
    h = FLEET.households[0]
    frame = archive_rows(spark, [row(h, 0), row(h, 10, kwh=60.0, offset=1)])
    checked = revalidate(frame, known_household_ids=FLEET.household_ids, clock=make_clock())
    verdicts = {
        r["kafka_offset"]: (r["is_valid"], r["quarantine_reason"]) for r in checked.collect()
    }
    assert verdicts[0] == (True, None)
    assert verdicts[1] == (False, "out_of_range")


def test_settlement_deduplicates_over_the_whole_day_without_a_watermark(spark):
    """Retransmissions are removed; a reading hours late is kept."""
    h1, h2 = FLEET.households[0], FLEET.households[1]
    frame = archive_rows(
        spark,
        [
            row(h1, 0, offset=0),
            row(h1, 0, offset=1),  # a retransmission
            row(h2, 0, offset=2),
            row(h2, -600, offset=3),  # a reading 10 hours late
        ],
    )
    checked = revalidate(frame, known_household_ids=FLEET.household_ids, clock=make_clock())
    settled = checked.filter(F.col("is_valid")).dropDuplicates(["event_id"])
    assert settled.count() == 3
    assert settled.filter(F.col("household_id") == h2.household_id).count() == 2
