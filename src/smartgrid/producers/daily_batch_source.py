from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, date, datetime, timedelta
from typing import Any

import boto3
from botocore.config import Config

from smartgrid.common.clock import SimulatedClock
from smartgrid.common.config import get_settings
from smartgrid.common.domain import Fleet, build_fleet_from_settings
from smartgrid.common.schemas import (
    TARIFF_RECORD_FIELDS,
    WEATHER_RECORD_FIELDS,
    TariffRecord,
    WeatherRecord,
)
from smartgrid.common.transformations import validate_record


TARIFF_RATES: dict[str, float] = {
    "DOMESTIC_LOW": 8.0,
    "DOMESTIC_STD": 12.0,
    "DOMESTIC_HIGH": 30.0,
    "INDUSTRIAL": 45.0,
}

FIXED_CHARGES: dict[str, float] = {
    "DOMESTIC_LOW": 150.0,
    "DOMESTIC_STD": 300.0,
    "DOMESTIC_HIGH": 600.0,
    "INDUSTRIAL": 1500.0,
}


def build_s3_client(settings: Any) -> Any:
    """Create an S3-compatible client for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3},
        ),
    )


def build_tariff_records(
    fleet: Fleet,
    billing_date: date,
) -> list[dict[str, Any]]:
    """Create one tariff record for every household."""
    records: list[dict[str, Any]] = []

    for household in fleet.households:
        record = TariffRecord(
            household_id=household.household_id,
            tariff_rate=TARIFF_RATES[household.tariff_tier],
            billing_tier=household.tariff_tier,
            subsidy_flag=household.subsidy_eligible,
            fixed_charge=FIXED_CHARGES[household.tariff_tier],
            effective_date=datetime.combine(
                billing_date,
                datetime.min.time(),
                tzinfo=UTC,
            ),
        )

        result = validate_record(
            record.to_dict(),
            specs=TARIFF_RECORD_FIELDS,
        )

        if not result.ok:
            raise ValueError(
                f"Invalid tariff record for {household.household_id}: "
                f"{result.reason} / {result.detail}"
            )

        records.append(result.record)

    return records


def build_weather_records(
    fleet: Fleet,
    forecast_date: date,
    seed: int,
) -> list[dict[str, Any]]:
    """Create one deterministic weather record for every grid zone."""
    rng = random.Random(seed + forecast_date.toordinal())
    records: list[dict[str, Any]] = []

    for zone in fleet.zones:
        cloud_cover = round(rng.uniform(5.0, 85.0), 2)
        temperature = round(rng.uniform(22.0, 34.0), 2)
        irradiance_index = round(max(0.0, 1.0 - cloud_cover / 100.0), 6)

        record = WeatherRecord(
            grid_zone=zone,
            forecast_date=datetime.combine(
                forecast_date,
                datetime.min.time(),
                tzinfo=UTC,
            ),
            cloud_cover_pct=cloud_cover,
            temperature_c=temperature,
            irradiance_index=irradiance_index,
        )

        result = validate_record(
            record.to_dict(),
            specs=WEATHER_RECORD_FIELDS,
        )

        if not result.ok:
            raise ValueError(
                f"Invalid weather record for {zone}: "
                f"{result.reason} / {result.detail}"
            )

        records.append(result.record)

    return records


def _write_json(
    s3_client: Any,
    bucket: str,
    key: str,
    records: list[dict[str, Any]],
) -> None:
    """Write records as a readable JSON document to MinIO."""
    body = json.dumps(
        {
            "record_count": len(records),
            "records": records,
        },
        indent=2,
        sort_keys=True,
    ).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    print(f"Wrote s3://{bucket}/{key} ({len(records)} records)")


def write_batch_files(
    settings: Any,
    billing_date: date,
    *,
    corrupt: bool = False,
    late: bool = False,
) -> None:
    """
    Generate and write the daily tariff and weather files.

    `corrupt=True` deliberately damages one tariff record so the Airflow
    quality gate can be demonstrated.

    `late=True` writes the files under the previous simulated date, allowing
    late-arrival handling to be tested.
    """
    fleet = build_fleet_from_settings(settings)
    s3_client = build_s3_client(settings)

    tariff_records = build_tariff_records(fleet, billing_date)
    weather_records = build_weather_records(
        fleet,
        billing_date,
        settings.sim_seed,
    )

    if corrupt:
        tariff_records[0]["tariff_rate"] = -1.0
        print(
            "WARNING: deliberately created a corrupt tariff record "
            f"for {tariff_records[0]['household_id']}"
        )

    output_date = billing_date - timedelta(days=1) if late else billing_date
    date_partition = output_date.isoformat()

    if late:
        print(
            f"WARNING: deliberately writing files under {date_partition} "
            f"instead of {billing_date.isoformat()}"
        )

    _write_json(
        s3_client,
        settings.minio_bucket_raw,
        f"tariffs/dt={date_partition}/tariffs.json",
        tariff_records,
    )

    _write_json(
        s3_client,
        settings.minio_bucket_raw,
        f"weather/dt={date_partition}/weather.json",
        weather_records,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate the daily tariff and weather batch files."
    )
    parser.add_argument(
        "--date",
        dest="billing_date",
        help="Simulated date in YYYY-MM-DD format. Defaults to the simulated clock date.",
    )
    parser.add_argument(
        "--corrupt",
        action="store_true",
        help="Create one invalid tariff record for quality-gate testing.",
    )
    parser.add_argument(
        "--late",
        action="store_true",
        help="Write files under the previous date to simulate late arrival.",
    )
    args = parser.parse_args()

    settings = get_settings()
    clock = SimulatedClock.from_settings(settings)

    billing_date = (
        date.fromisoformat(args.billing_date)
        if args.billing_date
        else clock.sim_date()
    )

    print(f"Generating batch files for simulated date {billing_date.isoformat()}")
    print(f"Sim clock: {clock.describe()}")

    write_batch_files(
        settings,
        billing_date,
        corrupt=args.corrupt,
        late=args.late,
    )


if __name__ == "__main__":
    main()