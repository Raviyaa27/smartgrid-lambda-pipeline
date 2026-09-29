from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum as spark_sum

from smartgrid.common.billing import calculate_bill
from smartgrid.common.config import get_settings


def create_spark_session() -> SparkSession:
    settings = get_settings()

    return (
        SparkSession.builder
        .appName("smartgrid-batch-settlement")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_root_user)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_root_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


def read_daily_usage(spark: SparkSession, billing_date: date):
    settings = get_settings()

    readings_path = (
        f"s3a://{settings.minio_bucket_lake}/readings/"
        f"event_date={billing_date.isoformat()}"
    )

    return (
        spark.read.parquet(readings_path)
        .groupBy("household_id")
        .agg(
            spark_sum("power_consumption_kwh").alias("consumption_kwh"),
            spark_sum("solar_generation_kwh").alias("generation_kwh"),
        )
    )


def read_daily_tariffs(spark: SparkSession, billing_date: date):
    settings = get_settings()

    tariff_path = (
        f"s3a://{settings.minio_bucket_raw}/tariffs/"
        f"dt={billing_date.isoformat()}/tariffs.json"
    )

    return (
        spark.read.json(tariff_path)
        .select(explode("records").alias("tariff"))
        .select("tariff.*")
    )


def ensure_batch_table() -> None:
    settings = get_settings()

    with psycopg.connect(settings.postgres_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS batch.bills (
                    household_id TEXT NOT NULL,
                    billing_date DATE NOT NULL,
                    tariff_tier TEXT NOT NULL,
                    gross_consumption_kwh NUMERIC NOT NULL,
                    solar_generation_kwh NUMERIC NOT NULL,
                    net_import_kwh NUMERIC NOT NULL,
                    net_export_kwh NUMERIC NOT NULL,
                    energy_charge NUMERIC NOT NULL,
                    fixed_charge NUMERIC NOT NULL,
                    export_credit NUMERIC NOT NULL,
                    subsidy_amount NUMERIC NOT NULL,
                    total_payable NUMERIC NOT NULL,
                    subsidy_applied BOOLEAN NOT NULL,
                    currency TEXT NOT NULL,
                    settled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (household_id, billing_date)
                )
                """
            )
        connection.commit()


def write_bills(bills: list[dict[str, Any]]) -> None:
    settings = get_settings()

    with psycopg.connect(settings.postgres_dsn) as connection:
        with connection.cursor() as cursor:
            for bill in bills:
                cursor.execute(
                    """
                    INSERT INTO batch.bills (
                        household_id,
                        billing_date,
                        tariff_tier,
                        gross_consumption_kwh,
                        solar_generation_kwh,
                        net_import_kwh,
                        net_export_kwh,
                        energy_charge,
                        fixed_charge,
                        export_credit,
                        subsidy_amount,
                        total_payable,
                        subsidy_applied,
                        currency
                    )
                    VALUES (
                        %(household_id)s,
                        %(billing_date)s,
                        %(tariff_tier)s,
                        %(gross_consumption_kwh)s,
                        %(solar_generation_kwh)s,
                        %(net_import_kwh)s,
                        %(net_export_kwh)s,
                        %(energy_charge)s,
                        %(fixed_charge)s,
                        %(export_credit)s,
                        %(subsidy_amount)s,
                        %(total_payable)s,
                        %(subsidy_applied)s,
                        %(currency)s
                    )
                    ON CONFLICT (household_id, billing_date)
                    DO UPDATE SET
                        tariff_tier = EXCLUDED.tariff_tier,
                        gross_consumption_kwh = EXCLUDED.gross_consumption_kwh,
                        solar_generation_kwh = EXCLUDED.solar_generation_kwh,
                        net_import_kwh = EXCLUDED.net_import_kwh,
                        net_export_kwh = EXCLUDED.net_export_kwh,
                        energy_charge = EXCLUDED.energy_charge,
                        fixed_charge = EXCLUDED.fixed_charge,
                        export_credit = EXCLUDED.export_credit,
                        subsidy_amount = EXCLUDED.subsidy_amount,
                        total_payable = EXCLUDED.total_payable,
                        subsidy_applied = EXCLUDED.subsidy_applied,
                        currency = EXCLUDED.currency,
                        settled_at = now()
                    """,
                    bill,
                )
        connection.commit()


def settle_date(billing_date: date) -> int:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        ensure_batch_table()

        usage = read_daily_usage(spark, billing_date)
        tariffs = read_daily_tariffs(spark, billing_date)

        joined = usage.join(tariffs, on="household_id", how="inner")

        bills: list[dict[str, Any]] = []

        for row in joined.collect():
            bill = calculate_bill(
                household_id=row["household_id"],
                billing_date=billing_date,
                tariff_tier=row["billing_tier"],
                consumption_kwh=Decimal(str(row["consumption_kwh"] or 0)),
                generation_kwh=Decimal(str(row["generation_kwh"] or 0)),
                subsidy_flag=bool(row["subsidy_flag"]),
                fixed_charge=Decimal(str(row["fixed_charge"])),
            )
            bills.append(bill.to_dict())

        write_bills(bills)
        return len(bills)

    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    billing_date = date.fromisoformat(args.date)
    count = settle_date(billing_date)
    print(f"Settled {count} household bills for {billing_date}")


if __name__ == "__main__":
    main()