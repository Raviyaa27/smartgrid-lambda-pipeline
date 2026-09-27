from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    from_json,
    sum as spark_sum,
    to_date,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from smartgrid.common.config import get_settings
from smartgrid.common.transformations import validate_frame


READING_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("meter_id", StringType(), False),
        StructField("household_id", StringType(), False),
        StructField("grid_zone", StringType(), False),
        StructField("power_consumption_kwh", DoubleType(), False),
        StructField("solar_generation_kwh", DoubleType(), False),
        StructField("voltage_v", DoubleType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("ingest_time", TimestampType(), True),
        StructField("correlation_id", StringType(), True),
        StructField("schema_version", StringType(), True),
    ]
)


VALIDATED_SCHEMA = StructType(
    READING_SCHEMA.fields
    + [
        StructField("quarantine_reason", StringType(), True),
        StructField("is_valid", BooleanType(), False),
    ]
)


def create_spark_session() -> SparkSession:
    settings = get_settings()

    return (
        SparkSession.builder
        .appName("smartgrid-speed-layer")
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


def apply_shared_validation(parsed: DataFrame) -> DataFrame:
    """
    Apply the same validation rules used by the Python producer and
    batch layer through the shared transformations module.
    """

    def validate_batches(iterator):
        for pandas_frame in iterator:
            yield validate_frame(pandas_frame)

    return parsed.mapInPandas(
        validate_batches,
        schema=VALIDATED_SCHEMA,
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    settings = get_settings()

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic_readings)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), READING_SCHEMA).alias("reading"))
        .select("reading.*")
    )


def write_master_dataset(
    valid_readings: DataFrame,
    checkpoint_path: str,
    output_path: str,
):
    readings = valid_readings.withColumn(
        "event_date",
        to_date(col("event_time")),
    )

    return (
        readings.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("event_date", "grid_zone")
        .trigger(processingTime="5 seconds")
        .start()
    )


def write_quarantine_dataset(
    invalid_readings: DataFrame,
    checkpoint_path: str,
    output_path: str,
):
    quarantined = invalid_readings.withColumn(
        "event_date",
        to_date(col("event_time")),
    )

    return (
        quarantined.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("event_date", "quarantine_reason")
        .trigger(processingTime="5 seconds")
        .start()
    )


def build_zone_aggregates(valid_readings: DataFrame) -> DataFrame:
    return (
        valid_readings
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "30 seconds"),
            col("grid_zone"),
        )
        .agg(
            spark_sum("power_consumption_kwh").alias(
                "total_consumption_kwh"
            ),
            spark_sum("solar_generation_kwh").alias(
                "total_generation_kwh"
            ),
            countDistinct("meter_id").alias("active_meters"),
        )
        .select(
            col("grid_zone"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_consumption_kwh"),
            col("total_generation_kwh"),
            (
                col("total_consumption_kwh")
                - col("total_generation_kwh")
            ).alias("net_kwh"),
            col("active_meters"),
        )
    )


def write_zone_aggregates(
    aggregates: DataFrame,
    checkpoint_path: str,
    output_path: str,
):
    return (
        aggregates.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 seconds")
        .start()
    )


def main() -> None:
    settings = get_settings()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    lake_root = f"s3a://{settings.minio_bucket_lake}"

    readings = read_kafka_stream(spark)
    validated = apply_shared_validation(readings)

    valid_readings = validated.filter(col("is_valid"))
    invalid_readings = validated.filter(~col("is_valid"))

    master_query = write_master_dataset(
        valid_readings=valid_readings,
        checkpoint_path="data/checkpoints/master-readings",
        output_path=f"{lake_root}/readings",
    )

    quarantine_query = write_quarantine_dataset(
        invalid_readings=invalid_readings,
        checkpoint_path="data/checkpoints/quarantine-readings",
        output_path=f"{lake_root}/quarantine",
    )

    aggregates = build_zone_aggregates(valid_readings)

    aggregate_query = write_zone_aggregates(
        aggregates=aggregates,
        checkpoint_path="data/checkpoints/zone-aggregates",
        output_path=f"{lake_root}/speed-aggregates",
    )

    print("Spark speed layer is running.")
    print(f"Kafka topic: {settings.kafka_topic_readings}")
    print(f"Master dataset: {lake_root}/readings")
    print(f"Quarantine dataset: {lake_root}/quarantine")
    print(f"Aggregates: {lake_root}/speed-aggregates")

    spark.streams.awaitAnyTermination()

    master_query.stop()
    quarantine_query.stop()
    aggregate_query.stop()
    spark.stop()


if __name__ == "__main__":
    main()