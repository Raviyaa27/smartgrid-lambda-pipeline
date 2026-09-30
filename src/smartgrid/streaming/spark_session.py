"""
One SparkSession configuration for both Lambda layers.

The speed layer and the batch layer build their sessions here,
so they share an engine AND its settings -- in particular the UTC session
time zone, without which `to_date(event_time)` would partition readings by
the container's local date and the two layers could disagree about which
day a reading belongs to.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from smartgrid.common.config import Settings


def build_spark(
    app_name: str, settings: Settings, *, streaming: bool = False, shuffle_partitions: int = 6
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        # Domain time is UTC everywhere (ADR-0007). Critical for the dt= key.
        .config("spark.sql.session.timeZone", "UTC")
        # Matches the 6 Kafka partitions; the default of 200 would schedule
        # 200 near-empty tasks per micro-batch on a two-core local cluster.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        # Our schema has 21 columns; Spark's default of 25 fields per plan
        # string truncates wider plans and warns on every start.
        .config("spark.sql.debug.maxToStringFields", "100")
        # MinIO through the S3A connector, exactly as a real S3 deployment
        # would be addressed apart from the endpoint (ADR-0005).
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_root_user)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_root_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Commit task output directly rather than by a second rename pass;
        # a rename on object storage is a full copy.
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )
    if streaming:
        # Adaptive query execution does not apply to streaming plans; Spark
        # disables it itself, with a warning, unless told explicitly.
        builder = builder.config("spark.sql.adaptive.enabled", "false")
    return builder.getOrCreate()
