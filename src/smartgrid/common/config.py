from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.getenv(
        "KAFKA_BOOTSTRAP_HOST",
        "localhost:9092",
    )
    readings_topic: str = os.getenv(
        "KAFKA_TOPIC_READINGS",
        "meter.readings.v1",
    )
    dlq_topic: str = os.getenv(
        "KAFKA_TOPIC_DLQ",
        "meter.readings.dlq.v1",
    )
    simulated_day_seconds: int = int(
        os.getenv("SIM_DAY_SECONDS", "300")
    )
    simulated_start_date: str = os.getenv(
        "SIM_START_DATE",
        "2026-01-01",
    )
    number_of_households: int = int(
        os.getenv("SIM_NUM_HOUSEHOLDS", "200")
    )
    number_of_zones: int = int(
        os.getenv("SIM_NUM_ZONES", "6")
    )
    emit_interval_seconds: int = int(
        os.getenv("SIM_EMIT_INTERVAL_SECONDS", "2")
    )
    # --- Add these fields inside the existing Settings dataclass ---
 
    minio_endpoint: str = os.getenv(
        "MINIO_ENDPOINT_HOST",
        "http://localhost:9000",
    )
    minio_access_key: str = os.getenv(
        "MINIO_ROOT_USER",
        "minioadmin",
    )
    minio_secret_key: str = os.getenv(
        "MINIO_ROOT_PASSWORD",
        "minioadmin",
    )
    minio_raw_bucket: str = os.getenv(
        "MINIO_BUCKET_RAW",
        "raw",
    )
    tariff_drop_prefix: str = os.getenv(
        "TARIFF_DROP_PREFIX",
        "tariffs",
    )
    # --- Add these fields inside the existing Settings dataclass, alongside the MinIO ones ---

    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_user: str = os.getenv("POSTGRES_USER", "postgres")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    postgres_serving_db: str = os.getenv("POSTGRES_SERVING_DB", "smartgrid")

    # Speed-layer windowing (real wall-clock seconds, independent of the
    # simulated day clock used by the producers)
    speed_window_seconds: int = int(os.getenv("SPEED_WINDOW_SECONDS", "30"))
    speed_watermark_seconds: int = int(os.getenv("SPEED_WATERMARK_SECONDS", "60"))
 


settings = Settings()