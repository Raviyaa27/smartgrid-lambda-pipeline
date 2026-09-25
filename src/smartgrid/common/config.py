"""
Typed configuration, loaded once from the repo-root `.env`.

Every service in the pipeline reads its settings through `get_settings()`.
Nothing anywhere else is allowed to call os.environ directly -- that keeps
the full configuration surface visible in one file and documented in
`.env.example`.

The host/docker split matters: the same broker and object store are
reachable at different addresses depending on whether the caller runs on
the laptop or inside the Compose network. `Settings` resolves that once so
no downstream code has to care.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# config.py -> common -> smartgrid -> src -> <repo root>
REPO_ROOT = Path(__file__).resolve().parents[3]


def _in_docker() -> bool:
    """True when this process is running inside a container."""
    return Path("/.dockerenv").exists()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # -- Kafka -----------------------------------------------------------
    kafka_bootstrap_host: str = "localhost:9092"
    kafka_bootstrap_docker: str = "kafka:29092"
    kafka_topic_readings: str = "meter.readings.v1"
    kafka_topic_dlq: str = "meter.readings.dlq.v1"

    # -- PostgreSQL ------------------------------------------------------
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "smartgrid"
    postgres_password: str = "smartgrid"
    postgres_serving_db: str = "smartgrid"
    postgres_airflow_db: str = "airflow"

    # -- MinIO / S3 ------------------------------------------------------
    minio_endpoint_host: str = "http://localhost:9000"
    minio_endpoint_docker: str = "http://minio:9000"
    minio_root_user: str = "minioadmin"
    minio_root_password: str = "minioadmin123"
    minio_bucket_raw: str = "raw"
    minio_bucket_lake: str = "lake"

    # -- Simulated clock -------------------------------------------------
    sim_day_seconds: float = 300.0
    sim_start_date: date = date(2026, 1, 1)

    # -- Simulation scale ------------------------------------------------
    sim_num_households: int = 200
    sim_num_zones: int = 6
    sim_emit_interval_seconds: float = 2.0
    sim_seed: int = 20260101

    # -- Runtime context -------------------------------------------------
    running_in_docker: bool = Field(default_factory=_in_docker)
    log_level: str = "INFO"

    # -- Resolved addresses ----------------------------------------------
    @property
    def kafka_bootstrap(self) -> str:
        """Broker address correct for wherever this process is running."""
        return self.kafka_bootstrap_docker if self.running_in_docker else self.kafka_bootstrap_host

    @property
    def s3_endpoint(self) -> str:
        return self.minio_endpoint_docker if self.running_in_docker else self.minio_endpoint_host

    @property
    def postgres_host_resolved(self) -> str:
        return "postgres" if self.running_in_docker else self.postgres_host

    @property
    def postgres_dsn(self) -> str:
        """libpq connection string for psycopg."""
        return (
            f"host={self.postgres_host_resolved} port={self.postgres_port} "
            f"user={self.postgres_user} password={self.postgres_password} "
            f"dbname={self.postgres_serving_db}"
        )

    @property
    def postgres_uri(self) -> str:
        """SQLAlchemy/JDBC-style URI, used by Airflow and Spark."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host_resolved}:{self.postgres_port}/{self.postgres_serving_db}"
        )

    @property
    def time_compression(self) -> float:
        """How many simulated seconds elapse per real second."""
        return 86_400.0 / self.sim_day_seconds


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton."""
    return Settings()
