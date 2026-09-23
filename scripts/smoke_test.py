"""
Infrastructure smoke test.

Verifies that every component brought up by docker-compose is reachable
AND correctly provisioned -- not merely running. Extended in later
sections as new services are added.

Usage (from the repo root, with the venv active):
    python scripts/smoke_test.py
Exit code 0 = all good, 1 = at least one check failed.
"""

from __future__ import annotations

import os
import sys
from typing import Callable

from dotenv import load_dotenv

load_dotenv()


def check_kafka() -> str:
    """Broker reachable from the host, and both topics exist."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_HOST"]})
    metadata = admin.list_topics(timeout=10)

    readings = os.environ["KAFKA_TOPIC_READINGS"]
    dlq = os.environ["KAFKA_TOPIC_DLQ"]
    missing = {readings, dlq} - set(metadata.topics)
    if missing:
        raise RuntimeError(f"topics not created: {sorted(missing)}")

    partitions = len(metadata.topics[readings].partitions)
    if partitions != 6:
        raise RuntimeError(f"'{readings}' has {partitions} partitions, expected 6")

    return f"{len(metadata.brokers)} broker(s), '{readings}' x{partitions}, DLQ present"


def check_postgres() -> str:
    """Serving DB reachable, Lambda schemas present, Airflow DB created."""
    import psycopg

    dsn = (
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ['POSTGRES_PORT']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']} "
        f"dbname={os.environ['POSTGRES_SERVING_DB']}"
    )
    with psycopg.connect(dsn, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT nspname FROM pg_namespace "
            "WHERE nspname IN ('speed', 'batch', 'ops') ORDER BY 1"
        )
        schemas = [row[0] for row in cur.fetchall()]

        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (os.environ["POSTGRES_AIRFLOW_DB"],),
        )
        airflow_db = cur.fetchone() is not None

        cur.execute("SELECT version FROM ops.schema_version ORDER BY applied_at DESC LIMIT 1")
        version = cur.fetchone()[0]

    if schemas != ["batch", "ops", "speed"]:
        raise RuntimeError(f"expected schemas batch/ops/speed, found {schemas}")
    if not airflow_db:
        raise RuntimeError("airflow metadata database was not created")

    return f"schemas={schemas}, airflow db ok, schema_version={version}"


def check_minio() -> str:
    """S3 API reachable and both buckets provisioned."""
    import boto3
    from botocore.config import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT_HOST"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", retries={"max_attempts": 2}),
    )
    buckets = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    expected = {os.environ["MINIO_BUCKET_RAW"], os.environ["MINIO_BUCKET_LAKE"]}
    missing = expected - buckets
    if missing:
        raise RuntimeError(f"buckets not created: {sorted(missing)}")

    return f"buckets={sorted(expected)}"


CHECKS: list[tuple[str, Callable[[], str]]] = [
    ("Kafka", check_kafka),
    ("PostgreSQL", check_postgres),
    ("MinIO", check_minio),
]


def main() -> int:
    print("\nsmartgrid-lambda-pipeline :: infrastructure smoke test")
    print("-" * 68)

    failures = 0
    for name, probe in CHECKS:
        try:
            print(f"  PASS  {name:<12}  {probe()}")
        except Exception as exc:  # noqa: BLE001 - report every failure, never abort early
            failures += 1
            print(f"  FAIL  {name:<12}  {type(exc).__name__}: {exc}")

    print("-" * 68)
    if failures:
        print(f"{failures} of {len(CHECKS)} check(s) failed.\n")
        return 1
    print("All checks passed. Section 1 complete.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())