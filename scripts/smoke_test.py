"""
Stack smoke test.

Verifies that every service brought up by docker compose is reachable AND
correctly provisioned -- not merely running: topics, schemas and buckets
exist; the API answers; Airflow's scheduler is alive; Prometheus has loaded
the alert rules; Grafana has the dashboard.

Usage (from the repo root, with the venv active):
    python scripts/smoke_test.py
Exit code 0 = all good, 1 = at least one check failed.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from collections.abc import Callable

from dotenv import load_dotenv

load_dotenv()


def get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


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


def check_api() -> str:
    """Serving API up and reading the store (degraded = serving, data stale)."""
    body = get_json("http://localhost:8000/health")
    if body["checks"].get("postgres") != "ok":
        raise RuntimeError(f"API cannot read PostgreSQL: {body}")
    return f"status={body['status']}"


def check_dashboard() -> str:
    with urllib.request.urlopen("http://localhost:8501/_stcore/health", timeout=10) as response:
        if response.read().strip() != b"ok":
            raise RuntimeError("Streamlit health check did not answer ok")
    return "Streamlit ok"


def check_airflow() -> str:
    body = get_json("http://localhost:8080/api/v2/monitor/health")
    for part in ("metadatabase", "scheduler"):
        if body[part]["status"] != "healthy":
            raise RuntimeError(f"Airflow {part} is {body[part]['status']}")
    return "metadatabase and scheduler healthy"


def check_prometheus() -> str:
    groups = get_json("http://localhost:9090/api/v1/rules")["data"]["groups"]
    rules = sum(len(group["rules"]) for group in groups)
    if rules != 8:
        raise RuntimeError(f"{rules} alert rules loaded, expected 8")
    targets = get_json("http://localhost:9090/api/v1/targets")["data"]["activeTargets"]
    up = sum(target["health"] == "up" for target in targets)
    return f"{rules} alert rules, {up} of {len(targets)} targets up"


def check_grafana() -> str:
    if get_json("http://localhost:3000/api/health")["database"] != "ok":
        raise RuntimeError("Grafana database not ok")
    dashboard = get_json("http://localhost:3000/api/dashboards/uid/smartgrid-operations")
    return f"dashboard '{dashboard['dashboard']['title']}' provisioned"


CHECKS: list[tuple[str, Callable[[], str]]] = [
    ("Kafka", check_kafka),
    ("PostgreSQL", check_postgres),
    ("MinIO", check_minio),
    ("Serving API", check_api),
    ("Dashboard", check_dashboard),
    ("Airflow", check_airflow),
    ("Prometheus", check_prometheus),
    ("Grafana", check_grafana),
]


def main() -> int:
    print("\nsmartgrid-lambda-pipeline :: stack smoke test")
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
    print("All checks passed.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())