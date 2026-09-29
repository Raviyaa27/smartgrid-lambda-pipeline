from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

from smartgrid.common.config import get_settings


def files_exist() -> bool:
    import boto3

    settings = get_settings()

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
        region_name="us-east-1",
    )

    date_value = datetime.utcnow().date().isoformat()

    required_files = [
        f"tariffs/dt={date_value}/tariffs.json",
        f"weather/dt={date_value}/weather.json",
    ]

    for key in required_files:
        try:
            client.head_object(
                Bucket=settings.minio_bucket_raw,
                Key=key,
            )
        except client.exceptions.ClientError:
            return False

    return True


with DAG(
    dag_id="smartgrid_daily_settlement",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args={
        "owner": "smartgrid",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:

    wait_for_batch_files = PythonSensor(
        task_id="wait_for_tariff_and_weather_files",
        python_callable=files_exist,
        poke_interval=20,
        timeout=180,
        mode="reschedule",
    )

    settle_bills = BashOperator(
        task_id="settle_bills",
        bash_command=(
            "spark-submit "
            "--packages "
            "org.apache.hadoop:hadoop-aws:3.3.4 "
            "-m smartgrid.batch.settlement "
            "--date '{{ ds }}'"
        ),
    )

    wait_for_batch_files >> settle_bills