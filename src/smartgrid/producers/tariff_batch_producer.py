"""
Daily-batch source: tariff/billing reference data.

Once per simulated day, generates a DailyTariff row for every household
and uploads the batch as a single JSON-lines file to the MinIO `raw`
bucket. This is the "daily-batch source" required by the project spec,
using the *same* simulated clock as meter_producer.py (SIM_DAY_SECONDS /
SIM_START_DATE) so both sources agree on what "day" it currently is.

Usage (from repo root, venv active, infra already up):
    python -m smartgrid.producers.tariff_batch_producer

Env vars used (see .env.example / config.py):
    MINIO_ENDPOINT_HOST, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD,
    MINIO_BUCKET_RAW, TARIFF_DROP_PREFIX,
    SIM_START_DATE, SIM_DAY_SECONDS, SIM_NUM_HOUSEHOLDS
"""

from __future__ import annotations

import io
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

from smartgrid.common.config import settings
from smartgrid.common.models import DailyTariff, model_to_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("tariff-batch-producer")

BILLING_TIERS = ["standard", "low_income", "commercial"]
TIER_BASE_RATE = {"standard": 0.25, "low_income": 0.15, "commercial": 0.35}
SUBSIDY_PROBABILITY = 0.20  # fraction of households flagged subsidised each day


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
    )


def build_tariff_batch(simulated_date: str) -> list[DailyTariff]:
    """One DailyTariff row per household for the given simulated day."""
    rows: list[DailyTariff] = []
    for household_number in range(1, settings.number_of_households + 1):
        tier = random.choice(BILLING_TIERS)
        base_rate = TIER_BASE_RATE[tier]
        rows.append(
            DailyTariff(
                household_id=f"HH-{household_number:04d}",
                simulated_date=simulated_date,
                tariff_rate=round(base_rate * random.uniform(0.9, 1.1), 4),
                billing_tier=tier,
                subsidy_flag=random.random() < SUBSIDY_PROBABILITY,
                # cloudier day -> lower effective solar credit; 1.0 = clear sky
                weather_factor=round(random.uniform(0.6, 1.0), 3),
            )
        )
    return rows


def upload_batch(s3, simulated_date: str, rows: list[DailyTariff]) -> str:
    body = "\n".join(json.dumps(model_to_dict(row)) for row in rows).encode("utf-8")
    key = f"{settings.tariff_drop_prefix}/{simulated_date}.jsonl"
    s3.upload_fileobj(io.BytesIO(body), settings.minio_raw_bucket, key)
    return key


def run() -> None:
    s3 = _s3_client()
    start_date = datetime.fromisoformat(settings.simulated_start_date).replace(tzinfo=timezone.utc)

    logger.info(
        "producer_started households=%s bucket=%s day_seconds=%s",
        settings.number_of_households,
        settings.minio_raw_bucket,
        settings.simulated_day_seconds,
    )

    day_index = 0
    try:
        while True:
            simulated_date = (start_date + timedelta(days=day_index)).date().isoformat()

            rows = build_tariff_batch(simulated_date)
            try:
                key = upload_batch(s3, simulated_date, rows)
                logger.info(
                    "tariff_batch_uploaded simulated_date=%s rows=%s key=%s",
                    simulated_date,
                    len(rows),
                    key,
                )
            except Exception as exc:  # noqa: BLE001 - log and keep the clock moving
                logger.error("tariff_batch_upload_failed simulated_date=%s error=%s", simulated_date, exc)

            day_index += 1
            time.sleep(settings.simulated_day_seconds)

    except KeyboardInterrupt:
        logger.info("producer_stopping")


if __name__ == "__main__":
    run()