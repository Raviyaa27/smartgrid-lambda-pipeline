from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

from smartgrid.common.config import settings
from smartgrid.common.models import MeterReading, model_to_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("meter-producer")


def delivery_report(err, message) -> None:
    """Called by Kafka after a message is delivered or fails."""
    if err is not None:
        logger.error(
            "kafka_delivery_failed topic=%s error=%s",
            message.topic(),
            err,
        )
        return

    logger.debug(
        "kafka_delivery_success topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def build_reading(
    household_number: int,
    zone_number: int,
    simulated_timestamp: datetime,
) -> MeterReading:
    """Create one simulated smart-meter reading."""
    household_id = f"HH-{household_number:04d}"
    meter_id = f"MTR-{household_number:04d}"
    zone = f"ZONE-{zone_number:02d}"

    consumption = round(random.uniform(0.2, 5.0), 3)
    solar_generation = round(
        random.uniform(0.0, min(consumption * 0.8, 4.0)),
        3,
    )

    return MeterReading(
        meter_id=meter_id,
        household_id=household_id,
        zone=zone,
        power_consumption_kwh=consumption,
        solar_generation_kwh=solar_generation,
        timestamp=simulated_timestamp,
    )


def run() -> None:
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": "smartgrid-meter-producer",
            "acks": "all",
        }
    )

    start_date = datetime.fromisoformat(
        settings.simulated_start_date
    ).replace(tzinfo=timezone.utc)

    simulated_seconds = 0
    logger.info(
        "producer_started households=%s zones=%s topic=%s",
        settings.number_of_households,
        settings.number_of_zones,
        settings.readings_topic,
    )

    try:
        while True:
            simulated_timestamp = start_date + timedelta(
                seconds=simulated_seconds
            )

            for household_number in range(
                1,
                settings.number_of_households + 1,
            ):
                zone_number = (
                    (household_number - 1)
                    % settings.number_of_zones
                ) + 1

                reading = build_reading(
                    household_number=household_number,
                    zone_number=zone_number,
                    simulated_timestamp=simulated_timestamp,
                )

                producer.produce(
                    topic=settings.readings_topic,
                    key=reading.meter_id,
                    value=json.dumps(
                        model_to_dict(reading)
                    ),
                    callback=delivery_report,
                )

            producer.poll(0)
            producer.flush(timeout=10)

            logger.info(
                "readings_emitted simulated_timestamp=%s count=%s",
                simulated_timestamp.isoformat(),
                settings.number_of_households,
            )

            simulated_seconds += settings.emit_interval_seconds
            simulated_seconds %= settings.simulated_day_seconds

            time.sleep(settings.emit_interval_seconds)

    except KeyboardInterrupt:
        logger.info("producer_stopping")
    finally:
        producer.flush()


if __name__ == "__main__":
    run()