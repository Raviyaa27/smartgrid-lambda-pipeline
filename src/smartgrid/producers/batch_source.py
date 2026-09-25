from __future__ import annotations

import csv
import logging
import random
from pathlib import Path

from smartgrid.common.config import settings
from smartgrid.common.models import DailyTariff, model_to_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("batch-source")


def generate_daily_tariff_file(
    output_directory: str = "data",
) -> Path:
    """Generate one daily tariff file for all households."""
    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)

    output_file = output_path / (
        f"tariffs_{settings.simulated_start_date}.csv"
    )

    fields = [
        "household_id",
        "simulated_date",
        "tariff_rate",
        "billing_tier",
        "subsidy_flag",
        "weather_factor",
    ]

    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()

        for household_number in range(
            1,
            settings.number_of_households + 1,
        ):
            tariff = DailyTariff(
                household_id=f"HH-{household_number:04d}",
                simulated_date=settings.simulated_start_date,
                tariff_rate=round(
                    random.uniform(0.15, 0.35),
                    3,
                ),
                billing_tier=random.choice(
                    ["standard", "peak", "off_peak"]
                ),
                subsidy_flag=random.random() < 0.2,
                weather_factor=round(
                    random.uniform(0.95, 1.10),
                    3,
                ),
            )

            writer.writerow(model_to_dict(tariff))

    logger.info(
        "batch_file_created path=%s records=%s",
        output_file,
        settings.number_of_households,
    )

    return output_file


if __name__ == "__main__":
    generate_daily_tariff_file()