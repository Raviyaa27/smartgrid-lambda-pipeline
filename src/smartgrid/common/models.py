from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class MeterReading(BaseModel):
    meter_id: str
    household_id: str
    zone: str
    power_consumption_kwh: float = Field(ge=0)
    solar_generation_kwh: float = Field(ge=0)
    timestamp: datetime


class DailyTariff(BaseModel):
    household_id: str
    simulated_date: str
    tariff_rate: float = Field(gt=0)
    billing_tier: str
    subsidy_flag: bool
    weather_factor: float = Field(gt=0)


class DailyBill(BaseModel):
    household_id: str
    simulated_date: str
    total_consumption_kwh: float
    total_solar_generation_kwh: float
    tariff_rate: float
    subsidy_flag: bool
    final_bill: float


def model_to_dict(model: BaseModel) -> dict[str, Any]:
    """Support Pydantic v2."""
    return model.model_dump(mode="json")