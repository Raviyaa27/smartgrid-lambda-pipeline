"""
Event contracts, declared as data.

`FieldSpec` tables are the SINGLE SOURCE OF TRUTH for every field the
pipeline handles. From them we derive:

  * pure-Python record validation          (transformations.validate_record)
  * vectorised pandas validation           (transformations.validate_frame)
  * the Spark StructType                   (streaming layer, Section 6)

Because all three are generated from the same table, the speed layer and the
batch layer cannot disagree about what a valid reading is. That is the
concrete answer to the standard objection that Lambda's two code paths drift
apart -- see docs/adr/0004.

Topic names carry a version suffix (`meter.readings.v1`). A breaking schema
change means a new topic, never a silent reinterpretation of the old one.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Literal

SCHEMA_VERSION = "1.0.0"

DType = Literal["string", "double", "integer", "boolean", "timestamp"]


@dataclass(frozen=True)
class FieldSpec:
    """One field of one event type, plus the rules that make it valid."""

    name: str
    dtype: DType
    required: bool = True
    min_value: float | None = None
    max_value: float | None = None
    description: str = ""


# -- Streaming source: smart-meter readings ------------------------------
# Ranges are physically motivated, not arbitrary. A domestic meter reporting
# 400 kWh in a two-second interval is a sensor fault, not a heavy load, and
# belongs in the dead-letter queue rather than in someone's bill.
METER_READING_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("event_id", "string", description="Unique per emission; the dedupe key"),
    FieldSpec("meter_id", "string", description="Physical meter"),
    FieldSpec("household_id", "string", description="Billable connection"),
    FieldSpec("grid_zone", "string", description="Kafka partition key"),
    FieldSpec(
        "power_consumption_kwh", "double", min_value=0.0, max_value=50.0,
        description="Energy drawn during the interval",
    ),
    FieldSpec(
        "solar_generation_kwh", "double", min_value=0.0, max_value=50.0,
        description="Energy generated during the interval",
    ),
    FieldSpec(
        "voltage_v", "double", required=False, min_value=180.0, max_value=280.0,
        description="Supply voltage; absent on older meter firmware",
    ),
    FieldSpec("event_time", "timestamp", description="When the meter took the reading"),
    FieldSpec(
        "ingest_time", "timestamp", required=False,
        description="When the producer handed it to Kafka; the gap is producer lag",
    ),
    FieldSpec("correlation_id", "string", required=False, description="End-to-end trace id"),
    FieldSpec("schema_version", "string", required=False),
)

# -- Daily batch source: tariff and billing reference --------------------
TARIFF_RECORD_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("household_id", "string"),
    FieldSpec(
        "tariff_rate", "double", min_value=0.0, max_value=500.0,
        description="Published base rate, LKR/kWh",
    ),
    FieldSpec("billing_tier", "string"),
    FieldSpec("subsidy_flag", "boolean"),
    FieldSpec("fixed_charge", "double", min_value=0.0, max_value=10_000.0),
    FieldSpec("effective_date", "timestamp"),
)

# -- Daily batch source: weather forecast, drives expected solar yield ---
WEATHER_RECORD_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("grid_zone", "string"),
    FieldSpec("forecast_date", "timestamp"),
    FieldSpec("cloud_cover_pct", "double", min_value=0.0, max_value=100.0),
    FieldSpec("temperature_c", "double", min_value=-10.0, max_value=55.0),
    FieldSpec(
        "irradiance_index", "double", min_value=0.0, max_value=1.0,
        description="Derived clear-sky fraction, 1.0 = cloudless",
    ),
)


def field_names(specs: tuple[FieldSpec, ...]) -> tuple[str, ...]:
    return tuple(spec.name for spec in specs)


def spec_index(specs: tuple[FieldSpec, ...]) -> dict[str, FieldSpec]:
    return {spec.name: spec for spec in specs}


# -- Typed constructors --------------------------------------------------
# Convenience for the producers. The dataclasses mirror the FieldSpec tables;
# tests/unit/test_schemas.py asserts they stay in step, so the two cannot
# silently diverge.


@dataclass(frozen=True)
class MeterReading:
    event_id: str
    meter_id: str
    household_id: str
    grid_zone: str
    power_consumption_kwh: float
    solar_generation_kwh: float
    event_time: datetime
    voltage_v: float | None = None
    ingest_time: datetime | None = None
    correlation_id: str | None = None
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready dict; timestamps become ISO-8601 strings."""
        payload = asdict(self)
        for key in ("event_time", "ingest_time"):
            if isinstance(payload[key], datetime):
                payload[key] = payload[key].isoformat()
        return payload


@dataclass(frozen=True)
class TariffRecord:
    household_id: str
    tariff_rate: float
    billing_tier: str
    subsidy_flag: bool
    fixed_charge: float
    effective_date: datetime

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["effective_date"] = self.effective_date.date().isoformat()
        return payload


@dataclass(frozen=True)
class WeatherRecord:
    grid_zone: str
    forecast_date: datetime
    cloud_cover_pct: float
    temperature_c: float
    irradiance_index: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["forecast_date"] = self.forecast_date.date().isoformat()
        return payload
