"""Guard against the dataclasses drifting from the FieldSpec tables."""

from dataclasses import fields

from smartgrid.common import schemas


def test_meter_reading_dataclass_matches_field_specs():
    declared = {spec.name for spec in schemas.METER_READING_FIELDS}
    dataclass_fields = {f.name for f in fields(schemas.MeterReading)}
    assert declared == dataclass_fields


def test_tariff_record_dataclass_matches_field_specs():
    declared = {spec.name for spec in schemas.TARIFF_RECORD_FIELDS}
    assert declared == {f.name for f in fields(schemas.TariffRecord)}


def test_weather_record_dataclass_matches_field_specs():
    declared = {spec.name for spec in schemas.WEATHER_RECORD_FIELDS}
    assert declared == {f.name for f in fields(schemas.WeatherRecord)}


def test_every_field_is_uniquely_named():
    for specs in (
        schemas.METER_READING_FIELDS,
        schemas.TARIFF_RECORD_FIELDS,
        schemas.WEATHER_RECORD_FIELDS,
    ):
        names = schemas.field_names(specs)
        assert len(names) == len(set(names))


def test_ranged_fields_declare_a_sane_interval():
    for specs in (
        schemas.METER_READING_FIELDS,
        schemas.TARIFF_RECORD_FIELDS,
        schemas.WEATHER_RECORD_FIELDS,
    ):
        for spec in specs:
            if spec.min_value is not None and spec.max_value is not None:
                assert spec.min_value < spec.max_value, spec.name
