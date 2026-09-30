"""The Lambda merge rule: which layer serves which figure (ADR-0001)."""

from datetime import date

import pytest

from smartgrid.serving.merge import (
    FutureDate,
    Layer,
    Status,
    bill_availability,
    date_range,
    zone_source,
)

TODAY = date(2026, 1, 5)
SETTLED = {date(2026, 1, 3): 41, date(2026, 1, 2): 38}


def test_today_is_always_provisional_from_the_speed_layer():
    source = zone_source(TODAY, TODAY, SETTLED)
    assert (source.status, source.layer, source.settlement_run_id) == (
        Status.PROVISIONAL,
        Layer.SPEED,
        None,
    )


def test_a_settled_day_is_served_by_the_batch_layer_with_its_current_run():
    source = zone_source(date(2026, 1, 3), TODAY, SETTLED)
    assert (source.status, source.layer, source.settlement_run_id) == (
        Status.SETTLED,
        Layer.BATCH,
        41,
    )


def test_an_ended_day_not_yet_settled_stays_provisional_and_says_why():
    source = zone_source(date(2026, 1, 4), TODAY, SETTLED)
    assert (source.status, source.layer) == (Status.PROVISIONAL, Layer.SPEED)
    assert "awaiting settlement" in source.reason


def test_the_future_is_refused():
    with pytest.raises(FutureDate):
        zone_source(date(2026, 1, 6), TODAY, SETTLED)
    with pytest.raises(FutureDate):
        bill_availability(date(2026, 1, 6), TODAY, SETTLED)


def test_bills_exist_only_for_settled_days():
    assert bill_availability(date(2026, 1, 2), TODAY, SETTLED).billed
    assert bill_availability(date(2026, 1, 2), TODAY, SETTLED).settlement_run_id == 38


@pytest.mark.parametrize("day", [date(2026, 1, 4), TODAY])
def test_there_is_never_a_provisional_bill(day):
    availability = bill_availability(day, TODAY, SETTLED)
    assert not availability.billed
    assert availability.settlement_run_id is None
    assert availability.reason


def test_date_ranges_are_inclusive_and_bounded():
    assert date_range(date(2026, 1, 1), date(2026, 1, 3), max_days=31) == [
        date(2026, 1, 1),
        date(2026, 1, 2),
        date(2026, 1, 3),
    ]
    with pytest.raises(ValueError, match="before it starts"):
        date_range(date(2026, 1, 3), date(2026, 1, 1), max_days=31)
    with pytest.raises(ValueError, match="exceeds the maximum"):
        date_range(date(2026, 1, 1), date(2026, 2, 5), max_days=31)
