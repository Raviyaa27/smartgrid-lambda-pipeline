"""The batch layer's pure logic: billing, reconciliation, scheduling, report."""

import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from smartgrid.batch.bills import (
    DropContents,
    HouseholdDay,
    parse_drop,
    settle_households,
    unbilled,
)
from smartgrid.batch.orchestration import (
    SettlementRefused,
    StaleSimulation,
    _run_and_find_run_id,
    check_simulation,
    due_business_dates,
    settlement_run_id,
)
from smartgrid.batch.reconcile import ZoneTotals, compare
from smartgrid.batch.report import ReportData, render
from smartgrid.common import drops
from smartgrid.common.billing import DEFAULT_SCHEDULE
from smartgrid.common.domain import build_fleet
from smartgrid.producers.daily_batch_source import render_drop

DAY = date(2026, 1, 3)
FLEET = build_fleet(num_households=20, num_zones=3, seed=5)


def drop_contents(schedule=DEFAULT_SCHEDULE) -> DropContents:
    files = render_drop(FLEET, DAY, 20260101, schedule)
    manifest = drops.build_manifest(DAY, 1, files, published_at_sim="x")
    return parse_drop(drops.LoadedDrop(DAY, 1, manifest.to_dict(), files))


# -- Billing -----------------------------------------------------------------


def test_every_household_in_the_drop_is_billed_even_with_no_readings():
    contents = drop_contents()
    bills = settle_households(DAY, usage={}, contents=contents)
    assert len(bills) == len(FLEET)
    for settled in bills:
        assert settled.readings == 0
        assert settled.bill.energy_charge == Decimal("0.00")
        assert settled.bill.fixed_charge > 0  # a silent meter still owes this


def test_bills_use_the_tariff_from_the_drop():
    household = FLEET.households[0].household_id
    usage = {household: HouseholdDay(household, 150, 12.0, 1.0)}
    base = settle_households(DAY, usage, drop_contents())
    revised = settle_households(
        DAY, usage, drop_contents(DEFAULT_SCHEDULE.with_rate_change(Decimal("1.10")))
    )
    before = next(s.bill for s in base if s.bill.household_id == household)
    after = next(s.bill for s in revised if s.bill.household_id == household)
    assert after.energy_charge > before.energy_charge


def test_readings_for_households_not_in_the_drop_are_reported():
    usage = {"HH-99999": HouseholdDay("HH-99999", 3, 1.0, 0.0)}
    assert unbilled(usage, drop_contents()) == ["HH-99999"]


def test_parse_drop_reads_schedule_tariffs_and_forecast():
    contents = drop_contents()
    assert set(contents.tariffs) == FLEET.household_ids
    assert set(contents.forecast) == set(FLEET.zones)
    assert contents.schedule == DEFAULT_SCHEDULE


# -- Reconciliation ----------------------------------------------------------


def test_reconciliation_measures_the_speed_layers_error():
    rows = compare(
        batch={"ZONE-A": ZoneTotals(100.0, 1000), "ZONE-B": ZoneTotals(50.0, 500)},
        speed={"ZONE-A": ZoneTotals(97.0, 970), "ZONE-B": ZoneTotals(50.0, 500)},
    )
    a, b = rows
    assert a.delta_kwh == pytest.approx(-3.0)
    assert a.delta_pct == pytest.approx(-3.0)
    assert a.missed_by_speed == 30  # late readings only batch saw
    assert b.delta_pct == pytest.approx(0.0)


def test_a_zone_the_speed_layer_never_saw_is_absent_not_zero():
    (row,) = compare(batch={"ZONE-C": ZoneTotals(10.0, 100)}, speed={})
    assert row.speed_kwh is None and row.delta_pct is None and row.missed_by_speed is None


def test_zero_consumption_does_not_divide_by_zero():
    (row,) = compare(batch={"ZONE-A": ZoneTotals(0.0, 0)}, speed={"ZONE-A": ZoneTotals(0.0, 0)})
    assert row.delta_pct is None


# -- Scheduling --------------------------------------------------------------


def at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


def test_a_day_is_not_due_until_it_has_ended_plus_grace():
    first = date(2026, 1, 1)
    assert due_business_dates(at(date(2026, 1, 1), 23, 59), first, []) == []
    assert due_business_dates(at(date(2026, 1, 2), 0, 30), first, []) == []
    assert due_business_dates(at(date(2026, 1, 2), 0, 45), first, []) == [first]


def test_days_already_triggered_are_not_triggered_again():
    first = date(2026, 1, 1)
    due = due_business_dates(at(date(2026, 1, 5), 12), first, [date(2026, 1, 1), date(2026, 1, 3)])
    assert due == [date(2026, 1, 2), date(2026, 1, 4)]


def test_a_backlog_is_worked_off_a_few_days_at_a_time():
    due = due_business_dates(at(date(2026, 1, 30), 12), date(2026, 1, 1), [], limit=4)
    assert due == [date(2026, 1, 1) + timedelta(days=i) for i in range(4)]


def test_run_ids_are_deterministic_per_simulation_and_day():
    assert settlement_run_id(123, DAY) == settlement_run_id(123, DAY) == "settle__123__2026-01-03"
    assert settlement_run_id(124, DAY) != settlement_run_id(123, DAY)


# -- Running the settlement child process --------------------------------------


def child(script: str) -> list[str]:
    return [sys.executable, "-c", script]


def test_the_run_id_is_found_by_its_marker_wherever_it_is_printed(capsys):
    run_id = _run_and_find_run_id(
        child("print('spark noise'); print('RUN=42'); print('shutdown noise')"), "RUN="
    )
    assert run_id == 42
    assert "spark noise" in capsys.readouterr().out  # the child's output reaches the task log


def test_a_failed_settlement_raises_with_the_end_of_its_output():
    with pytest.raises(RuntimeError, match=r"exited with 3(.|\n)*the cause"):
        _run_and_find_run_id(
            child("import sys; print('the cause', file=sys.stderr); sys.exit(3)"), "RUN="
        )


def test_a_settlement_that_reports_no_run_id_is_a_failure():
    with pytest.raises(RuntimeError, match="no run id"):
        _run_and_find_run_id(child("print('done')"), "RUN=")


def test_a_refusal_is_distinguished_from_a_crash_so_it_is_not_retried():
    with pytest.raises(SettlementRefused):
        _run_and_find_run_id(child("import sys; sys.exit(3)"), "RUN=", refused=frozenset({3}))


# -- Simulation guard ------------------------------------------------------------


def test_a_run_from_before_a_simulation_reset_is_refused():
    with pytest.raises(StaleSimulation, match="belongs to simulation 100"):
        check_simulation("100", current=200)


@pytest.mark.parametrize("expected", [None, "", "None", "200", 200])
def test_a_run_for_this_simulation_or_for_whichever_is_current_proceeds(expected):
    check_simulation(expected, current=200)


# -- Report ------------------------------------------------------------------


def report_data(**overrides) -> ReportData:
    data = ReportData(
        business_date=DAY,
        run_id=7,
        trigger="scheduled",
        drop_version=1,
        reason=None,
        finished_at=datetime(2026, 9, 30, 12, tzinfo=UTC),
        readings_archived=1200,
        readings_settled=1190,
        duplicates_removed=10,
        readings_rejected=0,
        households_billed=20,
        total_billed=Decimal("12345.67"),
        zones=[
            {
                "grid_zone": "ZONE-A",
                "consumption_kwh": 100.0,
                "generation_kwh": 25.0,
                "renewable_share": 0.25,
                "peak_load_kw": 9.5,
                "forecast_irradiance": 0.8,
                "delta_pct": -2.5,
                "missed_by_speed": 12,
            }
        ],
        tiers=[
            {
                "tariff_tier": "DOMESTIC_STD",
                "households": 20,
                "kwh": Decimal("75.5"),
                "billed": Decimal("12345.67"),
                "average": Decimal("617.28"),
            }
        ],
    )
    for key, value in overrides.items():
        setattr(data, key, value)
    return data


def test_report_states_the_business_answer_and_its_reliability():
    page = render(report_data())
    assert "SETTLED" in page
    assert "12,345.67" in page  # total billed
    assert "25.0%" in page  # renewable contribution
    assert "-2.50%" in page  # speed vs batch
    assert "12" in page  # late readings recovered


def test_a_restated_report_says_what_changed_and_why():
    page = render(
        report_data(
            trigger="restatement",
            reason="Regulator backdated revision",
            previous_total=Decimal("12000.00"),
        )
    )
    assert "Restatement" in page
    assert "345.67" in page
    assert "Regulator backdated revision" in page


def test_report_escapes_text_it_did_not_write():
    page = render(
        report_data(
            trigger="restatement", reason="<script>alert(1)</script>", previous_total=Decimal("1")
        )
    )
    assert "<script>" not in page
