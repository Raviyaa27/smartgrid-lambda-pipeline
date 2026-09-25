"""
Foundation checkpoint: exercises every module in smartgrid.common and
prints the result. Nothing here touches Kafka, Postgres or MinIO -- it is
pure logic, so it runs with the infrastructure stopped.

    python scripts/section2_demo.py
"""

from __future__ import annotations

import json
import time
from datetime import UTC
from decimal import Decimal

from smartgrid.common.billing import calculate_bill
from smartgrid.common.clock import SimulatedClock
from smartgrid.common.config import get_settings
from smartgrid.common.domain import build_fleet_from_settings
from smartgrid.common.logging import (
    PipelineStage,
    configure_logging,
    correlation_scope,
    get_logger,
)
from smartgrid.common.transformations import enrich_reading, validate_record

RULE = "-" * 72


def banner(title: str) -> None:
    print(f"\n{RULE}\n  {title}\n{RULE}")


def main() -> None:
    settings = get_settings()
    configure_logging(service="foundation-demo", level=settings.log_level)
    log = get_logger(__name__)

    # -- 1. Configuration ------------------------------------------------
    banner("1. Configuration resolved from .env")
    print(f"  kafka bootstrap  : {settings.kafka_bootstrap}")
    print(f"  s3 endpoint      : {settings.s3_endpoint}")
    print(f"  postgres         : {settings.postgres_host_resolved}:{settings.postgres_port}")
    print(f"  running_in_docker: {settings.running_in_docker}")

    # -- 2. Simulated clock ----------------------------------------------
    banner("2. Simulated clock")
    clock = SimulatedClock.from_settings(settings, real_start=time.time())
    print(f"  {clock.describe()}")
    for real_offset in (0, 75, 150, 300, 900):
        at = clock.real_start + real_offset
        print(
            f"  +{real_offset:>4}s real  ->  {clock.now(real_now=at).isoformat(timespec='seconds')}"
            f"   (day {clock.sim_date(real_now=at)},"
            f" {clock.day_fraction(real_now=at):.0%} through it)"
        )

    # -- 3. Fleet --------------------------------------------------------
    banner("3. Simulated fleet (deterministic)")
    fleet = build_fleet_from_settings(settings)
    print(f"  households : {len(fleet)}")
    print(f"  zones      : {', '.join(fleet.zones)}")
    print(f"  with solar : {fleet.solar_count} ({fleet.solar_count / len(fleet):.0%})")
    sample = fleet.households[0]
    print(
        f"  sample     : {sample.household_id} / {sample.meter_id} / {sample.grid_zone} /"
        f" {sample.tariff_tier} / solar={sample.solar_capacity_kw} kW"
    )

    # -- 4. Structured logging with a correlation id ---------------------
    banner("4. Structured logging (one JSON object per line)")
    with correlation_scope() as cid:
        log.info("trace bound", extra={"demo_trace": cid})
        with PipelineStage(log, "validate", source="demo") as st:
            st.records_in = 5
            st.records_out = 1
            st.records_quarantined = 4

    # -- 5. Validation, good and bad -------------------------------------
    banner("5. Validation against the shared rule table")
    now = clock.now()
    base = {
        "event_id": "evt-demo-1",
        "meter_id": sample.meter_id,
        "household_id": sample.household_id,
        "grid_zone": sample.grid_zone,
        "power_consumption_kwh": 0.85,
        "solar_generation_kwh": 0.30,
        "event_time": now.astimezone(UTC).isoformat(),
    }
    cases = {
        "valid reading": base,
        "negative consumption": {**base, "power_consumption_kwh": -2.0},
        "absurd consumption": {**base, "power_consumption_kwh": 5000.0},
        "unknown household": {**base, "household_id": "HH-99999"},
        "missing meter_id": {k: v for k, v in base.items() if k != "meter_id"},
    }
    for label, record in cases.items():
        result = validate_record(record, known_household_ids=fleet.household_ids, now=now)
        verdict = "ACCEPT" if result.ok else f"QUARANTINE [{result.reason}]"
        print(f"  {label:<24} {verdict}")
        if not result.ok:
            print(f"  {'':<24}   {result.detail}")

    # -- 6. Enrichment ---------------------------------------------------
    banner("6. Enrichment")
    accepted = validate_record(base, known_household_ids=fleet.household_ids, now=now)
    enriched = enrich_reading(accepted.record, fleet)
    keys = ("household_id", "tariff_tier", "net_kwh", "is_exporting", "renewable_share")
    print(json.dumps({k: str(enriched[k]) for k in keys}, indent=2))

    # -- 7. Tiered billing -----------------------------------------------
    banner("7. Tiered block billing (monthly settlement)")
    bill = calculate_bill(
        household_id=sample.household_id,
        billing_date=clock.sim_date(),
        tariff_tier="DOMESTIC_STD",
        consumption_kwh=145.0,
        generation_kwh=35.0,
        subsidy_flag=False,
        settlement_days=Decimal("30"),
    )
    print(
        f"  consumption {bill.gross_consumption_kwh} kWh,"
        f" solar {bill.solar_generation_kwh} kWh,"
        f" net import {bill.net_import_kwh} kWh\n"
    )
    print(f"  {'block':<7}{'units kWh':>12}{'rate':>10}{'amount':>14}")
    for line in bill.lines:
        print(f"  {line.block_index:<7}{line.units_kwh!s:>12}{line.rate!s:>10}{line.amount!s:>14}")
    print(f"\n  {'energy charge':<20}{bill.energy_charge!s:>14}")
    print(f"  {'fixed charge':<20}{bill.fixed_charge!s:>14}")
    print(f"  {'export credit':<20}{'-' + str(bill.export_credit):>14}")
    print(f"  {'TOTAL PAYABLE':<20}{bill.total_payable!s:>14} {bill.currency}")
    print()


if __name__ == "__main__":
    main()
