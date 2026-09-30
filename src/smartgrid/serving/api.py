"""
Serving API: one front door to both Lambda layers, applying the merge rule.

    python -m smartgrid.serving.api          # http://localhost:8000/docs

Every figure it returns says where it came from: `status` is SETTLED (batch
layer, the system of record) or PROVISIONAL (speed layer, fresh but
approximate), and `layer` names the layer. Which layer serves which request
is decided in `merge.py` and nowhere else (ADR-0001):

    today's zone figures       speed layer, PROVISIONAL
    a settled day's figures    batch layer, SETTLED -- the batch view always wins
    bills                      batch layer only; there is no provisional bill

Money is returned as decimal strings, never floats: a bill is exact to the
cent, and a float would quietly stop it being so.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any

import prometheus_client as prom
import psycopg
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from pydantic import BaseModel, Field

from smartgrid.common.clock import SimulatedClock
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.domain import Fleet, build_fleet_from_settings
from smartgrid.common.logging import configure_logging, get_logger
from smartgrid.serving.merge import (
    FutureDate,
    Layer,
    Status,
    bill_availability,
    date_range,
    zone_source,
)
from smartgrid.serving.repository import PostgresStore, Row, ServingStore

SERVICE = "serving-api"
log = get_logger("smartgrid.serving.api")

# R1: provisional figures must be at most 60 real seconds old.
FRESHNESS_TARGET_SECONDS = 60.0
API = "/api/v1"


# -- Response models ---------------------------------------------------------------


class Health(BaseModel):
    status: str = Field(description="ok, degraded (serving, but something is stale) or down")
    checks: dict[str, Any]


class ClockOut(BaseModel):
    simulated_now: datetime
    simulated_date: date
    simulation_started: date
    compression: float = Field(description="Simulated seconds per real second")
    sim_id: int


class ZoneInfo(BaseModel):
    grid_zone: str
    households: int
    with_solar: int


class ZoneWindow(BaseModel):
    grid_zone: str
    window_start: datetime
    window_end: datetime
    consumption_kwh: float
    generation_kwh: float
    net_kwh: float
    grid_load_kw: float = Field(description="Average draw across the window")
    solar_kw: float
    renewable_share: float | None = Field(description="generation / consumption; null if none")
    readings: int
    meters_reporting: int
    status: Status
    layer: Layer


class LiveZone(ZoneWindow):
    data_age_simulated_minutes: float
    data_age_real_seconds: float


class LiveOut(BaseModel):
    simulated_now: datetime
    status: Status = Status.PROVISIONAL
    stale: bool = Field(description="True if any zone's figure is older than the 60 s target")
    freshness_target_real_seconds: float
    total_grid_load_kw: float
    total_solar_kw: float
    renewable_share: float | None
    zones: list[LiveZone]


class DaySourceOut(BaseModel):
    date: date
    status: Status
    layer: Layer
    reason: str
    settlement_run_id: int | None


class DailyZone(BaseModel):
    grid_zone: str
    consumption_kwh: float
    generation_kwh: float
    renewable_share: float | None
    peak_load_kw: float
    readings: int
    meters_reporting: int
    forecast_irradiance: float | None = Field(None, description="From the daily drop; settled only")
    speed_vs_batch_pct: float | None = Field(
        None, description="How far the real-time view was from settlement; settled only"
    )
    late_readings_recovered: int | None = Field(None, description="Settled only")


class DailyOut(BaseModel):
    source: DaySourceOut
    total_consumption_kwh: float
    total_generation_kwh: float
    renewable_share: float | None
    zones: list[DailyZone]


class WindowsOut(BaseModel):
    grid_zone: str
    days: list[DaySourceOut] = Field(description="Which layer served each day, and why")
    windows: list[ZoneWindow]


class Bill(BaseModel):
    business_date: date
    household_id: str
    tariff_tier: str
    readings: int
    gross_consumption_kwh: Decimal
    solar_generation_kwh: Decimal
    net_import_kwh: Decimal
    net_export_kwh: Decimal
    energy_charge: Decimal
    fixed_charge: Decimal
    export_credit: Decimal
    subsidy_amount: Decimal
    total_payable: Decimal
    subsidy_applied: bool
    currency: str
    status: Status = Status.SETTLED
    settlement_run_id: int
    trigger: str
    drop_version: int
    restated: bool = Field(False, description="An earlier settlement of this day was superseded")


class BillRevision(Bill):
    current: bool = Field(description="The revision the serving layer shows")
    reason: str | None = Field(description="Why this settlement was run, for a restatement")


class Unbilled(BaseModel):
    date: date
    reason: str


class HouseholdBillsOut(BaseModel):
    household_id: str
    grid_zone: str
    tariff_tier: str
    bills: list[Bill]
    unbilled: list[Unbilled] = Field(description="Days with no bill yet, and why")
    total_payable: Decimal


class BillHistoryOut(BaseModel):
    household_id: str
    date: date
    revisions: list[BillRevision]


class TierTotal(BaseModel):
    tariff_tier: str
    households: int
    net_import_kwh: Decimal
    net_export_kwh: Decimal
    total_payable: Decimal
    in_credit: int


class DayBillsOut(BaseModel):
    date: date
    status: Status = Status.SETTLED
    settlement_run_id: int
    households_billed: int
    total_billed: Decimal
    tiers: list[TierTotal]
    limit: int
    offset: int
    bills: list[Bill]


class SettlementRun(BaseModel):
    run_id: int
    business_date: date
    status: str
    trigger: str
    reason: str | None
    drop_version: int
    current: bool
    readings_archived: int | None
    readings_settled: int | None
    duplicates_removed: int | None
    readings_rejected: int | None
    late_readings_recovered: int | None
    mean_abs_speed_gap_pct: float | None
    households_billed: int | None
    total_billed: Decimal | None
    started_at: datetime
    finished_at: datetime | None
    error: str | None
    report_key: str | None


class ReportOut(BaseModel):
    business_date: date
    settlement_run_id: int
    object_key: str
    created_at: datetime
    html_url: str


# -- Helpers -------------------------------------------------------------------------


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return start, start + timedelta(days=1)


def _share(generation: float, consumption: float) -> float | None:
    return generation / consumption if consumption > 0 else None


def _day_of(moment: datetime) -> date:
    return moment.astimezone(UTC).date()


def _source_out(source: Any) -> DaySourceOut:
    return DaySourceOut(
        date=source.day,
        status=source.status,
        layer=source.layer,
        reason=source.reason,
        settlement_run_id=source.settlement_run_id,
    )


def _bill(row: Row, **extra: Any) -> dict[str, Any]:
    return {
        **{k: v for k, v in row.items() if k in Bill.model_fields},
        "settlement_run_id": row["run_id"],
        **extra,
    }


# -- Dependencies ----------------------------------------------------------------------


def get_store(request: Request) -> ServingStore:
    return request.app.state.store


def get_fleet(request: Request) -> Fleet:
    return request.app.state.fleet


def get_config(request: Request) -> Settings:
    return request.app.state.settings


def get_clock(store: Annotated[ServingStore, Depends(get_store)]) -> SimulatedClock:
    clock = store.clock()
    if clock is None:
        raise HTTPException(503, "no simulation is running: start one with `sim-reset`")
    return clock


Store = Annotated[ServingStore, Depends(get_store)]
FleetDep = Annotated[Fleet, Depends(get_fleet)]
Config = Annotated[Settings, Depends(get_config)]
Clock = Annotated[SimulatedClock, Depends(get_clock)]


def _check_day(day: date, clock: SimulatedClock) -> date:
    start, today = clock.start.date(), clock.sim_date()
    if day < start:
        raise HTTPException(422, f"{day} is before the simulation started ({start})")
    if day > today:
        raise HTTPException(422, f"{day} is in the simulated future (today is {today})")
    return day


def _range(start: date, end: date, clock: SimulatedClock, settings: Settings) -> list[date]:
    _check_day(start, clock)
    _check_day(end, clock)
    try:
        return date_range(start, end, max_days=settings.api_max_range_days)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


def _zone(zone: str, fleet: Fleet) -> str:
    if zone not in fleet.zones:
        raise HTTPException(404, f"unknown grid zone {zone!r}; zones are {list(fleet.zones)}")
    return zone


# -- The application ----------------------------------------------------------------------


def create_app(
    store: ServingStore | None = None,
    settings: Settings | None = None,
    fleet: Fleet | None = None,
) -> FastAPI:
    """Build the app. Tests pass their own store; production builds a PostgresStore."""
    settings = settings or get_settings()
    owned = store is None
    store = store or PostgresStore(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if owned:
            store.open()  # type: ignore[union-attr]
        yield
        if owned:
            store.close()  # type: ignore[union-attr]

    app = FastAPI(
        title="Smart grid serving API",
        version="1.0",
        description=(
            "Grid load, renewable mix and household bills, merged across the Lambda layers. "
            "Every figure is labelled SETTLED (batch layer) or PROVISIONAL (speed layer). "
            "Dates and times are simulated (1 day = 300 real seconds)."
        ),
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.settings = settings
    app.state.fleet = fleet or build_fleet_from_settings(settings)

    # Per-app registry, so building the app twice (tests) never double-registers.
    registry = prom.CollectorRegistry()
    requests_total = prom.Counter(
        "smartgrid_api_requests_total",
        "HTTP requests, by route template and status.",
        ["method", "route", "status"],
        registry=registry,
    )
    latency = prom.Histogram(
        "smartgrid_api_request_seconds",
        "Request latency, by route template.",
        ["route"],
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
        registry=registry,
    )

    @app.middleware("http")
    async def observe(request: Request, call_next: Any) -> Response:
        started = time.perf_counter()
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        try:
            response = await call_next(request)
        except Exception:
            requests_total.labels(request.method, "unhandled", 500).inc()
            log.exception(
                "request failed", extra={"request_id": request_id, "path": request.url.path}
            )
            raise
        route = request.scope.get("route")
        template = getattr(route, "path", "unmatched")  # a template, not the raw URL
        elapsed = time.perf_counter() - started
        requests_total.labels(request.method, template, response.status_code).inc()
        latency.labels(template).observe(elapsed)
        response.headers["X-Request-ID"] = request_id
        # Scrapes and passing health probes arrive every few seconds; logging
        # them would bury the requests worth reading. They are still counted.
        routine = template == "/metrics" or (template == "/health" and response.status_code == 200)
        if not routine:
            log.info(
                "request",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "route": template,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": round(elapsed * 1000, 1),
                },
            )
        return response

    @app.exception_handler(psycopg.OperationalError)
    async def store_down(request: Request, exc: psycopg.OperationalError) -> JSONResponse:
        log.error("serving store unavailable", extra={"path": request.url.path, "error": str(exc)})
        return JSONResponse({"detail": "serving store unavailable"}, status_code=503)

    # -- Operations -------------------------------------------------------------

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse("/docs")

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(prom.generate_latest(registry), media_type=prom.CONTENT_TYPE_LATEST)

    @app.get("/health", response_model=Health, tags=["operations"])
    def health(store: Store) -> Any:
        """Is the API serving, and is what it serves fresh? 503 only if the store is down."""
        if not store.ping():
            return JSONResponse(
                {"status": "down", "checks": {"postgres": "unreachable"}}, status_code=503
            )
        checks: dict[str, Any] = {"postgres": "ok"}
        clock = store.clock()
        if clock is None:
            checks["simulation"] = "not started"
            return Health(status="degraded", checks=checks)
        now = clock.now()
        checks["simulation"] = {"simulated_now": now.isoformat(timespec="seconds")}
        archived = store.stream_progress().get("ingest")
        if archived is None:
            checks["speed_layer"] = {"fresh": False, "detail": "nothing archived yet"}
        else:
            lag_real = (now - archived).total_seconds() / clock.compression
            checks["speed_layer"] = {
                "archived_up_to": archived.isoformat(timespec="seconds"),
                "lag_real_seconds": round(lag_real, 1),
                "fresh": lag_real <= FRESHNESS_TARGET_SECONDS,
            }
        fresh = checks["speed_layer"]["fresh"]
        return Health(status="ok" if fresh else "degraded", checks=checks)

    @app.get(f"{API}/clock", response_model=ClockOut, tags=["operations"])
    def clock_now(clock: Clock) -> ClockOut:
        """The shared simulated clock: what 'today' means for every other endpoint."""
        return ClockOut(
            simulated_now=clock.now(),
            simulated_date=clock.sim_date(),
            simulation_started=clock.start.date(),
            compression=clock.compression,
            sim_id=int(clock.real_start),
        )

    # -- Grid (question 1) ------------------------------------------------------------

    @app.get(f"{API}/zones", response_model=list[ZoneInfo], tags=["grid"])
    def zones(fleet: FleetDep) -> list[ZoneInfo]:
        return [
            ZoneInfo(
                grid_zone=zone,
                households=len(fleet.in_zone(zone)),
                with_solar=sum(1 for h in fleet.in_zone(zone) if h.has_solar),
            )
            for zone in fleet.zones
        ]

    @app.get(f"{API}/zones/live", response_model=LiveOut, tags=["grid"])
    def zones_live(store: Store, clock: Clock, config: Config) -> LiveOut:
        """
        Current grid load and renewable mix by zone: each zone's latest COMPLETE
        15-minute window from the speed layer. Always PROVISIONAL.
        """
        now = clock.now()
        rows = store.latest_complete_windows(config.speed_window_minutes)
        zones = []
        for row in rows:
            age_sim = (now - row["window_end"]).total_seconds()
            zones.append(
                LiveZone(
                    **row,
                    status=Status.PROVISIONAL,
                    layer=Layer.SPEED,
                    data_age_simulated_minutes=round(age_sim / 60, 1),
                    data_age_real_seconds=round(age_sim / clock.compression, 1),
                )
            )
        oldest = max((z.data_age_real_seconds for z in zones), default=None)
        return LiveOut(
            simulated_now=now,
            stale=oldest is None or oldest > FRESHNESS_TARGET_SECONDS,
            freshness_target_real_seconds=FRESHNESS_TARGET_SECONDS,
            total_grid_load_kw=round(sum(z.grid_load_kw for z in zones), 3),
            total_solar_kw=round(sum(z.solar_kw for z in zones), 3),
            renewable_share=_share(
                sum(z.generation_kwh for z in zones), sum(z.consumption_kwh for z in zones)
            ),
            zones=zones,
        )

    @app.get(f"{API}/zones/daily", response_model=DailyOut, tags=["grid"])
    def zones_daily(
        store: Store,
        clock: Clock,
        day: Annotated[date | None, Query(alias="date", description="Default: today")] = None,
    ) -> DailyOut:
        """One day's totals per zone: SETTLED if the day is settled, otherwise PROVISIONAL."""
        day = _check_day(day or clock.sim_date(), clock)
        source = zone_source(day, clock.sim_date(), store.settled_runs(day, day))
        if source.layer is Layer.BATCH:
            rows = store.batch_daily(source.settlement_run_id)  # type: ignore[arg-type]
        else:
            rows = store.speed_daily(*_day_bounds(day))
        zones = [
            DailyZone(
                **{k: v for k, v in row.items() if k in DailyZone.model_fields},
                renewable_share=_share(row["generation_kwh"], row["consumption_kwh"]),
            )
            for row in rows
        ]
        consumption = sum(z.consumption_kwh for z in zones)
        generation = sum(z.generation_kwh for z in zones)
        return DailyOut(
            source=_source_out(source),
            total_consumption_kwh=round(consumption, 3),
            total_generation_kwh=round(generation, 3),
            renewable_share=_share(generation, consumption),
            zones=zones,
        )

    @app.get(f"{API}/zones/{{zone}}/windows", response_model=WindowsOut, tags=["grid"])
    def zone_windows(
        store: Store,
        fleet: FleetDep,
        clock: Clock,
        config: Config,
        zone: Annotated[str, Path(description="e.g. ZONE-A")],
        start: Annotated[date | None, Query(alias="from", description="Default: today")] = None,
        end: Annotated[date | None, Query(alias="to", description="Default: today")] = None,
    ) -> WindowsOut:
        """
        15-minute windows for a zone across a date range, merged: settled days
        from the batch layer, today and unsettled days from the speed layer.
        """
        zone = _zone(zone, fleet)
        today = clock.sim_date()
        days = _range(start or today, end or today, clock, config)
        settled = store.settled_runs(days[0], days[-1])
        try:
            sources = {day: zone_source(day, today, settled) for day in days}
        except FutureDate as exc:  # pragma: no cover - _range already refuses the future
            raise HTTPException(422, str(exc)) from exc

        batch_runs = [s.settlement_run_id for s in sources.values() if s.layer is Layer.BATCH]
        speed_days = [d for d, s in sources.items() if s.layer is Layer.SPEED]
        rows: list[tuple[Row, Status, Layer]] = [
            (r, Status.SETTLED, Layer.BATCH)
            for r in store.batch_windows(zone, batch_runs)  # type: ignore[arg-type]
        ]
        if speed_days:
            first, _ = _day_bounds(min(speed_days))
            _, last = _day_bounds(max(speed_days))
            rows += [
                (r, Status.PROVISIONAL, Layer.SPEED)
                for r in store.speed_windows(zone, first, last)
                if sources[_day_of(r["window_start"])].layer is Layer.SPEED
            ]
        rows.sort(key=lambda item: item[0]["window_start"])
        return WindowsOut(
            grid_zone=zone,
            days=[_source_out(s) for s in sources.values()],
            windows=[ZoneWindow(**r, status=status, layer=layer) for r, status, layer in rows],
        )

    # -- Billing (question 2) ------------------------------------------------------------

    @app.get(
        f"{API}/households/{{household_id}}/bills",
        response_model=HouseholdBillsOut,
        tags=["billing"],
    )
    def household_bills(
        store: Store,
        fleet: FleetDep,
        clock: Clock,
        config: Config,
        household_id: Annotated[str, Path(description="e.g. HH-00042")],
        start: Annotated[
            date | None, Query(alias="from", description="Default: 6 days ago")
        ] = None,
        end: Annotated[date | None, Query(alias="to", description="Default: today")] = None,
    ) -> HouseholdBillsOut:
        """A household's settled bills. Days without a settled bill are listed, with why."""
        household = fleet.by_household_id.get(household_id)
        if household is None:
            raise HTTPException(404, f"unknown household {household_id!r}")
        today = clock.sim_date()
        start = start or max(clock.start.date(), today - timedelta(days=6))
        days = _range(start, end or today, clock, config)
        settled = store.settled_runs(days[0], days[-1])
        bills = [Bill(**_bill(r)) for r in store.household_bills(household_id, days[0], days[-1])]
        billed = {b.business_date for b in bills}
        unbilled = [
            Unbilled(date=day, reason=bill_availability(day, today, settled).reason)
            for day in days
            if day not in billed
        ]
        return HouseholdBillsOut(
            household_id=household_id,
            grid_zone=household.grid_zone,
            tariff_tier=household.tariff_tier,
            bills=bills,
            unbilled=unbilled,
            total_payable=sum((b.total_payable for b in bills), Decimal("0.00")),
        )

    @app.get(
        f"{API}/households/{{household_id}}/bills/{{day}}/history",
        response_model=BillHistoryOut,
        tags=["billing"],
    )
    def bill_history(
        store: Store, fleet: FleetDep, clock: Clock, household_id: str, day: date
    ) -> BillHistoryOut:
        """Every settlement of one household's day: the original and each restatement."""
        if household_id not in fleet.by_household_id:
            raise HTTPException(404, f"unknown household {household_id!r}")
        _check_day(day, clock)
        rows = store.bill_history(household_id, day)
        return BillHistoryOut(
            household_id=household_id,
            date=day,
            revisions=[
                BillRevision(**_bill(r, current=r["current"], reason=r["reason"], restated=i > 0))
                for i, r in enumerate(rows)
            ],
        )

    @app.get(f"{API}/bills", response_model=DayBillsOut, tags=["billing"])
    def day_bills(
        store: Store,
        clock: Clock,
        day: Annotated[date, Query(alias="date")],
        tier: Annotated[str | None, Query(description="Filter by tariff tier")] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> DayBillsOut:
        """All settled bills for a day, with totals by tier. 404 if the day is not settled."""
        _check_day(day, clock)
        availability = bill_availability(day, clock.sim_date(), store.settled_runs(day, day))
        if not availability.billed:
            raise HTTPException(404, f"no bills for {day}: {availability.reason}")
        tiers = [TierTotal(**row) for row in store.day_bill_totals(day)]
        return DayBillsOut(
            date=day,
            settlement_run_id=availability.settlement_run_id,  # type: ignore[arg-type]
            households_billed=sum(t.households for t in tiers),
            total_billed=sum((t.total_payable for t in tiers), Decimal("0.00")),
            tiers=tiers,
            limit=limit,
            offset=offset,
            bills=[Bill(**_bill(r)) for r in store.day_bills(day, tier, limit, offset)],
        )

    # -- Settlements and reports -------------------------------------------------------------

    @app.get(f"{API}/settlements", response_model=list[SettlementRun], tags=["settlement"])
    def settlements(
        store: Store,
        day: Annotated[date | None, Query(alias="date")] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 20,
    ) -> list[SettlementRun]:
        """Settlement runs, newest first: status, trigger, restatement reason, and totals."""
        return [SettlementRun(**row) for row in store.settlement_runs(day, limit)]

    def _report(row: Row | None, what: str) -> ReportOut:
        if row is None:
            raise HTTPException(404, f"no daily report for {what}")
        return ReportOut(
            business_date=row["business_date"],
            settlement_run_id=row["run_id"],
            object_key=row["object_key"],
            created_at=row["created_at"],
            html_url=f"{API}/reports/{row['business_date'].isoformat()}/html",
        )

    @app.get(f"{API}/reports/latest", response_model=ReportOut, tags=["settlement"])
    def latest_report(store: Store) -> ReportOut:
        """The newest settled day's daily report."""
        return _report(store.current_report(None), "any day yet")

    @app.get(f"{API}/reports/{{day}}", response_model=ReportOut, tags=["settlement"])
    def report(store: Store, day: date) -> ReportOut:
        """The daily report of a day's current settlement."""
        return _report(store.current_report(day), str(day))

    @app.get(f"{API}/reports/{{day}}/html", response_class=HTMLResponse, tags=["settlement"])
    def report_html(store: Store, day: date) -> HTMLResponse:
        """The report itself, as published to object storage."""
        meta = _report(store.current_report(day), str(day))
        body = store.report_html(meta.object_key)
        if body is None:
            raise HTTPException(404, f"report object {meta.object_key} is missing")
        return HTMLResponse(body.decode("utf-8"))

    return app


def main() -> None:
    import uvicorn

    settings = get_settings()
    configure_logging(service=SERVICE, level=settings.log_level)
    log.info("serving API starting", extra={"port": settings.api_port})
    # log_config=None keeps our JSON logging; the middleware logs each request.
    uvicorn.run(
        create_app(settings=settings),
        host="0.0.0.0",
        port=settings.api_port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
