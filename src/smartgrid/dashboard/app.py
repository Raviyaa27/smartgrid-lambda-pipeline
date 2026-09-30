"""
Business dashboard: grid load, solar contribution and household bills.

    streamlit run src/smartgrid/dashboard/app.py          # http://localhost:8501

It answers the use case's question in its two halves (ADR-0001):

    Grid now         current load and solar share by zone    PROVISIONAL, refreshes itself
    Zone history     15-minute windows across days           settled days + today, merged
    Settlement       one settled day: zones, tiers, report   SETTLED
    Household bills  a household's bills and restatements    SETTLED only
    Runs             every settlement, including failures    audit trail

It reads nothing but the serving API, so it shows exactly what the API
decides. Every figure is labelled with its source: blue marks settled
figures from the batch layer, orange marks provisional ones from the speed
layer, and the words say the same so colour is never the only cue.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

import streamlit as st

from smartgrid.common.config import get_settings
from smartgrid.dashboard import charts, views
from smartgrid.dashboard.client import ApiClient, ApiError

settings = get_settings()
REFRESH = settings.dashboard_refresh_seconds

st.set_page_config(page_title="Smart grid dashboard", page_icon=":material/bolt:", layout="wide")


# -- Data: short-lived caches over the API ---------------------------------------------


@st.cache_resource
def api() -> ApiClient:
    return ApiClient(settings.api_url)


@st.cache_data(ttl=4, show_spinner=False)
def health() -> dict:
    return api().health()


@st.cache_data(ttl=4, show_spinner=False)
def clock() -> dict:
    return api().clock()


@st.cache_data(ttl=4, show_spinner=False)
def live() -> dict:
    return api().live()


@st.cache_data(ttl=10, show_spinner=False)
def windows(zone: str, start: date, end: date) -> dict:
    return api().windows(zone, start, end)


@st.cache_data(ttl=10, show_spinner=False)
def daily(day: date) -> dict:
    return api().daily(day)


@st.cache_data(ttl=10, show_spinner=False)
def day_bills(day: date) -> dict:
    return api().day_bills(day)


@st.cache_data(ttl=10, show_spinner=False)
def settlements() -> list[dict]:
    return api().settlements(limit=100)


@st.cache_data(ttl=10, show_spinner=False)
def household_bills(household_id: str, start: date, end: date) -> dict:
    return api().household_bills(household_id, start, end)


@st.cache_data(ttl=10, show_spinner=False)
def bill_history(household_id: str, day: date) -> dict:
    return api().bill_history(household_id, day)


@st.cache_data(ttl=10, show_spinner=False)
def report_html(day: date) -> str:
    return api().report_html(day)


@st.cache_data(ttl=3600, show_spinner=False)
def households() -> list[dict]:
    return api().households()


def mode() -> str:
    return "dark" if st.context.theme.type == "dark" else "light"


def show_error(exc: ApiError) -> None:
    if exc.status == 0:
        st.error(f"{exc.detail}. Is the `api` service running?", icon=":material/cloud_off:")
    elif exc.status == 503:
        st.warning(exc.detail, icon=":material/pause_circle:")
    else:
        st.error(exc.detail, icon=":material/error:")


def preselect(options: list[str], param: str) -> int:
    """Index of ?param=value in options, so a view can be shared as a link."""
    wanted = st.query_params.get(param)
    return options.index(wanted) if wanted in options else 0


def load(fetch: Callable[..., Any], *args: Any) -> Any | None:
    """Call the API; on failure say why, in place, and return None. Never a traceback."""
    try:
        return fetch(*args)
    except ApiError as exc:
        show_error(exc)
        return None


def source_badge(status: str) -> None:
    if status == "SETTLED":
        st.badge("SETTLED: batch layer", icon=":material/verified:", color="blue")
    else:
        st.badge("PROVISIONAL: speed layer", icon=":material/speed:", color="orange")


def plot(fig: Any) -> None:
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False}, theme=None)


# -- Header: clock and freshness, refreshed on a timer ---------------------------------


@st.fragment(run_every=REFRESH)
def header() -> dict | None:
    title, status = st.columns([3, 2], vertical_alignment="bottom")
    title.title("Smart grid: load, solar and bills", anchor=False)
    fresh = views.freshness(health())
    colour = {"good": "green", "warning": "orange", "critical": "red"}[fresh.level]
    with status:
        st.badge(fresh.label, icon=fresh.icon, color=colour)
        try:
            now = clock()
        except ApiError:
            return None
        st.caption(
            f"Simulated {now['simulated_now'][:16].replace('T', ' ')} UTC · "
            f"1 simulated day = {86400 / now['compression']:.0f} real seconds "
            f"({now['compression']:.0f}×) · simulation {now['sim_id']}"
        )
    return now


header()
try:
    NOW = clock()
except ApiError as exc:
    show_error(exc)
    st.stop()
TODAY = date.fromisoformat(NOW["simulated_date"])
START = date.fromisoformat(NOW["simulation_started"])


# -- Grid now (question 1) -------------------------------------------------------------


@st.fragment(run_every=REFRESH)
def grid_now() -> None:
    try:
        body = live()
    except ApiError as exc:
        show_error(exc)
        return
    frame = views.live_frame(body)
    if frame.empty:
        st.info(
            "No live data yet: the speed layer has not completed a window.",
            icon=":material/hourglass_empty:",
        )
        return

    source_badge("PROVISIONAL")
    st.caption(
        "Each zone's latest complete 15-minute window, at most "
        f"{frame['Age (real s)'].max():.0f} real seconds old. These figures are replaced by "
        "settled ones once the day ends and the batch layer settles it."
    )
    load, solar, share, zones = st.columns(4)
    load.metric("Grid load", f"{body['total_grid_load_kw']:,.0f} kW")
    solar.metric("Solar output", f"{frame['Solar (kW)'].sum():,.0f} kW")
    share.metric(
        "Solar share of consumption",
        "–" if body["renewable_share"] is None else f"{body['renewable_share']:.0%}",
    )
    zones.metric("Zones reporting", f"{len(frame)}")

    if body["stale"]:
        st.warning(
            "Live data is older than the 60-second freshness target: the speed layer is behind "
            "or stopped.",
            icon=":material/warning:",
        )
    low = views.low_renewable_zones(
        body,
        settings.renewable_alert_floor,
        settings.renewable_alert_start_hour,
        settings.renewable_alert_end_hour,
    )
    if low:
        st.warning(
            f"**Low solar share** (below {settings.renewable_alert_floor:.0%} between "
            f"{settings.renewable_alert_start_hour:02d}:00 and "
            f"{settings.renewable_alert_end_hour:02d}:00): "
            + ", ".join(f"{zone} {value:.0%}" for zone, value in low),
            icon=":material/wb_cloudy:",
        )

    left, right = st.columns(2)
    with left:
        plot(
            charts.category_bars(
                frame["Zone"],
                frame["Grid load (kW)"],
                title="Grid load by zone",
                axis_title="kW, average over the window",
                mode=mode(),
                suffix=" kW",
            )
        )
    with right:
        plot(
            charts.category_bars(
                frame["Zone"],
                frame["Solar share (%)"],
                title="Solar as a share of consumption",
                axis_title="% of consumption (over 100% = zone exports)",
                mode=mode(),
                suffix="%",
                threshold=100 * settings.renewable_alert_floor,
                threshold_label=f"alert floor {settings.renewable_alert_floor:.0%}",
            )
        )
    with st.expander("Table view"):
        st.dataframe(frame, hide_index=True, width="stretch")


# -- Zone history: the merge rule, visible ---------------------------------------------


def date_range(key: str, default_days: int) -> tuple[date, date]:
    chosen = st.date_input(
        "Simulated dates",
        value=(max(START, TODAY - timedelta(days=default_days - 1)), TODAY),
        min_value=START,
        max_value=TODAY,
        key=key,
    )
    if isinstance(chosen, tuple) and len(chosen) == 2:
        return chosen[0], chosen[1]
    single = chosen[0] if isinstance(chosen, tuple) else chosen
    return single, single


def history_view() -> None:
    people = load(households)
    if not people:
        return
    zone_col, range_col = st.columns([1, 2])
    zone_names = sorted({h["grid_zone"] for h in people})
    zone = zone_col.selectbox(
        "Zone", zone_names, index=preselect(zone_names, "zone"), key="history_zone"
    )
    with range_col:
        start, end = date_range("history_range", default_days=3)
    body = load(windows, zone, start, end)
    if body is None:
        return
    st.markdown(
        "  ".join(
            f":blue-badge[{d['date']} settled · run {d['settlement_run_id']}]"
            if d["status"] == "SETTLED"
            else f":orange-badge[{d['date']} provisional · {d['reason']}]"
            for d in body["days"]
        )
    )
    frame = views.windows_frame(body)
    if frame.empty:
        st.info("No windows for these dates yet.", icon=":material/hourglass_empty:")
        return
    plot(
        charts.by_source_over_time(
            frame,
            "grid_load_kw",
            title=f"{zone}: grid load, 15-minute windows",
            axis_title="kW",
            mode=mode(),
            suffix=" kW",
        )
    )
    plot(
        charts.by_source_over_time(
            frame,
            "solar_share_pct",
            title=f"{zone}: solar as a share of consumption",
            axis_title="% of consumption",
            mode=mode(),
            suffix="%",
        )
    )
    st.caption(
        "Settled days come from the batch layer, which recomputed them from the whole day's "
        "readings, late ones included. Today, and any day not yet settled, comes from the "
        "speed layer."
    )
    with st.expander("Table view"):
        st.dataframe(frame, hide_index=True, width="stretch")


# -- Settlement: one day, settled (question 2 at zone and tier level) ------------------


def settlement_view() -> None:
    runs = load(settlements) or []
    settled_days = sorted({date.fromisoformat(r["business_date"]) for r in runs if r["current"]})
    day = st.date_input(
        "Simulated date",
        value=settled_days[-1] if settled_days else TODAY,
        min_value=START,
        max_value=TODAY,
        key="settle_day",
    )
    body = load(daily, day)
    if body is None:
        return
    source = body["source"]
    source_badge(source["status"])
    settled = source["status"] == "SETTLED"
    if not settled:
        st.info(
            f"{day} is not settled ({source['reason']}). The figures below are provisional, and "
            "no bills exist for it yet: bills are issued only after settlement.",
            icon=":material/schedule:",
        )
    bills = load(day_bills, day) if settled else None
    consumed, solar_share, billed, households_billed = st.columns(4)
    consumed.metric("Energy consumed", f"{body['total_consumption_kwh']:,.0f} kWh")
    solar_share.metric(
        "Solar share of consumption",
        "–" if body["renewable_share"] is None else f"{body['renewable_share']:.1%}",
    )
    billed.metric("Total billed", views.money(bills["total_billed"]) if bills else "no bills yet")
    households_billed.metric("Households billed", bills["households_billed"] if bills else "–")

    zones = views.zones_daily_frame(body)
    if bills:
        tiers = views.tiers_frame(bills)
        left, right = st.columns(2)
        with left:
            plot(
                charts.category_bars(
                    tiers["Tier"],
                    tiers["Billed (LKR)"],
                    title="Billed by tariff tier",
                    axis_title="LKR",
                    mode=mode(),
                )
            )
        with right:
            plot(
                charts.category_bars(
                    zones["Zone"],
                    # As a positive shortfall: labels then sit at the bar ends, clear
                    # of the zone names (the table keeps the signed figure).
                    -zones["Real-time vs settled (%)"],
                    title="How far the real-time view was below the settled figure",
                    axis_title="% below settled",
                    mode=mode(),
                    value_format=".2f",
                    suffix="%",
                )
            )
        st.caption(
            "The real-time view is low because readings that arrived after its watermark were "
            "missed; settlement recovered them (late readings per zone in the table)."
        )
    st.dataframe(zones, hide_index=True, width="stretch")

    if settled:
        report_col, download_col = st.columns([1, 1])
        report_col.link_button(
            "Open the daily report",
            f"{settings.api_public_url}/api/v1/reports/{day.isoformat()}/html",
            icon=":material/open_in_new:",
        )
        html = load(report_html, day)
        if html:
            download_col.download_button(
                "Download the report (HTML)",
                html,
                file_name=f"daily_report_{day.isoformat()}.html",
                mime="text/html",
                icon=":material/download:",
            )


# -- Household bills (question 2 at household level) -----------------------------------


def bills_view() -> None:
    people = load(households)
    if not people:
        return
    by_id = {h["household_id"]: h for h in people}
    pick_col, range_col = st.columns([1, 2])
    household_id = pick_col.selectbox(
        "Household",
        list(by_id),
        index=preselect(list(by_id), "household"),
        format_func=lambda h: f"{h} · {by_id[h]['grid_zone']} · {by_id[h]['tariff_tier']}",
        key="bills_household",
    )
    with range_col:
        start, end = date_range("bills_range", default_days=7)
    profile = by_id[household_id]
    solar = (
        f"rooftop solar {profile['solar_capacity_kw']:.1f} kW"
        if profile["has_solar"]
        else "no solar"
    )
    st.caption(f"{profile['grid_zone']} · tier {profile['tariff_tier']} · {solar}")
    body = load(household_bills, household_id, start, end)
    if body is None:
        return
    source_badge("SETTLED")
    total, days_billed, days_waiting = st.columns(3)
    total.metric("Payable over these days", views.money(body["total_payable"]))
    days_billed.metric("Days billed", len(body["bills"]))
    days_waiting.metric("Days without a bill yet", len(body["unbilled"]))

    if len(body["bills"]) >= 2:  # one bill is a number, not a chart: the tile above has it
        series = views.payable_series(body["bills"])
        plot(
            charts.columns_by_date(
                series["date"],
                series["payable"],
                title="Payable per day (negative = credit from solar export)",
                axis_title="LKR",
                mode=mode(),
            )
        )
    if body["bills"]:
        st.dataframe(views.bills_frame(body["bills"]), hide_index=True, width="stretch")
    if body["unbilled"]:
        st.info(
            "Days without a bill: "
            + "; ".join(f"{u['date']} ({u['reason']})" for u in body["unbilled"]),
            icon=":material/schedule:",
        )
    billed_days = [b["business_date"] for b in body["bills"]]
    if not billed_days:
        return
    st.subheader("Bill history", anchor=False)
    wanted = st.query_params.get("day")  # ?day=YYYY-MM-DD, else the latest billed day
    index = billed_days.index(wanted) if wanted in billed_days else len(billed_days) - 1
    chosen = st.selectbox("Settled day", billed_days, index=index, key="history_day")
    history = load(bill_history, household_id, date.fromisoformat(chosen))
    if history is None:
        return
    frame = views.history_frame(history)
    st.dataframe(frame, hide_index=True, width="stretch")
    if len(frame) > 1:
        st.caption(
            "This day was restated. Every settlement is kept; the one marked 'shown now' is "
            "the one the household is billed on."
        )


# -- Settlement runs: the audit trail --------------------------------------------------


def runs_view() -> None:
    runs = load(settlements)
    if runs is None:
        return
    st.caption(
        "Every settlement run, newest first. A restated day keeps its earlier runs; "
        "'shown now' marks the run whose figures the dashboard and the API serve."
    )
    st.dataframe(views.runs_frame(runs), hide_index=True, width="stretch")


# -- Navigation: one page per view, each with its own URL -------------------------------

pages = [
    st.Page(grid_now, title="Grid now", icon=":material/bolt:", default=True),  # served at /
    st.Page(history_view, title="Zone history", icon=":material/timeline:", url_path="history"),
    st.Page(settlement_view, title="Settlement", icon=":material/verified:", url_path="settlement"),
    st.Page(bills_view, title="Household bills", icon=":material/receipt_long:", url_path="bills"),
    st.Page(runs_view, title="Settlement runs", icon=":material/fact_check:", url_path="runs"),
]
st.navigation(pages, position="top").run()
st.caption(
    f"Data from the serving API at {settings.api_public_url}. Dates and times are simulated."
)
