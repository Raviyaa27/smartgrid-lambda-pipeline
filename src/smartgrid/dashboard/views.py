"""
Pure shaping of API responses for the dashboard: no Streamlit, no network.

Everything a screen shows is computed here, so it is tested on its own and
the Streamlit script stays a thin layout.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd

SETTLED = "Settled (batch layer)"
PROVISIONAL = "Provisional (speed layer)"
# Fixed order: colour follows the source, never its position in a result.
SOURCES = (SETTLED, PROVISIONAL)


def source_label(status: str) -> str:
    return SETTLED if status == "SETTLED" else PROVISIONAL


# -- Status --------------------------------------------------------------------------


@dataclass(frozen=True)
class Freshness:
    level: str  # good | warning | critical -- never shown by colour alone
    label: str
    icon: str


def freshness(health: dict[str, Any]) -> Freshness:
    """One line for the header: is what the dashboard shows current?"""
    status = health.get("status")
    if status == "unreachable":
        return Freshness("critical", "Serving API unreachable", ":material/cloud_off:")
    if status == "down":
        return Freshness("critical", "Serving store unreachable", ":material/error:")
    checks = health.get("checks", {})
    if checks.get("simulation") == "not started":
        return Freshness("warning", "No simulation running", ":material/pause_circle:")
    speed = checks.get("speed_layer") or {}
    lag = speed.get("lag_real_seconds")
    if speed.get("fresh"):
        return Freshness("good", f"Live data fresh, {lag:.0f} s behind", ":material/check_circle:")
    if lag is None:
        return Freshness("warning", "No live data yet", ":material/hourglass_empty:")
    return Freshness("warning", f"Live data stale, {_duration(lag)} behind", ":material/warning:")


def _duration(seconds: float) -> str:
    if seconds < 120:
        return f"{seconds:.0f} s"
    if seconds < 7200:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.1f} h"


# -- Grid now ------------------------------------------------------------------------------


def live_frame(live: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {
            "Zone": z["grid_zone"],
            "Grid load (kW)": z["grid_load_kw"],
            "Solar (kW)": z["solar_kw"],
            "Solar share (%)": _pct(z["renewable_share"]),
            "Window (simulated)": _parse(z["window_start"]).strftime("%Y-%m-%d %H:%M"),
            "Age (real s)": z["data_age_real_seconds"],
            "Status": z["status"],
        }
        for z in live.get("zones", [])
    ]
    columns = ["Zone", "Grid load (kW)", "Solar (kW)", "Solar share (%)",
               "Window (simulated)", "Age (real s)", "Status"]  # fmt: skip
    return pd.DataFrame(rows, columns=columns)


def in_alert_hours(moment: datetime, start_hour: int, end_hour: int) -> bool:
    return start_hour <= moment.hour < end_hour


def low_renewable_zones(
    live: dict[str, Any], floor: float, start_hour: int, end_hour: int
) -> list[tuple[str, float]]:
    """
    Zones whose solar share is below `floor` while the sun is high. Judged on
    each zone's own window time: at night every zone is at 0%, which is not
    news, so outside the alert hours nothing is flagged.
    """
    flagged = []
    for zone in live.get("zones", []):
        share = zone["renewable_share"]
        window = _parse(zone["window_start"])
        if share is not None and share < floor and in_alert_hours(window, start_hour, end_hour):
            flagged.append((zone["grid_zone"], share))
    return flagged


# -- Zone history -----------------------------------------------------------------------------


def windows_frame(body: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {
            "time": _parse(w["window_start"]),
            "grid_load_kw": w["grid_load_kw"],
            "solar_kw": w["solar_kw"],
            "solar_share_pct": _pct(w["renewable_share"]),
            "source": source_label(w["status"]),
        }
        for w in body.get("windows", [])
    ]
    return pd.DataFrame(
        rows, columns=["time", "grid_load_kw", "solar_kw", "solar_share_pct", "source"]
    )


# -- Settlement and bills ------------------------------------------------------------------------


def money(value: str | Decimal | None) -> str:
    if value is None:
        return "-"
    amount = Decimal(str(value))
    sign = "-" if amount < 0 else ""
    return f"{sign}LKR {abs(amount):,.2f}"


def zones_daily_frame(daily: dict[str, Any]) -> pd.DataFrame:
    settled = daily["source"]["status"] == "SETTLED"
    rows = []
    for z in daily.get("zones", []):
        row = {
            "Zone": z["grid_zone"],
            "Consumed (kWh)": round(z["consumption_kwh"], 1),
            "Solar (kWh)": round(z["generation_kwh"], 1),
            "Solar share (%)": _pct(z["renewable_share"]),
            "Peak load (kW)": round(z["peak_load_kw"], 1),
        }
        if settled:
            row["Forecast irradiance"] = z.get("forecast_irradiance")
            row["Real-time vs settled (%)"] = _round(z.get("speed_vs_batch_pct"), 2)
            row["Late readings recovered"] = z.get("late_readings_recovered")
        rows.append(row)
    return pd.DataFrame(rows)


def tiers_frame(day_bills: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {
            "Tier": t["tariff_tier"],
            "Households": t["households"],
            "Net import (kWh)": float(t["net_import_kwh"]),
            "Billed (LKR)": float(t["total_payable"]),
            "In credit": t["in_credit"],
        }
        for t in day_bills.get("tiers", [])
    ]
    return pd.DataFrame(rows)


def bills_frame(bills: list[dict[str, Any]]) -> pd.DataFrame:
    rows = [
        {
            "Date": b["business_date"],
            "Tier": b["tariff_tier"],
            "Net import (kWh)": b["net_import_kwh"],
            "Net export (kWh)": b["net_export_kwh"],
            "Energy charge": money(b["energy_charge"]),
            "Fixed charge": money(b["fixed_charge"]),
            "Export credit": money(b["export_credit"]),
            "Subsidy": money(b["subsidy_amount"]),
            "Payable": money(b["total_payable"]),
            "Restated": "yes" if b.get("restated") else "",
            "Run": b["settlement_run_id"],
        }
        for b in bills
    ]
    return pd.DataFrame(rows)


def payable_series(bills: list[dict[str, Any]]) -> pd.DataFrame:
    """Daily amounts for the chart. Floats only for drawing: the table shows exact money."""
    return pd.DataFrame(
        [{"date": b["business_date"], "payable": float(b["total_payable"])} for b in bills],
        columns=["date", "payable"],
    )


def history_frame(history: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {
            "Run": r["settlement_run_id"],
            "Trigger": r["trigger"],
            "Tariff drop": f"v{r['drop_version']}",
            "Energy charge": money(r["energy_charge"]),
            "Payable": money(r["total_payable"]),
            "Shown now": "yes" if r["current"] else "",
            "Reason": r.get("reason") or "",
        }
        for r in history.get("revisions", [])
    ]
    return pd.DataFrame(rows)


_RUN_STATUS = {"succeeded": "✔ succeeded", "failed": "✖ failed", "running": "… running"}


def runs_frame(runs: list[dict[str, Any]]) -> pd.DataFrame:
    rows = [
        {
            "Run": r["run_id"],
            "Date": r["business_date"],
            "Status": _RUN_STATUS.get(r["status"], r["status"]),
            "Shown now": "yes" if r["current"] else "",
            "Trigger": r["trigger"],
            "Drop": f"v{r['drop_version']}",
            "Readings settled": r["readings_settled"],
            "Duplicates removed": r["duplicates_removed"],
            "Late recovered": r["late_readings_recovered"],
            "Mean |gap| (%)": _round(r["mean_abs_speed_gap_pct"], 2),
            "Billed": money(r["total_billed"]),
            "Reason / error": r.get("reason") or r.get("error") or "",
        }
        for r in runs
    ]
    return pd.DataFrame(rows)


# -- Helpers ---------------------------------------------------------------------------------------


def _parse(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _pct(share: float | None) -> float | None:
    return None if share is None else round(100 * share, 1)


def _round(value: float | None, digits: int) -> float | None:
    return None if value is None else round(value, digits)
