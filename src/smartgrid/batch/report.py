"""
The consolidated daily report: one self-contained HTML page per settlement.

Answers the business question for a settled day -- grid load and renewable
contribution by zone, and what every household owes -- and states how
trustworthy the real-time view was (speed vs batch). Stored in MinIO at
lake/reports/dt=<date>/run=<id>/daily_report.html and indexed in
ops.daily_reports, so the serving layer can hand out the latest one.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg

from smartgrid.common import storage
from smartgrid.common.config import Settings


@dataclass
class ReportData:
    business_date: date
    run_id: int
    trigger: str
    drop_version: int
    reason: str | None
    finished_at: datetime | None
    readings_archived: int
    readings_settled: int
    duplicates_removed: int
    readings_rejected: int
    households_billed: int
    total_billed: Decimal
    zones: list[dict[str, Any]] = field(default_factory=list)
    tiers: list[dict[str, Any]] = field(default_factory=list)
    top_bills: list[dict[str, Any]] = field(default_factory=list)
    in_credit: int = 0
    previous_total: Decimal | None = None  # the run this one restates, if any


def _fmt(value: Any, digits: int = 1) -> str:
    if value is None:
        return "—"
    if isinstance(value, Decimal):
        return f"{value:,.2f}"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    return html.escape(str(value))


def render(data: ReportData) -> str:
    """Pure: data in, HTML out."""
    kwh = sum(z["consumption_kwh"] for z in data.zones)
    solar = sum(z["generation_kwh"] for z in data.zones)
    missed = sum(z.get("missed_by_speed") or 0 for z in data.zones)
    restated = ""
    if data.previous_total is not None:
        change = data.total_billed - data.previous_total
        restated = (
            f'<p class="note"><strong>Restatement.</strong> This run supersedes an earlier '
            f"settlement of the same day. Total billed changed by LKR {_fmt(change)} "
            f"(from {_fmt(data.previous_total)}). Reason: {_fmt(data.reason)}</p>"
        )

    zone_rows = "".join(
        f"<tr><td>{_fmt(z['grid_zone'])}</td><td>{_fmt(z['consumption_kwh'])}</td>"
        f"<td>{_fmt(z['generation_kwh'])}</td>"
        f"<td>{_fmt(None if z['renewable_share'] is None else 100 * z['renewable_share'])}%</td>"
        f"<td>{_fmt(z['peak_load_kw'])}</td><td>{_fmt(z.get('forecast_irradiance'), 2)}</td>"
        f"<td>{_fmt(z.get('delta_pct'), 2)}{'%' if z.get('delta_pct') is not None else ''}</td>"
        f"<td>{_fmt(z.get('missed_by_speed'))}</td></tr>"
        for z in data.zones
    )
    tier_rows = "".join(
        f"<tr><td>{_fmt(t['tariff_tier'])}</td><td>{t['households']}</td>"
        f"<td>{_fmt(float(t['kwh']))}</td><td>{_fmt(t['billed'])}</td><td>{_fmt(t['average'])}</td></tr>"
        for t in data.tiers
    )
    top_rows = "".join(
        f"<tr><td>{_fmt(b['household_id'])}</td><td>{_fmt(b['tariff_tier'])}</td>"
        f"<td>{_fmt(b['net_import_kwh'])}</td><td>{_fmt(b['total_payable'])}</td></tr>"
        for b in data.top_bills
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Settlement {data.business_date}</title>
<style>
 body {{ font: 14px/1.5 system-ui, sans-serif; margin: 32px auto; max-width: 980px; color: #1b1b1b; padding: 0 16px; }}
 h1 {{ font-size: 22px; margin-bottom: 4px; }} h2 {{ font-size: 16px; margin-top: 28px; }}
 .meta {{ color: #5a6672; }} .kpis {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 18px 0; }}
 .kpi {{ border: 1px solid #d4dbe0; border-radius: 6px; padding: 10px 12px; }}
 .kpi b {{ display: block; font-size: 20px; }} .kpi span {{ color: #5a6672; font-size: 12px; }}
 table {{ border-collapse: collapse; width: 100%; }} th, td {{ padding: 6px 8px; border-bottom: 1px solid #e3e8eb; text-align: right; }}
 th:first-child, td:first-child {{ text-align: left; }} th {{ background: #edf3f5; font-weight: 600; }}
 .note {{ background: #fdf4e7; border-left: 3px solid #b35309; padding: 8px 12px; }}
 .settled {{ color: #14556b; font-weight: 600; }}
</style></head><body>
<h1>Daily settlement — {data.business_date} <span class="settled">SETTLED</span></h1>
<p class="meta">Run {data.run_id} · {html.escape(data.trigger)} · tariff drop v{data.drop_version}
 · finished {_fmt(data.finished_at)} (real time) · all figures in simulated time</p>
{restated}
<div class="kpis">
 <div class="kpi"><b>{data.households_billed}</b><span>households billed</span></div>
 <div class="kpi"><b>LKR {_fmt(data.total_billed)}</b><span>total billed</span></div>
 <div class="kpi"><b>{_fmt(kwh)} kWh</b><span>energy consumed</span></div>
 <div class="kpi"><b>{_fmt(100 * solar / kwh if kwh else None)}%</b><span>renewable contribution</span></div>
</div>
<h2>Grid by zone</h2>
<table><tr><th>Zone</th><th>Consumed kWh</th><th>Solar kWh</th><th>Renewable</th><th>Peak load kW</th>
<th>Forecast irradiance</th><th>Speed vs batch</th><th>Late readings</th></tr>{zone_rows}</table>
<p class="meta">"Speed vs batch" is how far the real-time (provisional) view was from these settled
 figures. "Late readings" arrived after the real-time view's watermark: {missed} in total, all
 included here.</p>
<h2>Billing by tier</h2>
<table><tr><th>Tier</th><th>Households</th><th>Net import kWh</th><th>Billed LKR</th><th>Average LKR</th></tr>{tier_rows}</table>
<p class="meta">{data.in_credit} households finished the day in credit (net exporters).</p>
<h2>Largest bills</h2>
<table><tr><th>Household</th><th>Tier</th><th>Net import kWh</th><th>Payable LKR</th></tr>{top_rows}</table>
<h2>Data quality</h2>
<table>
 <tr><td>Readings in the archive</td><td>{data.readings_archived:,}</td></tr>
 <tr><td>Retransmissions removed</td><td>{data.duplicates_removed:,}</td></tr>
 <tr><td>Rejected under current rules</td><td>{data.readings_rejected:,}</td></tr>
 <tr><td>Readings settled</td><td>{data.readings_settled:,}</td></tr>
</table>
</body></html>
"""


def gather(dsn: str, run_id: int) -> ReportData:
    with psycopg.connect(dsn) as conn:
        run = conn.execute(
            "SELECT business_date, trigger, drop_version, reason, finished_at, readings_archived, "
            "readings_settled, duplicates_removed, readings_rejected, households_billed, total_billed "
            "FROM ops.settlement_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        data = ReportData(
            run_id=run_id,
            business_date=run[0],
            trigger=run[1],
            drop_version=run[2],
            reason=run[3],
            finished_at=run[4],
            readings_archived=run[5] or 0,
            readings_settled=run[6] or 0,
            duplicates_removed=run[7] or 0,
            readings_rejected=run[8] or 0,
            households_billed=run[9] or 0,
            total_billed=run[10] or Decimal("0"),
        )
        columns = [
            "grid_zone",
            "consumption_kwh",
            "generation_kwh",
            "renewable_share",
            "peak_load_kw",
            "forecast_irradiance",
            "delta_pct",
            "missed_by_speed",
        ]
        data.zones = [
            dict(zip(columns, row, strict=True))
            for row in conn.execute(
                "SELECT z.grid_zone, z.consumption_kwh, z.generation_kwh, z.renewable_share, "
                "z.peak_load_kw, z.forecast_irradiance, r.delta_pct, r.missed_by_speed "
                "FROM batch.zone_daily z LEFT JOIN ops.reconciliation r "
                "ON r.run_id = z.run_id AND r.grid_zone = z.grid_zone "
                "WHERE z.run_id = %s ORDER BY z.grid_zone",
                (run_id,),
            ).fetchall()
        ]
        data.tiers = [
            dict(zip(["tariff_tier", "households", "kwh", "billed", "average"], row, strict=True))
            for row in conn.execute(
                "SELECT tariff_tier, count(*), sum(net_import_kwh), sum(total_payable), "
                "round(avg(total_payable), 2) FROM batch.bills WHERE run_id = %s "
                "GROUP BY tariff_tier ORDER BY tariff_tier",
                (run_id,),
            ).fetchall()
        ]
        data.top_bills = [
            dict(
                zip(
                    ["household_id", "tariff_tier", "net_import_kwh", "total_payable"],
                    row,
                    strict=True,
                )
            )
            for row in conn.execute(
                "SELECT household_id, tariff_tier, net_import_kwh, total_payable FROM batch.bills "
                "WHERE run_id = %s ORDER BY total_payable DESC LIMIT 5",
                (run_id,),
            ).fetchall()
        ]
        (data.in_credit,) = conn.execute(
            "SELECT count(*) FROM batch.bills WHERE run_id = %s AND total_payable < 0", (run_id,)
        ).fetchone()
        previous = conn.execute(
            "SELECT total_billed FROM ops.settlement_runs WHERE business_date = %s "
            "AND status = 'succeeded' AND run_id < %s ORDER BY run_id DESC LIMIT 1",
            (data.business_date, run_id),
        ).fetchone()
        data.previous_total = previous[0] if previous else None
    return data


def publish(settings: Settings, run_id: int) -> str:
    data = gather(settings.postgres_dsn, run_id)
    key = f"reports/dt={data.business_date.isoformat()}/run={run_id}/daily_report.html"
    storage.put_bytes(
        storage.s3_client(settings),
        settings.minio_bucket_lake,
        key,
        render(data).encode("utf-8"),
        "text/html; charset=utf-8",
    )
    with psycopg.connect(settings.postgres_dsn) as conn:
        conn.execute(
            "INSERT INTO ops.daily_reports (run_id, business_date, object_key) VALUES (%s, %s, %s) "
            "ON CONFLICT (run_id) DO UPDATE SET object_key = EXCLUDED.object_key, created_at = now()",
            (run_id, data.business_date, key),
        )
    return key
