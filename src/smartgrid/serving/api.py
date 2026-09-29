from __future__ import annotations

from datetime import date
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException

from smartgrid.common.config import get_settings


app = FastAPI(
    title="SmartGrid Serving API",
    version="0.1.0",
)


def fetch_all(query: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    settings = get_settings()

    with psycopg.connect(settings.postgres_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            columns = [description.name for description in cursor.description]
            return [
                dict(zip(columns, row, strict=True))
                for row in cursor.fetchall()
            ]


@app.get("/health")
def health() -> dict[str, str]:
    settings = get_settings()

    try:
        with psycopg.connect(settings.postgres_dsn) as connection:
            connection.execute("SELECT 1")
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"database unavailable: {exc}",
        ) from exc

    return {"status": "ok"}


@app.get("/zones/current")
def current_zone_metrics() -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT DISTINCT ON (zone_name)
            zone_name,
            snapshot_ts,
            total_consumption_kwh,
            total_generation_kwh,
            net_kwh,
            renewable_share,
            active_meters
        FROM speed.zone_snapshot
        ORDER BY zone_name, snapshot_ts DESC
        """
    )


@app.get("/bills/{household_id}")
def household_bills(household_id: str) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM batch.bills
        WHERE household_id = %s
        ORDER BY billing_date DESC
        """,
        (household_id,),
    )


@app.get("/view/{household_id}")
def household_view(household_id: str) -> dict[str, Any]:
    bills = household_bills(household_id)

    if bills:
        return {
            "status": "SETTLED",
            "household_id": household_id,
            "bill": bills[0],
        }

    zone_rows = fetch_all(
        """
        SELECT DISTINCT ON (zone_name)
            zone_name,
            snapshot_ts,
            total_consumption_kwh,
            total_generation_kwh,
            net_kwh,
            renewable_share,
            active_meters
        FROM speed.zone_snapshot
        ORDER BY zone_name, snapshot_ts DESC
        """
    )

    return {
        "status": "PROVISIONAL",
        "household_id": household_id,
        "message": "No settled bill exists for this household.",
        "current_zone_metrics": zone_rows,
    }