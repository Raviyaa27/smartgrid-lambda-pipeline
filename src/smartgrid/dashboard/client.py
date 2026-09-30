"""
The dashboard's only data source: the serving API.

The dashboard never reads PostgreSQL or MinIO itself. If it did, it would
have to re-implement the merge rule, and the one thing ADR-0001 insists on --
that every figure says whether it is settled or provisional -- could drift
between the API and the screen.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import requests


class ApiError(RuntimeError):
    """The API refused, failed, or could not be reached. `status` 0 = unreachable."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(f"{status}: {detail}" if status else detail)
        self.status = status
        self.detail = detail


class ApiClient:
    def __init__(
        self, base_url: str, *, timeout: float = 5.0, session: requests.Session | None = None
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()

    def get(self, path: str, **params: Any) -> Any:
        query = {k: v.isoformat() if isinstance(v, date) else v for k, v in params.items()}
        query = {k: v for k, v in query.items() if v is not None}
        try:
            response = self.session.get(self.base_url + path, params=query, timeout=self.timeout)
        except requests.RequestException as exc:
            raise ApiError(0, f"serving API unreachable at {self.base_url}") from exc
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise ApiError(response.status_code, str(detail))
        if "json" in response.headers.get("content-type", ""):
            return response.json()
        return response.text

    # -- Named calls, one per endpoint the dashboard uses ----------------------------

    def health(self) -> dict:
        # /health answers 503 with a body when the store is down; keep the body.
        try:
            return self.get("/health")
        except ApiError as exc:
            if exc.status == 0:
                return {"status": "unreachable", "checks": {"api": exc.detail}}
            return {"status": "down", "checks": {"detail": exc.detail}}

    def clock(self) -> dict:
        return self.get("/api/v1/clock")

    def live(self) -> dict:
        return self.get("/api/v1/zones/live")

    def daily(self, day: date) -> dict:
        return self.get("/api/v1/zones/daily", date=day)

    def windows(self, zone: str, start: date, end: date) -> dict:
        return self.get(f"/api/v1/zones/{zone}/windows", **{"from": start, "to": end})

    def households(self) -> list[dict]:
        return self.get("/api/v1/households")

    def household_bills(self, household_id: str, start: date, end: date) -> dict:
        return self.get(f"/api/v1/households/{household_id}/bills", **{"from": start, "to": end})

    def bill_history(self, household_id: str, day: date) -> dict:
        return self.get(f"/api/v1/households/{household_id}/bills/{day.isoformat()}/history")

    def day_bills(self, day: date) -> dict:
        return self.get("/api/v1/bills", date=day, limit=1)

    def settlements(self, limit: int = 50) -> list[dict]:
        return self.get("/api/v1/settlements", limit=limit)

    def report_html(self, day: date) -> str:
        return self.get(f"/api/v1/reports/{day.isoformat()}/html")
