"""The dashboard's logic, without Streamlit or a network: views, charts, API client."""

from __future__ import annotations

from datetime import date

import pytest
import requests

from smartgrid.dashboard import charts, views
from smartgrid.dashboard.client import ApiClient, ApiError


def zone(name: str, share: float | None, hour: int, load: float = 50.0) -> dict:
    return {
        "grid_zone": name,
        "window_start": f"2026-01-02T{hour:02d}:00:00Z",
        "window_end": f"2026-01-02T{hour:02d}:15:00Z",
        "grid_load_kw": load,
        "solar_kw": 10.0,
        "renewable_share": share,
        "data_age_real_seconds": 9.5,
        "status": "PROVISIONAL",
    }


# -- Freshness -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("health", "level", "words"),
    [
        (
            {"status": "ok", "checks": {"speed_layer": {"fresh": True, "lag_real_seconds": 7.7}}},
            "good",
            "8 s behind",
        ),
        (
            {
                "status": "degraded",
                "checks": {"speed_layer": {"fresh": False, "lag_real_seconds": 9444}},
            },
            "warning",
            "2.6 h behind",
        ),
        (
            {"status": "degraded", "checks": {"simulation": "not started"}},
            "warning",
            "No simulation",
        ),
        ({"status": "down", "checks": {}}, "critical", "store unreachable"),
        ({"status": "unreachable", "checks": {}}, "critical", "API unreachable"),
    ],
)
def test_freshness_says_what_is_wrong_in_words_not_just_colour(health, level, words):
    fresh = views.freshness(health)
    assert fresh.level == level
    assert words in fresh.label
    assert fresh.icon


# -- Low renewable share ------------------------------------------------------------------


def test_low_solar_is_flagged_only_while_the_sun_is_high():
    live = {
        "zones": [
            zone("ZONE-A", 0.25, hour=11),  # low, midday: flagged
            zone("ZONE-B", 0.80, hour=11),  # healthy
            zone("ZONE-C", 0.00, hour=22),  # night: zero is normal
            zone("ZONE-D", 0.10, hour=16),  # low, but late afternoon: outside the hours
            zone("ZONE-E", None, hour=12),  # no consumption: nothing to judge
        ]
    }
    assert views.low_renewable_zones(live, 0.30, 10, 14) == [("ZONE-A", 0.25)]


# -- Merge rule on screen ---------------------------------------------------------------------


def test_windows_are_labelled_by_source_and_share_is_a_percentage():
    body = {
        "windows": [
            {**zone("ZONE-A", 0.5, 0), "status": "SETTLED"},
            {**zone("ZONE-A", 1.25, 12), "status": "PROVISIONAL"},
        ]
    }
    frame = views.windows_frame(body)
    assert list(frame["source"]) == [views.SETTLED, views.PROVISIONAL]
    assert list(frame["solar_share_pct"]) == [50.0, 125.0]  # a zone can export: over 100%


def test_money_is_formatted_from_the_exact_string():
    assert views.money("871.06") == "LKR 871.06"
    assert views.money("-249.34") == "-LKR 249.34"
    assert views.money("24605.74") == "LKR 24,605.74"
    assert views.money(None) == "-"


def test_bill_history_marks_the_revision_that_is_shown():
    history = {
        "revisions": [
            {
                "settlement_run_id": 35,
                "trigger": "scheduled",
                "drop_version": 1,
                "energy_charge": "782.77",
                "total_payable": "792.77",
                "current": False,
                "reason": None,
            },
            {
                "settlement_run_id": 36,
                "trigger": "restatement",
                "drop_version": 2,
                "energy_charge": "861.06",
                "total_payable": "871.06",
                "current": True,
                "reason": "Regulator revision",
            },
        ]
    }
    frame = views.history_frame(history)
    assert list(frame["Shown now"]) == ["", "yes"]
    assert list(frame["Payable"]) == ["LKR 792.77", "LKR 871.06"]


# -- Charts ----------------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["light", "dark"])
def test_each_source_keeps_its_colour_even_when_the_other_is_absent(mode):
    only_provisional = views.windows_frame(
        {"windows": [{**zone("ZONE-A", 0.5, 11), "status": "PROVISIONAL"}]}
    )
    fig = charts.by_source_over_time(
        only_provisional, "grid_load_kw", title="t", axis_title="kW", mode=mode
    )
    line = next(t for t in fig.data if t.mode == "lines")
    assert line.name == views.PROVISIONAL
    assert line.line.color == charts.PALETTE[mode]["series"][1]  # orange, not "first colour"


def test_charts_never_have_a_second_y_axis():
    frame = views.windows_frame(
        {"windows": [{**zone("ZONE-A", 0.5, h), "status": "SETTLED"} for h in range(3)]}
    )
    figs = [
        charts.by_source_over_time(frame, "grid_load_kw", title="t", axis_title="kW", mode="light"),
        charts.category_bars(["A", "B"], [1.0, 2.0], title="t", axis_title="x", mode="dark"),
        charts.columns_by_date(["2026-01-01"], [5.0], title="t", axis_title="LKR", mode="light"),
    ]
    for fig in figs:
        assert "yaxis2" not in fig.to_dict()["layout"]


def test_bars_are_capped_at_24px_and_labelled_in_text_ink_not_series_colour():
    fig = charts.category_bars(
        ["ZONE-A", "ZONE-B"], [10.0, 20.0], title="t", axis_title="kW", mode="light"
    )
    bar = fig.data[0]
    band_px = (fig.layout.height - 90) / 2
    assert bar.width * band_px <= charts.MAX_BAR_PX + 1e-9
    assert bar.textfont.color == charts.PALETTE["light"]["secondary"]
    assert bar.marker.color == charts.PALETTE["light"]["series"][0]


@pytest.mark.parametrize("days", [2, 7, 31])
def test_columns_stay_thin_however_few_days_there_are(days):
    fig = charts.columns_by_date(
        [f"2026-01-{d + 1:02d}" for d in range(days)], [1.0] * days,
        title="t", axis_title="LKR", mode="light",
    )  # fmt: skip
    assert fig.data[0].width * (1000 / days) <= charts.MAX_BAR_PX + 1e-9


def test_the_alert_floor_is_drawn_as_a_labelled_threshold():
    fig = charts.category_bars(
        ["ZONE-A"],
        [25.0],
        title="t",
        axis_title="%",
        mode="light",
        threshold=30.0,
        threshold_label="alert floor 30%",
    )
    assert fig.layout.shapes[0].x0 == 30.0
    assert fig.layout.annotations[0].text == "alert floor 30%"


# -- API client -----------------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status: int, body: object, content_type: str = "application/json") -> None:
        self.status_code = status
        self._body = body
        self.headers = {"content-type": content_type}
        self.text = str(body)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, response=None, error=None) -> None:
        self.response, self.error, self.calls = response, error, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        if self.error:
            raise self.error
        return self.response


def test_the_client_sends_dates_as_iso_strings_and_drops_empty_parameters():
    session = FakeSession(FakeResponse(200, {"windows": []}))
    ApiClient("http://api:8000/", session=session).windows(
        "ZONE-A", date(2026, 1, 1), date(2026, 1, 2)
    )
    url, params = session.calls[0]
    assert url == "http://api:8000/api/v1/zones/ZONE-A/windows"
    assert params == {"from": "2026-01-01", "to": "2026-01-02"}


def test_the_clients_errors_carry_the_apis_reason():
    session = FakeSession(FakeResponse(404, {"detail": "no bills for 2026-01-02: day in progress"}))
    with pytest.raises(ApiError) as caught:
        ApiClient("http://api:8000", session=session).day_bills(date(2026, 1, 2))
    assert caught.value.status == 404
    assert "day in progress" in caught.value.detail


def test_an_unreachable_api_is_status_zero_and_health_still_answers():
    session = FakeSession(error=requests.ConnectionError("refused"))
    client = ApiClient("http://api:8000", session=session)
    with pytest.raises(ApiError) as caught:
        client.live()
    assert caught.value.status == 0
    assert client.health()["status"] == "unreachable"
