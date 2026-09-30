"""
Plotly figures for the dashboard, drawn to one set of rules:

- One axis per chart. Load and solar share are different scales, so they
  are two charts, never one chart with two y-axes.
- Colour follows identity and never rank. The only categorical pair is the
  data's SOURCE -- settled (slot 1, blue) and provisional (slot 2, orange) --
  and each source keeps its colour even when the other is absent. A
  single-series chart uses slot 1 for every bar.
- The palette is the validated reference palette (CVD delta-E 24.7 light,
  26.8 dark between the two slots), with its own steps for dark mode, not an
  automatic inversion.
- Thin marks: bars at most 24 px thick with 4 px rounded ends, 2 px lines,
  hairline solid grids. Value labels use text ink, never the series colour.
"""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import plotly.graph_objects as go

from smartgrid.dashboard.views import SOURCES

FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'

PALETTE: dict[str, dict] = {
    "light": {
        "surface": "#fcfcfb",
        "ink": "#0b0b0b",
        "secondary": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "baseline": "#c3c2b7",
        "series": ("#2a78d6", "#eb6834"),
    },
    "dark": {
        "surface": "#1a1a19",
        "ink": "#ffffff",
        "secondary": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "baseline": "#383835",
        "series": ("#3987e5", "#d95926"),
    },
}

MAX_BAR_PX = 24


def source_colour(source: str, mode: str) -> str:
    return PALETTE[mode]["series"][SOURCES.index(source)]


def _style(fig: go.Figure, mode: str, title: str, height: int, *, legend: bool) -> go.Figure:
    p = PALETTE[mode]
    fig.update_layout(
        title={"text": title, "x": 0, "xanchor": "left", "font": {"size": 14, "color": p["ink"]}},
        font={"family": FONT, "size": 12, "color": p["secondary"]},
        paper_bgcolor=p["surface"],
        plot_bgcolor=p["surface"],
        height=height,
        margin={"l": 8, "r": 24, "t": 64 if legend else 44, "b": 8},
        showlegend=legend,
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.0,
            "xanchor": "left",
            "x": 0,
            "font": {"color": p["secondary"]},
        },
        hoverlabel={
            "bgcolor": p["surface"],
            "bordercolor": p["baseline"],
            "font": {"family": FONT, "color": p["ink"]},
        },
        barcornerradius=4,
    )
    axis = {
        "gridcolor": p["grid"],
        "gridwidth": 1,
        "griddash": "solid",
        "linecolor": p["baseline"],
        "zerolinecolor": p["baseline"],
        "zerolinewidth": 1,
        "tickfont": {"color": p["muted"]},
        "automargin": True,
        "title": {"font": {"color": p["secondary"]}},
    }
    fig.update_xaxes(**axis)
    fig.update_yaxes(**axis)
    return fig


def _bar_fraction(count: int, height: int, chrome: int = 90) -> float:
    """Bar thickness as a fraction of its band, capped at MAX_BAR_PX."""
    band = max(1.0, (height - chrome) / max(count, 1))
    return min(0.7, MAX_BAR_PX / band)


def category_bars(
    categories: Sequence[str],
    values: Sequence[float],
    *,
    title: str,
    axis_title: str,
    mode: str,
    value_format: str = ",.0f",
    suffix: str = "",
    threshold: float | None = None,
    threshold_label: str = "",
) -> go.Figure:
    """Horizontal bars, one series: magnitude by category, first category on top."""
    p = PALETTE[mode]
    height = max(220, 70 + 38 * len(categories))
    labels = [f"{v:{value_format}}{suffix}" if v is not None else "" for v in values]
    fig = go.Figure(
        go.Bar(
            x=list(values),
            y=list(categories),
            orientation="h",
            width=_bar_fraction(len(categories), height),
            marker={"color": p["series"][0], "line": {"width": 0}},
            text=labels,
            textposition="outside",
            textfont={"color": p["secondary"]},
            cliponaxis=False,
            hovertemplate=f"%{{y}}: %{{x:{value_format}}}{suffix}<extra></extra>",
        )
    )
    if threshold is not None:
        fig.add_vline(
            x=threshold,
            line={"color": p["muted"], "width": 1, "dash": "dash"},  # a threshold, not a grid
            annotation={
                "text": threshold_label,
                "font": {"color": p["secondary"], "size": 11},
                "yanchor": "bottom",
            },
            annotation_position="top",
        )
    # automargin: the category labels decide the left margin, so none is clipped.
    fig.update_yaxes(autorange="reversed", showgrid=False, automargin=True, ticksuffix="  ")
    fig.update_xaxes(title={"text": axis_title})
    return _style(fig, mode, title, height, legend=False)


def by_source_over_time(
    frame: pd.DataFrame,
    y: str,
    *,
    title: str,
    axis_title: str,
    mode: str,
    hover_format: str = ",.1f",
    suffix: str = "",
) -> go.Figure:
    """One line per source, settled then provisional, on one shared axis."""
    p = PALETTE[mode]
    fig = go.Figure()
    for source in SOURCES:
        part = frame[frame["source"] == source]
        if part.empty:
            continue
        colour = source_colour(source, mode)
        fig.add_trace(
            go.Scatter(
                x=part["time"],
                y=part[y],
                name=source,
                mode="lines",
                line={"color": colour, "width": 2, "shape": "linear"},
                hovertemplate=f"%{{y:{hover_format}}}{suffix}<extra>{source}</extra>",
            )
        )
        last = part.iloc[-1]
        fig.add_trace(  # end marker: 8 px with a 2 px surface ring
            go.Scatter(
                x=[last["time"]],
                y=[last[y]],
                mode="markers",
                marker={"size": 8, "color": colour, "line": {"color": p["surface"], "width": 2}},
                showlegend=False,
                hoverinfo="skip",
            )
        )
    fig.update_layout(hovermode="x unified")
    fig.update_xaxes(showspikes=True, spikecolor=p["muted"], spikethickness=1, spikemode="across")
    fig.update_yaxes(title={"text": axis_title}, rangemode="tozero")
    return _style(fig, mode, title, 320, legend=True)


def columns_by_date(
    dates: Sequence[str],
    values: Sequence[float],
    *,
    title: str,
    axis_title: str,
    mode: str,
    prefix: str = "",
) -> go.Figure:
    """Vertical columns, one series; negative values (credits) fall below the baseline."""
    p = PALETTE[mode]
    height = 300
    fig = go.Figure(
        go.Bar(
            x=list(dates),
            y=list(values),
            width=_bar_fraction(len(dates), 1000, chrome=0),
            marker={"color": p["series"][0], "line": {"width": 0}},
            text=[f"{prefix}{v:,.2f}" for v in values],
            textposition="outside",
            textfont={"color": p["secondary"]},
            cliponaxis=False,
            hovertemplate=f"%{{x}}: {prefix}%{{y:,.2f}}<extra></extra>",
        )
    )
    fig.update_xaxes(type="category", showgrid=False)
    fig.update_yaxes(title={"text": axis_title}, zeroline=True)
    return _style(fig, mode, title, height, legend=False)
