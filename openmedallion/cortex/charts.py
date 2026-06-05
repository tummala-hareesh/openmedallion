"""cortex/charts.py — Shared Plotly figure factory functions (Data Studio Pro theme)."""
from __future__ import annotations

import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from openmedallion.cortex.theme import (
    CHART_PALETTE, FONT_UI, TEXT_DIM, TEXT_MUTED, WHITE,
)

_LAYOUT_BASE = dict(
    font={"family": FONT_UI, "color": TEXT_DIM},
    paper_bgcolor=WHITE,
    plot_bgcolor=WHITE,
    margin={"l": 16, "r": 16, "t": 36, "b": 16},
    title_font={"size": 12, "color": TEXT_DIM, "family": FONT_UI},
    legend={"font": {"size": 11, "family": FONT_UI}},
    hoverlabel={"font": {"family": FONT_UI, "size": 12}},
)


def bar_chart(
    df: pl.DataFrame,
    x: str,
    y: str,
    title: str = "",
    color: str | None = None,
) -> go.Figure:
    fig = px.bar(
        df.to_pandas(), x=x, y=y, title=title, color=color,
        template="plotly_white",
        color_discrete_sequence=CHART_PALETTE,
    )
    fig.update_layout(**_LAYOUT_BASE)
    fig.update_traces(marker_line_width=0)
    fig.update_xaxes(showgrid=False, tickfont={"size": 11, "family": FONT_UI})
    fig.update_yaxes(gridcolor="#F1F5F9", tickfont={"size": 11, "family": FONT_UI})
    return fig


def line_chart(
    df: pl.DataFrame,
    x: str,
    y: str | list[str],
    title: str = "",
) -> go.Figure:
    fig = px.line(
        df.to_pandas(), x=x, y=y, title=title,
        template="plotly_white", markers=True,
        color_discrete_sequence=CHART_PALETTE,
    )
    fig.update_layout(**_LAYOUT_BASE)
    fig.update_traces(line_width=2.5, marker_size=5)
    fig.update_xaxes(showgrid=False, tickfont={"size": 11, "family": FONT_UI})
    fig.update_yaxes(gridcolor="#F1F5F9", tickfont={"size": 11, "family": FONT_UI})
    return fig


def pie_chart(
    df: pl.DataFrame,
    names: str,
    values: str,
    title: str = "",
) -> go.Figure:
    fig = px.pie(
        df.to_pandas(), names=names, values=values, title=title,
        template="plotly_white", hole=0.35,
        color_discrete_sequence=CHART_PALETTE,
    )
    fig.update_layout(**_LAYOUT_BASE)
    fig.update_traces(
        textfont={"family": FONT_UI, "size": 11},
        marker_line_color=WHITE,
        marker_line_width=2,
    )
    return fig


def empty_fig(message: str = "Ask a question to see data") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        **_LAYOUT_BASE,
        height=240,
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        xaxis_visible=False,
        yaxis_visible=False,
        annotations=[{
            "text": message,
            "x": 0.5, "y": 0.5,
            "xref": "paper", "yref": "paper",
            "showarrow": False,
            "font": {"color": TEXT_MUTED, "size": 13, "family": FONT_UI},
        }],
    )
    return fig


def kpi_card(label: str, value: float | int | str, color: str = "#10B981") -> go.Figure:
    """Legacy Plotly Indicator KPI — kept for backward compatibility."""
    numeric = float(value) if isinstance(value, (int, float)) else 0.0
    fig = go.Figure(go.Indicator(
        mode="number",
        value=numeric,
        title={"text": label, "font": {"size": 12, "family": FONT_UI, "color": TEXT_DIM}},
        number={"font": {"size": 28, "color": color, "family": FONT_UI}},
    ))
    fig.update_layout(
        height=120,
        margin={"l": 16, "r": 16, "t": 40, "b": 8},
        paper_bgcolor=WHITE,
    )
    return fig
