"""cortex/charts.py — Shared Plotly figure factory functions."""
from __future__ import annotations

import plotly.express as px
import plotly.graph_objects as go
import polars as pl


def bar_chart(
    df: pl.DataFrame,
    x: str,
    y: str,
    title: str = "",
    color: str | None = None,
) -> go.Figure:
    """Grouped bar chart from a Polars DataFrame."""
    return px.bar(
        df.to_pandas(), x=x, y=y, title=title, color=color,
        template="plotly_white",
    )


def line_chart(
    df: pl.DataFrame,
    x: str,
    y: str | list[str],
    title: str = "",
) -> go.Figure:
    """Line chart (with markers) from a Polars DataFrame."""
    return px.line(
        df.to_pandas(), x=x, y=y, title=title,
        template="plotly_white", markers=True,
    )


def pie_chart(
    df: pl.DataFrame,
    names: str,
    values: str,
    title: str = "",
) -> go.Figure:
    """Pie / donut chart from a Polars DataFrame."""
    return px.pie(
        df.to_pandas(), names=names, values=values, title=title,
        template="plotly_white", hole=0.3,
    )


def kpi_card(label: str, value: float | int | str, color: str = "#1a73e8") -> go.Figure:
    """Single numeric KPI indicator figure."""
    numeric = float(value) if isinstance(value, (int, float)) else 0.0
    fig = go.Figure(go.Indicator(
        mode="number",
        value=numeric,
        title={"text": label, "font": {"size": 13}},
        number={"font": {"size": 30, "color": color}},
    ))
    fig.update_layout(
        height=130,
        margin={"l": 16, "r": 16, "t": 40, "b": 8},
        paper_bgcolor="#f8f9fa",
    )
    return fig
