"""cortex/viz.py — Dynamic visualization engine.

Given a Polars DataFrame and the original question text, ``build_dashboard``
returns a list of Dash components (dbc.Row children) that form the most
relevant dashboard layout for that specific result set.

Decision logic
--------------
1.  Empty / no data          → placeholder card
2.  Single-row result        → large centred KPI card(s), no charts
3.  Temporal axis detected   → line chart (full-width if 1 metric, side-by-side
                               if 2+ metrics) + KPI summary row
4.  Intent = distribution    → pie/donut chart + supporting bar
5.  Intent = comparison      → bar chart per metric + optional pie if cardinality ≤ 8
6.  Intent = aggregate / general with string + numeric cols
                             → bar chart(s) + KPI row
7.  Pure-numeric multi-row   → KPI row + histogram-style bar
"""
from __future__ import annotations

import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from dash import html
import dash_bootstrap_components as dbc

from openmedallion.cortex.charts import bar_chart, line_chart, pie_chart
from openmedallion.cortex.theme import (
    BORDER, CHART_PALETTE, FONT_UI, GREEN_DARK, RED_DARK, TEAL,
    TEXT_DIM, TEXT_MUTED, TEXT_PRIMARY, WHITE,
)

# ── Keyword sets for intent detection ─────────────────────────────────────────

_TREND_KWS = {
    "trend", "over time", "monthly", "weekly", "quarterly", "annual",
    "yearly", "by month", "by year", "by quarter", "historical",
    "timeline", "time series", "growth", "change over",
}
_DIST_KWS = {
    "distribution", "breakdown", "percentage", "share", "proportion",
    "split", "composition", "ratio", "mix", "makeup",
}
_COMPARE_KWS = {
    "compare", "comparison", "vs ", "versus", "across", "difference",
    "rank", "ranking", "top ", "bottom ", "highest", "lowest",
    "best", "worst", "leading",
}
_AGG_KWS = {
    "total", "sum", "count", "how many", "how much", "overall",
    "aggregate", "grand total", "subtotal",
}
_TIME_NAME_KWS = {
    "month", "year", "date", "day", "week", "quarter",
    "period", "time", "created", "updated", "fiscal",
}
_RATE_SUFFIXES = {
    "rate", "pct", "percent", "ratio", "avg", "mean",
    "average", "score", "index",
}


# ── Public entry point ────────────────────────────────────────────────────────

def build_dashboard(
    rows: list[dict],
    question: str = "",
    regions: list | None = None,
    categories: list | None = None,
) -> list:
    """Return a list of dbc.Row / html.Div components for the dashboard body."""
    if not rows:
        return [_empty_placeholder("Ask a question to populate the dashboard")]

    df = pl.DataFrame(rows)

    if regions and "region" in df.columns:
        df = df.filter(pl.col("region").is_in(regions))
    if categories and "category" in df.columns:
        df = df.filter(pl.col("category").is_in(categories))

    if df.is_empty():
        return [_empty_placeholder("No data matches the selected filters")]

    num_cols  = _numeric_cols(df)
    str_cols  = _string_cols(df)
    time_cols = _temporal_cols(df)
    intent    = _detect_intent(question)
    nrows     = len(df)

    # ── Single-row result: big KPI display only ────────────────────────────
    if nrows == 1 and num_cols:
        return _single_row_layout(df, num_cols)

    content: list = []

    # ── KPI summary row (always shown when there are numeric cols) ─────────
    if num_cols:
        content.append(_kpi_row(df, num_cols))

    # ── Chart section ──────────────────────────────────────────────────────
    charts: list[tuple[go.Figure, int]] = []   # (figure, bootstrap_col_width)

    if time_cols and num_cols:
        # Time-series: one line chart per metric (max 3)
        x = time_cols[0]
        metrics = num_cols[:3]
        if len(metrics) == 1:
            charts.append((_line(df, x, metrics[0]), 12))
        else:
            for m in metrics:
                charts.append((_line(df, x, m), 6))

    elif intent == "distribution" and str_cols and num_cols:
        charts.append((_pie(df, str_cols[0], num_cols[0]), 5))
        charts.append((_bar(df, str_cols[0], num_cols[0]), 7))

    elif intent == "comparison" and str_cols and num_cols:
        for m in num_cols[:2]:
            width = 12 if len(num_cols) == 1 else 6
            charts.append((_bar(df, str_cols[0], m), width))
        if len(num_cols) >= 2:
            charts.append((_bar(df, str_cols[0], num_cols[1]), 6))

    elif str_cols and num_cols:
        # General / aggregate: bar + optional pie
        x = str_cols[0]
        charts.append((_bar(df, x, num_cols[0]), 12 if len(num_cols) == 1 else 7))
        cardinality = df[x].n_unique()
        if cardinality <= 8:
            charts.append((_pie(df, x, num_cols[0]), 5))
        if len(num_cols) >= 2:
            charts.append((_bar(df, x, num_cols[1]), 6))

    elif num_cols and not str_cols:
        # Pure-numeric multi-row: histograms
        for m in num_cols[:2]:
            fig = _histogram(df, m)
            charts.append((fig, 12 if len(num_cols) == 1 else 6))

    content.extend(_layout_charts(charts))

    # ── Fallback: no charts could be generated ─────────────────────────────
    if len(content) <= 1:   # only the KPI row, no charts
        content.append(_empty_placeholder("No chart applicable for this result shape"))

    return content


# ── Layout builders ───────────────────────────────────────────────────────────

def _single_row_layout(df: pl.DataFrame, num_cols: list[str]) -> list:
    """Large centred KPI cards for a single-row aggregation result."""
    n = min(len(num_cols), 4)
    width  = {1: 4, 2: 4, 3: 4, 4: 3}.get(n, 3)
    offset = {1: 4, 2: 2, 3: 0, 4: 0}.get(n, 0)
    cols = []
    for i, col in enumerate(num_cols[:n]):
        val = float(df[col][0])
        lbl = _agg_label(col)
        cls = f"offset-{offset}" if i == 0 and offset else ""
        cols.append(dbc.Col(_big_kpi_card(lbl, val), width=width, className=cls))
    return [dbc.Row(cols, className="mb-3 mt-2")]


def _kpi_row(df: pl.DataFrame, num_cols: list[str]) -> dbc.Row:
    n = min(len(num_cols), 4)
    width = {1: 3, 2: 6, 3: 4, 4: 3}.get(n, 3)
    cols = [
        dbc.Col(_kpi_card(_agg_label(col), _aggregate(df, col)), width=width)
        for col in num_cols[:n]
    ]
    return dbc.Row(cols, className="mb-3")


def _layout_charts(charts: list[tuple[go.Figure, int]]) -> list[dbc.Row]:
    rows: list[dbc.Row] = []
    i = 0
    while i < len(charts):
        fig1, w1 = charts[i]
        remaining = 12 - w1
        if remaining > 0 and i + 1 < len(charts) and charts[i + 1][1] <= remaining:
            fig2, w2 = charts[i + 1]
            rows.append(dbc.Row([
                dbc.Col(_chart_card(fig1), width=w1),
                dbc.Col(_chart_card(fig2), width=w2),
            ], className="mb-3"))
            i += 2
        else:
            rows.append(dbc.Row([dbc.Col(_chart_card(fig1), width=w1)], className="mb-3"))
            i += 1
    return rows


# ── Card components ───────────────────────────────────────────────────────────

def _kpi_card(label: str, value: float) -> html.Div:
    return html.Div([
        html.Div(label, style={
            "fontSize": "11px", "fontWeight": "600", "color": TEXT_MUTED,
            "textTransform": "uppercase", "letterSpacing": "0.8px", "marginBottom": "10px",
        }),
        html.Div(_fmt(value), style={
            "fontSize": "24px", "fontWeight": "700", "color": TEXT_PRIMARY,
            "letterSpacing": "-0.8px", "fontFamily": FONT_UI,
        }),
    ], style={
        "background": WHITE, "border": f"1px solid {BORDER}",
        "borderLeft": f"3px solid {TEAL}",
        "borderRadius": "8px", "padding": "18px 20px",
    })


def _big_kpi_card(label: str, value: float) -> html.Div:
    return html.Div([
        html.Div(label, style={
            "fontSize": "12px", "fontWeight": "600", "color": TEXT_MUTED,
            "textTransform": "uppercase", "letterSpacing": "0.8px",
            "marginBottom": "14px", "textAlign": "center",
        }),
        html.Div(_fmt(value), style={
            "fontSize": "42px", "fontWeight": "700", "color": TEXT_PRIMARY,
            "letterSpacing": "-2px", "textAlign": "center", "fontFamily": FONT_UI,
        }),
    ], style={
        "background": WHITE, "border": f"1px solid {BORDER}",
        "borderTop": f"3px solid {TEAL}",
        "borderRadius": "8px", "padding": "32px 24px",
        "textAlign": "center",
    })


def _chart_card(fig: go.Figure) -> html.Div:
    from dash import dcc
    return html.Div(
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
        style={
            "background": WHITE, "border": f"1px solid {BORDER}",
            "borderRadius": "8px", "padding": "4px",
        },
    )


def _empty_placeholder(msg: str) -> html.Div:
    return html.Div(
        html.Div([
            html.I(className="bi bi-bar-chart-line", style={"fontSize": "28px", "color": "#CBD5E1", "display": "block", "marginBottom": "12px"}),
            html.Div(msg, style={"fontSize": "13px", "color": TEXT_MUTED}),
        ], style={"textAlign": "center", "padding": "48px 0"}),
    )


# ── Plotly figure wrappers ────────────────────────────────────────────────────

def _bar(df: pl.DataFrame, x: str, y: str) -> go.Figure:
    return bar_chart(df, x=x, y=y,
                     title=f"{_label(y)} by {_label(x)}")


def _line(df: pl.DataFrame, x: str, y: str) -> go.Figure:
    return line_chart(df, x=x, y=y,
                      title=f"{_label(y)} over {_label(x)}")


def _pie(df: pl.DataFrame, names: str, values: str) -> go.Figure:
    return pie_chart(df, names=names, values=values,
                     title=f"{_label(values)} Distribution")


def _histogram(df: pl.DataFrame, col: str) -> go.Figure:
    fig = px.histogram(
        df.to_pandas(), x=col,
        title=f"{_label(col)} Distribution",
        template="plotly_white",
        color_discrete_sequence=CHART_PALETTE,
        nbins=min(20, len(df)),
    )
    fig.update_layout(
        font={"family": FONT_UI, "color": TEXT_DIM},
        paper_bgcolor=WHITE,
        plot_bgcolor=WHITE,
        margin={"l": 16, "r": 16, "t": 36, "b": 16},
        title_font={"size": 12, "color": TEXT_DIM, "family": FONT_UI},
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="#F1F5F9")
    return fig


# ── Data helpers ──────────────────────────────────────────────────────────────

def _numeric_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if df[c].dtype.is_numeric()]


def _string_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if df[c].dtype in (pl.String, pl.Categorical)]


def _temporal_cols(df: pl.DataFrame) -> list[str]:
    result = []
    for col in df.columns:
        if df[col].dtype in (pl.Date, pl.Datetime, pl.Time, pl.Duration):
            result.append(col)
        elif df[col].dtype in (pl.String, pl.Categorical, pl.Int32, pl.Int64,
                               pl.UInt32, pl.UInt64):
            if any(kw in col.lower() for kw in _TIME_NAME_KWS):
                result.append(col)
    return result


def _detect_intent(question: str) -> str:
    q = question.lower()
    if any(kw in q for kw in _TREND_KWS):
        return "trend"
    if any(kw in q for kw in _DIST_KWS):
        return "distribution"
    if any(kw in q for kw in _COMPARE_KWS):
        return "comparison"
    if any(kw in q for kw in _AGG_KWS):
        return "aggregate"
    return "general"


def _is_rate_col(col: str) -> bool:
    return any(s in col.lower() for s in _RATE_SUFFIXES)


def _aggregate(df: pl.DataFrame, col: str) -> float:
    if _is_rate_col(col):
        return round(float(df[col].mean()), 4)
    return round(float(df[col].sum()), 2)


def _agg_label(col: str) -> str:
    base = col.replace("_", " ").title()
    if _is_rate_col(col):
        return f"Avg {base}"
    return base


def _label(col: str) -> str:
    return col.replace("_", " ").title()


def _fmt(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if abs(v) >= 1_000:
        s = f"{v:,.0f}" if v == int(v) else f"{v:,.1f}K".replace(",", "")
        return f"{v / 1_000:.1f}K" if abs(v) < 10_000 else f"{v:,.0f}"
    if v != int(v):
        return f"{v:,.2f}"
    return f"{int(v):,}"
