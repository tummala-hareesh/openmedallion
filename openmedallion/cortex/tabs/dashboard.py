"""cortex/tabs/dashboard.py — Dashboard tab: KPI cards + charts + filters + PDF export."""
from __future__ import annotations

import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, callback, dcc, html
import dash_bootstrap_components as dbc

from openmedallion.cortex.charts import bar_chart, kpi_card, line_chart, pie_chart


def layout() -> html.Div:
    return html.Div([
        # Filter + PDF export row
        dbc.Row([
            dbc.Col([
                html.Label("Region", className="text-muted small"),
                dcc.Dropdown(
                    id="filter-region", multi=True,
                    placeholder="All regions…",
                ),
            ], width=3),
            dbc.Col([
                html.Label("Category", className="text-muted small"),
                dcc.Dropdown(
                    id="filter-category", multi=True,
                    placeholder="All categories…",
                ),
            ], width=3),
            dbc.Col(
                dbc.Button(
                    "Export PDF", id="btn-pdf",
                    color="outline-danger", size="sm",
                ),
                width="auto",
                className="d-flex align-items-end pb-1",
            ),
        ], className="mb-3"),
        dcc.Download(id="download-pdf"),

        # KPI cards
        dbc.Row(id="kpi-row", className="mb-3"),

        # Line + bar charts
        dbc.Row([
            dbc.Col(dcc.Graph(id="chart-bar"),  width=6),
            dbc.Col(dcc.Graph(id="chart-line"), width=6),
        ], className="mb-3"),

        # Pie chart
        dbc.Row([
            dbc.Col(dcc.Graph(id="chart-pie"), width=5),
            dbc.Col(
                html.Div(
                    id="dashboard-placeholder",
                    style={"height": "300px"},
                ),
                width=7,
            ),
        ]),
    ], style={"padding": "20px"})


def register_callbacks() -> None:
    @callback(
        Output("filter-region",   "options"),
        Output("filter-category", "options"),
        Input("store-query-results", "data"),
    )
    def update_filter_options(rows):
        if not rows:
            return [], []
        df = pl.DataFrame(rows)
        return _unique_opts(df, "region"), _unique_opts(df, "category")

    @callback(
        Output("kpi-row",    "children"),
        Output("chart-bar",  "figure"),
        Output("chart-line", "figure"),
        Output("chart-pie",  "figure"),
        Input("store-query-results", "data"),
        Input("filter-region",       "value"),
        Input("filter-category",     "value"),
    )
    def update_dashboard(rows, regions, categories):
        if not rows:
            return _empty_kpi_row(), _empty_fig(), _empty_fig(), _empty_fig()

        df = _apply_filters(pl.DataFrame(rows), regions, categories)

        return (
            _build_kpi_row(df),
            _best_bar(df),
            _best_line(df),
            _best_pie(df),
        )

    @callback(
        Output("download-pdf", "data"),
        Input("btn-pdf", "n_clicks"),
        prevent_initial_call=True,
    )
    def export_pdf(n_clicks):
        # PDF export requires kaleido; stub returns None (no-op)
        return None


# ── private helpers ───────────────────────────────────────────────────────────

def _unique_opts(df: pl.DataFrame, col: str) -> list[dict]:
    if col not in df.columns:
        return []
    vals = df[col].drop_nulls().unique().sort().to_list()
    return [{"label": str(v), "value": v} for v in vals]


def _apply_filters(
    df: pl.DataFrame,
    regions: list | None,
    categories: list | None,
) -> pl.DataFrame:
    if regions and "region" in df.columns:
        df = df.filter(pl.col("region").is_in(regions))
    if categories and "category" in df.columns:
        df = df.filter(pl.col("category").is_in(categories))
    return df


def _numeric_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if df[c].dtype.is_numeric()]


def _string_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if df[c].dtype in (pl.String, pl.Categorical)]


def _build_kpi_row(df: pl.DataFrame) -> list:
    num_cols = _numeric_cols(df)[:4]
    if not num_cols:
        return _empty_kpi_row()
    return [
        dbc.Col(
            dcc.Graph(
                figure=kpi_card(col.replace("_", " ").title(), round(float(df[col].sum()), 2)),
                config={"displayModeBar": False},
            ),
            width=3,
        )
        for col in num_cols
    ]


def _empty_kpi_row() -> list:
    labels = ["Total Revenue", "Orders", "Avg Value", "Customers"]
    return [
        dbc.Col(
            dcc.Graph(
                figure=kpi_card(lbl, 0, color="#adb5bd"),
                config={"displayModeBar": False},
            ),
            width=3,
        )
        for lbl in labels
    ]


def _empty_fig() -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor="#f8f9fa",
        plot_bgcolor="#f8f9fa",
        xaxis_visible=False,
        yaxis_visible=False,
        annotations=[{
            "text": "Ask a question to populate charts",
            "x": 0.5, "y": 0.5,
            "xref": "paper", "yref": "paper",
            "showarrow": False,
            "font": {"color": "#adb5bd", "size": 13},
        }],
    )
    return fig


def _best_bar(df: pl.DataFrame) -> go.Figure:
    str_cols = _string_cols(df)
    num_cols = _numeric_cols(df)
    if str_cols and num_cols:
        x_lbl = str_cols[0].replace("_", " ").title()
        y_lbl = num_cols[0].replace("_", " ").title()
        return bar_chart(df, x=str_cols[0], y=num_cols[0], title=f"{y_lbl} by {x_lbl}")
    return _empty_fig()


def _best_line(df: pl.DataFrame) -> go.Figure:
    str_cols = _string_cols(df)
    num_cols = _numeric_cols(df)
    if str_cols and num_cols:
        return line_chart(df, x=str_cols[0], y=num_cols[:2], title="Trend")
    return _empty_fig()


def _best_pie(df: pl.DataFrame) -> go.Figure:
    str_cols = _string_cols(df)
    num_cols = _numeric_cols(df)
    if str_cols and num_cols:
        lbl = num_cols[0].replace("_", " ").title()
        return pie_chart(df, names=str_cols[0], values=num_cols[0], title=f"{lbl} Distribution")
    return _empty_fig()
