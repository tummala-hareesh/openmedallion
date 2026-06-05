"""cortex/tabs/dashboard.py — Dynamic dashboard tab.

Layout adapts to every query result:
  - 1-row aggregation    → large centred KPI card(s)
  - Time-series data     → line chart (full-width or side-by-side)
  - Distribution intent  → pie + bar
  - Comparison intent    → bar chart(s) per metric
  - General categorical  → bar + pie (if cardinality ≤ 8)
  - Pure-numeric multi   → histograms

All chart decisions are made by ``cortex.viz.build_dashboard``.
"""
from __future__ import annotations

import datetime

from dash import Input, Output, callback, dcc, html
import dash_bootstrap_components as dbc

from openmedallion.cortex.viz import build_dashboard
from openmedallion.cortex.theme import BORDER, FONT_UI, TEXT_MUTED, WHITE


def layout() -> html.Div:
    return html.Div([

        # ── Filter + export row ───────────────────────────────────────────
        dbc.Row([
            dbc.Col([
                html.Label(
                    "Region",
                    style={
                        "fontSize": "11px", "fontWeight": "600", "color": TEXT_MUTED,
                        "textTransform": "uppercase", "letterSpacing": "0.7px",
                        "display": "block", "marginBottom": "4px",
                    },
                ),
                dcc.Dropdown(
                    id="filter-region", multi=True, placeholder="All regions…",
                    style={"fontSize": "13px", "fontFamily": FONT_UI},
                ),
            ], width=3),
            dbc.Col([
                html.Label(
                    "Category",
                    style={
                        "fontSize": "11px", "fontWeight": "600", "color": TEXT_MUTED,
                        "textTransform": "uppercase", "letterSpacing": "0.7px",
                        "display": "block", "marginBottom": "4px",
                    },
                ),
                dcc.Dropdown(
                    id="filter-category", multi=True, placeholder="All categories…",
                    style={"fontSize": "13px", "fontFamily": FONT_UI},
                ),
            ], width=3),
            dbc.Col(
                dbc.Button(
                    "Export PDF", id="btn-pdf", color="outline-secondary", size="sm",
                    style={"fontFamily": FONT_UI, "fontSize": "12.5px"},
                ),
                width="auto", className="d-flex align-items-end pb-1",
            ),
            dbc.Col(
                html.Div(
                    id="dashboard-refresh-time",
                    style={"fontSize": "11px", "color": TEXT_MUTED, "textAlign": "right", "paddingBottom": "2px"},
                ),
                className="d-flex align-items-end justify-content-end pb-1",
            ),
        ], className="mb-3"),

        dcc.Download(id="download-pdf"),

        # ── Dynamic content — rebuilt on every query ──────────────────────
        html.Div(id="dashboard-dynamic-content"),

    ], style={"padding": "24px 28px"})


def register_callbacks() -> None:

    @callback(
        Output("filter-region",   "options"),
        Output("filter-category", "options"),
        Input("store-query-results", "data"),
    )
    def update_filter_options(rows):
        if not rows:
            return [], []
        import polars as pl
        df = pl.DataFrame(rows)
        return _unique_opts(df, "region"), _unique_opts(df, "category")

    @callback(
        Output("dashboard-dynamic-content", "children"),
        Output("dashboard-refresh-time",    "children"),
        Input("store-query-results",        "data"),
        Input("store-last-question",        "data"),
        Input("filter-region",              "value"),
        Input("filter-category",            "value"),
    )
    def update_dashboard(rows, question, regions, categories):
        now = datetime.datetime.now().strftime("Refreshed %b %d, %H:%M")
        content = build_dashboard(
            rows or [],
            question=question or "",
            regions=regions,
            categories=categories,
        )
        return content, (now if rows else "")

    @callback(
        Output("download-pdf", "data"),
        Input("btn-pdf", "n_clicks"),
        prevent_initial_call=True,
    )
    def export_pdf(n_clicks):
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _unique_opts(df, col: str) -> list[dict]:
    if col not in df.columns:
        return []
    vals = df[col].drop_nulls().unique().sort().to_list()
    return [{"label": str(v), "value": v} for v in vals]
