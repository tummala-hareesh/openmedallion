"""cortex/tabs/table.py — Table tab: DataTable + collapsible SQL panel + downloads + row count."""
from __future__ import annotations

import io

import polars as pl
from dash import Input, Output, State, callback, dash_table, dcc, html
import dash_bootstrap_components as dbc

from openmedallion.cortex.theme import (
    BORDER, FONT_MONO, FONT_UI, SQL_BG, SQL_BORDER, SQL_TEXT,
    TEAL, TEAL_BG, TEXT_MUTED, TEXT_SECONDARY,
)


def layout() -> html.Div:
    return html.Div([

        # ── SQL panel (collapsible, collapsed by default) ──────────────────
        html.Div([
            html.Div([
                html.Span(
                    "LAST QUERY",
                    style={
                        "fontSize": "11px", "fontWeight": "600",
                        "color": TEXT_MUTED, "letterSpacing": "0.7px",
                        "textTransform": "uppercase",
                    },
                ),
                html.Button(
                    "Show SQL ▸",
                    id="btn-toggle-sql",
                    n_clicks=0,
                    style={
                        "background": "transparent",
                        "border": "none",
                        "cursor": "pointer",
                        "fontSize": "11.5px",
                        "fontWeight": "600",
                        "color": TEAL,
                        "fontFamily": FONT_UI,
                        "padding": "0",
                        "outline": "none",
                    },
                ),
            ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "6px"}),

            dbc.Collapse(
                html.Pre(
                    id="table-sql-display",
                    style={
                        "background": SQL_BG,
                        "border": f"1px solid {SQL_BORDER}",
                        "fontFamily": FONT_MONO,
                        "fontSize": "12px",
                        "color": SQL_TEXT,
                        "padding": "14px 16px",
                        "borderRadius": "6px",
                        "overflowX": "auto",
                        "lineHeight": "1.65",
                        "margin": "0",
                        "whiteSpace": "pre-wrap",
                        "wordBreak": "break-all",
                    },
                ),
                id="sql-collapse",
                is_open=False,
            ),
        ], style={"marginBottom": "18px"}),

        # ── Recommended prompt ─────────────────────────────────────────────
        html.Div(
            id="table-recommended",
            style={"display": "none", "marginBottom": "16px"},
        ),

        # ── DataTable + download controls ──────────────────────────────────
        html.Div([

            # Header row: title + download buttons (right-aligned)
            html.Div([
                html.Span(
                    id="table-row-count",
                    style={"fontSize": "12px", "color": TEXT_MUTED, "fontWeight": "500"},
                ),
                html.Div([
                    dbc.Button(
                        "↓ CSV",
                        id="btn-csv",
                        color="outline-secondary",
                        size="sm",
                        style={"fontFamily": FONT_UI, "fontSize": "12.5px", "fontWeight": "500"},
                    ),
                    dbc.Button(
                        "↓ Excel",
                        id="btn-excel",
                        color="outline-secondary",
                        size="sm",
                        style={"fontFamily": FONT_UI, "fontSize": "12.5px", "fontWeight": "500"},
                    ),
                ], style={"display": "flex", "gap": "8px"}),
            ], style={
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
                "marginBottom": "10px",
            }),

            dcc.Download(id="download-csv"),
            dcc.Download(id="download-excel"),

            dash_table.DataTable(
                id="results-table",
                page_size=20,
                style_table={"overflowX": "auto", "border": f"1px solid {BORDER}", "borderRadius": "8px"},
                style_cell={
                    "fontFamily": FONT_UI,
                    "fontSize": "13px",
                    "padding": "10px 14px",
                    "textAlign": "left",
                    "color": TEXT_SECONDARY,
                    "border": "1px solid #F1F5F9",
                    "whiteSpace": "normal",
                    "height": "auto",
                },
                style_header={
                    "backgroundColor": "#F8FAFC",
                    "color": TEXT_MUTED,
                    "fontWeight": "600",
                    "fontSize": "11px",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.7px",
                    "border": f"1px solid {BORDER}",
                    "borderBottom": f"2px solid {BORDER}",
                    "fontFamily": FONT_UI,
                },
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#FAFCFF"},
                    {"if": {"state": "selected"}, "backgroundColor": TEAL_BG, "border": f"1px solid {TEAL}"},
                ],
            ),

        ]),

    ], style={"padding": "24px 28px"})


def register_callbacks() -> None:

    @callback(
        Output("results-table",     "data"),
        Output("results-table",     "columns"),
        Output("results-table",     "style_cell_conditional"),
        Output("table-sql-display", "children"),
        Output("table-row-count",   "children"),
        Output("table-recommended", "children"),
        Output("table-recommended", "style"),
        Input("store-query-results", "data"),
        Input("store-last-sql",      "data"),
        Input("store-recommended",   "data"),
    )
    def update_table(rows, sql, recommended):
        if not rows:
            return [], [], [], "", "", "", {"display": "none"}

        cols = [{"name": k, "id": k} for k in rows[0].keys()]
        right_align = _numeric_column_ids(rows)
        style_cond = [
            {"if": {"column_id": c}, "textAlign": "right", "fontVariantNumeric": "tabular-nums"}
            for c in right_align
        ]
        count_label = f"{len(rows):,} row{'s' if len(rows) != 1 else ''}"

        rec_children = []
        rec_style = {"display": "none"}
        if recommended:
            rec_children = [
                html.Span("→ Suggested: ", style={"fontSize": "11.5px", "fontWeight": "600", "color": TEXT_MUTED}),
                html.Span(recommended, style={"fontSize": "12px", "color": TEAL, "fontStyle": "italic"}),
            ]
            rec_style = {"display": "flex", "gap": "6px", "alignItems": "center", "marginBottom": "14px"}

        return rows, cols, style_cond, sql or "", count_label, rec_children, rec_style

    @callback(
        Output("sql-collapse",   "is_open"),
        Output("btn-toggle-sql", "children"),
        Input("btn-toggle-sql",  "n_clicks"),
        State("sql-collapse",    "is_open"),
        prevent_initial_call=True,
    )
    def toggle_sql(n_clicks, is_open):
        new_open = not is_open
        label = "Hide SQL ▾" if new_open else "Show SQL ▸"
        return new_open, label

    @callback(
        Output("download-csv", "data"),
        Input("btn-csv",              "n_clicks"),
        State("store-query-results",  "data"),
        prevent_initial_call=True,
    )
    def download_csv(n_clicks, rows):
        if not n_clicks or not rows:
            return None
        return dcc.send_string(pl.DataFrame(rows).write_csv(), "query_results.csv")

    @callback(
        Output("download-excel", "data"),
        Input("btn-excel",            "n_clicks"),
        State("store-query-results",  "data"),
        prevent_initial_call=True,
    )
    def download_excel(n_clicks, rows):
        if not n_clicks or not rows:
            return None
        buf = io.BytesIO()
        pl.DataFrame(rows).write_excel(buf)
        return dcc.send_bytes(buf.getvalue(), "query_results.xlsx")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _numeric_column_ids(rows: list[dict]) -> list[str]:
    """Return column names whose values are numeric (for right-alignment)."""
    if not rows:
        return []
    return [k for k, v in rows[0].items() if isinstance(v, (int, float))]
