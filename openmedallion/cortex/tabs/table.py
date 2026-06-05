"""cortex/tabs/table.py — Table tab: DataTable + SQL context panel + CSV/Excel download."""
from __future__ import annotations

import io

import polars as pl
from dash import Input, Output, State, callback, dash_table, dcc, html
import dash_bootstrap_components as dbc


def layout() -> html.Div:
    return html.Div([
        dbc.Row([
            # Left: SQL + recommended prompt
            dbc.Col([
                html.H6("SQL", className="text-muted mb-1"),
                html.Pre(
                    id="table-sql-display",
                    style={
                        "background": "#f8f9fa",
                        "border": "1px solid #dee2e6",
                        "borderRadius": "6px",
                        "padding": "10px",
                        "fontSize": "11px",
                        "minHeight": "100px",
                        "whiteSpace": "pre-wrap",
                        "wordBreak": "break-all",
                    },
                ),
                html.Hr(className="my-2"),
                html.H6("Recommended Prompt", className="text-muted mb-1"),
                html.P(
                    id="table-recommended",
                    style={"fontSize": "12px", "color": "#495057"},
                ),
            ], width=4),

            # Right: DataTable + download buttons
            dbc.Col([
                dbc.Row([
                    dbc.Col(
                        dbc.Button(
                            "Download CSV", id="btn-csv",
                            color="outline-secondary", size="sm",
                            className="me-2",
                        ),
                        width="auto",
                    ),
                    dbc.Col(
                        dbc.Button(
                            "Download Excel", id="btn-excel",
                            color="outline-secondary", size="sm",
                        ),
                        width="auto",
                    ),
                ], className="mb-2"),
                dcc.Download(id="download-csv"),
                dcc.Download(id="download-excel"),
                dash_table.DataTable(
                    id="results-table",
                    page_size=20,
                    style_table={"overflowX": "auto"},
                    style_cell={
                        "fontSize": "12px",
                        "padding": "5px 10px",
                        "textAlign": "left",
                    },
                    style_header={
                        "backgroundColor": "#343a40",
                        "color": "white",
                        "fontWeight": "bold",
                        "fontSize": "12px",
                    },
                    style_data_conditional=[
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "#f8f9fa",
                        }
                    ],
                ),
            ], width=8),
        ]),
    ], style={"padding": "20px"})


def register_callbacks() -> None:
    @callback(
        Output("results-table",     "data"),
        Output("results-table",     "columns"),
        Output("table-sql-display", "children"),
        Output("table-recommended", "children"),
        Input("store-query-results", "data"),
        Input("store-last-sql",      "data"),
        Input("store-recommended",   "data"),
    )
    def update_table(rows, sql, recommended):
        if not rows:
            return [], [], "", ""
        cols = [{"name": k, "id": k} for k in rows[0].keys()]
        return rows, cols, sql or "", recommended or ""

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
