"""cortex/app.py — Plotly Dash application: layout, shared dcc.Store state, tab router.

Tabs
----
Tab 1 — Chat       Scrollable message history, multi-line input, Ask button
Tab 2 — Table      DataTable, SQL context panel, CSV / Excel download
Tab 3 — Dashboard  KPI cards, line + bar + pie charts, region/category filters

Shared state (dcc.Store)
------------------------
store-last-question   Most recent question text
store-last-sql        SQL that was last executed
store-query-results   List[dict] rows from the last query
store-recommended     Canonical recommended prompt string
store-project         Project name (set at startup, read-only)

Environment variables
---------------------
USE_MOCK_CLIENT=1    Use MockClient instead of NeuronClient (offline / CI mode)
"""
from __future__ import annotations

import os

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html

from openmedallion.cortex.client         import MockClient, NeuronClient
from openmedallion.cortex.tabs           import chat as chat_tab
from openmedallion.cortex.tabs           import dashboard as dashboard_tab
from openmedallion.cortex.tabs           import table as table_tab


def create_app(
    project: str,
    neuron_url: str = "http://localhost:8000",
) -> dash.Dash:
    """Create and wire up the Dash application.

    Parameters
    ----------
    project:
        Project name — forwarded to neuron on every /query call.
    neuron_url:
        Base URL for the neuron server.  Set ``USE_MOCK_CLIENT=1`` to use the
        deterministic offline stub instead (no network, no Ollama required).
    """
    use_mock = os.getenv("USE_MOCK_CLIENT", "0") == "1"
    client   = MockClient() if use_mock else NeuronClient(base_url=neuron_url)

    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        suppress_callback_exceptions=True,
    )
    app.title = f"openmedallion · {project}"

    app.layout = dbc.Container([

        # ── header ────────────────────────────────────────────────────────
        dbc.Row(dbc.Col(
            html.H4([
                html.Span("openmedallion", style={"color": "#1a73e8", "fontWeight": "700"}),
                html.Span(f"  ·  {project}", style={"fontWeight": "400", "color": "#495057"}),
            ], className="my-3"),
        )),

        # ── shared state stores ───────────────────────────────────────────
        dcc.Store(id="store-last-question"),
        dcc.Store(id="store-last-sql"),
        dcc.Store(id="store-query-results"),
        dcc.Store(id="store-recommended"),
        dcc.Store(id="store-project", data=project),

        # ── tabs ──────────────────────────────────────────────────────────
        dbc.Tabs(
            [
                dbc.Tab(chat_tab.layout(),      label="Chat",      tab_id="tab-chat"),
                dbc.Tab(table_tab.layout(),     label="Table",     tab_id="tab-table"),
                dbc.Tab(dashboard_tab.layout(), label="Dashboard", tab_id="tab-dashboard"),
            ],
            id="main-tabs",
            active_tab="tab-chat",
        ),

    ], fluid=True)

    # Register callbacks for each tab
    chat_tab.register_callbacks(client)
    table_tab.register_callbacks()
    dashboard_tab.register_callbacks()

    return app


def run(
    project: str,
    *,
    neuron_url: str = "http://localhost:8000",
    host: str = "0.0.0.0",
    port: int = 8050,
    debug: bool = False,
) -> None:
    """Create the app and start the Dash dev server."""
    app = create_app(project, neuron_url=neuron_url)
    app.run(host=host, port=port, debug=debug)
