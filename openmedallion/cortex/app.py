"""cortex/app.py — Plotly Dash application shell: sidebar nav + tab content routing.

Layout
------
Sidebar (220 px dark)   Project brand · Nav (Chat / Table / Dashboard) · LLM status
Main                    Topbar (section title) · Content panel (one tab visible at a time)

Shared dcc.Store ids
--------------------
store-last-question   Most recent question text
store-last-sql        SQL that was last executed
store-query-results   List[dict] rows from the last query
store-recommended     Canonical recommended prompt string
store-project         Project name (set at startup, read-only)
store-active-tab      Currently visible tab: "chat" | "table" | "dashboard"
store-chat-metadata   List[dict] — one {question, sql, columns, row_count}
                      entry per assistant turn, appended in chat.py's
                      handle_ask(); read by the thumbs up/down callback to
                      know what to submit via client.feedback() without
                      re-deriving it from the rendered chat bubbles.
"""
from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

from openmedallion.config            import settings
from openmedallion.cortex.client     import MockClient, NeuronClient
from openmedallion.cortex.tabs       import chat as chat_tab
from openmedallion.cortex.tabs       import dashboard as dashboard_tab
from openmedallion.cortex.tabs       import table as table_tab
from openmedallion.cortex.theme      import (
    BORDER, FONT_UI, MAIN_BG, SIDEBAR_BG, SIDEBAR_LINE,
    TEAL, TEXT_MUTED, TEXT_PRIMARY, WHITE,
    nav_style, panel_style,
)

_TABS = ["chat", "table", "dashboard"]
_TAB_LABELS = {"chat": "Chat", "table": "Table", "dashboard": "Dashboard"}

# Bootstrap Icons names for each nav item (rendered as <i> tags)
_TAB_ICONS = {
    "chat":      "bi-chat-text",
    "table":     "bi-table",
    "dashboard": "bi-bar-chart-line",
}


def _nav_button(tab: str) -> html.Button:
    return html.Button(
        [
            html.I(className=f"bi {_TAB_ICONS[tab]}", style={"fontSize": "15px", "width": "20px"}),
            html.Span(_TAB_LABELS[tab], style={"marginLeft": "10px"}),
        ],
        id=f"nav-btn-{tab}",
        n_clicks=0,
        style=nav_style(active=(tab == "chat")),
    )


def create_app(
    project: str,
    neuron_url: str = "http://localhost:8000",
) -> dash.Dash:
    client = MockClient() if settings.USE_MOCK_CLIENT else NeuronClient(base_url=neuron_url)

    model_label = getattr(settings, "LLM_MODEL", "llama3.2")

    app = dash.Dash(
        __name__,
        external_stylesheets=[
            dbc.themes.BOOTSTRAP,
            dbc.icons.BOOTSTRAP,
        ],
        suppress_callback_exceptions=True,
    )
    app.title = f"openmedallion · {project}"

    app.layout = html.Div([

        # ── Shared state ──────────────────────────────────────────────────
        dcc.Store(id="store-last-question"),
        dcc.Store(id="store-last-sql"),
        dcc.Store(id="store-query-results"),
        dcc.Store(id="store-recommended"),
        dcc.Store(id="store-project",    data=project),
        dcc.Store(id="store-active-tab", data="chat"),
        dcc.Store(id="store-chat-metadata", data=[]),

        # ── App shell: sidebar + main ─────────────────────────────────────
        html.Div([

            # ── Sidebar ───────────────────────────────────────────────────
            html.Aside([

                # Brand
                html.Div([
                    html.Div(
                        "openmedallion",
                        style={
                            "color": WHITE, "fontSize": "15px", "fontWeight": "700",
                            "letterSpacing": "-0.4px",
                        },
                    ),
                    html.Div(
                        project,
                        style={"color": TEAL, "fontSize": "11.5px", "fontWeight": "500", "marginTop": "2px"},
                    ),
                ], style={
                    "padding": "20px 20px 18px",
                    "borderBottom": f"1px solid {SIDEBAR_LINE}",
                }),

                # Navigation
                html.Nav(
                    [_nav_button(t) for t in _TABS],
                    style={"padding": "12px 0", "flex": "1"},
                ),

                # Status footer
                html.Div([
                    html.Span(
                        "●",
                        style={"color": TEAL, "marginRight": "7px", "fontSize": "9px",
                               "verticalAlign": "middle"},
                    ),
                    html.Span(
                        f"neuron · {model_label}",
                        style={"color": TEXT_MUTED, "fontSize": "11.5px"},
                    ),
                ], style={
                    "padding": "14px 20px",
                    "borderTop": f"1px solid {SIDEBAR_LINE}",
                }),

            ], style={
                "width": "220px",
                "background": SIDEBAR_BG,
                "display": "flex",
                "flexDirection": "column",
                "flexShrink": "0",
                "height": "100vh",
                "overflow": "hidden",
            }),

            # ── Main content ──────────────────────────────────────────────
            html.Div([

                # Topbar
                html.Div([
                    html.Span(
                        id="topbar-title",
                        children="Chat",
                        style={
                            "fontSize": "14px", "fontWeight": "600",
                            "color": TEXT_PRIMARY,
                        },
                    ),
                ], style={
                    "background": WHITE,
                    "borderBottom": f"1px solid {BORDER}",
                    "padding": "0 28px",
                    "height": "52px",
                    "display": "flex",
                    "alignItems": "center",
                    "flexShrink": "0",
                }),

                # Tab content panels (all in DOM; show/hide via callback)
                html.Div([
                    html.Div(chat_tab.layout(),      id="content-chat",      style={"height": "100%"}),
                    html.Div(table_tab.layout(),     id="content-table",     style={"display": "none"}),
                    html.Div(dashboard_tab.layout(), id="content-dashboard", style={"display": "none"}),
                ], style={"flex": "1", "overflowY": "auto", "background": MAIN_BG}),

            ], style={
                "flex": "1",
                "display": "flex",
                "flexDirection": "column",
                "overflow": "hidden",
                "minWidth": "0",
            }),

        ], style={
            "display": "flex",
            "height": "100vh",
            "overflow": "hidden",
            "fontFamily": FONT_UI,
        }),

    ], style={"margin": "0", "padding": "0"})

    # Register tab callbacks, then per-tab callbacks
    _register_nav_callbacks()
    chat_tab.register_callbacks(client)
    table_tab.register_callbacks()
    dashboard_tab.register_callbacks()

    return app


def _register_nav_callbacks() -> None:
    @callback(
        Output("store-active-tab", "data"),
        Output("topbar-title",     "children"),
        Input("nav-btn-chat",      "n_clicks"),
        Input("nav-btn-table",     "n_clicks"),
        Input("nav-btn-dashboard", "n_clicks"),
        prevent_initial_call=True,
    )
    def _set_active_tab(*_):
        from dash import ctx
        tab = ctx.triggered_id.replace("nav-btn-", "")
        return tab, _TAB_LABELS.get(tab, tab.capitalize())

    @callback(
        Output("content-chat",      "style"),
        Output("content-table",     "style"),
        Output("content-dashboard", "style"),
        Output("nav-btn-chat",      "style"),
        Output("nav-btn-table",     "style"),
        Output("nav-btn-dashboard", "style"),
        Input("store-active-tab",   "data"),
    )
    def _switch_display(active):
        content = [panel_style(t == active) for t in _TABS]
        nav     = [nav_style(t == active)   for t in _TABS]
        return *content, *nav


def run(
    project: str,
    *,
    neuron_url: str = "http://localhost:8000",
    host: str = "0.0.0.0",
    port: int = 8050,
    debug: bool = False,
) -> None:
    app = create_app(project, neuron_url=neuron_url)
    app.run(host=host, port=port, debug=debug)
