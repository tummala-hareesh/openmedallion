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
store-active-tab      Currently visible tab: "chat" | "table" | "dashboard" | "history"
store-chat-metadata   List[dict] — one {question, sql, columns, row_count}
                      entry per assistant turn, appended in chat.py's
                      handle_ask(); read by the thumbs up/down callback to
                      know what to submit via client.feedback() without
                      re-deriving it from the rendered chat bubbles.
store-username        Display name for per-person chat history (no real
                      auth — a label). storage_type="local": persists across
                      page reloads in the browser's localStorage. Sent as an
                      X-Medallion-User header on every client.ask()/
                      feedback()/history() call. Defaults to "local_user"
                      when never set.
store-session-id      Ephemeral per-browser-session id (crypto.randomUUID(),
                      generated client-side once per tab via a clientside
                      callback — Python can't generate a per-tab value).
                      storage_type="session": cleared automatically when the
                      tab/browser session ends. Additive to store-username,
                      never a replacement. Sent as X-Medallion-Session.
                      Closing a session (button / idle timeout / tab-close
                      beacon) triggers curation promotion via
                      client.end_session() — NOT a data-sync call, since
                      chat_history is already written server-side in real
                      time on every /query.
store-last-activity   Timestamp (ms since epoch) of the last chat-ask-btn
                      click — compared against a 5-minute idle threshold by
                      a dcc.Interval to auto-trigger session end.
"""
from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html
from dash.exceptions import PreventUpdate

from openmedallion.config            import settings
from openmedallion.cortex.client     import MockClient, NeuronClient
from openmedallion.cortex.tabs       import chat as chat_tab
from openmedallion.cortex.tabs       import dashboard as dashboard_tab
from openmedallion.cortex.tabs       import history as history_tab
from openmedallion.cortex.tabs       import table as table_tab
from openmedallion.cortex.theme      import (
    BORDER, FONT_UI, MAIN_BG, SIDEBAR_BG, SIDEBAR_LINE,
    TEAL, TEXT_MUTED, TEXT_PRIMARY, WHITE,
    nav_style, panel_style,
)

_TABS = ["chat", "table", "dashboard", "history"]
_TAB_LABELS = {"chat": "Chat", "table": "Table", "dashboard": "Dashboard", "history": "History"}

# Bootstrap Icons names for each nav item (rendered as <i> tags)
_TAB_ICONS = {
    "chat":      "bi-chat-text",
    "table":     "bi-table",
    "dashboard": "bi-bar-chart-line",
    "history":   "bi-clock-history",
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
        dcc.Store(id="store-username", storage_type="local"),
        dcc.Store(id="store-session-id", storage_type="session"),
        dcc.Store(id="store-last-activity", data=0),
        dcc.Interval(id="idle-check-interval", interval=60_000),

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

                # Display name — labels per-person chat history (no real
                # auth; see cortex/tabs/history.py + neuron/chat_history.py).
                # Persisted client-side via store-username (storage_type="local").
                html.Div([
                    html.Label(
                        "YOUR NAME",
                        style={
                            "fontSize": "10px", "fontWeight": "600",
                            "color": TEXT_MUTED, "letterSpacing": "0.6px",
                            "display": "block", "marginBottom": "5px",
                        },
                    ),
                    dcc.Input(
                        id="input-username",
                        type="text",
                        placeholder="local_user",
                        debounce=True,
                        style={
                            "width": "100%", "background": "rgba(255,255,255,0.06)",
                            "border": f"1px solid {SIDEBAR_LINE}", "borderRadius": "5px",
                            "color": WHITE, "fontSize": "12.5px", "padding": "6px 8px",
                            "fontFamily": FONT_UI, "outline": "none",
                        },
                    ),
                ], style={
                    "padding": "14px 20px",
                    "borderBottom": f"1px solid {SIDEBAR_LINE}",
                }),

                # End Session — curation-promotion trigger (not a data sync;
                # chat_history is already written server-side in real time).
                # Also fires automatically on 5min idle, or best-effort on
                # tab close (see _register_session_end_callbacks()).
                html.Div([
                    html.Button(
                        "End Session",
                        id="end-session-btn",
                        n_clicks=0,
                        style={
                            "width": "100%", "background": "transparent",
                            "border": f"1px solid {SIDEBAR_LINE}", "borderRadius": "5px",
                            "color": TEXT_MUTED, "fontSize": "11.5px", "fontWeight": "600",
                            "padding": "6px 8px", "cursor": "pointer",
                            "fontFamily": FONT_UI, "outline": "none",
                        },
                    ),
                    html.Span(id="end-session-status", style={
                        "display": "block", "fontSize": "10.5px",
                        "color": TEXT_MUTED, "marginTop": "5px",
                    }),
                ], style={
                    "padding": "10px 20px",
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
                    html.Div(history_tab.layout(),   id="content-history",   style={"display": "none"}),
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
    _register_username_callbacks()
    _register_session_id_callback()
    _register_session_end_callbacks(app, client, neuron_url)
    chat_tab.register_callbacks(client)
    table_tab.register_callbacks()
    dashboard_tab.register_callbacks()
    history_tab.register_callbacks(client)

    return app


def _register_username_callbacks() -> None:
    @callback(
        Output("store-username", "data"),
        Input("input-username",  "value"),
        prevent_initial_call=True,
    )
    def _save_username(value):
        return (value or "").strip() or "local_user"

    @callback(
        Output("input-username", "value"),
        Input("store-username",  "data"),
    )
    def _restore_username(stored):
        return stored or ""


def _register_session_id_callback() -> None:
    """Generate store-session-id once per browser session (crypto.randomUUID()
    — Python can't produce a per-tab value; storage_type="session" means it's
    cleared automatically when the tab/browser session ends, so this only
    ever fires once per real session)."""
    dash.clientside_callback(
        """
        function(_project, existing) {
            if (existing) { return existing; }
            return crypto.randomUUID();
        }
        """,
        Output("store-session-id", "data"),
        Input("store-project", "data"),
        State("store-session-id", "data"),
    )


def _register_session_end_callbacks(app: dash.Dash, client, neuron_url: str) -> None:
    """Session-end is a curation-promotion trigger, NOT a data sync —
    chat_history is already written server-side in real time on every
    /query. Three ways to fire it:
      1. Explicit "End Session" button.
      2. Idle timeout (5 min, checked every 60s via dcc.Interval).
      3. Best-effort tab-close (navigator.sendBeacon — can't carry an
         Authorization header, so this path only works when neuron has no
         MEDALLION_API_KEY configured; a known, documented limitation).
    """
    _IDLE_THRESHOLD_MS = 5 * 60 * 1000

    # 1 & 2: server-side, since these call client.end_session() (the Python
    # NeuronClient/MockClient), not a raw browser fetch.
    @callback(
        Output("end-session-status", "children"),
        Output("store-last-activity", "data", allow_duplicate=True),
        Input("idle-check-interval", "n_intervals"),
        Input("end-session-btn",     "n_clicks"),
        State("store-last-activity", "data"),
        State("store-project",       "data"),
        State("store-session-id",    "data"),
        State("store-username",      "data"),
        prevent_initial_call=True,
    )
    def _maybe_end_session(_n_intervals, _n_clicks, last_activity, project, session_id, username):
        from dash import ctx
        import time as _time

        triggered_id = ctx.triggered_id
        is_explicit = triggered_id == "end-session-btn"
        idle_ms = (_time.time() * 1000) - (last_activity or 0)
        is_idle = (
            triggered_id == "idle-check-interval"
            and last_activity
            and idle_ms >= _IDLE_THRESHOLD_MS
        )
        if not (is_explicit or is_idle) or not session_id:
            raise PreventUpdate

        try:
            counts = client.end_session(project or "", session_id, username=username)
            status = f"Session closed — {counts.get('harvested', 0)} learned, {counts.get('failed', 0)} noted."
        except Exception:
            status = "Could not close session."

        return status, 0  # reset last-activity so the idle timer doesn't refire immediately

    # Track last activity — client-side, trivial JS, avoids a server round
    # trip just to stamp a click timestamp.
    dash.clientside_callback(
        "function(n_clicks) { return Date.now(); }",
        Output("store-last-activity", "data"),
        Input("chat-ask-btn", "n_clicks"),
        prevent_initial_call=True,
    )

    # 3: tab-close beacon — inherently client-only (the page is unloading,
    # there's no time for a Dash server round trip). Uses a raw fetch-free
    # navigator.sendBeacon straight to neuron, bypassing the Python client
    # entirely. No cortex UI test covers this (matches the existing
    # convention — no cortex UI test file exists for any tab); verify via a
    # real browser session.
    dash.clientside_callback(
        f"""
        function(session_id, project, username) {{
            if (!window._omBeaconBound) {{
                window._omBeaconBound = true;
                window.addEventListener('beforeunload', function() {{
                    if (!window._omSessionId) {{ return; }}
                    var payload = JSON.stringify({{
                        project: window._omProject,
                        session_id: window._omSessionId,
                        username: window._omUsername || ''
                    }});
                    navigator.sendBeacon(
                        {neuron_url!r} + '/session/end',
                        new Blob([payload], {{type: 'application/json'}})
                    );
                }});
            }}
            window._omSessionId = session_id;
            window._omProject = project;
            window._omUsername = username;
            return window.dash_clientside.no_update;
        }}
        """,
        Output("end-session-status", "children", allow_duplicate=True),
        Input("store-session-id", "data"),
        Input("store-project",    "data"),
        Input("store-username",   "data"),
        prevent_initial_call=True,
    )


def _register_nav_callbacks() -> None:
    @callback(
        Output("store-active-tab", "data"),
        Output("topbar-title",     "children"),
        Input("nav-btn-chat",      "n_clicks"),
        Input("nav-btn-table",     "n_clicks"),
        Input("nav-btn-dashboard", "n_clicks"),
        Input("nav-btn-history",   "n_clicks"),
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
        Output("content-history",   "style"),
        Output("nav-btn-chat",      "style"),
        Output("nav-btn-table",     "style"),
        Output("nav-btn-dashboard", "style"),
        Output("nav-btn-history",   "style"),
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
