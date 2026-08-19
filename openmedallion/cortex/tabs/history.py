"""cortex/tabs/history.py — History tab: lists the current person's past
turns (per-person chat history, neuron's GET /history), each with a "↻ reuse"
button that reloads it into the chat tab.

UI/audit only — history is never fed back into the LLM prompt (see
openmedallion/neuron/chat_history.py). No cortex UI test file exists in this
repo yet (a pre-existing gap noted in CLAUDE.md) — verified via a real
browser session (/browse) instead of pytest, same as chat.py's thumbs
up/down feature.
"""
from __future__ import annotations

from dash import ALL, Input, Output, State, callback, ctx, dcc, html
from dash.exceptions import PreventUpdate

from openmedallion.cortex.theme import (
    BORDER, FONT_MONO, FONT_UI, SQL_BG, SQL_BORDER, SQL_TEXT,
    TEAL, TEAL_BG, TEAL_BORDER, TEXT_DIM, TEXT_MUTED, TEXT_PRIMARY, WHITE,
)


def layout() -> html.Div:
    return html.Div([

        html.Div([
            html.Span(
                "YOUR PAST QUESTIONS",
                style={
                    "fontSize": "11px", "fontWeight": "600",
                    "color": TEXT_MUTED, "letterSpacing": "0.7px",
                    "textTransform": "uppercase",
                },
            ),
            html.Button(
                "↻ Refresh",
                id="history-refresh-btn",
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
        ], style={
            "display": "flex", "justifyContent": "space-between",
            "alignItems": "center", "marginBottom": "14px",
        }),

        dcc.Loading(
            html.Div(id="history-list", children=[]),
            type="circle",
            color=TEAL,
        ),
        dcc.Store(id="store-history-turns", data=[]),

    ], style={"padding": "24px 28px"})


def _turn_card(turn: dict, index: int) -> html.Div:
    return html.Div([
        html.Div([
            html.Span(
                turn.get("question", ""),
                style={"fontSize": "13.5px", "fontWeight": "600", "color": TEXT_PRIMARY},
            ),
            html.Span(
                turn.get("ts", ""),
                style={"fontSize": "11px", "color": TEXT_MUTED, "marginLeft": "10px"},
            ),
        ], style={"marginBottom": "8px"}),

        html.Pre(
            turn.get("sql", ""),
            style={
                "background": SQL_BG, "border": f"1px solid {SQL_BORDER}",
                "fontFamily": FONT_MONO, "fontSize": "11.5px", "color": SQL_TEXT,
                "padding": "10px 12px", "borderRadius": "6px",
                "overflowX": "auto", "margin": "0 0 8px", "whiteSpace": "pre-wrap",
            },
        ),

        html.Div([
            html.Span(
                f"{turn.get('row_count', 0)} row(s)",
                style={"fontSize": "11.5px", "color": TEXT_DIM},
            ),
            html.Button(
                "↻ Reuse",
                id={"type": "history-reuse-btn", "index": index},
                n_clicks=0,
                style={
                    "background": TEAL_BG, "border": f"1px solid {TEAL_BORDER}",
                    "color": TEAL, "fontSize": "11.5px", "fontWeight": "600",
                    "borderRadius": "5px", "padding": "4px 10px",
                    "cursor": "pointer", "fontFamily": FONT_UI, "outline": "none",
                },
            ),
        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),

    ], style={
        "background": WHITE, "border": f"1px solid {BORDER}",
        "borderRadius": "8px", "padding": "14px 16px", "marginBottom": "10px",
    })


def _empty_state() -> html.Div:
    return html.Div(
        "No past questions yet — ask something in the Chat tab.",
        style={"color": TEXT_MUTED, "fontSize": "13px", "padding": "20px 0"},
    )


def register_callbacks(client) -> None:

    @callback(
        Output("history-list",       "children"),
        Output("store-history-turns", "data"),
        Input("history-refresh-btn", "n_clicks"),
        Input("store-active-tab",    "data"),
        State("store-project",       "data"),
        State("store-username",      "data"),
    )
    def load_history(_n_clicks, active_tab, project, username):
        if active_tab != "history":
            raise PreventUpdate
        try:
            turns = client.history(project or "", username=username)
        except Exception:
            return html.Div(
                "Could not load history.",
                style={"color": TEXT_MUTED, "fontSize": "13px"},
            ), []
        if not turns:
            return _empty_state(), []
        return [_turn_card(t, i) for i, t in enumerate(turns)], turns

    @callback(
        Output("chat-input",       "value", allow_duplicate=True),
        Output("store-active-tab", "data",  allow_duplicate=True),
        Input({"type": "history-reuse-btn", "index": ALL}, "n_clicks"),
        State("store-history-turns", "data"),
        prevent_initial_call=True,
    )
    def reuse_question(n_clicks_list, turns):
        triggered = ctx.triggered_id
        if not triggered or not any(n_clicks_list):
            raise PreventUpdate
        index = triggered["index"]
        turns = turns or []
        if index >= len(turns):
            raise PreventUpdate
        return turns[index].get("question", ""), "chat"
