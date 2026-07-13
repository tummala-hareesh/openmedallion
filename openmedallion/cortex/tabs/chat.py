"""cortex/tabs/chat.py — Chat tab: message history, input, empty state,
suggested follow-up, thumbs up/down feedback (RAG roadmap Phase 3, build
order step 13).

Thumbs up/down never writes harvested.jsonl/failures.jsonl directly — cortex
never imports cerebrum/pipeline internals, only talks to neuron over HTTP
(a pre-existing convention) — so a click just calls ``client.feedback()``,
which POSTs to neuron's ``/feedback`` endpoint. ``store-chat-metadata``
(declared in ``cortex/app.py``) holds one ``{question, sql, columns,
row_count}`` entry per assistant turn so the feedback callback can look up
what to submit without re-deriving it from the rendered chat bubbles.
"""
from __future__ import annotations

from dash import MATCH, Input, Output, State, callback, ctx, dcc, html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from openmedallion.cortex.theme import (
    BORDER, FONT_MONO, FONT_UI, RED_DARK, SQL_BG, SQL_BORDER,
    SQL_TEXT, TEAL, TEAL_BG, TEAL_BORDER, TEAL_DARK,
    TEXT_DIM, TEXT_MUTED, TEXT_PRIMARY, TEXT_SECONDARY, WHITE,
)

# ── Example questions shown in the empty state ────────────────────────────────
_EXAMPLE_QUESTIONS = [
    "What were the total revenues last quarter?",
    "Which region had the highest win rate?",
    "Show monthly trends for the current year",
]


def layout() -> html.Div:
    return html.Div([

        # ── Chat history ──────────────────────────────────────────────────
        dcc.Loading(
            html.Div(
                id="chat-history",
                children=[],
                style={
                    "height": "calc(100vh - 310px)",
                    "minHeight": "200px",
                    "overflowY": "auto",
                    "marginBottom": "14px",
                    "display": "flex",
                    "flexDirection": "column",
                    "gap": "16px",
                },
            ),
            type="circle",
            color=TEAL,
        ),

        # ── Empty state (hidden once first question is asked) ─────────────
        dbc.Collapse(
            html.Div([
                html.Div(
                    "Ask anything about your data",
                    style={
                        "fontSize": "15px", "fontWeight": "600",
                        "color": TEXT_SECONDARY, "marginBottom": "8px",
                    },
                ),
                html.Div(
                    "Try one of these to get started:",
                    style={"fontSize": "12.5px", "color": TEXT_MUTED, "marginBottom": "14px"},
                ),
                html.Div([
                    html.Button(
                        q,
                        id=f"chip-q{i}",
                        n_clicks=0,
                        style={
                            "background": WHITE,
                            "border": f"1px solid {BORDER}",
                            "borderRadius": "20px",
                            "padding": "7px 16px",
                            "fontSize": "12.5px",
                            "color": TEXT_DIM,
                            "cursor": "pointer",
                            "fontFamily": FONT_UI,
                            "fontWeight": "500",
                            "marginRight": "8px",
                            "marginBottom": "8px",
                            "transition": "border-color 0.15s, color 0.15s",
                        },
                    )
                    for i, q in enumerate(_EXAMPLE_QUESTIONS)
                ], style={"display": "flex", "flexWrap": "wrap"}),
            ], style={"padding": "24px 0 12px"}),
            id="chat-empty-collapse",
            is_open=True,
        ),

        # ── Suggested follow-up (appears after each AI response) ──────────
        html.Div(
            id="chat-suggested",
            style={"display": "none", "marginBottom": "12px", "paddingLeft": "2px"},
        ),

        # ── Input row ─────────────────────────────────────────────────────
        html.Div([
            dbc.Textarea(
                id="chat-input",
                placeholder="Ask a question about your data…",
                style={
                    "resize": "none",
                    "border": f"1px solid {BORDER}",
                    "borderRadius": "8px",
                    "padding": "10px 14px",
                    "fontFamily": FONT_UI,
                    "fontSize": "13.5px",
                    "color": TEXT_PRIMARY,
                    "background": WHITE,
                    "flex": "1",
                    "outline": "none",
                },
                rows=3,
            ),
            dbc.Button(
                "Ask",
                id="chat-ask-btn",
                style={
                    "background": TEAL,
                    "border": "none",
                    "borderRadius": "8px",
                    "padding": "0 28px",
                    "fontSize": "14px",
                    "fontWeight": "600",
                    "cursor": "pointer",
                    "fontFamily": FONT_UI,
                    "color": WHITE,
                    "flexShrink": "0",
                    "alignSelf": "stretch",
                    "minWidth": "80px",
                },
            ),
        ], style={"display": "flex", "gap": "12px", "alignItems": "flex-end"}),

    ], style={"padding": "24px 28px", "display": "flex", "flexDirection": "column", "height": "100%"})


def register_callbacks(client) -> None:

    @callback(
        Output("chat-history",         "children"),
        Output("chat-input",           "value"),
        Output("store-last-question",  "data"),
        Output("store-last-sql",       "data"),
        Output("store-query-results",  "data"),
        Output("store-recommended",    "data"),
        Output("chat-empty-collapse",  "is_open"),
        Output("chat-suggested",       "children"),
        Output("chat-suggested",       "style"),
        Output("store-chat-metadata",  "data"),
        Input("chat-ask-btn",          "n_clicks"),
        State("chat-input",            "value"),
        State("chat-history",          "children"),
        State("store-project",         "data"),
        State("store-chat-metadata",   "data"),
        prevent_initial_call=True,
    )
    def handle_ask(n_clicks, question, history, project, chat_metadata):
        chat_metadata = list(chat_metadata or [])
        if not question or not question.strip():
            return history, question, None, None, None, None, True, [], {"display": "none"}, chat_metadata

        history = list(history or [])
        history.append(_user_bubble(question.strip()))

        try:
            result = client.ask(question.strip(), project or "")
            index = len(chat_metadata)
            history.append(_assistant_bubble(result.answer, result.sql, index))
            chat_metadata.append({
                "question": question.strip(),
                "sql":      result.sql,
                "columns":  result.columns,
                "row_count": result.row_count,
            })
            suggested_children, suggested_style = _suggested_ui(result.recommended_prompt)
            return (
                history, "",
                result.answer, result.sql,
                result.rows, result.recommended_prompt,
                False,
                suggested_children, suggested_style,
                chat_metadata,
            )
        except Exception as exc:
            history.append(_error_bubble(str(exc)))
            return history, question, None, None, None, None, False, [], {"display": "none"}, chat_metadata

    @callback(
        Output({"type": "thumb-up",     "index": MATCH}, "disabled"),
        Output({"type": "thumb-down",   "index": MATCH}, "disabled"),
        Output({"type": "thumb-status", "index": MATCH}, "children"),
        Input({"type": "thumb-up",      "index": MATCH}, "n_clicks"),
        Input({"type": "thumb-down",    "index": MATCH}, "n_clicks"),
        State("store-chat-metadata",    "data"),
        State("store-project",         "data"),
        prevent_initial_call=True,
    )
    def handle_feedback(up_clicks, down_clicks, chat_metadata, project):
        triggered = ctx.triggered_id
        if not triggered or not (up_clicks or down_clicks):
            raise PreventUpdate

        index = triggered["index"]
        chat_metadata = chat_metadata or []
        if index >= len(chat_metadata):
            raise PreventUpdate
        entry = chat_metadata[index]
        thumbs_up = triggered["type"] == "thumb-up"

        try:
            client.feedback(
                entry["question"], entry["sql"],
                entry.get("columns", []), entry.get("row_count", 0),
                thumbs_up, project or "",
            )
            status = "Thanks for the feedback!"
        except Exception:
            status = "Could not record feedback."

        return True, True, status

    @callback(
        Output("chat-input", "value", allow_duplicate=True),
        Input("chip-q0", "n_clicks"),
        Input("chip-q1", "n_clicks"),
        Input("chip-q2", "n_clicks"),
        prevent_initial_call=True,
    )
    def fill_from_chip(*_):
        from dash import ctx
        chip_map = {f"chip-q{i}": q for i, q in enumerate(_EXAMPLE_QUESTIONS)}
        return chip_map.get(ctx.triggered_id, "")


# ── Bubble helpers ─────────────────────────────────────────────────────────────

def _user_bubble(text: str) -> html.Div:
    return html.Div(
        html.Div(
            text,
            style={
                "background": TEAL_BG,
                "color": "#064E3B",
                "border": f"1px solid {TEAL_BORDER}",
                "borderRadius": "12px 12px 2px 12px",
                "padding": "10px 16px",
                "display": "inline-block",
                "maxWidth": "66%",
                "fontSize": "13.5px",
                "lineHeight": "1.5",
                "fontFamily": FONT_UI,
            },
        ),
        style={"textAlign": "right"},
    )


def _assistant_bubble(answer: str, sql: str, index: int) -> html.Div:
    children: list = [
        html.Div(
            answer,
            style={
                "background": WHITE,
                "border": f"1px solid {BORDER}",
                "borderRadius": "2px 12px 12px 12px",
                "padding": "12px 16px",
                "maxWidth": "74%",
                "fontSize": "13.5px",
                "lineHeight": "1.65",
                "color": TEXT_SECONDARY,
                "boxShadow": "0 1px 4px rgba(0,0,0,0.05)",
                "fontFamily": FONT_UI,
            },
        ),
    ]

    # SQL block — collapsed by default via html.Details (no JS/callback needed)
    if sql and sql.strip():
        children.append(
            html.Details([
                html.Summary(
                    "Show SQL",
                    style={
                        "cursor": "pointer",
                        "fontSize": "11.5px",
                        "fontWeight": "600",
                        "color": TEAL_DARK,
                        "userSelect": "none",
                        "padding": "4px 0",
                        "listStyle": "none",
                        "display": "flex",
                        "alignItems": "center",
                        "gap": "5px",
                    },
                ),
                html.Pre(
                    sql,
                    style={
                        "background": SQL_BG,
                        "border": f"1px solid {SQL_BORDER}",
                        "fontFamily": FONT_MONO,
                        "fontSize": "11.5px",
                        "color": SQL_TEXT,
                        "padding": "12px 16px",
                        "borderRadius": "6px",
                        "overflowX": "auto",
                        "lineHeight": "1.7",
                        "margin": "6px 0 0 0",
                    },
                ),
            ], style={"maxWidth": "74%", "marginTop": "6px"}),
        )

    children.append(_thumbs_row(index))

    return html.Div(children, style={"display": "flex", "flexDirection": "column", "gap": "0"})


def _thumbs_row(index: int) -> html.Div:
    _btn_style = {
        "background": "transparent",
        "border": "none",
        "cursor": "pointer",
        "fontSize": "14px",
        "padding": "2px 4px",
        "opacity": "0.55",
        "transition": "opacity 0.15s",
    }
    return html.Div([
        html.Button("👍", id={"type": "thumb-up",   "index": index}, n_clicks=0, style=_btn_style),
        html.Button("👎", id={"type": "thumb-down", "index": index}, n_clicks=0, style=_btn_style),
        html.Span(
            id={"type": "thumb-status", "index": index},
            style={"fontSize": "11px", "color": TEXT_MUTED, "marginLeft": "4px", "fontFamily": FONT_UI},
        ),
    ], style={"display": "flex", "alignItems": "center", "gap": "2px", "marginTop": "6px"})


def _error_bubble(msg: str) -> html.Div:
    return html.Div(
        html.Div(
            f"Error: {msg}",
            style={
                "background": "#FEF2F2",
                "border": "1px solid #FECACA",
                "borderRadius": "8px",
                "padding": "10px 16px",
                "color": RED_DARK,
                "maxWidth": "74%",
                "fontSize": "13.5px",
                "fontFamily": FONT_UI,
            },
        ),
    )


def _suggested_ui(recommended: str) -> tuple[list, dict]:
    if not recommended or not recommended.strip():
        return [], {"display": "none"}
    children = [
        html.Span(
            "→ Suggested: ",
            style={"fontSize": "11.5px", "fontWeight": "600", "color": TEXT_MUTED, "flexShrink": "0"},
        ),
        html.Span(
            recommended,
            style={"fontSize": "12px", "color": TEAL_DARK, "fontStyle": "italic"},
        ),
    ]
    style = {
        "display": "flex",
        "alignItems": "flex-start",
        "gap": "6px",
        "paddingLeft": "4px",
        "flexWrap": "wrap",
    }
    return children, style
