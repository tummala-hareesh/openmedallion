"""cortex/tabs/chat.py — Chat tab: scrollable history + multi-line input + Ask button."""
from __future__ import annotations

from dash import Input, Output, State, callback, dcc, html
import dash_bootstrap_components as dbc


def layout() -> html.Div:
    return html.Div([
        # Scrollable message history — dcc.Loading shows a spinner while the
        # handle_ask callback is running (chat-history is a callback output)
        dcc.Loading(
            html.Div(
                id="chat-history",
                style={
                    "height": "52vh",
                    "overflowY": "auto",
                    "border": "1px solid #dee2e6",
                    "borderRadius": "8px",
                    "padding": "16px",
                    "background": "#f8f9fa",
                    "marginBottom": "14px",
                },
            ),
            type="circle",
            color="#1a73e8",
        ),
        # Input row
        dbc.Row([
            dbc.Col(
                dbc.Textarea(
                    id="chat-input",
                    placeholder="Ask a question about your data…",
                    style={"resize": "none"},
                    rows=3,
                ),
                width=10,
            ),
            dbc.Col(
                dbc.Button(
                    "Ask", id="chat-ask-btn", color="primary",
                    className="w-100 h-100",
                ),
                width=2,
            ),
        ]),
    ], style={"padding": "20px"})


def register_callbacks(client) -> None:
    @callback(
        Output("chat-history",         "children"),
        Output("chat-input",           "value"),
        Output("store-last-question",  "data"),
        Output("store-last-sql",       "data"),
        Output("store-query-results",  "data"),
        Output("store-recommended",    "data"),
        Input("chat-ask-btn",          "n_clicks"),
        State("chat-input",            "value"),
        State("chat-history",          "children"),
        State("store-project",         "data"),
        prevent_initial_call=True,
    )
    def handle_ask(n_clicks, question, history, project):
        if not question or not question.strip():
            return history, question, None, None, None, None

        history = history or []
        history.append(_user_bubble(question.strip()))

        try:
            result = client.ask(question.strip(), project or "")
            history.append(_assistant_bubble(result.answer, result.sql))
            return (
                history, "",
                result.answer, result.sql,
                result.rows, result.recommended_prompt,
            )
        except Exception as exc:
            history.append(_error_bubble(str(exc)))
            return history, question, None, None, None, None


# ── bubble helpers ────────────────────────────────────────────────────────────

def _user_bubble(text: str) -> html.Div:
    return html.Div(
        html.Div(
            text,
            style={
                "background": "#d1ecf1", "borderRadius": "8px",
                "padding": "10px 14px", "display": "inline-block",
                "maxWidth": "80%",
            },
        ),
        style={"textAlign": "right", "marginBottom": "10px"},
    )


def _assistant_bubble(answer: str, sql: str) -> html.Div:
    return html.Div([
        html.Div(
            answer,
            style={
                "background": "#ffffff", "border": "1px solid #dee2e6",
                "borderRadius": "8px", "padding": "10px 14px",
                "marginBottom": "4px", "maxWidth": "82%",
            },
        ),
        html.Pre(
            sql,
            style={
                "fontSize": "11px", "color": "#6c757d",
                "background": "#f1f3f4", "borderRadius": "4px",
                "padding": "6px 10px", "overflowX": "auto",
                "maxWidth": "82%",
            },
        ),
    ], style={"marginBottom": "14px"})


def _error_bubble(msg: str) -> html.Div:
    return html.Div(
        html.Div(
            f"Error: {msg}",
            style={
                "background": "#f8d7da", "borderRadius": "8px",
                "padding": "10px 14px", "color": "#721c24",
                "maxWidth": "80%",
            },
        ),
        style={"marginBottom": "10px"},
    )
