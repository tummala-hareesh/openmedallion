"""neuron/models.py — Pydantic request / response models for the /query and
/feedback endpoints."""
from __future__ import annotations

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    question: str = Field(..., description="Natural-language question about the data")
    project:  str = Field(..., description="Project name — resolves the silver layer path")
    use_templates: bool = Field(
        default=False,
        description="Template-Routed Query Layer roadmap: a high-confidence match to a curated "
                    "templated:true example skips SQL generation entirely, filling params instead. "
                    "Off by default — opt-in, matches CerebrumPipeline's zero-cost-by-default flag.",
    )


class QueryResponse(BaseModel):
    answer:             str        = Field(..., description="Plain-language answer summary")
    sql:                str        = Field(..., description="DuckDB SQL that was executed")
    rows:               list[dict] = Field(..., description="Query result rows as JSON records")
    recommended_prompt: str        = Field(..., description="Canonical reproducible question")
    row_count:          int        = Field(..., description="Number of rows returned")
    columns:            list[str]  = Field(..., description="Column names in result order")
    turn_id:            str        = Field(default="", description="chat_history turn id — pass back on /feedback to set accepted in place")


class FeedbackRequest(BaseModel):
    """Body for POST /feedback (cortex thumbs up/down).

    Updates the matching chat_history turn's `accepted` field in place
    (via chat_history.update_turn_accepted) rather than writing
    harvested.jsonl/failures.jsonl directly — those are now written later,
    at session-end curation (chat_history.promote_session), reusing the
    unchanged examples/feedback.py:record_feedback(). `turn_id` comes back
    from the /query response that produced this answer.
    """

    project:   str  = Field(..., description="Project name — resolves the chat_history/ path")
    turn_id:   str  = Field(..., description="The turn_id returned by /query for this answer")
    thumbs_up: bool = Field(..., description="True = accepted, False = rejected")


class FeedbackResponse(BaseModel):
    status: str = Field(default="ok")


class ChatHistoryTurn(BaseModel):
    """One past turn from a project's chat_history/<username>.jsonl."""

    ts:                 str            = Field(..., description="UTC timestamp the turn was recorded")
    turn_id:            str            = Field(default="", description="Unique id — target for /feedback's accepted update")
    session_id:         str            = Field(default="", description="Ephemeral per-browser-session id")
    question:           str            = Field(..., description="The question that was asked")
    sql:                str            = Field(..., description="The SQL that was executed")
    answer:             str            = Field(..., description="Plain-language answer summary")
    row_count:          int            = Field(default=0, description="Number of rows returned")
    columns:            list[str]      = Field(default_factory=list, description="Result column names")
    response_generated: bool           = Field(default=True, description="False if the pipeline failed before producing SQL")
    accepted:           bool | None    = Field(default=None, description="Three-state: True/False (explicit) or None (no feedback)")
    promoted:            bool          = Field(default=False, description="True once rolled into harvested/failures.jsonl")


class ChatHistoryResponse(BaseModel):
    turns: list[ChatHistoryTurn] = Field(default_factory=list)


class SessionEndRequest(BaseModel):
    """Body for POST /session/end — the curation-promotion trigger (idle
    timeout, explicit "End Session" button, or best-effort tab-close beacon
    in cortex). Carries project/session_id in the body (not headers) because
    the tab-close path uses navigator.sendBeacon, which can't send custom
    headers."""

    project:    str = Field(..., description="Project name")
    session_id: str = Field(..., description="The session being closed")
    username:   str = Field(default="", description="Falls back to local_user if empty")


class SessionEndResponse(BaseModel):
    harvested: int = Field(default=0, description="Turns promoted into harvested.jsonl")
    failed:    int = Field(default=0, description="Turns promoted into failures.jsonl")
