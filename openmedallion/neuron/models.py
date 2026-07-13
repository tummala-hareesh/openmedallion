"""neuron/models.py — Pydantic request / response models for the /query and
/feedback endpoints."""
from __future__ import annotations

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    question: str = Field(..., description="Natural-language question about the data")
    project:  str = Field(..., description="Project name — resolves the silver layer path")


class QueryResponse(BaseModel):
    answer:             str        = Field(..., description="Plain-language answer summary")
    sql:                str        = Field(..., description="DuckDB SQL that was executed")
    rows:               list[dict] = Field(..., description="Query result rows as JSON records")
    recommended_prompt: str        = Field(..., description="Canonical reproducible question")
    row_count:          int        = Field(..., description="Number of rows returned")
    columns:            list[str]  = Field(..., description="Column names in result order")


class FeedbackRequest(BaseModel):
    """Body for POST /feedback (build order step 13 — cortex thumbs up/down).

    cortex never writes harvested.jsonl/failures.jsonl directly — it never
    imports cerebrum/pipeline internals, only talks to neuron over HTTP.
    This request carries everything needed to reconstruct the feedback
    entry without cortex touching the filesystem itself.
    """

    project:   str       = Field(..., description="Project name — resolves the examples/ path")
    question:  str       = Field(..., description="The question that was asked")
    sql:       str       = Field(..., description="The SQL that was executed")
    columns:   list[str] = Field(default_factory=list, description="Result column names")
    row_count: int       = Field(default=0, description="Number of rows returned")
    thumbs_up: bool      = Field(..., description="True = harvested.jsonl (candidate), False = failures.jsonl")


class FeedbackResponse(BaseModel):
    status: str = Field(default="ok")
