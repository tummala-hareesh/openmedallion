"""neuron/models.py — Pydantic request / response models for the /query endpoint."""
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
