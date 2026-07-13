"""neuron/server.py — FastAPI app exposing the cerebrum pipeline over HTTP.

Endpoints
---------
POST /query
    Body:    QueryRequest  { question, project }
    Returns: QueryResponse { answer, sql, rows, recommended_prompt, row_count, columns }

POST /feedback
    Body:    FeedbackRequest  { project, question, sql, columns, row_count, thumbs_up }
    Returns: FeedbackResponse { status }
    Records cortex thumbs up/down (RAG roadmap Phase 3, build order step 13)
    — the single integration point that writes harvested.jsonl/failures.jsonl,
    since cortex itself never touches project/pipeline data directly.

GET /health
    Returns: { "status": "ok" }

Environment variables
---------------------
MEDALLION_PROJECTS_ROOT   Parent directory of per-project folders (default: ".")
MEDALLION_LLM_PROVIDER    LLM backend: ollama | openrouter | openai | custom (default: "ollama")
MEDALLION_LLM_MODEL       Model identifier (default: "llama3.2")
MEDALLION_LLM_API_KEY     API key for non-Ollama providers
MEDALLION_LLM_BASE_URL    Override provider endpoint URL
MEDALLION_OLLAMA_URL      Ollama base URL (default: "http://localhost:11434")
MEDALLION_API_KEY         If set, enables bearer-token auth
MEDALLION_RATE_LIMIT      Max requests/min per IP (default: 60)
MEDALLION_AUDIT_LOG       JSONL audit log path (default: "medallion_audit.jsonl")
"""
from __future__ import annotations

from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from openmedallion.cerebrum.pipeline import CerebrumPipeline
from openmedallion.config            import settings
from openmedallion.neuron.middleware  import AuditMiddleware, RateLimitMiddleware, verify_api_key
from openmedallion.neuron.models      import (
    FeedbackRequest,
    FeedbackResponse,
    QueryRequest,
    QueryResponse,
)

app = FastAPI(
    title="openmedallion neuron",
    description="LLM-powered natural-language query layer for medallion pipelines",
    version="1.0.0",
)

# Allow cortex (localhost:8050) to call the API from the browser
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8050", "http://127.0.0.1:8050"],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(AuditMiddleware)


def _silver_dir(project: str) -> Path:
    from openmedallion.config.loader import load_project
    cfg = load_project(project, settings.PROJECTS_ROOT)
    return Path(cfg["paths"]["silver"])


def _examples_dir(project: str) -> Path | None:
    path = Path(settings.PROJECTS_ROOT) / project / "examples"
    return path if path.exists() else None


def _load_metadata(project: str):
    from openmedallion.metadata.loader import load_metadata
    return load_metadata(project, settings.PROJECTS_ROOT)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/query", response_model=QueryResponse)
async def query_endpoint(
    body: QueryRequest,
    request: Request,
    _auth=Depends(verify_api_key),
):
    silver = _silver_dir(body.project)
    if not silver.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Silver layer not found for project '{body.project}': {silver}",
        )

    pipeline = CerebrumPipeline(
        silver,
        examples_dir=_examples_dir(body.project),
        metadata=_load_metadata(body.project),
        model=settings.LLM_MODEL,
        provider=settings.LLM_PROVIDER,
        api_key=settings.LLM_API_KEY,
        base_url=settings.LLM_BASE_URL,
    )

    try:
        qr = pipeline.ask(body.question)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (401, 403):
            raise HTTPException(
                status_code=503,
                detail=(
                    f"LLM provider '{settings.LLM_PROVIDER}' rejected the request "
                    "(authentication failed). Check MEDALLION_LLM_API_KEY."
                ),
            )
        raise HTTPException(status_code=500, detail=str(exc))
    except (httpx.ConnectError, httpx.ConnectTimeout):
        if settings.LLM_PROVIDER == "ollama":
            _url = settings.LLM_BASE_URL or settings.OLLAMA_URL
            detail = f"Ollama is not reachable at {_url}. Start it with: ollama serve"
        else:
            _url = settings.LLM_BASE_URL or settings.LLM_PROVIDER
            detail = f"LLM provider '{settings.LLM_PROVIDER}' is not reachable at {_url}."
        raise HTTPException(status_code=503, detail=detail)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    rows   = qr.result.to_dicts()
    answer = (
        f"Found {len(rows)} row(s) for: {body.question}"
        if rows
        else f"No results found for: {body.question}"
    )

    return QueryResponse(
        answer=answer,
        sql=qr.sql,
        rows=rows,
        recommended_prompt=qr.recommended_prompt,
        row_count=len(rows),
        columns=qr.result.columns,
    )


@app.post("/feedback", response_model=FeedbackResponse)
async def feedback_endpoint(
    body: FeedbackRequest,
    _auth=Depends(verify_api_key),
):
    from openmedallion.examples.feedback import record_feedback

    record_feedback(
        body.project, settings.PROJECTS_ROOT,
        question=body.question, sql=body.sql,
        columns=body.columns, row_count=body.row_count,
        thumbs_up=body.thumbs_up,
    )
    return FeedbackResponse()
