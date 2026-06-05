"""neuron/server.py — FastAPI app exposing the cerebrum pipeline over HTTP.

Single endpoint
---------------
POST /query
    Body:    QueryRequest  { question, project }
    Returns: QueryResponse { answer, sql, rows, recommended_prompt, row_count, columns }

GET /health
    Returns: { "status": "ok" }

Environment variables
---------------------
MEDALLION_PROJECTS_ROOT   Parent directory of per-project folders (default: ".")
MEDALLION_LLM_MODEL       Ollama model tag (default: "llama3.2")
MEDALLION_OLLAMA_URL      Ollama base URL   (default: "http://localhost:11434")
MEDALLION_API_KEY         If set, enables bearer-token auth
MEDALLION_RATE_LIMIT      Max requests/min per IP (default: 60)
MEDALLION_AUDIT_LOG       JSONL audit log path (default: "medallion_audit.jsonl")
"""
from __future__ import annotations

import os
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from openmedallion.cerebrum.pipeline import CerebrumPipeline
from openmedallion.neuron.middleware  import AuditMiddleware, RateLimitMiddleware, verify_api_key
from openmedallion.neuron.models      import QueryRequest, QueryResponse

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
    projects_root = os.getenv("MEDALLION_PROJECTS_ROOT", ".")
    from openmedallion.config.loader import load_project
    cfg = load_project(project, projects_root)
    return Path(cfg["paths"]["silver"])


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

    model      = os.getenv("MEDALLION_LLM_MODEL", "llama3.2")
    ollama_url = os.getenv("MEDALLION_OLLAMA_URL", "http://localhost:11434")
    pipeline   = CerebrumPipeline(silver, model=model, ollama_base_url=ollama_url)

    try:
        qr = pipeline.ask(body.question)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except (httpx.ConnectError, httpx.ConnectTimeout):
        ollama_url = os.getenv("MEDALLION_OLLAMA_URL", "http://localhost:11434")
        raise HTTPException(
            status_code=503,
            detail=(
                f"Ollama is not reachable at {ollama_url}. "
                "Start it with: ollama serve"
            ),
        )
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
