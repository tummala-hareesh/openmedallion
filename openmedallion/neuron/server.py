"""neuron/server.py — FastAPI app exposing the cerebrum pipeline over HTTP.

Endpoints
---------
POST /query
    Body:    QueryRequest  { question, project, use_templates }
    Returns: QueryResponse { answer, sql, rows, recommended_prompt, row_count, columns }
    On success, also appends a turn to
    <project>/chat_history/<username>.jsonl (best-effort — never blocks a
    successful answer), keyed by the X-Medallion-User header (default
    "local_user"). UI/audit only, never fed back into the LLM prompt.

GET /history?project=<project>
    Header:  X-Medallion-User (optional, default "local_user")
    Returns: ChatHistoryResponse { turns: [...] } — the calling person's own
    past turns for that project, newest first.

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
    ChatHistoryResponse,
    FeedbackRequest,
    FeedbackResponse,
    QueryRequest,
    QueryResponse,
    SessionEndRequest,
    SessionEndResponse,
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


def _username(request: Request) -> str:
    """Resolve the calling person's display name from the X-Medallion-User
    header (sent by cortex, persisted client-side in browser localStorage).

    A plain label, not an auth mechanism — orthogonal to verify_api_key.
    Defaults to "local_user" when absent, matching CLI single-user usage.
    """
    return request.headers.get("X-Medallion-User") or "local_user"


def _session_id(request: Request) -> str:
    """Resolve the ephemeral per-browser-session id from X-Medallion-Session
    (dcc.Store(storage_type="session") in cortex, fresh per tab). Additive to
    _username(), never a replacement — empty string when absent (e.g. a bare
    API call with no session concept)."""
    return request.headers.get("X-Medallion-Session") or ""


def _log_failed_turn(project: str, request: Request, question: str, error: str) -> None:
    """Best-effort: record a response_generated=False turn before an
    exception propagates as an HTTP error. Never raises — a logging failure
    must never mask the real error being reported to the caller."""
    from openmedallion.neuron.chat_history import record_chat_turn

    try:
        record_chat_turn(
            project, settings.PROJECTS_ROOT, _username(request),
            question=question, sql="", answer=error,
            row_count=0, columns=[], session_id=_session_id(request),
            response_generated=False,
        )
    except Exception:
        pass


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

    try:
        pipeline = CerebrumPipeline(
            silver,
            examples_dir=_examples_dir(body.project),
            metadata=_load_metadata(body.project),
            model=settings.LLM_MODEL,
            provider=settings.LLM_PROVIDER,
            api_key=settings.LLM_API_KEY,
            base_url=settings.LLM_BASE_URL,
            nlg_model=settings.LLM_NLG_MODEL,
            nlg_provider=settings.LLM_NLG_PROVIDER,
            nlg_api_key=settings.LLM_NLG_API_KEY,
            nlg_base_url=settings.LLM_NLG_BASE_URL,
            use_templates=body.use_templates,
        )
        qr = pipeline.ask(body.question)
    except ValueError as exc:
        _log_failed_turn(body.project, request, body.question, str(exc))
        raise HTTPException(status_code=422, detail=str(exc))
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (401, 403):
            detail = (
                f"LLM provider '{settings.LLM_PROVIDER}' rejected the request "
                "(authentication failed). Check MEDALLION_LLM_API_KEY."
            )
            _log_failed_turn(body.project, request, body.question, detail)
            raise HTTPException(status_code=503, detail=detail)
        _log_failed_turn(body.project, request, body.question, str(exc))
        raise HTTPException(status_code=500, detail=str(exc))
    except (httpx.ConnectError, httpx.ConnectTimeout):
        if settings.LLM_PROVIDER == "ollama":
            _url = settings.LLM_BASE_URL or settings.OLLAMA_URL
            detail = f"Ollama is not reachable at {_url}. Start it with: ollama serve"
        else:
            _url = settings.LLM_BASE_URL or settings.LLM_PROVIDER
            detail = f"LLM provider '{settings.LLM_PROVIDER}' is not reachable at {_url}."
        _log_failed_turn(body.project, request, body.question, detail)
        raise HTTPException(status_code=503, detail=detail)
    except Exception as exc:
        _log_failed_turn(body.project, request, body.question, str(exc))
        raise HTTPException(status_code=500, detail=str(exc))

    rows   = qr.result.to_dicts()
    answer = qr.answer or (
        f"Found {len(rows)} row(s) for: {body.question}"
        if rows
        else f"No results found for: {body.question}"
    )

    from openmedallion.neuron.chat_history import record_chat_turn

    turn_id = ""
    try:
        turn_id = record_chat_turn(
            body.project, settings.PROJECTS_ROOT, _username(request),
            question=body.question, sql=qr.sql, answer=answer,
            row_count=len(rows), columns=qr.result.columns,
            session_id=_session_id(request),
        )
    except Exception:
        pass  # chat history is best-effort — never break a successful answer

    return QueryResponse(
        answer=answer,
        sql=qr.sql,
        turn_id=turn_id,
        rows=rows,
        recommended_prompt=qr.recommended_prompt,
        row_count=len(rows),
        columns=qr.result.columns,
    )


@app.post("/feedback", response_model=FeedbackResponse)
async def feedback_endpoint(
    body: FeedbackRequest,
    request: Request,
    _auth=Depends(verify_api_key),
):
    """Thumbs up/down: set the matching chat_history turn's `accepted` field
    in place (three-state: True/False/None — never inferred). Does NOT write
    harvested.jsonl/failures.jsonl directly — that happens later, at
    session-end curation (see /session/end), reusing the unchanged
    examples/feedback.py:record_feedback()."""
    from openmedallion.neuron.chat_history import update_turn_accepted

    update_turn_accepted(
        body.project, settings.PROJECTS_ROOT, _username(request),
        body.turn_id, accepted=body.thumbs_up,
    )
    return FeedbackResponse()


@app.get("/history", response_model=ChatHistoryResponse)
async def history_endpoint(
    project: str,
    request: Request,
    _auth=Depends(verify_api_key),
):
    """Return the calling person's own past turns for a project (cortex's
    "History" tab). UI/audit only — never fed back into the LLM prompt."""
    from openmedallion.neuron.chat_history import list_chat_history

    turns = list_chat_history(project, settings.PROJECTS_ROOT, _username(request))
    return ChatHistoryResponse(turns=turns)


@app.post("/session/end", response_model=SessionEndResponse)
async def session_end_endpoint(
    body: SessionEndRequest,
    _auth=Depends(verify_api_key),
):
    """Curation-promotion trigger — NOT a data-sync endpoint (chat_history is
    already written server-side in real time on every /query). Rolls the
    closed session's rated-but-unpromoted turns into
    harvested.jsonl/failures.jsonl. Fired by cortex on idle timeout, an
    explicit "End Session" button, or a best-effort tab-close beacon — hence
    project/session_id/username live in the body, not headers
    (navigator.sendBeacon can't set custom headers)."""
    from openmedallion.neuron.chat_history import promote_session

    username = body.username or "local_user"
    counts = promote_session(body.project, settings.PROJECTS_ROOT, username, body.session_id)
    return SessionEndResponse(**counts)
