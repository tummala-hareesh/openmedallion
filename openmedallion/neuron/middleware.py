"""neuron/middleware.py — Auth stub, sliding-window rate limiter, JSONL audit log.

Auth (``MEDALLION_API_KEY``):
    If the env var is set, every request must supply a matching
    ``Authorization: Bearer <key>`` header. If the var is absent,
    all requests are allowed (development / local mode).

Rate limiting (``MEDALLION_RATE_LIMIT``, default 60 req/min per IP):
    Simple in-memory sliding window. Good enough for local deployments;
    replace with a Redis-backed implementation for multi-instance setups.

Audit log (``MEDALLION_AUDIT_LOG``, default ``medallion_audit.jsonl``):
    Every request + response status + latency is appended as one JSON line.
"""
from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from pathlib import Path

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from openmedallion.config import settings

_AUDIT_PATH     = Path(settings.AUDIT_LOG)
_WINDOW_SECONDS = 60
_MAX_REQUESTS   = settings.RATE_LIMIT

_request_times: dict[str, list[float]] = defaultdict(list)


def _client_id(request: Request) -> str:
    return request.client.host if request.client else "unknown"


class AuditMiddleware(BaseHTTPMiddleware):
    """Append one JSON line per request to the JSONL audit log."""

    async def dispatch(self, request: Request, call_next):
        start    = time.monotonic()
        response = await call_next(request)
        latency  = round((time.monotonic() - start) * 1000, 2)

        record = {
            "ts":         time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "method":     request.method,
            "path":       request.url.path,
            "client":     _client_id(request),
            "status":     response.status_code,
            "latency_ms": latency,
        }
        with _AUDIT_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")

        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding-window rate limiter: _MAX_REQUESTS per _WINDOW_SECONDS per IP."""

    async def dispatch(self, request: Request, call_next):
        client = _client_id(request)
        now    = time.monotonic()
        cutoff = now - _WINDOW_SECONDS

        bucket = _request_times[client]
        _request_times[client] = [t for t in bucket if t >= cutoff]

        if len(_request_times[client]) >= _MAX_REQUESTS:
            return JSONResponse(
                status_code=429,
                content={"detail": f"Rate limit: {_MAX_REQUESTS} req/{_WINDOW_SECONDS}s exceeded"},
            )
        _request_times[client].append(now)
        return await call_next(request)


def verify_api_key(request: Request) -> None:
    """Raise HTTP 401 if ``MEDALLION_API_KEY`` is set and the header doesn't match."""
    expected = os.getenv("MEDALLION_API_KEY")  # read per-request to support monkeypatching
    if not expected:
        return  # auth disabled in local / dev mode
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[len("Bearer "):] != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
