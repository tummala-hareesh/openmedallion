"""config/settings.py — Single source of truth for all MEDALLION_* configuration.

Values are resolved in this priority order (highest wins):
  1. Environment variable   — MEDALLION_LLM_MODEL=mistral medallion ask my_project
  2. settings.yaml file     — searched in CWD, then ~/.medallion/settings.yaml
  3. Built-in default       — hardcoded fallback below

settings.yaml format
--------------------
    llm:
      model:      llama3.2
      ollama_url: http://localhost:11434

    neuron:
      rate_limit: 60
      audit_log:  medallion_audit.jsonl
      # api_key:  your-secret-key   # uncomment to enable bearer-token auth

    cortex:
      mock_client: false            # true = offline stub, no Ollama needed

Environment variable reference
------------------------------
    MEDALLION_LLM_PROVIDER    LLM backend                 (default: ollama)
                              Choices: ollama | openrouter | openai | <custom>
    MEDALLION_LLM_MODEL       Model identifier            (default: llama3.2)
                              Ollama tag or provider model ID (e.g. openai/gpt-4o)
    MEDALLION_LLM_API_KEY     API key for non-Ollama providers (unset = Ollama only)
    MEDALLION_LLM_BASE_URL    Override provider endpoint URL  (unset = provider default)
    MEDALLION_OLLAMA_URL      Ollama base URL             (default: http://localhost:11434)
    MEDALLION_PROJECTS_ROOT   Parent dir of project dirs  (default: .)
    MEDALLION_API_KEY         Bearer token for neuron     (unset = auth disabled)
    MEDALLION_RATE_LIMIT      Max requests/min per IP     (default: 60)
    MEDALLION_AUDIT_LOG       JSONL audit log path        (default: medallion_audit.jsonl)
    USE_MOCK_CLIENT           1 = offline stub in cortex  (default: 0)
"""
from __future__ import annotations

import os
from pathlib import Path


def _load_yaml() -> dict:
    """Return the first settings.yaml found in CWD or ~/.medallion/."""
    import yaml

    candidates = [
        Path.cwd() / "settings.yaml",
        Path.home() / ".medallion" / "settings.yaml",
    ]
    for path in candidates:
        if path.is_file():
            with open(path) as fh:
                return yaml.safe_load(fh) or {}
    return {}


def _env(var: str) -> str | None:
    """Return env var value, or None if unset (empty string treated as unset)."""
    val = os.environ.get(var)
    return val if val else None


_y = _load_yaml()
_llm = _y.get("llm",    {})
_nrn = _y.get("neuron", {})
_ctx = _y.get("cortex", {})

# ── LLM provider ──────────────────────────────────────────────────────────────
LLM_PROVIDER : str        = _env("MEDALLION_LLM_PROVIDER") or _llm.get("provider",  "ollama")
LLM_MODEL    : str        = _env("MEDALLION_LLM_MODEL")    or _llm.get("model",      "llama3.2")
LLM_API_KEY  : str | None = _env("MEDALLION_LLM_API_KEY")  or _llm.get("api_key")
LLM_BASE_URL : str | None = _env("MEDALLION_LLM_BASE_URL") or _llm.get("base_url")
OLLAMA_URL   : str        = _env("MEDALLION_OLLAMA_URL")    or _llm.get("ollama_url", "http://localhost:11434")

# ── Neuron server ─────────────────────────────────────────────────────────────
PROJECTS_ROOT : str        = _env("MEDALLION_PROJECTS_ROOT") or _nrn.get("projects_root", ".")
API_KEY       : str | None = _env("MEDALLION_API_KEY")       or _nrn.get("api_key")
RATE_LIMIT    : int        = int(_env("MEDALLION_RATE_LIMIT") or _nrn.get("rate_limit", 60))
AUDIT_LOG     : str        = _env("MEDALLION_AUDIT_LOG")      or _nrn.get("audit_log",  "medallion_audit.jsonl")

# ── Cortex UI ─────────────────────────────────────────────────────────────────
USE_MOCK_CLIENT : bool = (
    _env("USE_MOCK_CLIENT") == "1"
    if _env("USE_MOCK_CLIENT") is not None
    else bool(_ctx.get("mock_client", False))
)
