"""cerebrum/llm.py — LLM client abstraction for Ollama and OpenAI-compatible providers.

Supported providers
-------------------
``"ollama"``
    Local Ollama server via ``/api/generate``.  Requires no API key.

``"openrouter"``
    OpenRouter.ai via ``/chat/completions``.  Requires ``MEDALLION_LLM_API_KEY``.

``"openai"``
    OpenAI API via ``/chat/completions``.  Requires ``MEDALLION_LLM_API_KEY``.

Any other string
    Treated as a generic OpenAI-compatible endpoint.  Requires both ``api_key``
    and ``base_url`` (e.g. LM Studio, Groq, Anyscale, Together AI…).

Use :func:`get_client` to obtain an :class:`LLMClient` instance from a provider
name, or instantiate :class:`OllamaClient` / :class:`OpenAICompatibleClient`
directly.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

import httpx

from openmedallion.config import settings

_TIMEOUT = 120.0  # seconds — LLMs can be slow on CPU


# ── Protocol ─────────────────────────────────────────────────────────────────

@runtime_checkable
class LLMClient(Protocol):
    """Callable that sends a prompt and returns the model's response text."""

    def __call__(self, prompt: str) -> str: ...


# ── Concrete clients ──────────────────────────────────────────────────────────

class OllamaClient:
    """Thin Ollama client — calls ``/api/generate`` with ``stream=False``."""

    def __init__(
        self,
        model: str,
        base_url: str,
        *,
        timeout: float = _TIMEOUT,
    ) -> None:
        self._model    = model
        self._base_url = base_url.rstrip("/")
        self._timeout  = timeout

    def __call__(self, prompt: str) -> str:
        payload = {"model": self._model, "prompt": prompt, "stream": False}
        with httpx.Client(timeout=self._timeout) as client:
            r = client.post(f"{self._base_url}/api/generate", json=payload)
            r.raise_for_status()
        data = r.json()
        if "response" not in data:
            raise RuntimeError(
                f"Unexpected Ollama response shape — keys: {list(data.keys())}"
            )
        return data["response"].strip()


class OpenAICompatibleClient:
    """OpenAI-compatible ``/chat/completions`` client.

    Works with OpenRouter, OpenAI, Groq, LM Studio, Anyscale, Together AI, or
    any endpoint that speaks the OpenAI chat completions protocol.
    """

    def __init__(
        self,
        model: str,
        api_key: str,
        base_url: str,
        *,
        timeout: float = _TIMEOUT,
    ) -> None:
        self._model    = model
        self._api_key  = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout  = timeout

    def __call__(self, prompt: str) -> str:
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
        }
        with httpx.Client(timeout=self._timeout) as client:
            r = client.post(
                f"{self._base_url}/chat/completions",
                headers=headers,
                json=payload,
            )
            r.raise_for_status()
        data = r.json()
        try:
            return data["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError) as exc:
            raise RuntimeError(
                f"Unexpected response shape from {self._base_url} — "
                f"keys: {list(data.keys())}"
            ) from exc


# ── Factory ───────────────────────────────────────────────────────────────────

_OPENROUTER_URL = "https://openrouter.ai/api/v1"
_OPENAI_URL     = "https://api.openai.com/v1"

_PROVIDER_DEFAULTS: dict[str, str] = {
    "openrouter": _OPENROUTER_URL,
    "openai":     _OPENAI_URL,
}


def get_client(
    provider: str,
    model: str,
    *,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: float = _TIMEOUT,
) -> LLMClient:
    """Return an :class:`LLMClient` for *provider*.

    Parameters
    ----------
    provider:
        ``"ollama"`` | ``"openrouter"`` | ``"openai"`` | any custom label.
    model:
        Model identifier (e.g. ``"llama3.2"``, ``"openai/gpt-4o"``).
    api_key:
        API key for non-Ollama providers.  Falls back to
        ``settings.LLM_API_KEY`` / ``MEDALLION_LLM_API_KEY``.
    base_url:
        Override the provider's default endpoint URL.
    timeout:
        HTTP timeout in seconds.

    Raises
    ------
    ValueError
        If a required parameter (``api_key`` for cloud providers, ``base_url``
        for unknown custom providers) is missing.
    """
    if provider == "ollama":
        return OllamaClient(
            model,
            base_url or settings.OLLAMA_URL,
            timeout=timeout,
        )

    # Cloud / OpenAI-compatible providers
    resolved_key = api_key or settings.LLM_API_KEY
    if not resolved_key and provider not in ("openai",):
        # openai SDK sometimes allows key-less local proxies; keep it lenient
        if provider in _PROVIDER_DEFAULTS or base_url is None:
            raise ValueError(
                f"provider='{provider}' requires an API key. "
                "Set MEDALLION_LLM_API_KEY or pass api_key= to CerebrumPipeline."
            )

    resolved_url = base_url or _PROVIDER_DEFAULTS.get(provider)
    if resolved_url is None:
        raise ValueError(
            f"Unknown provider '{provider}' — pass base_url= to specify the endpoint "
            "(e.g. base_url='http://localhost:1234/v1' for LM Studio)."
        )

    return OpenAICompatibleClient(
        model,
        resolved_key or "",
        resolved_url,
        timeout=timeout,
    )


# ── Backwards-compatible module-level function ────────────────────────────────

def query(
    prompt: str,
    *,
    model: str = settings.LLM_MODEL,
    base_url: str = settings.OLLAMA_URL,
    timeout: float = _TIMEOUT,
) -> str:
    """Send *prompt* to a local Ollama server and return the response.

    Kept for backwards compatibility.  Prefer :func:`get_client` for new code.
    """
    return OllamaClient(model, base_url, timeout=timeout)(prompt)
