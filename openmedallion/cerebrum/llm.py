"""cerebrum/llm.py — Thin Ollama client via httpx (no SDK).

Sends a generate request to the local Ollama server and returns the model
response as a stripped string (expected to be a SQL query).
"""
from __future__ import annotations

import httpx

_DEFAULT_BASE_URL = "http://localhost:11434"
_DEFAULT_MODEL    = "llama3.2"
_TIMEOUT          = 120.0  # seconds — LLMs can be slow on CPU


def query(
    prompt: str,
    *,
    model: str = _DEFAULT_MODEL,
    base_url: str = _DEFAULT_BASE_URL,
    timeout: float = _TIMEOUT,
) -> str:
    """Send *prompt* to Ollama and return the stripped response text.

    Uses the ``/api/generate`` endpoint with ``stream=False``.

    Parameters
    ----------
    prompt:
        The full prompt string to send to the model.
    model:
        Ollama model tag (e.g. ``"llama3.2"``, ``"mistral"``).
    base_url:
        Base URL of the Ollama server (default: ``http://localhost:11434``).
    timeout:
        Request timeout in seconds.

    Returns
    -------
    str
        Stripped model response.

    Raises
    ------
    httpx.HTTPStatusError
        If the Ollama server returns a non-2xx status.
    httpx.TimeoutException
        If the server does not respond within *timeout* seconds.
    RuntimeError
        If the response JSON is missing the expected ``response`` key.
    """
    payload = {
        "model":  model,
        "prompt": prompt,
        "stream": False,
    }
    with httpx.Client(timeout=timeout) as client:
        r = client.post(f"{base_url}/api/generate", json=payload)
        r.raise_for_status()

    data = r.json()
    if "response" not in data:
        raise RuntimeError(
            f"Unexpected Ollama response shape — keys: {list(data.keys())}"
        )
    return data["response"].strip()
