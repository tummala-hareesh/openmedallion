"""config/errors.py — translate pydantic.ValidationError into readable messages.

Shared by config/validator.py and metadata/loader.py (and any future Pydantic-
backed loader) so every part of openmedallion raises the same
``[tag] path: message`` shaped ``ValueError`` regardless of which schema
module rejected the input.
"""
from pydantic import ValidationError


def _format_loc(loc: tuple) -> str:
    """Render a pydantic error ``loc`` tuple as ``a.b[0].c``."""
    out = ""
    for tok in loc:
        if isinstance(tok, int):
            out += f"[{tok}]"
        else:
            out += f".{tok}" if out else str(tok)
    return out


def _format_error(err: dict) -> str:
    loc   = err["loc"]
    etype = err["type"]
    msg   = err["msg"]

    if msg.startswith("Value error, "):
        msg = msg[len("Value error, "):]
        prefix = _format_loc(loc)
        return f"{prefix}: {msg}" if prefix else msg

    if etype == "missing":
        field = loc[-1]
        parent = _format_loc(loc[:-1])
        if field in ("file", "function"):
            return f"{parent}: '{field}' is required" if parent else f"'{field}' is required"
        full = _format_loc(loc)
        return f"{full} is required"

    return f"{_format_loc(loc)}: {msg}"


def format_validation_error(e: ValidationError, tag: str) -> str:
    """Translate a ``pydantic.ValidationError`` into a ``[tag] msg1; msg2`` string."""
    messages = [_format_error(err) for err in e.errors()]
    return f"[{tag}] {'; '.join(messages)}"
