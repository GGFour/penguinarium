from __future__ import annotations

import logging
import contextvars
from typing import Optional


# Context variable for request correlation id
_request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


def get_request_id() -> str:
    return _request_id_var.get()


def set_request_id(request_id: str) -> contextvars.Token[str]:
    return _request_id_var.set(request_id)


def reset_request_id(token: contextvars.Token[str]) -> None:
    try:
        _request_id_var.reset(token)
    except Exception:
        # Safe-guard: avoid raising from logging cleanup
        pass


class RequestContextFilter(logging.Filter):
    """Injects contextual fields (like request_id) into log records."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        # Ensure attribute exists for formatter
        if not hasattr(record, "request_id"):
            try:
                record.request_id = get_request_id()
            except Exception:
                record.request_id = "-"
        return True


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Convenience to get a namespaced logger (defaults to 'api')."""
    return logging.getLogger(name or "api")
