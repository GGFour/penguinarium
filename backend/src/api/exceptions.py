from typing import Optional, Any
import logging
from .logging import get_request_id
from rest_framework.views import exception_handler
from rest_framework.response import Response
from typing import Any as _Any


def api_exception_handler(exc: Any, context: dict[str, Any]) -> Optional[Response]:
    response = exception_handler(exc, context)
    # Always log exceptions handled by DRF at WARNING for 4xx and ERROR for 5xx
    view = context.get("view")
    logger = logging.getLogger("api")
    rid = get_request_id()
    if response is None:
        # Let Django handle it further, but record here as error with context
        logger.exception("Unhandled API exception [req=%s] in %s", rid, getattr(view, "__class__", type("", (), {})).__name__)
        return response

    # Normalize to {"error": { code, message, target, status, request_id }}
    code = getattr(exc, "default_code", None) or getattr(getattr(exc, "detail", None), "code", None) or "error"
    detail: _Any = getattr(exc, "detail", None)
    if detail is None:
        message = str(exc)
    else:
        # Convert DRF error detail structures and other types to a readable string
        message = str(detail)

    target = getattr(context.get("view"), "__class__", type("", (), {})).__name__
    payload: dict[str, Any] = {
        "error": {
            "code": str(code),
            "message": message,
            "target": target,
            "status": response.status_code,
            "request_id": rid,
        }
    }
    response.data = payload
    try:
        level = logging.WARNING if 400 <= response.status_code < 500 else logging.ERROR
        logger.log(level, "API error [req=%s] %s: %s", rid, code, message)
    except Exception:
        pass
    return response
