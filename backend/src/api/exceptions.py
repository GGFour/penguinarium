from typing import Optional, Any
import logging
from .logging import get_request_id
from rest_framework.views import exception_handler
from rest_framework.response import Response


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

    # Normalize to {"error": { code, message, target }}
    code = getattr(exc, "default_code", "error")
    message = getattr(exc, "detail", "An error occurred")
    if isinstance(message, (list, dict)):
        message = str(message)
    payload = {"error": {"code": str(code), "message": str(message), "target": getattr(context.get("view"), "__class__", type("", (), {})).__name__}}
    response.data = payload
    try:
        level = logging.WARNING if 400 <= response.status_code < 500 else logging.ERROR
        logger.log(level, "API error [req=%s] %s: %s", rid, code, message)
    except Exception:
        pass
    return response
