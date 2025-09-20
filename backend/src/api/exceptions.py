from typing import Optional
from rest_framework.views import exception_handler
from rest_framework.response import Response
from rest_framework import status


def api_exception_handler(exc, context) -> Optional[Response]:
    response = exception_handler(exc, context)
    if response is None:
        return response

    # Normalize to {"error": { code, message, target }}
    code = getattr(exc, "default_code", "error")
    message = getattr(exc, "detail", "An error occurred")
    if isinstance(message, (list, dict)):
        message = str(message)
    payload = {"error": {"code": str(code), "message": str(message), "target": getattr(context.get("view"), "__class__", type("", (), {})).__name__}}
    response.data = payload
    return response
