import time
import uuid
from typing import Callable
from django.http import HttpRequest, HttpResponse
from django.utils.deprecation import MiddlewareMixin

from .logging import set_request_id, reset_request_id, get_logger


class RequestLoggingMiddleware(MiddlewareMixin):
    """Logs each API request with timing and a request id.

    - Generates a request id (or uses X-Request-ID header)
    - Adds X-Request-ID to response headers
    - Logs request method, path, status code, and duration
    """

    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]):
        super().__init__(get_response)
        self.logger = get_logger("api.request")

    def process_request(self, request: HttpRequest):
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
        token = set_request_id(rid)
        request._request_id_token = token  # type: ignore[attr-defined]
        request._request_id = rid  # type: ignore[attr-defined]
        request._start_time = time.perf_counter()  # type: ignore[attr-defined]

    def process_response(self, request: HttpRequest, response: HttpResponse):
        rid = getattr(request, "_request_id", None)
        start = getattr(request, "_start_time", None)
        if start is not None:
            duration_ms = (time.perf_counter() - start) * 1000.0
        else:
            duration_ms = -1.0

        if rid:
            response["X-Request-ID"] = rid

        self.logger.info(
            "%s %s -> %s (%.2f ms)",
            request.method,
            request.get_full_path(),
            getattr(response, "status_code", "-"),
            duration_ms,
        )

        token = getattr(request, "_request_id_token", None)
        if token is not None:
            reset_request_id(token)
        return response

    def process_exception(self, request: HttpRequest, exception: Exception):
        # Log at error level and preserve request id
        self.logger.exception(
            "Unhandled exception for %s %s: %s",
            getattr(request, "method", "-"),
            getattr(request, "get_full_path", lambda: "-")(),
            exception,
        )
        token = getattr(request, "_request_id_token", None)
        if token is not None:
            reset_request_id(token)
        # Let Django continue handling the exception
        return None
