import time
import uuid
from collections import defaultdict
from typing import Callable
from django.http import JsonResponse, HttpRequest, HttpResponse
from django.utils.deprecation import MiddlewareMixin

from .logging import set_request_id, reset_request_id, get_logger


class RateLimitMiddleware:
    """Very simple per-API-key rate limiting (1000 req/min).

    For production, use Redis or a more robust algorithm.
    """

    RATE_LIMIT = 1000
    WINDOW = 60

    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]):
        self.get_response = get_response
        self.buckets: dict[str, list[float]] = defaultdict(list)

    def __call__(self, request: HttpRequest) -> HttpResponse:
        # Only apply to versioned API paths
        if request.path.startswith("/api/v1/"):
            key = self._key_for_request(request)
            now = time.time()
            window_start = now - self.WINDOW
            # prune
            bucket = self.buckets[key] = [t for t in self.buckets[key] if t > window_start]
            remaining = self.RATE_LIMIT - len(bucket)
            reset = int(now + self.WINDOW)
            if remaining <= 0:
                resp = JsonResponse({
                    "error": {
                        "code": "rate_limited",
                        "message": "Too many requests.",
                        "target": "rate_limit"
                    }
                }, status=429)
                resp["X-RateLimit-Limit"] = str(self.RATE_LIMIT)
                resp["X-RateLimit-Remaining"] = "0"
                resp["X-RateLimit-Reset"] = str(reset)
                return resp

            bucket.append(now)
            response = self.get_response(request)
            response["X-RateLimit-Limit"] = str(self.RATE_LIMIT)
            response["X-RateLimit-Remaining"] = str(max(0, remaining - 1))
            response["X-RateLimit-Reset"] = str(reset)
            return response

        return self.get_response(request)

    def _key_for_request(self, request: HttpRequest) -> str:
        auth = request.META.get("HTTP_AUTHORIZATION", "")
        return auth or f"ip:{request.META.get('REMOTE_ADDR','unknown')}"


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
