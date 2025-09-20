import time
from collections import defaultdict
from typing import Callable
from django.http import JsonResponse, HttpRequest, HttpResponse


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
