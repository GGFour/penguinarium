from rest_framework.permissions import BasePermission
from rest_framework.request import Request
from rest_framework.views import APIView
from rest_framework.exceptions import NotAuthenticated


class RequireBearerKey(BasePermission):
    """Require a Bearer Authorization header and an authenticated user.

    - If the Authorization header is missing or not Bearer, raise 401.
    - Otherwise, require request.user.is_authenticated to be True.
    """

    def has_permission(self, request: Request, view: APIView) -> bool:
        auth = request.META.get("HTTP_AUTHORIZATION", "")
        # Accept scheme in a case-insensitive manner (e.g., "bearer" or "Bearer")
        if not auth or not auth.lower().startswith("bearer "):
            raise NotAuthenticated()
        return bool(getattr(request.user, "is_authenticated", False))
