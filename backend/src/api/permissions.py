from rest_framework.permissions import BasePermission
from rest_framework.exceptions import NotAuthenticated


class RequireBearerKey(BasePermission):
    """Require a Bearer Authorization header and an authenticated user.

    - If the Authorization header is missing or not Bearer, raise 401.
    - Otherwise, require request.user.is_authenticated to be True.
    """

    def has_permission(self, request, view) -> bool:
        auth = request.META.get("HTTP_AUTHORIZATION", "")
        if not auth or not auth.startswith("Bearer "):
            raise NotAuthenticated()
        return bool(getattr(request.user, "is_authenticated", False))
