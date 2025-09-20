from typing import Optional, Tuple, Any
from django.utils.translation import gettext_lazy as _
from rest_framework.authentication import BaseAuthentication, get_authorization_header
from rest_framework import exceptions
from rest_framework.request import Request

from .models.api_key import ApiKey


class BearerAPIKeyAuthentication(BaseAuthentication):
    # Scheme should be treated case-insensitively per RFC 6750
    keyword = b"bearer"

    def authenticate(self, request: Request) -> Optional[Tuple[Any, ApiKey]]:
        auth = get_authorization_header(request)
        if not auth:
            return None
        parts = auth.split()
        if len(parts) != 2 or parts[0].lower() != self.keyword:
            raise exceptions.AuthenticationFailed({
                "error": {"code": "invalid_auth", "message": _("Invalid Authorization header."), "target": "authorization"}
            })

        key = parts[1].decode()
        try:
            api_key = ApiKey.objects.select_related("user").get(key=key, revoked=False)
        except ApiKey.DoesNotExist:
            raise exceptions.AuthenticationFailed({
                "error": {"code": "unauthorized", "message": _("Invalid API key."), "target": "authorization"}
            })
        # Return a (user, auth) tuple per DRF contract
        return (api_key.user, api_key)
