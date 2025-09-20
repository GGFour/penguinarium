from typing import Optional, Tuple
from django.utils.translation import gettext_lazy as _
from rest_framework.authentication import BaseAuthentication, get_authorization_header
from rest_framework import exceptions

from .models.api_key import ApiKey


class BearerAPIKeyAuthentication(BaseAuthentication):
    keyword = b"Bearer"

    def authenticate(self, request) -> Optional[Tuple[object, ApiKey]]:
        auth = get_authorization_header(request)
        if not auth:
            return None
        parts = auth.split()
        if len(parts) != 2 or parts[0] != self.keyword:
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

        return (api_key.user, api_key)
