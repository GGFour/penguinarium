from django.db import models
from django.contrib.auth import get_user_model


class ApiKey(models.Model):
    """Simple API key linked to a user.

    Note: For production, prefer hashing the key and storing prefix only.
    """

    key = models.CharField(max_length=128, unique=True)
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE, related_name="api_keys")
    created_at = models.DateTimeField(auto_now_add=True)
    revoked = models.BooleanField(default=False)

    class Meta:
        indexes = [models.Index(fields=["key"])]

    def __str__(self) -> str:  # pragma: no cover
        return f"ApiKey(user={self.user_id}, revoked={self.revoked})"
