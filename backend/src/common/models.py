import uuid
from django.db import models


class BaseModel(models.Model):
    class Meta:
        abstract = True

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    global_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    is_deleted = models.BooleanField(default=False)