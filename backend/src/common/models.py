import uuid
from django.db import models


class BaseModel(models.Model):
    class Meta:
        abstract = True

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    global_id = models.CharField(
        max_length=255, unique=True, default=lambda: str(uuid.uuid4()))
    is_deleted = models.BooleanField(default=False)