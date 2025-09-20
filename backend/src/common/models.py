import uuid
from django.db import models


def generate_global_id():
    """Generate a unique global ID using UUID4."""
    return str(uuid.uuid4())


class BaseModel(models.Model):
    class Meta:
        abstract = True

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    global_id = models.CharField(
        max_length=255, unique=True, default=generate_global_id)
    is_deleted = models.BooleanField(default=False)