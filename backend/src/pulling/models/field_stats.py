from django.db import models

from common.models import BaseModel
from .field_metadata import FieldMetadata


class FieldStats(BaseModel):
    """Per-column statistics snapshot.

    Stores an arbitrary JSON payload with metrics for a specific column (field)
    at a given collection time.
    """

    field_stats_id = models.AutoField(primary_key=True)

    field = models.ForeignKey(
        FieldMetadata,
        on_delete=models.CASCADE,
        related_name="field_stats_set",
        help_text="The column/field this statistic belongs to",
    )

    stat_date = models.DateTimeField(
        help_text="Timestamp when the statistics were collected"
    )

    value = models.JSONField(
        help_text="Arbitrary JSON statistics payload for the column"
    )

    class Meta(BaseModel.Meta):
        verbose_name = "Field Statistics"
        verbose_name_plural = "Field Statistics"
        ordering = ["-stat_date", "field_stats_id"]
        indexes = [
            models.Index(fields=["field", "-stat_date"], name="idx_field_date_desc"),
        ]

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"FieldStats(field_id={self.field_id}, at={self.stat_date.isoformat()})"
