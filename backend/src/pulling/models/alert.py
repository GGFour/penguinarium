from __future__ import annotations

from django.db import models

from common.models import BaseModel
from .data_source import DataSource
from .table_metadata import TableMetadata
from .field_metadata import FieldMetadata


def default_details() -> dict[str, object]:
    return {}


class Alert(BaseModel):
    class Severity(models.TextChoices):
        INFO = ("info", "Info")
        WARNING = ("warning", "Warning")
        CRITICAL = ("critical", "Critical")

    class Status(models.TextChoices):
        ACTIVE = ("active", "Active")
        RESOLVED = ("resolved", "Resolved")
        SNOOZED = ("snoozed", "Snoozed")

    alert_id = models.AutoField(primary_key=True)

    data_source = models.ForeignKey(
        DataSource,
        on_delete=models.CASCADE,
        related_name="alerts",
        help_text="Data source this alert belongs to",
    )

    table = models.ForeignKey(
        TableMetadata,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="alerts",
        help_text="Optional table this alert is associated with",
    )

    field = models.ForeignKey(
        FieldMetadata,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="alerts",
        help_text="Optional field this alert is associated with",
    )

    name = models.CharField(max_length=255)
    severity = models.CharField(max_length=16, choices=Severity.choices, default=Severity.WARNING)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.ACTIVE)
    details = models.JSONField(default=default_details)
    triggered_at = models.DateTimeField()

    class Meta(BaseModel.Meta):
        verbose_name = "Alert"
        verbose_name_plural = "Alerts"
        ordering = ["-triggered_at", "-created_at"]
        indexes = [
            models.Index(fields=["data_source", "status"]),
            models.Index(fields=["severity"]),
        ]

    def __str__(self) -> str:
        return f"[{self.severity}] {self.name}"
