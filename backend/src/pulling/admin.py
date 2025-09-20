from django.contrib import admin
from .models import Alert


@admin.register(Alert)
class AlertAdmin(admin.ModelAdmin):  # type: ignore[type-arg]
	list_display = ("name", "severity", "status", "data_source", "triggered_at")
	list_filter = ("severity", "status")
	search_fields = ("name",)
