from django.contrib import admin

from .models import ApiKey


@admin.register(ApiKey)
class ApiKeyAdmin(admin.ModelAdmin):
	list_display = ("key", "user", "revoked", "created_at")
	search_fields = ("key", "user__username", "user__email")
	list_filter = ("revoked",)

# Register your models here.
