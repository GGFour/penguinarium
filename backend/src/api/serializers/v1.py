# pyright: reportMissingTypeArgument=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
from typing import Any
from datetime import timezone
from rest_framework import serializers
from pulling.models import Alert

# models are referenced dynamically via instance types; explicit imports not required here


class UserSerializer(serializers.Serializer):
    id = serializers.CharField(read_only=True)
    name = serializers.CharField(read_only=True)
    email = serializers.EmailField(read_only=True)
    created_at = serializers.CharField(read_only=True)

    def _format_dt_z(self, dt: Any) -> str:
        try:
            # Convert to UTC and emit trailing 'Z' with seconds precision
            return (
                dt.astimezone(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except Exception:
            return ""

    def to_representation(self, instance: Any) -> dict[str, Any]:
        full = getattr(instance, "get_full_name", lambda: "")()
        name = full or getattr(instance, "username", None) or getattr(instance, "email", "")
        created = getattr(instance, "date_joined", None)
        created_iso = self._format_dt_z(created) if created is not None else ""
        return {
            "id": f"user_{getattr(instance, 'pk', '')}",
            "name": name,
            "email": getattr(instance, "email", ""),
            "created_at": created_iso,
        }


class DataSourceV1Serializer(serializers.Serializer):
    id = serializers.CharField(read_only=True)
    user_id = serializers.CharField(read_only=True, allow_null=True)
    type = serializers.CharField(read_only=True)
    name = serializers.CharField(read_only=True)
    created_at = serializers.CharField(read_only=True)

    def to_representation(self, instance: Any) -> dict[str, Any]:
        obj = instance
        gid = str(obj.global_id).replace("-", "")
        try:
            created_iso = (
                obj.created_at.astimezone(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except Exception:
            created_iso = ""
        uid = getattr(obj, "user_id", None)
        return {
            "id": f"ds_{gid[:10]}",
            "user_id": f"user_{uid}" if uid else None,
            "type": obj.type,
            "name": obj.name,
            "created_at": created_iso,
        }


class DataSourceStatusSerializer(serializers.Serializer):
    datasource_id = serializers.CharField()
    status = serializers.CharField()
    last_checked_at = serializers.DateTimeField(allow_null=True)
    error_message = serializers.CharField(allow_null=True)


class AlertSerializer(serializers.Serializer):
    id = serializers.CharField(read_only=True)
    datasource_id = serializers.CharField(read_only=True)
    name = serializers.CharField(read_only=True)
    severity = serializers.CharField(read_only=True)
    status = serializers.CharField(read_only=True)
    details = serializers.JSONField(read_only=True)
    triggered_at = serializers.DateTimeField(read_only=True)

    def to_representation(self, instance: Alert) -> dict[str, object]:  # type: ignore[override]
        gid = str(instance.global_id).replace("-", "")
        dsgid = str(instance.data_source.global_id).replace("-", "")
        return {
            "id": f"al_{gid[:10]}",
            "datasource_id": f"ds_{dsgid[:10]}",
            "name": instance.name,
            "severity": instance.severity,
            "status": instance.status,
            "details": instance.details,
            "triggered_at": instance.triggered_at,
        }


class TableSerializer(serializers.Serializer):
    id = serializers.CharField(read_only=True)
    datasource_id = serializers.CharField(read_only=True)
    schema_name = serializers.CharField(read_only=True)
    table_name = serializers.CharField(read_only=True)
    row_count = serializers.IntegerField(read_only=True)
    last_updated_at = serializers.DateTimeField(read_only=True)

    def to_representation(self, instance: Any) -> dict[str, Any]:
        obj = instance
        gid = str(obj.global_id).replace("-", "")
        dsgid = str(obj.data_source.global_id).replace("-", "")
        return {
            "id": f"tbl_{gid[:10]}",
            "datasource_id": f"ds_{dsgid[:10]}",
            "schema_name": obj.metadata.get("schema_name") or obj.metadata.get("schema") or "public",
            "table_name": obj.name,
            "row_count": obj.metadata.get("row_count") or 0,
            "last_updated_at": obj.updated_at,
        }
