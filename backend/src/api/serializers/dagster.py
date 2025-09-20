from __future__ import annotations

from typing import Any

from rest_framework import serializers


class SourceConfigSerializer(serializers.Serializer):
    type = serializers.RegexField(r"^[A-Za-z0-9_]+$", max_length=64)
    config = serializers.DictField(child=serializers.JSONField(), required=False)
    dataset = serializers.DictField(child=serializers.JSONField(), required=False)


class DagsterRunSerializer(serializers.Serializer):
    source = SourceConfigSerializer(required=True)
    run_config = serializers.DictField(child=serializers.JSONField(), required=False)
    tags = serializers.DictField(child=serializers.CharField(), required=False)
    mode = serializers.CharField(required=False, allow_blank=True)
    op_selection = serializers.ListField(
        child=serializers.CharField(), required=False, allow_empty=False
    )
    source_op_key = serializers.RegexField(
        r"^[A-Za-z0-9_]+$", max_length=128, required=False, default="load_dataset_op"
    )

    def validate_tags(self, value: dict[str, Any]) -> dict[str, str]:
        return {str(key): str(val) for key, val in value.items()}
