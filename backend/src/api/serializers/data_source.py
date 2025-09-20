from rest_framework import serializers

from pulling.models.data_source import DataSource


class DataSourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataSource
        # Expose key fields; include id alias for convenience
        fields = (
            "data_source_id",
            "global_id",
            "name",
            "type",
            "connection_info",
            "created_at",
            "updated_at",
        )
        read_only_fields = ("data_source_id", "global_id", "created_at", "updated_at")
