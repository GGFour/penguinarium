from rest_framework import serializers

from pulling.models.field_stats import FieldStats


class FieldStatsSerializer(serializers.ModelSerializer[FieldStats]):
    id = serializers.IntegerField(source="field_stats_id", read_only=True)
    column_id = serializers.IntegerField(source="field_id", read_only=True)

    class Meta:
        model = FieldStats
        fields = (
            "id",
            "column_id",
            "stat_date",
            "value",
            "created_at",
            "updated_at",
        )
        read_only_fields = ("id", "column_id", "created_at", "updated_at")
