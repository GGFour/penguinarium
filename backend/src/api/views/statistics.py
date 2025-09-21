from rest_framework import viewsets, filters
from rest_framework.permissions import AllowAny
from django.db.models import QuerySet
from typing import cast

from pulling.models.field_stats import FieldStats
from pulling.models.field_metadata import FieldMetadata
from ..serializers.statistics import FieldStatsSerializer
from ..pagination import EnvelopeLimitOffsetPagination


class FieldStatsViewSet(viewsets.ReadOnlyModelViewSet[FieldStats]):
    queryset = FieldStats.objects.filter(is_deleted=False)
    serializer_class = FieldStatsSerializer
    permission_classes = [AllowAny]
    authentication_classes = []
    pagination_class = EnvelopeLimitOffsetPagination
    filter_backends = [filters.OrderingFilter]
    ordering_fields = ["stat_date", "created_at", "updated_at"]
    ordering = ["-stat_date"]

    def get_queryset(self) -> QuerySet[FieldStats]:
        qs = cast(QuerySet[FieldStats], super().get_queryset())
        col = self.request.query_params.get("column") or self.request.query_params.get("field")
        if col:
            # support integer PK for FieldMetadata
            try:
                col_id = int(col)
                qs = qs.filter(field_id=col_id)
            except ValueError:
                # optionally support global_id hex prefix like fld_abcdefghij
                if col.startswith("fld_"):
                    # best-effort match by FieldMetadata.global_id prefix
                    prefix = col[4:]
                    try:
                        fm = FieldMetadata.objects.filter(global_id__startswith=prefix).values_list("field_metadata_id", flat=True).first()
                        if fm:
                            qs = qs.filter(field_id=fm)
                    except Exception:
                        pass
        return qs
