# pyright: reportMissingTypeArgument=false
from rest_framework import viewsets, filters
from rest_framework.permissions import AllowAny

from pulling.models.data_source import DataSource
from ..serializers.data_source import DataSourceSerializer
from django.db.models import QuerySet
from typing import cast


class DataSourceViewSet(viewsets.ModelViewSet):
	"""CRUD endpoints for DataSource (list, create, update, partial_update, retrieve, destroy*).

	Note: destroy is available by default from ModelViewSet but can be disabled if needed.
	"""

	queryset = DataSource.objects.filter(is_deleted=False)
	serializer_class = DataSourceSerializer
	permission_classes = [AllowAny]

	# Basic search/order support; filter by type via query param ?type=api|database|...
	filter_backends = [filters.SearchFilter, filters.OrderingFilter]
	search_fields = ["name"]
	ordering_fields = ["name", "created_at", "updated_at"]
	ordering = ["name"]

	def get_queryset(self) -> QuerySet[DataSource]:
		qs = cast(QuerySet[DataSource], super().get_queryset())
		ds_type = self.request.query_params.get("type")
		if ds_type:
			qs = qs.filter(type=ds_type)
		return qs
