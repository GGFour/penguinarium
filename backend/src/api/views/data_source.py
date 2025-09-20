# pyright: reportMissingTypeArgument=false
from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.request import Request
from datetime import datetime, timezone
from rest_framework.permissions import AllowAny

from pulling.models.data_source import DataSource
from pulling.models.table_metadata import TableMetadata
from ..serializers.data_source import DataSourceSerializer
from ..serializers.v1 import TableSerializer
from django.db.models import QuerySet
from typing import cast


class DataSourceViewSet(viewsets.ModelViewSet):
	"""CRUD endpoints for DataSource (list, create, update, partial_update, retrieve, destroy*).

	Note: destroy is available by default from ModelViewSet but can be disabled if needed.
	"""

	queryset = DataSource.objects.filter(is_deleted=False)
	serializer_class = DataSourceSerializer
	permission_classes = [AllowAny]
	authentication_classes = []

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

	def perform_create(self, serializer: DataSourceSerializer) -> None:  # type: ignore[override]
		user = getattr(self.request, "user", None)
		if getattr(user, "is_authenticated", False):
			serializer.save(user=user)
		else:
			serializer.save()

	@action(detail=True, methods=["get"], url_path="status")
	def status(self, request: Request, pk: str | None = None) -> Response:
		"""Return a simple connection status for the data source.

		This mirrors the v1 status concept but uses the internal numeric id.
		"""
		ds = cast(DataSource, self.get_object())
		gid = str(getattr(ds, "global_id", "")).replace("-", "")[:10]
		payload: dict[str, object] = {
			"datasource_id": f"ds_{gid}",
			"status": "connected",
			"last_checked_at": datetime.now(timezone.utc),
			"error_message": None,
		}
		return Response(payload)

	@action(detail=True, methods=["get"], url_path="tables")
	def tables(self, request: Request, pk: str | None = None) -> Response:
		"""List tables registered for this data source.

		Endpoint: GET /api/data-sources/<id>/tables
		Returns a simple list (no pagination envelope) of table metadata.
		"""
		ds = cast(DataSource, self.get_object())
		qs = TableMetadata.objects.filter(data_source=ds, is_deleted=False).order_by("name")
		ser = TableSerializer(qs, many=True)
		# Convert DRF ReturnList to a plain list for clearer typing
		data = list(ser.data)
		return Response(data)

	@action(detail=True, methods=["get"], url_path="alerts")
	def alerts(self, request: Request, pk: str | None = None) -> Response:
		"""List alerts for this data source.

		Endpoint: GET /api/data-sources/<id>/alerts
		Currently returns an empty list until an Alert model is introduced.
		"""
		# ds is retrieved for 404 behavior on non-existent pk
		_ = cast(DataSource, self.get_object())
		# TODO: Replace with real Alert queryset and serializer when model exists
		return Response([])
