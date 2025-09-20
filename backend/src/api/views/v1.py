# pyright: reportMissingTypeArgument=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportIncompatibleMethodOverride=false, reportGeneralTypeIssues=false
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from django.contrib.auth import get_user_model
from django.http import Http404
import re
from rest_framework.views import APIView
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from rest_framework import status
from rest_framework.generics import ListAPIView, RetrieveAPIView

from pulling.models.data_source import DataSource
from pulling.models.table_metadata import TableMetadata
from pulling.models import Alert

from ..serializers.v1 import (
    UserSerializer,
    DataSourceV1Serializer,
    DataSourceStatusSerializer,
    AlertSerializer,
    TableSerializer,
)
from ..pagination import EnvelopeLimitOffsetPagination
from django.db.models.functions import Cast
from django.db.models import CharField


USER_ID_RE = re.compile(r"^user_(\d+)$")
DS_ID_RE = re.compile(r"^ds_([A-Za-z0-9]+)$")


def parse_user_id(user_id: str) -> int:
    m = USER_ID_RE.match(user_id)
    if not m:
        raise Http404("User not found")
    return int(m.group(1))


def ds_lookup_from_public_id(public_id: str) -> DataSource:
    m = DS_ID_RE.match(public_id)
    if not m:
        raise Http404("Data source not found")
    prefix = m.group(1)
    qs = DataSource.objects.annotate(gid_str=Cast("global_id", CharField()))
    try:
        return qs.get(gid_str__startswith=prefix)
    except DataSource.DoesNotExist:
        raise Http404("Data source not found")


def _gid10_from(obj: object) -> str:
    try:
        return str(getattr(obj, "global_id", "")).replace("-", "")[:10]
    except Exception:
        return ""



# Auth is temporarily disabled globally via settings; keep imports minimal here


class UsersCreateView(APIView):
    permission_classes = [AllowAny]

    def post(self, request: Request):
        User = get_user_model()
        email = request.data.get("email")
        if not email:
            return Response({"error": {"code": "invalid_parameter", "message": "'email' is required", "target": "email"}}, status=400)

        # Use email as username fallback; avoid stub issues by using create()
        user = User.objects.create(username=email, email=email)
        # Try to set name if supported
        # Optional: In a real app we'd store name fields; keeping minimal here per stubs.

        data = UserSerializer(user).data
        return Response(data, status=status.HTTP_201_CREATED)


class UsersRetrieveView(RetrieveAPIView[Any]):
    serializer_class = UserSerializer
    permission_classes = [AllowAny]

    def get_object(self):
        uid = parse_user_id(self.kwargs["user_id"])
        User = get_user_model()
        try:
            return User.objects.get(id=uid)
        except User.DoesNotExist:
            raise Http404("User not found")


class UserDataSourcesListView(ListAPIView[Any]):
    serializer_class = DataSourceV1Serializer
    permission_classes = [AllowAny]
    pagination_class = EnvelopeLimitOffsetPagination

    def get_queryset(self):
        uid = parse_user_id(self.kwargs["user_id"])
        return DataSource.objects.filter(user_id=uid, is_deleted=False).order_by("name")


class DataSourceRetrieveView(RetrieveAPIView[Any]):
    serializer_class = DataSourceV1Serializer
    permission_classes = [AllowAny]

    def get_object(self):
        ds = ds_lookup_from_public_id(self.kwargs["datasource_id"])
        return ds


class DataSourceStatusView(APIView):
    permission_classes = [AllowAny]

    def get(self, request: Request, datasource_id: str):
        ds = ds_lookup_from_public_id(datasource_id)
        gid = _gid10_from(ds)
        payload: dict[str, Any] = {
            "datasource_id": f"ds_{gid[:10]}",
            "status": "connected",
            "last_checked_at": datetime.now(timezone.utc),
            "error_message": None,
        }
        return Response(DataSourceStatusSerializer(payload).data)


class DataSourceTablesListView(ListAPIView[Any]):
    serializer_class = TableSerializer
    permission_classes = [AllowAny]
    pagination_class = EnvelopeLimitOffsetPagination

    def get_queryset(self):
        ds = ds_lookup_from_public_id(self.kwargs["datasource_id"])  # raises 404 if invalid
        return TableMetadata.objects.filter(data_source=ds, is_deleted=False).order_by("name")


class DataSourceAlertsListView(ListAPIView[Any]):
    serializer_class = AlertSerializer
    permission_classes = [AllowAny]
    pagination_class = EnvelopeLimitOffsetPagination

    def get_queryset(self):
        ds = ds_lookup_from_public_id(self.kwargs["datasource_id"])  # raises 404 if invalid
        return Alert.objects.filter(data_source=ds, is_deleted=False).order_by("-triggered_at")


class AlertRetrieveView(APIView):
    permission_classes = [AllowAny]

    def get(self, request: Request, alert_id: str):
        # Expect format al_<prefix>
        if not re.match(r"^al_[A-Za-z0-9]+$", alert_id or ""):
            raise Http404("Alert not found")
        prefix = (alert_id.split("_", 1)[1] if "_" in alert_id else "").lower()
        try:
            # Match by global_id prefix similar to DataSource lookup
            obj = Alert.objects.get(global_id__istartswith=prefix)
        except Alert.DoesNotExist:
            raise Http404("Alert not found")
        return Response(AlertSerializer(obj).data)
