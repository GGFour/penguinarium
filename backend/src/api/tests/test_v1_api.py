from __future__ import annotations

from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework.test import APITestCase
from rest_framework import status
from pulling.models.data_source import DataSource
from pulling.models.table_metadata import TableMetadata
from pulling.models import Alert
from datetime import datetime, timezone, timedelta


def ds_public_id(ds: DataSource) -> str:
    gid = str(ds.global_id).replace("-", "")
    return f"ds_{gid[:10]}"


class V1ApiTests(APITestCase):
    def setUp(self):
        User = get_user_model()
        # Users
        self.user = User.objects.create(username="u1@example.com", email="u1@example.com")
        self.user2 = User.objects.create(username="u2@example.com", email="u2@example.com")
    # No keys needed when auth is disabled

        # Data sources for user1 and user2
        self.ds1 = DataSource.objects.create(
            user=self.user,
            name="Production Analytics DB",
            type=DataSource.DataSourceType.DATABASE,
            connection_info={"host": "localhost"},
        )
        self.ds2 = DataSource.objects.create(
            user=self.user2,
            name="Another DS",
            type=DataSource.DataSourceType.API,
            connection_info={"base_url": "https://api"},
        )

        # Tables for ds1
        TableMetadata.objects.create(
            data_source=self.ds1,
            name="user_events",
            metadata={"schema_name": "public", "row_count": 123},
        )
        TableMetadata.objects.create(
            data_source=self.ds1,
            name="orders",
            metadata={"schema": "sales", "row_count": 45},
        )

        # Alerts for ds1
        self.alert1 = Alert.objects.create(
            data_source=self.ds1,
            table=None,
            field=None,
            name="Row count dropped",
            severity=Alert.Severity.WARNING,
            status=Alert.Status.ACTIVE,
            details={"table": "orders", "expected_min": 50, "actual": 45},
            triggered_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        self.alert2 = Alert.objects.create(
            data_source=self.ds1,
            table=None,
            field=None,
            name="Freshness exceeded",
            severity=Alert.Severity.CRITICAL,
            status=Alert.Status.RESOLVED,
            details={"table": "user_events", "max_age_m": 60, "actual_age_m": 120},
            triggered_at=datetime.now(timezone.utc) - timedelta(hours=2),
        )

    def auth(self, key: str):
        # No-op: authentication disabled
        self.client.credentials()

    def test_users_create(self):
        url = reverse("v1-users-create")
        resp = self.client.post(url, {"name": "John Smith", "email": "john.smith@example.com"}, format="json")
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertIn("id", resp.data)
        self.assertIn("created_at", resp.data)

    def test_users_create_missing_email(self):
        url = reverse("v1-users-create")
        resp = self.client.post(url, {"name": "No Email"}, format="json")
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("error", resp.data)

    def test_auth_not_required_anymore(self):
        # With auth disabled globally, endpoints should be accessible
        self.client.credentials()
        url = reverse("v1-users-retrieve", args=[f"user_{self.user.id}"])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

    def test_users_retrieve_not_found(self):
        url = reverse("v1-users-retrieve", args=["user_999999"])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)

    def test_get_user_self(self):
        self.client.credentials()
        url = reverse("v1-users-retrieve", args=[f"user_{self.user.id}"])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data["id"], f"user_{self.user.id}")
        self.assertIn("created_at", resp.data)

    def test_user_datasources_list_with_pagination(self):
        # Add more DS for user to test pagination
        for i in range(3):
            DataSource.objects.create(
                user=self.user,
                name=f"extra-{i}",
                type=DataSource.DataSourceType.API,
                connection_info={},
            )
        self.client.credentials()
        url = reverse("v1-user-datasources", args=[f"user_{self.user.id}"])
        resp = self.client.get(url + "?limit=2&offset=0")
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertIn("data", resp.data)
        self.assertIn("pagination", resp.data)
        self.assertEqual(resp.data["pagination"]["limit"], 2)
        # Only owned DS should be included
        owned_ids = {item["id"] for item in resp.data["data"]}
        self.assertIn(ds_public_id(self.ds1), owned_ids)

    def test_datasource_retrieve_and_status(self):
        self.client.credentials()
        ds_id = ds_public_id(self.ds1)
        # retrieve
        url = reverse("v1-datasource-retrieve", args=[ds_id])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data["id"], ds_id)
        self.assertEqual(resp.data["name"], self.ds1.name)
        # status
        url = reverse("v1-datasource-status", args=[ds_id])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data["datasource_id"], ds_id)
    # With rate limiting disabled, no rate limit headers are guaranteed

    def test_tables_list_mapping(self):
        self.client.credentials()
        ds_id = ds_public_id(self.ds1)
        url = reverse("v1-datasource-tables", args=[ds_id])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertIsInstance(resp.data["data"], list)
        # Check mapping of first entry
        item = resp.data["data"][0]
        self.assertIn("schema_name", item)
        self.assertIn("table_name", item)
        self.assertIn("row_count", item)
        self.assertIn("last_updated_at", item)

    def test_alerts_list_has_items(self):
        self.client.credentials()
        ds_id = ds_public_id(self.ds1)
        url = reverse("v1-datasource-alerts", args=[ds_id]) + "?limit=1&offset=0"
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data["pagination"]["limit"], 1)
        self.assertGreaterEqual(resp.data["pagination"]["total"], 2)
        self.assertGreaterEqual(len(resp.data["data"]), 1)
        self.assertIn("pagination", resp.data)

    def test_v1_alert_retrieve_404_shape(self):
        # Unknown alert id should return 404 with normalized error payload
        url = reverse("v1-alert-retrieve", args=["alert_NON_EXISTENT"])  # format is arbitrary here
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn("error", resp.data)
        self.assertIn("code", resp.data["error"])
        self.assertIn("message", resp.data["error"])

    def test_alert_retrieve_success(self):
        # Build public id from global_id prefix
        gid = str(self.alert1.global_id).replace("-", "")[:10]
        url = reverse("v1-alert-retrieve", args=[f"al_{gid}"])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data["name"], self.alert1.name)

    def test_v1_datasource_alerts_invalid_id(self):
        # Invalid datasource id format should 404
        url = reverse("v1-datasource-alerts", args=["ds_NONEXISTENT"])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn("error", resp.data)

    def test_error_format_404(self):
        self.client.credentials()
        url = reverse("v1-datasource-retrieve", args=["ds_NONEXISTENT"])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn("error", resp.data)
        self.assertIn("code", resp.data["error"])
        self.assertIn("message", resp.data["error"])

    def test_rate_limit_disabled(self):
        # No 429s when rate limiting is disabled
        self.client.credentials()
        ds_id = ds_public_id(self.ds1)
        url = reverse("v1-datasource-status", args=[ds_id])
        r1 = self.client.get(url)
        self.assertEqual(r1.status_code, status.HTTP_200_OK)
        r2 = self.client.get(url)
        self.assertEqual(r2.status_code, status.HTTP_200_OK)
