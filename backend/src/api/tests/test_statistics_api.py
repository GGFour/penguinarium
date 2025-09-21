from datetime import datetime, timedelta, timezone

from django.test import TestCase
from rest_framework.test import APIClient
from rest_framework import status

from pulling.models import DataSource, TableMetadata, FieldMetadata
from pulling.models.field_stats import FieldStats


class FieldStatsAPITests(TestCase):
    databases = {"default"}

    def setUp(self) -> None:
        self.client = APIClient()
        self.ds = DataSource.objects.create(
            name="DS",
            type=DataSource.DataSourceType.DATABASE,
            connection_info={"engine": "sqlite"},
        )
        self.tbl = TableMetadata.objects.create(
            data_source=self.ds,
            name="users",
            description="",
            metadata={},
        )
        self.col = FieldMetadata.objects.create(
            table=self.tbl,
            name="age",
            dtype=FieldMetadata.DataType.INTEGER,
            metadata={},
        )

        now = datetime.now(timezone.utc)
        self.s1 = FieldStats.objects.create(field=self.col, stat_date=now - timedelta(days=2), value={"count": 100, "min": 18})
        self.s2 = FieldStats.objects.create(field=self.col, stat_date=now - timedelta(days=1), value={"count": 120, "min": 17})

    def test_list_statistics(self):
        res = self.client.get("/api/statistics/")
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertIn("data", res.data)
        self.assertIn("pagination", res.data)
        self.assertGreaterEqual(res.data["pagination"]["total"], 2)

    def test_filter_by_column(self):
        res = self.client.get(f"/api/statistics/?column={self.col.field_metadata_id}")
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        ids = [row["id"] for row in res.data["data"]]
        self.assertIn(self.s1.field_stats_id, ids)
        self.assertIn(self.s2.field_stats_id, ids)

    def test_pagination(self):
        res = self.client.get("/api/statistics/?limit=1")
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(len(res.data["data"]), 1)
        self.assertEqual(res.data["pagination"]["limit"], 1)

    def test_retrieve_single(self):
        res = self.client.get(f"/api/statistics/{self.s1.field_stats_id}/")
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertEqual(res.data["id"], self.s1.field_stats_id)
        self.assertEqual(res.data["column_id"], self.col.field_metadata_id)
        self.assertIsInstance(res.data["value"], dict)
