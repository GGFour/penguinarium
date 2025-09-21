from __future__ import annotations

from unittest import mock

from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase


class TestDagsterRunView(APITestCase):
    def test_post_returns_202_with_run_id(self):
        with mock.patch("dagster.views.trigger_job", return_value={"run_id": "r1", "status": "submitted"}) as m:
            url = reverse("dagster-run-job", args=["statistics_job"])  # type: ignore[arg-type]
            resp = self.client.post(url, data={"config": {"k": 1}, "tags": {"env": "test"}}, format="json")
            self.assertEqual(resp.status_code, status.HTTP_202_ACCEPTED)
            self.assertEqual(resp.data.get("run_id"), "r1")
            m.assert_called_once()

    def test_get_returns_202_with_run_id(self):
        with mock.patch("dagster.views.trigger_job", return_value={"run_id": "r2", "status": "submitted"}) as m:
            url = reverse("dagster-run-job", args=["statistics_job"])  # type: ignore[arg-type]
            resp = self.client.get(url)
            self.assertEqual(resp.status_code, status.HTTP_202_ACCEPTED)
            self.assertEqual(resp.data.get("run_id"), "r2")
            m.assert_called_once()

    def test_legacy_route_supported(self):
        with mock.patch("dagster.views.trigger_job", return_value={"run_id": "r3", "status": "submitted"}) as m:
            url = reverse("dagster-run-job-legacy", args=["statistics_job"])  # type: ignore[arg-type]
            resp = self.client.get(url)
            self.assertEqual(resp.status_code, status.HTTP_202_ACCEPTED)
            self.assertEqual(resp.data.get("run_id"), "r3")
            m.assert_called_once()

    def test_invalid_job_name_returns_400(self):
        url = reverse("dagster-run-job", args=["bad name !!"])  # type: ignore[arg-type]
        resp = self.client.post(url)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("error", resp.data)
