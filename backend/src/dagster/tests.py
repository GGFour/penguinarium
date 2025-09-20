from __future__ import annotations

from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase


class TestDagsterRunEndpoint(APITestCase):
	def test_run_job_returns_202(self):
		url = reverse("dagster-run-job", args=["example_job"])  # type: ignore[arg-type]
		resp = self.client.post(url, data={"config": {"key": "value"}, "tags": {"env": "test"}}, format="json")
		self.assertEqual(resp.status_code, status.HTTP_202_ACCEPTED)
		self.assertIn("run_id", resp.data)
		self.assertTrue(isinstance(resp.data.get("run_id"), str))
