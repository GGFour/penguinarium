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

	def test_run_job_invalid_name_400(self):
		url = reverse("dagster-run-job", args=["bad name !!"])  # type: ignore[arg-type]
		resp = self.client.post(url, data={}, format="json")
		self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
		self.assertIn("error", resp.data)
		self.assertIn("code", resp.data["error"])
		self.assertEqual(resp.data["error"]["code"], "invalid_parameter")

	def test_run_job_no_body_ok(self):
		url = reverse("dagster-run-job", args=["simple_job"])  # type: ignore[arg-type]
		# No body provided
		resp = self.client.post(url)
		self.assertEqual(resp.status_code, status.HTTP_202_ACCEPTED)
		self.assertIn("run_id", resp.data)
