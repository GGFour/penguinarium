from __future__ import annotations

from typing import Any, ClassVar

from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from api.views.dagster import DagsterRunView

from .client import DagsterClientError


class DummyLauncher:
    last_call: ClassVar[dict[str, Any] | None] = None
    run_id: ClassVar[str] = "run-id"

    @classmethod
    def from_settings(cls) -> "DummyLauncher":
        return cls()

    def launch_job(self, **kwargs: Any) -> str:
        DummyLauncher.last_call = kwargs
        return self.run_id


class FailingLauncher(DummyLauncher):
    def launch_job(self, **kwargs: Any) -> str:
        raise DagsterClientError("failure")


class TestDagsterRunEndpoint(APITestCase):
    def setUp(self) -> None:
        super().setUp()
        self._original_launcher = DagsterRunView.launcher_class

    def tearDown(self) -> None:
        DagsterRunView.launcher_class = self._original_launcher
        DummyLauncher.last_call = None
        super().tearDown()

    def _set_launcher(self, launcher):
        DagsterRunView.launcher_class = launcher

    def _payload(self) -> dict[str, Any]:
        return {
            "source": {
                "type": "csv",
                "config": {"path": "/tmp"},
            },
            "tags": {"env": "test"},
        }

    def test_run_job_returns_202(self):
        self._set_launcher(DummyLauncher)
        url = reverse("dagster-run-job", args=["example_job"])
        resp = self.client.post(url, data=self._payload(), format="json")
        self.assertEqual(resp.status_code, status.HTTP_202_ACCEPTED)
        self.assertEqual(resp.data["run_id"], DummyLauncher.run_id)
        assert DummyLauncher.last_call is not None
        run_config = DummyLauncher.last_call["run_config"]
        self.assertEqual(
            run_config["ops"]["load_dataset_op"]["config"]["source"]["type"],
            "csv",
        )

    def test_run_job_invalid_name_400(self):
        self._set_launcher(DummyLauncher)
        url = reverse("dagster-run-job", args=["bad name !!"])  # type: ignore[arg-type]
        resp = self.client.post(url, data={}, format="json")
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["error"]["code"], "invalid_parameter")

    def test_run_job_missing_source(self):
        self._set_launcher(DummyLauncher)
        url = reverse("dagster-run-job", args=["simple_job"])
        resp = self.client.post(url, data={}, format="json")
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("error", resp.data)

    def test_run_job_launch_failure(self):
        self._set_launcher(FailingLauncher)
        url = reverse("dagster-run-job", args=["example_job"])
        resp = self.client.post(url, data=self._payload(), format="json")
        self.assertEqual(resp.status_code, status.HTTP_502_BAD_GATEWAY)
        self.assertEqual(resp.data["error"]["code"], "dagster_error")
