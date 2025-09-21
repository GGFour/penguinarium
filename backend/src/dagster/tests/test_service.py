from __future__ import annotations

import json
from typing import Any, Dict
from unittest import TestCase, mock

from dagster.service import trigger_job


def _mk_response(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")


class DummyHTTPResponse:
    def __init__(self, payload: Dict[str, Any]):
        self._payload: Dict[str, Any] = payload

    def read(self) -> bytes:
        return _mk_response(self._payload)

    def __enter__(self) -> "DummyHTTPResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class TestService(TestCase):
    def test_trigger_job_launchRun_success(self) -> None:
        # repositories discovery returns our job
        repos_payload: Dict[str, Any] = {
            "data": {
                "repositoriesOrError": {
                    "nodes": [
                        {
                            "name": "repo",
                            "location": {"name": "loc"},
                            "jobs": [{"name": "statistics_job"}],
                        }
                    ]
                }
            }
        }
        launch_payload: Dict[str, Any] = {
            "data": {
                "launchRun": {"__typename": "LaunchRunSuccess", "run": {"runId": "RUN123"}}
            }
        }

        seq = [DummyHTTPResponse(repos_payload), DummyHTTPResponse(launch_payload)]

        def fake_urlopen(req: object, timeout: float = 5.0) -> DummyHTTPResponse:
            return seq.pop(0)

        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            res = trigger_job("statistics_job", config={"a": 1}, tags={"env": "test"})
            self.assertEqual(res["run_id"], "RUN123")
            self.assertEqual(res["status"], "submitted")

    def test_trigger_job_raises_when_unreachable(self) -> None:
        with mock.patch("urllib.request.urlopen", side_effect=OSError("no route")):
            with self.assertRaises(RuntimeError):
                trigger_job("statistics_job")
