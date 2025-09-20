"""Utilities for launching Dagster runs from Django."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Mapping
from urllib import request
from urllib.error import HTTPError, URLError



class DagsterClientError(RuntimeError):
    """Raised when a Dagster GraphQL request fails."""


@dataclass
class DagsterRunLauncher:
    graphql_url: str
    location_name: str
    repository_name: str
    api_token: str | None = None
    default_mode: str = "default"
    timeout: float | None = 10.0

    logger: logging.Logger = logging.getLogger("dagster.launcher")

    @classmethod
    def from_settings(cls) -> "DagsterRunLauncher":
        from django.conf import settings

        return cls(
            graphql_url=getattr(settings, "DAGSTER_GRAPHQL_URL", "http://dagster:3000/graphql"),
            location_name=getattr(settings, "DAGSTER_LOCATION_NAME", "penguinarium"),
            repository_name=getattr(settings, "DAGSTER_REPOSITORY_NAME", "dagster_app"),
            api_token=getattr(settings, "DAGSTER_API_TOKEN", None),
            default_mode=getattr(settings, "DAGSTER_DEFAULT_MODE", "default"),
            timeout=getattr(settings, "DAGSTER_GRAPHQL_TIMEOUT", 10.0),
        )

    def launch_job(
        self,
        *,
        job_name: str,
        run_config: Mapping[str, Any] | None = None,
        tags: Mapping[str, Any] | None = None,
        mode: str | None = None,
        op_selection: list[str] | None = None,
    ) -> str:
        payload = self._build_payload(
            job_name=job_name,
            run_config=run_config,
            tags=tags,
            mode=mode,
            op_selection=op_selection,
        )

        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        req = request.Request(self.graphql_url, data=data, headers=headers)

        try:
            self.logger.info("Launching Dagster job '%s'", job_name)
            with request.urlopen(req, timeout=self.timeout) as response:  # type: ignore[call-arg]
                body = response.read().decode("utf-8")
        except HTTPError as error:  # pragma: no cover - network error path
            raise DagsterClientError(
                f"Dagster responded with HTTP {error.code}: {error.reason}"
            ) from error
        except URLError as error:  # pragma: no cover - network error path
            raise DagsterClientError(f"Failed to reach Dagster GraphQL endpoint: {error.reason}") from error

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as error:
            raise DagsterClientError("Invalid JSON response from Dagster") from error

        if "errors" in payload:
            raise DagsterClientError(str(payload["errors"]))

        data = payload.get("data", {})
        launch = data.get("launchRun", {})
        typename = launch.get("__typename")
        if typename != "LaunchRunSuccess":
            raise DagsterClientError(str(launch))

        run = launch.get("run") or {}
        run_id = run.get("runId")
        if not run_id:
            raise DagsterClientError("Dagster did not return a run id")
        self.logger.info("Dagster run launched successfully: %s", run_id)
        return str(run_id)

    def _build_payload(
        self,
        *,
        job_name: str,
        run_config: Mapping[str, Any] | None,
        tags: Mapping[str, Any] | None,
        mode: str | None,
        op_selection: list[str] | None,
    ) -> Mapping[str, Any]:
        selector: dict[str, Any] = {
            "repositoryLocationName": self.location_name,
            "repositoryName": self.repository_name,
            "pipelineName": job_name,
        }
        if op_selection:
            selector["solidSelection"] = op_selection

        tag_list = [
            {"key": key, "value": str(value)}
            for key, value in (tags or {}).items()
        ]

        execution_params: dict[str, Any] = {
            "selector": selector,
            "runConfigData": run_config or {},
        }
        execution_mode = mode or self.default_mode
        if execution_mode:
            execution_params["mode"] = execution_mode
        if tag_list:
            execution_params["tags"] = tag_list

        return {
            "query": (
                "mutation LaunchRun($executionParams: RunExecutionParams!) {"
                "  launchRun(executionParams: $executionParams) {"
                "    __typename"
                "    ... on LaunchRunSuccess { run { runId } }"
                "    ... on PythonError { message stack }"
                "    ... on LaunchRunError { error { message } }"
                "  }"
                "}"
            ),
            "variables": {"executionParams": execution_params},
        }


__all__ = ["DagsterClientError", "DagsterRunLauncher"]
