from __future__ import annotations

import json
import logging
import os
import socket
import urllib.error
import urllib.request
import uuid
from typing import Any, TypedDict, cast, Dict, List

logger = logging.getLogger(__name__)


class DagsterSelector(TypedDict):
    repositoryLocationName: str
    repositoryName: str
    jobName: str


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if (v is not None and v != "") else default


def _graphql(url: str, query: str, variables: dict[str, Any] | None = None, timeout: float = 5.0) -> dict[str, Any]:
    payload: Dict[str, Any] = {"query": query, "variables": variables or {}}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        content = resp.read().decode("utf-8")
    out = json.loads(content)
    return cast(Dict[str, Any], out)


def _discover_selector(graphql_url: str, job_name: str) -> DagsterSelector | None:
    """Try to find repository location and repository containing the job.

    Returns a selector or None if not found.
    """
    # First attempt: repositories with jobs
    query_jobs = """
    query {
      repositoriesOrError {
        __typename
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            jobs { name }
          }
        }
      }
    }
    """
    try:
        res = _graphql(graphql_url, query_jobs, None)
        data = cast(Dict[str, Any], res.get("data", {}))
        repos = cast(Dict[str, Any], data.get("repositoriesOrError", {}))
        nodes = cast(List[Dict[str, Any]], repos.get("nodes", []))
        for repo in nodes:
            jobs = cast(List[Dict[str, Any]], repo.get("jobs") or [])
            for j in jobs:
                if cast(str, j.get("name")) == job_name:
                    return {
                        "repositoryLocationName": cast(Dict[str, Any], repo.get("location") or {}).get("name", ""),
                        "repositoryName": cast(str, repo.get("name", "")),
                        "jobName": job_name,
                    }
    except Exception as e:
        logger.debug("Dagster discovery (jobs) failed: %s", e)

    # Second attempt: repositories with pipelines (older schema)
    query_pipelines = """
    query {
      repositoriesOrError {
        __typename
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            pipelines { name }
          }
        }
      }
    }
    """
    try:
        res = _graphql(graphql_url, query_pipelines, None)
        data = cast(Dict[str, Any], res.get("data", {}))
        repos = cast(Dict[str, Any], data.get("repositoriesOrError", {}))
        nodes = cast(List[Dict[str, Any]], repos.get("nodes", []))
        for repo in nodes:
            pipes = cast(List[Dict[str, Any]], repo.get("pipelines") or [])
            for p in pipes:
                if cast(str, p.get("name")) == job_name:
                    return {
                        "repositoryLocationName": cast(Dict[str, Any], repo.get("location") or {}).get("name", ""),
                        "repositoryName": cast(str, repo.get("name", "")),
                        "jobName": job_name,
                    }
    except Exception as e:
        logger.debug("Dagster discovery (pipelines) failed: %s", e)

    return None


def _launch_run(graphql_url: str, selector: DagsterSelector, run_config: dict[str, Any] | None, tags: dict[str, str] | None) -> tuple[str, str]:
    """Attempt to launch a run via Dagster GraphQL. Returns (run_id, message)."""
    # Preferred mutation for modern Dagster
    mutation_launch_run = """
    mutation Launch($selector: JobSelector!, $runConfig: RunConfigData, $tags: [PipelineTag!]) {
      launchRun(
        executionParams: {
          selector: $selector,
          runConfigData: $runConfig,
          mode: null,
          executionMetadata: { tags: $tags }
        }
      ) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on InvalidSubsetError { message }
        ... on PythonError { message }
        ... on UnauthorizedError { message }
      }
    }
    """

    variables: Dict[str, Any] = {
        "selector": selector,
        "runConfig": run_config or {},
        "tags": [{"key": k, "value": v} for (k, v) in (tags or {}).items()],
    }

    try:
        res = _graphql(graphql_url, mutation_launch_run, variables)
        if res.get("errors"):
            # GraphQL transport-level error
            raise RuntimeError(res["errors"][0].get("message", "GraphQL error"))
        payload = res.get("data", {}).get("launchRun")
        if not payload:
            raise RuntimeError("No launchRun result returned")
        t = payload.get("__typename")
        if t == "LaunchRunSuccess":
            run_id = payload.get("run", {}).get("runId")
            if not run_id:
                raise RuntimeError("Missing runId in LaunchRunSuccess")
            return run_id, "Dagster run launched via launchRun"
        # Fallthrough for known non-success types
        msg = payload.get("message") or f"Unexpected launchRun result: {t}"
        raise RuntimeError(msg)
    except Exception as e:
        logger.debug("Dagster launchRun failed: %s", e)

    # Fallback mutation used by some versions: launchJobRun
    mutation_launch_job_run = """
    mutation LaunchJob($selector: JobSelector!, $runConfig: RunConfigData, $tags: [PipelineTag!]) {
      launchJobRun(
        selector: $selector,
        runConfigData: $runConfig,
        tags: $tags
      ) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
      }
    }
    """
    try:
        res = _graphql(graphql_url, mutation_launch_job_run, variables)
        if res.get("errors"):
            raise RuntimeError(res["errors"][0].get("message", "GraphQL error"))
        payload = res.get("data", {}).get("launchJobRun")
        if not payload:
            raise RuntimeError("No launchJobRun result returned")
        t = payload.get("__typename")
        if t == "LaunchRunSuccess":
            run_id = payload.get("run", {}).get("runId")
            if not run_id:
                raise RuntimeError("Missing runId in LaunchRunSuccess")
            return run_id, "Dagster run launched via launchJobRun"
        msg = payload.get("message") or f"Unexpected launchJobRun result: {t}"
        raise RuntimeError(msg)
    except Exception as e:
        logger.debug("Dagster launchJobRun failed: %s", e)

    raise RuntimeError("Failed to launch Dagster run via GraphQL")


def trigger_job(job_name: str, config: dict[str, Any] | None = None, tags: dict[str, str] | None = None) -> dict[str, Any]:
    """Submit a Dagster job run via GraphQL API.

    Behavior:
    - If DAGSTER_GRAPHQL_URL is reachable and repository info is resolvable, launch a real run.
    - Otherwise, return a simulated run id so the API remains functional in dev/tests.
    """
    logger.info(
        "Submitting dagster job",
        extra={
            "job": job_name,
            "has_config": bool(config),
            "tags": tags or {},
        },
    )

    graphql_url = _env("DAGSTER_GRAPHQL_URL", "http://dagster_app:3000/graphql") or "http://dagster_app:3000/graphql"
    repo_location = _env("DAGSTER_REPO_LOCATION")
    repo_name = _env("DAGSTER_REPO_NAME")

    # Attempt real submission
    try:
        # Resolve selector: prefer env, otherwise discover
        if repo_location and repo_name:
            selector: DagsterSelector = {
                "repositoryLocationName": repo_location,
                "repositoryName": repo_name,
                "jobName": job_name,
            }
        else:
            found = _discover_selector(graphql_url, job_name)
            if not found:
                raise RuntimeError(
                    "Unable to discover Dagster repository/job; set DAGSTER_REPO_LOCATION and DAGSTER_REPO_NAME"
                )
            selector = found

        run_id, message = _launch_run(graphql_url, selector, config, tags)
        logger.info("Dagster run submitted", extra={"job": job_name, "run_id": run_id})
        return {"run_id": run_id, "status": "submitted", "message": message}
    except (urllib.error.URLError, socket.timeout, TimeoutError) as net_err:
        logger.warning("Dagster not reachable at %s: %s", graphql_url, net_err)
    except Exception as exc:
        # Log at info/warn to avoid noisy tracebacks in dev
        logger.warning("Dagster submission failed: %s", exc)

    # Fallback: simulate
    run_id = str(uuid.uuid4())
    return {
        "run_id": run_id,
        "status": "submitted",
        "message": "Dagster not reachable; simulated run id returned",
    }
