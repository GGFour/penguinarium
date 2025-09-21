from __future__ import annotations

import json
import logging
import os
import socket
import urllib.error
import urllib.request
import uuid
from typing import Any, TypedDict, cast, Dict, List, Optional

logger = logging.getLogger(__name__)


class DagsterSelector(TypedDict):
    repositoryLocationName: str
    repositoryName: str
    jobName: str


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if (v is not None and v != "") else default


def _safe_truncate(s: str, n: int = 500) -> str:
    """Truncate long strings for safe logging."""
    return s if len(s) <= n else s[: n - 3] + "..."


def _dict_keys(d: Optional[dict[str, Any]]) -> List[str]:
    return list((d or {}).keys())


def _graphql(
    url: str,
    query: str,
    variables: dict[str, Any] | None = None,
    timeout: float = 10.0,
    attempt_id: str | None = None,
) -> dict[str, Any]:
    payload: Dict[str, Any] = {"query": query, "variables": variables or {}}
    data = json.dumps(payload).encode("utf-8")
    # Allow custom headers (e.g., auth) via env var with JSON content
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    try:
        hdr_env = os.getenv("DAGSTER_GRAPHQL_HEADERS_JSON")
        if hdr_env:
            headers.update(cast(Dict[str, str], json.loads(hdr_env)))
    except Exception as e:
        logger.warning("Invalid DAGSTER_GRAPHQL_HEADERS_JSON: %s", e)

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    logger.debug(
        "GraphQL request",
        extra={
            "attempt_id": attempt_id,
            "url": url,
            "timeout": timeout,
            "query_preview": _safe_truncate(query.replace("\n", " "), 200),
            "variables_keys": _dict_keys(variables),
            "headers_keys": list(headers.keys()),
        },
    )
    import time

    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        # Many GraphQL servers return 400 with a JSON body containing errors
        try:
            body = e.read().decode("utf-8")
        except Exception:
            body = ""
        logger.debug(
            "GraphQL HTTPError",
            extra={
                "attempt_id": attempt_id,
                "url": url,
                "status": getattr(e, "code", None),
                "reason": getattr(e, "reason", None),
                "body_preview": _safe_truncate(body.replace("\n", " "), 500),
            },
        )
        # If body looks like JSON, return it to the caller so they can extract 'errors'
        try:
            out = json.loads(body)
            duration_s = time.perf_counter() - start
            logger.debug(
                "GraphQL error response parsed",
                extra={
                    "attempt_id": attempt_id,
                    "url": url,
                    "duration_ms": round(duration_s * 1000, 2),
                    "has_errors": bool(out.get("errors")),
                    "data_keys": list(out.get("data", {}).keys()) if isinstance(out.get("data"), dict) else None,
                },
            )
            return cast(Dict[str, Any], out)
        except Exception:
            raise
    duration_s = time.perf_counter() - start
    out = json.loads(content)
    logger.debug(
        "GraphQL response",
        extra={
            "attempt_id": attempt_id,
            "url": url,
            "duration_ms": round(duration_s * 1000, 2),
            "has_errors": bool(out.get("errors")),
            "data_keys": list(out.get("data", {}).keys()) if isinstance(out.get("data"), dict) else None,
        },
    )
    return cast(Dict[str, Any], out)


def _discover_selector(graphql_url: str, job_name: str, attempt_id: str | None = None) -> DagsterSelector | None:
    """Try to find repository location and repository containing the job.

    Returns a selector or None if not found.
    """
    # Attempt A: direct jobs listing with repository context (modern schema)
    query_jobs_nodes = """
    query {
      jobsOrError {
        __typename
        ... on Jobs {
          nodes {
            name
            repository { name location { name } }
          }
        }
      }
    }
    """
    logger.debug("Discovery A: jobsOrError.nodes", extra={"attempt_id": attempt_id, "url": graphql_url, "job": job_name})
    try:
        res = _graphql(graphql_url, query_jobs_nodes, None, attempt_id=attempt_id)
        data = cast(Dict[str, Any], res.get("data", {}))
        jobs_or_error = cast(Dict[str, Any], data.get("jobsOrError", {}))
        nodes = cast(List[Dict[str, Any]], jobs_or_error.get("nodes", []))
        for node in nodes:
            if cast(str, node.get("name")) == job_name:
                repo = cast(Dict[str, Any], node.get("repository") or {})
                loc = cast(Dict[str, Any], repo.get("location") or {})
                return {
                    "repositoryLocationName": cast(str, loc.get("name", "")),
                    "repositoryName": cast(str, repo.get("name", "")),
                    "jobName": job_name,
                }
    except Exception as e:
        logger.debug("Dagster discovery (jobsOrError.nodes) failed: %s", e)

    # Attempt B: repositories with jobs
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
    logger.debug("Discovery B: repositories.jobs", extra={"attempt_id": attempt_id, "url": graphql_url, "job": job_name})
    try:
        res = _graphql(graphql_url, query_jobs, None, attempt_id=attempt_id)
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

    # Attempt C: repositories with pipelines (older schema)
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
    logger.debug("Discovery C: repositories.pipelines", extra={"attempt_id": attempt_id, "url": graphql_url, "job": job_name})
    try:
        res = _graphql(graphql_url, query_pipelines, None, attempt_id=attempt_id)
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

    # Attempt D: repository locations list with nested repositories
    query_repo_locs = """
    query {
      repositoryLocationsOrError {
        __typename
        ... on RepositoryLocationConnection {
          nodes {
            name
            repositories {
              name
              jobs { name }
              pipelines { name }
            }
          }
        }
      }
    }
    """
    logger.debug("Discovery D: repositoryLocations", extra={"attempt_id": attempt_id, "url": graphql_url, "job": job_name})
    try:
        res = _graphql(graphql_url, query_repo_locs, None, attempt_id=attempt_id)
        data = cast(Dict[str, Any], res.get("data", {}))
        rloe = cast(Dict[str, Any], data.get("repositoryLocationsOrError", {}))
        nodes = cast(List[Dict[str, Any]], rloe.get("nodes", []))
        for node in nodes:
            loc_name = cast(str, node.get("name", ""))
            repos = cast(List[Dict[str, Any]], node.get("repositories", []))
            for repo in repos:
                rname = cast(str, repo.get("name", ""))
                jobs = cast(List[Dict[str, Any]], repo.get("jobs") or [])
                for j in jobs:
                    if cast(str, j.get("name")) == job_name:
                        return {
                            "repositoryLocationName": loc_name,
                            "repositoryName": rname,
                            "jobName": job_name,
                        }
                pipes = cast(List[Dict[str, Any]], repo.get("pipelines") or [])
                for p in pipes:
                    if cast(str, p.get("name")) == job_name:
                        return {
                            "repositoryLocationName": loc_name,
                            "repositoryName": rname,
                            "jobName": job_name,
                        }
    except Exception as e:
        logger.debug("Dagster discovery (repositoryLocationsOrError) failed: %s", e)

    # Attempt E: workspace entries -> repository locations -> repositories
    query_workspace = """
    query {
        workspaceOrError {
            __typename
            ... on Workspace {
                locationEntries {
                    name
                    locationOrLoadError {
                        __typename
                        ... on RepositoryLocation {
                            name
                            repositories {
                                name
                                jobs { name }
                                pipelines { name }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    logger.debug("Discovery E: workspace.locationEntries", extra={"attempt_id": attempt_id, "url": graphql_url, "job": job_name})
    try:
        res = _graphql(graphql_url, query_workspace, None, attempt_id=attempt_id)
        data = cast(Dict[str, Any], res.get("data", {}))
        ws = cast(Dict[str, Any], data.get("workspaceOrError", {}))
        entries = cast(List[Dict[str, Any]], ws.get("locationEntries", []))
        for entry in entries:
            loc = cast(Dict[str, Any], entry.get("locationOrLoadError", {}))
            if loc.get("__typename") != "RepositoryLocation":
                continue
            loc_name = cast(str, loc.get("name", ""))
            repos = cast(List[Dict[str, Any]], loc.get("repositories", []))
            for repo in repos:
                rname = cast(str, repo.get("name", ""))
                # Prefer jobs, then pipelines
                jobs = cast(List[Dict[str, Any]], repo.get("jobs") or [])
                for j in jobs:
                    if cast(str, j.get("name")) == job_name:
                        return {
                            "repositoryLocationName": loc_name,
                            "repositoryName": rname,
                            "jobName": job_name,
                        }
                pipes = cast(List[Dict[str, Any]], repo.get("pipelines") or [])
                for p in pipes:
                    if cast(str, p.get("name")) == job_name:
                        return {
                            "repositoryLocationName": loc_name,
                            "repositoryName": rname,
                            "jobName": job_name,
                        }
    except Exception as e:
        logger.debug("Dagster discovery (workspace) failed: %s", e)

    return None


def _launch_run(
    graphql_url: str,
    selector: DagsterSelector,
    run_config: dict[str, Any] | None,
    tags: dict[str, str] | None,
    attempt_id: str | None = None,
    mode: Optional[str] | None = None,
) -> tuple[str, str]:
    """Attempt to launch a run via Dagster GraphQL. Returns (run_id, message)."""

    # Preferred mutation for modern Dagster (launchRun)
    mutation_launch_run = """
    mutation Launch($executionParams: ExecutionParams!) {
      launchRun(executionParams: $executionParams) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on InvalidSubsetError { message }
        ... on PythonError { message }
        ... on UnauthorizedError { message }
      }
    }
    """

    exec_params: Dict[str, Any] = {
        "selector": selector,
        "runConfigData": run_config or {},
    }
    if mode is not None:
        exec_params["mode"] = mode
    variables: Dict[str, Any] = {"executionParams": exec_params}

    logger.info(
        "Launching Dagster run (launchRun)",
        extra={
            "attempt_id": attempt_id,
            "url": graphql_url,
            "selector": selector,
            "run_config_keys": _dict_keys(run_config),
            "tags": tags,
        },
    )
    try:
        res = _graphql(graphql_url, mutation_launch_run, variables, attempt_id=attempt_id)
        if res.get("errors"):
            raise RuntimeError(res["errors"][0].get("message", "GraphQL error"))
        payload = res.get("data", {}).get("launchRun")
        if not payload:
            raise RuntimeError("No launchRun result returned")
        t = payload.get("__typename")
        if t == "LaunchRunSuccess":
            run_id = payload.get("run", {}).get("runId")
            if not run_id:
                raise RuntimeError("Missing runId in LaunchRunSuccess")
            logger.info(
                "Dagster run launched",
                extra={"attempt_id": attempt_id, "url": graphql_url, "run_id": run_id, "via": "launchRun"},
            )
            return run_id, "Dagster run launched via launchRun"
        msg = payload.get("message") or f"Unexpected launchRun result: {t}"
        raise RuntimeError(msg)
    except Exception as e:
        logger.debug("Dagster launchRun failed: %s", e)

    # Fallback mutation used by some versions: launchJobRun
    mutation_launch_job_run = """
    mutation LaunchJob($executionParams: ExecutionParams!) {
      launchJobRun(executionParams: $executionParams) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
      }
    }
    """
    logger.info(
        "Launching Dagster run (launchJobRun)",
        extra={
            "attempt_id": attempt_id,
            "url": graphql_url,
            "selector": selector,
            "run_config_keys": _dict_keys(run_config),
            "tags": tags,
        },
    )
    try:
        res = _graphql(graphql_url, mutation_launch_job_run, variables, attempt_id=attempt_id)
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
            logger.info(
                "Dagster run launched",
                extra={"attempt_id": attempt_id, "url": graphql_url, "run_id": run_id, "via": "launchJobRun"},
            )
            return run_id, "Dagster run launched via launchJobRun"
        msg = payload.get("message") or f"Unexpected launchJobRun result: {t}"
        raise RuntimeError(msg)
    except Exception as e:
        logger.debug("Dagster launchJobRun failed: %s", e)

    # Final fallback for older Dagster: PipelineSelector + launchPipelineExecution
    mutation_launch_pipeline = """
    mutation LaunchPipeline($executionParams: ExecutionParams!) {
      launchPipelineExecution(executionParams: $executionParams) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
      }
    }
    """

    pipeline_selector: Dict[str, Any] = {
        "repositoryLocationName": selector["repositoryLocationName"],
        "repositoryName": selector["repositoryName"],
        "pipelineName": selector["jobName"],
    }
    exec_params2: Dict[str, Any] = {
        "selector": pipeline_selector,
        "runConfigData": run_config or {},
    }
    if mode is not None:
        exec_params2["mode"] = mode
    variables2: Dict[str, Any] = {"executionParams": exec_params2}
    logger.info(
        "Launching Dagster run (launchPipelineExecution)",
        extra={
            "attempt_id": attempt_id,
            "url": graphql_url,
            "pipeline_selector": pipeline_selector,
            "run_config_keys": _dict_keys(run_config),
            "tags": tags,
        },
    )
    try:
        res = _graphql(graphql_url, mutation_launch_pipeline, variables2, attempt_id=attempt_id)
        if res.get("errors"):
            raise RuntimeError(res["errors"][0].get("message", "GraphQL error"))
        payload = res.get("data", {}).get("launchPipelineExecution")
        if not payload:
            raise RuntimeError("No launchPipelineExecution result returned")
        t = payload.get("__typename")
        if t == "LaunchRunSuccess":
            run_id = payload.get("run", {}).get("runId")
            if not run_id:
                raise RuntimeError("Missing runId in LaunchRunSuccess")
            logger.info(
                "Dagster run launched",
                extra={"attempt_id": attempt_id, "url": graphql_url, "run_id": run_id, "via": "launchPipelineExecution"},
            )
            return run_id, "Dagster run launched via launchPipelineExecution"
        msg = payload.get("message") or f"Unexpected launchPipelineExecution result: {t}"
        raise RuntimeError(msg)
    except Exception as e:
        logger.debug("Dagster launchPipelineExecution failed: %s", e)

    # If all attempts failed, raise to caller (avoid returning None)
    raise RuntimeError("Failed to launch Dagster run via GraphQL")


def trigger_job(job_name: str, config: dict[str, Any] | None = None, tags: dict[str, str] | None = None) -> dict[str, Any]:
    """Submit a Dagster job run via GraphQL API.

    Behavior:
    - If DAGSTER_GRAPHQL_URL is reachable and repository info is resolvable, launch a real run.
    - Otherwise, raise an error (no simulation fallback).
    """
    attempt_id = str(uuid.uuid4())
    logger.info(
        "Submitting dagster job",
        extra={
            "attempt_id": attempt_id,
            "job": job_name,
            "has_config": bool(config),
            "tags": tags or {},
        },
    )

    # Prefer explicit env var; otherwise choose default based on environment.
    # - Inside Docker (/.dockerenv present): use the compose service name for reliable networking.
    # - Outside Docker: default to 0.0.0.0 so a locally running Dagster on all interfaces is reachable.
    # Determine candidate GraphQL URLs (comma-separated env overrides default list)
    urls_env = _env("DAGSTER_GRAPHQL_URLS")
    if urls_env:
        candidates = [u.strip() for u in urls_env.split(",") if u.strip()]
    else:
        # Single explicit URL takes precedence if provided
        single = _env("DAGSTER_GRAPHQL_URL")
        if single:
            candidates = [single]
        else:
            in_docker = os.path.exists("/.dockerenv")
            if in_docker:
                candidates = [
                    "http://dagster_app:3000/graphql",
                    "http://dagster:3000/graphql",
                    "http://dagster-webserver:3000/graphql",
                ]
            else:
                candidates = [
                    "http://localhost:3000/graphql",
                    "http://127.0.0.1:3000/graphql",
                    "http://dagster_app:3000/graphql",
                ]
    logger.debug(
        "Candidate GraphQL endpoints",
        extra={"attempt_id": attempt_id, "candidates": candidates},
    )
    repo_location = _env("DAGSTER_REPO_LOCATION")
    repo_name = _env("DAGSTER_REPO_NAME")

    # Optional run mode (legacy Dagster may require it)
    run_mode = _env("DAGSTER_RUN_MODE")

    errors: List[str] = []
    # Attempt real submission across candidates
    for graphql_url in candidates:
        try:
            # Resolve selector: prefer env, otherwise discover
            if repo_location and repo_name:
                selector: DagsterSelector = {
                    "repositoryLocationName": repo_location,
                    "repositoryName": repo_name,
                    "jobName": job_name,
                }
                logger.info(
                    "Using selector from env",
                    extra={"attempt_id": attempt_id, "selector": selector, "url": graphql_url},
                )
            else:
                logger.debug(
                    "Discovering selector",
                    extra={"attempt_id": attempt_id, "url": graphql_url, "job": job_name},
                )
                found = _discover_selector(graphql_url, job_name, attempt_id=attempt_id)
                if not found:
                    raise RuntimeError(
                        f"Unable to discover Dagster repository/job at {graphql_url}; set DAGSTER_REPO_LOCATION and DAGSTER_REPO_NAME"
                    )
                selector = found
                logger.info(
                    "Discovered selector",
                    extra={"attempt_id": attempt_id, "selector": selector, "url": graphql_url},
                )

            # If a mode is specified by env, include it by temporarily wrapping run_config (handled inside _launch_run already with mode=None)
            if run_mode:
                # Attach as tag to aid debugging (GraphQL param still sends mode=null for jobs API)
                tags = {**(tags or {}), "dagster.run.mode": run_mode}

            run_id, message = _launch_run(
                graphql_url,
                selector,
                config,
                tags,
                attempt_id=attempt_id,
                mode=run_mode,
            )
            logger.info("Dagster run submitted", extra={"job": job_name, "run_id": run_id, "url": graphql_url})
            return {"run_id": run_id, "status": "submitted", "message": message}
        except (urllib.error.URLError, socket.timeout, TimeoutError) as net_err:
            msg = f"Dagster not reachable at {graphql_url}: {net_err}"
            logger.warning(msg)
            errors.append(msg)
            continue
        except Exception as exc:
            # Capture and continue to next candidate
            msg = f"Dagster submission failed at {graphql_url}: {exc}"
            logger.warning(msg)
            errors.append(msg)
            continue

    # No simulation fallback: fail with accumulated errors
    logger.error(
        "Dagster submission failed across all endpoints",
        extra={"attempt_id": attempt_id, "errors": errors},
    )
    raise RuntimeError("; ".join(errors) if errors else "Dagster submission failed: unknown error")
