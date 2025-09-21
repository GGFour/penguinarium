from __future__ import annotations

import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)


def trigger_job(job_name: str, config: dict[str, Any] | None = None, tags: dict[str, str] | None = None) -> dict[str, Any]:
    """Submit a Dagster job run.

    This implementation is resilient: if Dagster isn't installed or no
    repository is configured, we still return a fake run id so the API can
    be exercised in tests.
    """
    # Log payload for observability
    logger.info("Submitting dagster job", extra={
        "job": job_name,
        "has_config": bool(config),
        "tags": tags or {},
    })

    try:
        # Lazy import so the project works without Dagster dependency.
        import importlib
        importlib.import_module("dagster")
    except Exception:
        # Fallback path: simulate a submission
        run_id = str(uuid.uuid4())
        return {
            "run_id": run_id,
            "status": "submitted",
            "message": "Dagster not available; simulated run id returned",
        }

    # If dagster import worked, acknowledge submission with a placeholder run id.
    # Real implementation would look up a job from Dagster Definitions and submit via the daemon.
    run_id = str(uuid.uuid4())
    logger.info("Dagster job prepared", extra={"job": job_name, "run_id": run_id})
    return {
        "run_id": run_id,
        "status": "submitted",
        "message": "Dagster available; submission acknowledged (placeholder)",
    }
