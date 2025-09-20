from __future__ import annotations

from dagster import job, op

from ..ops.agentic_checks import agentic_loop
from ..ops.extract import extract_schema
from ..ops.static_checks import compute_static_stats


@op
def agentic_params():
    return {"max_steps": 3}


@job
def agentic_checker_job():
    schemas = extract_schema(agentic_params())
    stats = compute_static_stats(schemas)
    agentic_loop(schemas, stats, agentic_params())
