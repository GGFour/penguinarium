from __future__ import annotations

from dagster import job, op

from ..ops.extract import extract_schema
from ..ops.static_checks import compute_static_stats


@op
def start_params():
    return {}


@job
def static_checker_job():
    schemas = extract_schema(start_params())
    compute_static_stats(schemas)
