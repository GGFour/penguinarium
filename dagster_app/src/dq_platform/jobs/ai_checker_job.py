from __future__ import annotations

from dagster import job, op

from ..ops.ai_checks import ai_review
from ..ops.extract import extract_schema
from ..ops.static_checks import compute_static_stats


@op
def ai_params():
    return {}


@job
def ai_checker_job():
    schemas = extract_schema(ai_params())
    stats = compute_static_stats(schemas)
    ai_review(schemas, stats)
