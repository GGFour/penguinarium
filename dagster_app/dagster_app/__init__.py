"""Dagster definitions for the dagster_app service."""

from dagster import Definitions

from .jobs.metadata import metadata_job
from .jobs.statistics import statistics_job

__all__ = ["defs", "metadata_job", "statistics_job"]

defs = Definitions(jobs=[metadata_job, statistics_job])
