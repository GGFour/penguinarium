"""Job responsible for executing statistical checks against the dataset."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from dagster import In, Nothing, Out, job, op

from ..ops import load_dataset_op
from ..utils.statistics import DatasetStatistics, compute_statistics, write_statistics

STATISTICS_STORAGE_DIR = Path(__file__).resolve().parents[2] / "storage" / "statistics"


@op(ins={"dataset": In(dict)}, out=Out(DatasetStatistics))
def compute_statistics_op(context, dataset):  # type: ignore[no-untyped-def]
    context.log.info("Running statistical checks")
    statistics = compute_statistics(dataset)
    context.log.info("Computed statistics for %d columns", len(statistics.columns))
    return statistics


@op(ins={"statistics": In(DatasetStatistics)}, out=Out(Nothing))
def persist_statistics_op(context, statistics: DatasetStatistics):
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    destination = STATISTICS_STORAGE_DIR / f"statistics_{timestamp}.json"
    path = write_statistics(statistics, destination)
    context.log.info("Statistics stored at %s", path)


@job(name="statistics_job")
def statistics_job():
    statistics = compute_statistics_op(load_dataset_op())
    persist_statistics_op(statistics)
