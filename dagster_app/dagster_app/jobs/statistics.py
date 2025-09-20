"""Job responsible for executing statistical checks against the dataset."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from dagster import Field, Failure, In, Nothing, Out, job, op

from ..utils.dataset import DatasetNotFoundError, load_dataset, resolve_dataset_dir
from ..utils.statistics import DatasetStatistics, compute_statistics, write_statistics

STATISTICS_STORAGE_DIR = Path(__file__).resolve().parents[2] / "storage" / "statistics"


@op(
    out=Out(dict),
    config_schema={"dataset_dir": Field(str, is_required=False)},
    required_resource_keys=set(),
)
def load_dataset_op(context):  # type: ignore[no-untyped-def]
    config = context.op_config or {}
    dataset_dir = resolve_dataset_dir(config.get("dataset_dir"))
    context.log.info("Loading dataset from %s", dataset_dir)
    try:
        dataset = load_dataset(dataset_dir)
    except DatasetNotFoundError as error:
        raise Failure(str(error)) from error
    context.log.info("Loaded %d tables", len(dataset))
    return dataset


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
