"""Job responsible for executing statistical checks against the dataset."""

from __future__ import annotations

from dagster import In, Nothing, Out, job, op

from ..ops import load_dataset_op
from ..utils.persistence import persist_dataset_statistics
from ..utils.statistics import DatasetStatistics, compute_statistics


@op(ins={"dataset": In(dict)}, out=Out(DatasetStatistics))
def compute_statistics_op(context, dataset):  # type: ignore[no-untyped-def]
    context.log.info("Running statistical checks")
    statistics = compute_statistics(dataset)
    context.log.info("Computed statistics for %d columns", len(statistics.columns))
    return statistics


@op(ins={"statistics": In(DatasetStatistics)}, out=Out(Nothing))
def persist_statistics_op(context, statistics: DatasetStatistics):
    result = persist_dataset_statistics(statistics)
    context.log.info(
        "Updated statistics for %d/%d columns in data source '%s' (id=%s)",
        result.fields_updated,
        result.columns_processed,
        result.data_source_name,
        result.data_source_id,
    )
    if result.missing_columns:
        context.log.warning(
            "Statistics skipped for %d columns without metadata entries: %s",
            len(result.missing_columns),
            ", ".join(result.missing_columns),
        )


@job(name="statistics_job")
def statistics_job():
    statistics = compute_statistics_op(load_dataset_op())
    persist_statistics_op(statistics)
