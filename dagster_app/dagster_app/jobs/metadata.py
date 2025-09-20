"""Job responsible for extracting and persisting dataset metadata."""

from __future__ import annotations

from dagster import In, Nothing, Out, job, op

from ..ops import load_dataset_op
from ..utils.metadata import DatasetMetadata, build_metadata
from ..utils.persistence import persist_dataset_metadata


@op(ins={"dataset": In(dict)}, out=Out(DatasetMetadata))
def build_metadata_op(context, dataset):  # type: ignore[no-untyped-def]
    context.log.info("Building metadata")
    metadata = build_metadata(dataset)
    context.log.info("Extracted metadata for %d tables", len(metadata.tables))
    return metadata


@op(ins={"metadata": In(DatasetMetadata)}, out=Out(Nothing))
def persist_metadata_op(context, metadata: DatasetMetadata):
    result = persist_dataset_metadata(metadata)
    context.log.info(
        "Metadata stored in PostgreSQL for data source '%s' (id=%s). Tables: %d, fields: %d, relations: %d",
        result.data_source_name,
        result.data_source_id,
        result.tables_created,
        result.fields_created,
        result.relations_created,
    )
    if result.skipped_relations:
        context.log.warning(
            "Skipped %d inferred relations because matching fields were not found: %s",
            len(result.skipped_relations),
            ", ".join(result.skipped_relations),
        )


@job(name="metadata_job")
def metadata_job():
    metadata = build_metadata_op(load_dataset_op())
    persist_metadata_op(metadata)
