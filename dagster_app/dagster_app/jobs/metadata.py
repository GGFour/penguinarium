"""Job responsible for extracting and persisting dataset metadata."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from dagster import In, Nothing, Out, job, op

from ..ops import load_dataset_op
from ..utils.metadata import DatasetMetadata, build_metadata, write_metadata

METADATA_STORAGE_DIR = Path(__file__).resolve().parents[2] / "storage" / "metadata"


@op(ins={"dataset": In(dict)}, out=Out(DatasetMetadata))
def build_metadata_op(context, dataset):  # type: ignore[no-untyped-def]
    context.log.info("Building metadata")
    metadata = build_metadata(dataset)
    context.log.info("Extracted metadata for %d tables", len(metadata.tables))
    return metadata


@op(ins={"metadata": In(DatasetMetadata)}, out=Out(Nothing))
def persist_metadata_op(context, metadata: DatasetMetadata):
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    destination = METADATA_STORAGE_DIR / f"metadata_{timestamp}.json"
    path = write_metadata(metadata, destination)
    context.log.info("Metadata stored at %s", path)


@job(name="metadata_job")
def metadata_job():
    metadata = build_metadata_op(load_dataset_op())
    persist_metadata_op(metadata)
