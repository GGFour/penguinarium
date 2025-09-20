"""Job responsible for extracting and persisting dataset metadata."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from dagster import Field, Failure, In, Nothing, Out, job, op

from ..utils.dataset import DatasetNotFoundError, load_dataset, resolve_dataset_dir
from ..utils.metadata import DatasetMetadata, build_metadata, write_metadata

METADATA_STORAGE_DIR = Path(__file__).resolve().parents[2] / "storage" / "metadata"


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
