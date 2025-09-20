"""Dataset related Dagster ops."""

from dagster import Field, Failure, Out, op

from ..utils.dataset import DatasetNotFoundError, load_dataset, resolve_dataset_dir


@op(
    out=Out(dict),
    config_schema={"dataset_dir": Field(str, is_required=False)},
    required_resource_keys=set(),
)
def load_dataset_op(context):  # type: ignore[no-untyped-def]
    """Load dataset tables from CSV files using the configured location."""

    config = context.op_config or {}
    dataset_dir = resolve_dataset_dir(config.get("dataset_dir"))
    context.log.info("Loading dataset from %s", dataset_dir)
    try:
        dataset = load_dataset(dataset_dir)
    except DatasetNotFoundError as error:
        raise Failure(str(error)) from error
    context.log.info("Loaded %d tables", len(dataset))
    return dataset
