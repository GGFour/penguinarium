"""Dataset related Dagster ops."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from dagster import Field, Failure, Out, Permissive, op

from ..utils.connectors import (
    DEFAULT_SOURCE_CONFIG,
    SourceConfigurationError,
    SourceConnectorError,
    UnknownConnectorError,
    get_source_connector,
)


def _build_source_config(raw_config: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not raw_config:
        return dict(DEFAULT_SOURCE_CONFIG)

    config = deepcopy(dict(raw_config))
    config.setdefault("type", DEFAULT_SOURCE_CONFIG["type"])
    config.setdefault("config", {})
    config.setdefault("dataset", {})
    return config


@op(
    out=Out(dict),
    config_schema={
        "source": Field(
            Permissive(),
            is_required=False,
            description="Configuration describing which data source to load.",
        ),
    },
    required_resource_keys=set(),
)
def load_dataset_op(context):  # type: ignore[no-untyped-def]
    """Load dataset tables from the configured data source."""

    config = context.op_config or {}
    raw_source_config = config.get("source")
    source_config = _build_source_config(raw_source_config)

    connector_type = str(source_config.get("type")).lower()
    context.log.info("Loading dataset using '%s' connector", connector_type)

    try:
        connector = get_source_connector(
            connector_type,
            config=source_config.get("config") or {},
            logger=context.log,
        )
        dataset = connector.load_dataset(source_config.get("dataset") or {})
    except UnknownConnectorError as error:
        context.log.error("Unknown data source connector '%s'", connector_type)
        raise Failure(str(error)) from error
    except SourceConfigurationError as error:
        context.log.error("Invalid configuration for connector '%s': %s", connector_type, error)
        raise Failure(str(error)) from error
    except SourceConnectorError as error:
        context.log.error("Connector '%s' failed to load dataset: %s", connector_type, error)
        raise Failure(str(error)) from error

    context.log.info("Loaded %d tables", len(dataset))
    return dataset
