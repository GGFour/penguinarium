"""Pluggable source connectors for loading datasets."""

from __future__ import annotations

from typing import Any, Mapping

from .base import (
    SourceConfigurationError,
    SourceConnector,
    SourceConnectorError,
    UnknownConnectorError,
    get_source_connector,
    register_connector,
)
from .csv import DEFAULT_SOURCE_CONFIG

# Import built-in connectors to register them with the registry.
from .csv import CSVConnector  # noqa: F401  # pylint: disable=unused-import
from .sql import (  # noqa: F401  # pylint: disable=unused-import
    OracleConnector,
    PostgresConnector,
    SnowflakeConnector,
)
from .hdfs import HDFSConnector  # noqa: F401  # pylint: disable=unused-import

__all__ = [
    "DEFAULT_SOURCE_CONFIG",
    "HDFSConnector",
    "OracleConnector",
    "PostgresConnector",
    "SnowflakeConnector",
    "SourceConfigurationError",
    "SourceConnector",
    "SourceConnectorError",
    "UnknownConnectorError",
    "get_source_connector",
    "register_connector",
]


def create_run_config(
    base_run_config: Mapping[str, Any] | None,
    *,
    source_config: Mapping[str, Any],
    op_key: str = "load_dataset_op",
) -> dict[str, Any]:
    """Inject the ``source_config`` into a Dagster run configuration."""

    from copy import deepcopy

    run_config = deepcopy(dict(base_run_config or {}))
    ops_config = run_config.setdefault("ops", {})
    op_config = ops_config.setdefault(op_key, {})
    existing_config = dict(op_config.get("config") or {})
    existing_config["source"] = dict(source_config)
    op_config["config"] = existing_config
    ops_config[op_key] = op_config
    run_config["ops"] = ops_config
    return run_config
