"""Base classes and registry for dataset source connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
import logging
from typing import Any, ClassVar, Dict, Mapping, MutableMapping

import pandas as pd


class SourceConnectorError(RuntimeError):
    """Base error raised by source connectors."""


class SourceConfigurationError(SourceConnectorError):
    """Raised when the provided configuration is incomplete or invalid."""


class UnknownConnectorError(SourceConnectorError):
    """Raised when attempting to resolve an unknown connector type."""


@dataclass
class ConnectorContext:
    """Context object passed to connectors for observability."""

    logger: logging.Logger

    def debug(self, message: str, *args: object) -> None:
        self.logger.debug(message, *args)

    def info(self, message: str, *args: object) -> None:
        self.logger.info(message, *args)

    def warning(self, message: str, *args: object) -> None:
        self.logger.warning(message, *args)

    def error(self, message: str, *args: object) -> None:
        self.logger.error(message, *args)


class SourceConnector(ABC):
    """Base class for all dataset source connectors."""

    type_name: ClassVar[str]

    def __init__(
        self,
        *,
        config: Mapping[str, Any] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config: Mapping[str, Any] = dict(config or {})
        self._context = ConnectorContext(logger or logging.getLogger(self.__class__.__name__))

    @property
    def config(self) -> Mapping[str, Any]:
        return self._config

    def require(self, key: str) -> Any:
        if key not in self._config:
            raise SourceConfigurationError(f"Missing required configuration key '{key}'")
        return self._config[key]

    @staticmethod
    def ensure_mapping(value: Mapping[str, Any] | MutableMapping[str, Any] | None) -> Dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, MutableMapping):
            return dict(value)
        raise SourceConfigurationError(
            f"Expected mapping for dataset configuration, received {type(value).__name__}"
        )

    def log_configuration(self) -> None:
        sanitized = {key: ("***" if "password" in key.lower() else value) for key, value in self._config.items()}
        self._context.debug("Connector configuration: %s", json.dumps(sanitized, default=str))

    @abstractmethod
    def load_dataset(self, dataset_config: Mapping[str, Any] | None = None) -> Dict[str, pd.DataFrame]:
        """Load dataset tables into pandas DataFrames."""


_CONNECTOR_REGISTRY: dict[str, type[SourceConnector]] = {}


def register_connector(connector_cls: type[SourceConnector]) -> type[SourceConnector]:
    connector_type = getattr(connector_cls, "type_name", None)
    if not connector_type:
        raise ValueError("Connector classes must define a 'type_name' attribute")
    key = str(connector_type).lower()
    if key in _CONNECTOR_REGISTRY:
        raise ValueError(f"Connector type '{connector_type}' already registered")
    _CONNECTOR_REGISTRY[key] = connector_cls
    return connector_cls


def get_source_connector(
    connector_type: str,
    *,
    config: Mapping[str, Any] | None = None,
    logger: logging.Logger | None = None,
) -> SourceConnector:
    connector_cls = _CONNECTOR_REGISTRY.get(connector_type.lower())
    if connector_cls is None:
        raise UnknownConnectorError(f"Data source connector '{connector_type}' is not registered")
    return connector_cls(config=config, logger=logger)


__all__ = [
    "SourceConnector",
    "SourceConnectorError",
    "SourceConfigurationError",
    "UnknownConnectorError",
    "register_connector",
    "get_source_connector",
]
