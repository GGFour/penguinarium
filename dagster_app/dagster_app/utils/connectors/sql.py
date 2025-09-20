"""SQL-based source connectors implemented with SQLAlchemy."""

from __future__ import annotations

from typing import Any, Dict, Mapping

import pandas as pd

from .base import SourceConfigurationError, SourceConnector, register_connector


class SQLAlchemyConnector(SourceConnector):
    """Base connector for sources available via SQLAlchemy."""

    dialect: str

    def _load_sqlalchemy(self):
        try:
            from sqlalchemy import create_engine, text
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise SourceConfigurationError(
                "sqlalchemy must be installed to use SQL-based connectors"
            ) from exc
        return create_engine, text

    def build_url(self) -> str:
        url = self.config.get("sqlalchemy_url")
        if url:
            return str(url)
        raise SourceConfigurationError(
            "SQL connectors require 'sqlalchemy_url' or subclass-specific credentials"
        )

    def load_dataset(self, dataset_config: Mapping[str, Any] | None = None) -> Dict[str, pd.DataFrame]:
        dataset_config = self.ensure_mapping(dataset_config)
        create_engine, text = self._load_sqlalchemy()

        url = self.build_url()
        self._context.info("Connecting to %s", url)
        engine = create_engine(url)

        queries = dataset_config.get("queries")
        tables = dataset_config.get("tables")
        schema = dataset_config.get("schema") or self.config.get("schema")

        if not queries and not tables:
            raise SourceConfigurationError("Provide either 'queries' or 'tables' to load from SQL")

        dataset: Dict[str, pd.DataFrame] = {}

        try:
            if queries:
                dataset.update(self._run_queries(engine, queries, text))
            if tables:
                dataset.update(self._read_tables(engine, tables, schema))
        finally:
            engine.dispose()

        if not dataset:
            raise SourceConfigurationError("No data returned from SQL connector")

        return dataset

    def _run_queries(self, engine, queries, text):  # type: ignore[no-untyped-def]
        if isinstance(queries, Mapping):
            query_items = queries.items()
        elif isinstance(queries, list):
            query_items = [(f"query_{idx}", query) for idx, query in enumerate(queries)]
        elif isinstance(queries, str):
            query_items = [("query", queries)]
        else:
            raise SourceConfigurationError("'queries' must be a string, list, or mapping")

        dataset: Dict[str, pd.DataFrame] = {}
        for name, query in query_items:
            dataset[str(name)] = pd.read_sql_query(text(str(query)), engine)
        return dataset

    def _read_tables(self, engine, tables, schema):  # type: ignore[no-untyped-def]
        if isinstance(tables, str):
            table_list = [tables]
        elif isinstance(tables, list):
            table_list = [str(table) for table in tables]
        else:
            raise SourceConfigurationError("'tables' must be a string or list of strings")

        dataset: Dict[str, pd.DataFrame] = {}
        for table in table_list:
            dataset[table] = pd.read_sql_table(table, engine, schema=schema or None)
        return dataset


@register_connector
class PostgresConnector(SQLAlchemyConnector):
    type_name = "postgres"

    def build_url(self) -> str:
        url = self.config.get("sqlalchemy_url")
        if url:
            return str(url)

        user = self.require("user")
        password = self.require("password")
        host = self.require("host")
        port = self.config.get("port", 5432)
        database = self.require("database")
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


@register_connector
class OracleConnector(SQLAlchemyConnector):
    type_name = "oracle"

    def build_url(self) -> str:
        url = self.config.get("sqlalchemy_url")
        if url:
            return str(url)

        user = self.require("user")
        password = self.require("password")
        dsn = self.config.get("dsn")
        if dsn:
            return f"oracle+oracledb://{user}:{password}@{dsn}"

        host = self.require("host")
        port = self.config.get("port", 1521)
        service_name = self.require("service_name")
        return f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}"


@register_connector
class SnowflakeConnector(SQLAlchemyConnector):
    type_name = "snowflake"

    def build_url(self) -> str:
        url = self.config.get("sqlalchemy_url")
        if url:
            return str(url)

        user = self.require("user")
        password = self.require("password")
        account = self.require("account")
        database = self.require("database")
        schema = self.config.get("schema")
        warehouse = self.config.get("warehouse")
        role = self.config.get("role")

        url = f"snowflake://{user}:{password}@{account}/{database}"
        if schema:
            url += f"/{schema}"

        params: list[str] = []
        if warehouse:
            params.append(f"warehouse={warehouse}")
        if role:
            params.append(f"role={role}")
        if params:
            url += "?" + "&".join(params)
        return url


__all__ = [
    "OracleConnector",
    "PostgresConnector",
    "SnowflakeConnector",
]
