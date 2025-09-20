from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from dagster import Config, ConfigurableResource, get_dagster_logger
from sqlalchemy import create_engine, inspect, text


class DataSourceConfig(Config):
    url: str
    default_schema: Optional[str] = None


@dataclass
class _EngineCache:
    url: str
    engine: Any


class DataSourceResource(ConfigurableResource):
    config: Optional[DataSourceConfig] = None
    _engine_cache: Optional[_EngineCache] = None

    @property
    def engine(self):  # type: ignore[override]
        if not self.config:
            raise RuntimeError("DataSourceResource requires config.url at runtime.")

        if self._engine_cache and self._engine_cache.url == self.config.url:
            return self._engine_cache.engine

        engine = create_engine(self.config.url, pool_pre_ping=True)
        self._engine_cache = _EngineCache(url=self.config.url, engine=engine)
        return engine

    def inspector(self):
        return inspect(self.engine)

    def run_sql(self, sql: str):
        if not sql:
            raise ValueError("SQL must be provided")
        log = get_dagster_logger()
        log.info("Executing SQL: %s...", sql[:400])
        with self.engine.connect() as conn:
            return conn.execute(text(sql))

    def get_tables(self, schema: Optional[str] = None):
        insp = self.inspector()
        schema = schema or (self.config.default_schema if self.config else None)
        tables = insp.get_table_names(schema=schema)
        return [(schema, t) for t in tables]
