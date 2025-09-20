"""CSV data source connector."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Mapping

import pandas as pd

from ..dataset import DatasetNotFoundError, resolve_dataset_dir
from .base import SourceConfigurationError, SourceConnector, register_connector

DEFAULT_SOURCE_CONFIG: dict[str, object] = {
    "type": "csv",
    "config": {},
    "dataset": {},
}


@register_connector
class CSVConnector(SourceConnector):
    """Load datasets from CSV files stored in a directory."""

    type_name = "csv"

    def load_dataset(self, dataset_config: Mapping[str, object] | None = None) -> Dict[str, pd.DataFrame]:
        dataset_config = self.ensure_mapping(dataset_config)

        dataset_dir = resolve_dataset_dir(
            dataset_config.get("path") or self.config.get("path")
        )
        pattern = str(dataset_config.get("pattern") or "*.csv")
        tables = dataset_config.get("tables")

        self._context.info("Scanning CSV directory %s", dataset_dir)

        if tables is not None:
            table_names = _normalize_table_names(tables)
            return self._load_selected_tables(dataset_dir, table_names)

        dataset: Dict[str, pd.DataFrame] = {}
        for csv_file in sorted(dataset_dir.glob(pattern)):
            if csv_file.is_file() and csv_file.suffix.lower() == ".csv":
                dataset[csv_file.stem] = pd.read_csv(csv_file)

        if not dataset:
            raise DatasetNotFoundError(
                f"No CSV files were found in dataset directory '{dataset_dir}'."
            )
        return dataset

    def _load_selected_tables(
        self, dataset_dir: Path, tables: Iterable[str]
    ) -> Dict[str, pd.DataFrame]:
        dataset: Dict[str, pd.DataFrame] = {}
        missing: list[str] = []
        for table in tables:
            csv_path = dataset_dir / f"{table}.csv"
            if not csv_path.exists():
                missing.append(table)
                continue
            dataset[table] = pd.read_csv(csv_path)

        if missing:
            raise DatasetNotFoundError(
                "CSV files missing for tables: " + ", ".join(sorted(missing))
            )
        if not dataset:
            raise DatasetNotFoundError(
                f"No tables were loaded from directory '{dataset_dir}'."
            )
        return dataset


def _normalize_table_names(tables: object) -> Iterable[str]:
    if isinstance(tables, str):
        return [tables]
    if isinstance(tables, Iterable):  # type: ignore[redundant-expr]
        return [str(name) for name in tables]
    raise SourceConfigurationError("'tables' must be a string or iterable of strings")


__all__ = ["CSVConnector", "DEFAULT_SOURCE_CONFIG"]
