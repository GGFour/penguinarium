"""HDFS connector backed by ``pyarrow.fs`` if available."""

from __future__ import annotations

from typing import Dict, Mapping

import pandas as pd

from .base import SourceConfigurationError, SourceConnector, register_connector


@register_connector
class HDFSConnector(SourceConnector):
    type_name = "hdfs"

    def _load_pyarrow(self):
        try:
            from pyarrow import fs
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise SourceConfigurationError(
                "pyarrow must be installed to use the HDFS connector"
            ) from exc
        return fs

    def load_dataset(self, dataset_config: Mapping[str, object] | None = None) -> Dict[str, pd.DataFrame]:
        dataset_config = self.ensure_mapping(dataset_config)
        host = str(self.config.get("host") or "localhost")
        port = int(self.config.get("port") or 8020)
        user = self.config.get("user")
        path = dataset_config.get("path")
        if not path:
            raise SourceConfigurationError("HDFS connector requires a dataset 'path'")

        fs = self._load_pyarrow()
        self._context.info("Connecting to HDFS %s:%s", host, port)
        hdfs = fs.HadoopFileSystem(host=host, port=port, user=user)

        dataset: Dict[str, pd.DataFrame] = {}
        tables = dataset_config.get("tables")

        if tables:
            table_names = tables if isinstance(tables, list) else [tables]
            for table in table_names:
                csv_path = f"{path.rstrip('/')}/{table}.csv"
                with hdfs.open_input_file(csv_path) as handle:
                    dataset[str(table)] = pd.read_csv(handle)
        else:
            for info in hdfs.get_file_info(fs.FileSelector(str(path), recursive=False)):
                if info.type == fs.FileType.File and info.path.endswith(".csv"):
                    with hdfs.open_input_file(info.path) as handle:
                        table_name = info.path.split("/")[-1].removesuffix(".csv")
                        dataset[table_name] = pd.read_csv(handle)

        if not dataset:
            raise SourceConfigurationError(f"No CSV files discovered in HDFS path '{path}'")

        return dataset


__all__ = ["HDFSConnector"]
