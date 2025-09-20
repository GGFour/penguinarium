from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest
from dagster import build_op_context

from dagster_app.ops.dataset import load_dataset_op
from dagster_app.utils.connectors import (
    CSVConnector,
    SourceConnectorError,
    UnknownConnectorError,
    create_run_config,
    get_source_connector,
)


def _create_csv(tmp_path: Path, name: str, rows: list[dict[str, object]]) -> None:
    df = pd.DataFrame(rows)
    df.to_csv(tmp_path / f"{name}.csv", index=False)


def test_csv_connector_loads_all_files(tmp_path: Path) -> None:
    _create_csv(tmp_path, "table_a", [{"value": 1}])
    _create_csv(tmp_path, "table_b", [{"value": 2}])

    connector = CSVConnector(config={"path": tmp_path})
    dataset = connector.load_dataset()

    assert set(dataset.keys()) == {"table_a", "table_b"}
    assert dataset["table_a"].iloc[0]["value"] == 1


def test_csv_connector_missing_table(tmp_path: Path) -> None:
    connector = CSVConnector(config={"path": tmp_path})
    with pytest.raises(SourceConnectorError):
        connector.load_dataset({"tables": ["missing"]})


def test_get_source_connector_unknown() -> None:
    with pytest.raises(UnknownConnectorError):
        get_source_connector("does_not_exist")


def test_run_config_injection() -> None:
    run_config = create_run_config({}, source_config={"type": "csv", "config": {}})
    assert run_config["ops"]["load_dataset_op"]["config"]["source"]["type"] == "csv"


def test_load_dataset_op_with_default_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _create_csv(tmp_path, "customers", [{"id": 1}])
    monkeypatch.setenv("DATASET_DIR", str(tmp_path))

    context = build_op_context()
    dataset = load_dataset_op(context)
    assert set(dataset.keys()) == {"customers"}


def test_load_dataset_op_with_custom_source(tmp_path: Path) -> None:
    _create_csv(tmp_path, "payments", [{"id": 5}])

    context = build_op_context(
        op_config={
            "source": {
                "type": "csv",
                "config": {"path": str(tmp_path)},
            }
        }
    )
    dataset = load_dataset_op(context)
    assert "payments" in dataset


def test_load_dataset_op_with_invalid_connector() -> None:
    context = build_op_context(op_config={"source": {"type": "unknown"}})
    with pytest.raises(Exception) as exc:
        load_dataset_op(context)
    assert "unknown" in str(exc.value).lower()
