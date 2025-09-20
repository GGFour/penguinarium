from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from dagster import build_op_context

from dq_platform.ops.extract import extract_schema
from dq_platform.ops.static_checks import compute_static_stats
from dq_platform.resources.datasource import DataSourceConfig, DataSourceResource
from dq_platform.resources.storage import ResultSink


@pytest.fixture()
def sqlite_table(tmp_path: Path) -> Path:
    db_path = tmp_path / "example.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE items(id INTEGER, name TEXT)")
    conn.executemany("INSERT INTO items VALUES (?, ?)", [(1, "apple"), (2, None), (3, "apple")])
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture()
def resources(tmp_path: Path, sqlite_table: Path):
    datasource = DataSourceResource(config=DataSourceConfig(url=f"sqlite:///{sqlite_table}"))
    sink_path = tmp_path / "results"
    sink = ResultSink(base_path=str(sink_path))
    return {"datasource": datasource, "result_sink": sink}


def test_extract_schema_writes_result(resources, tmp_path: Path):
    context = build_op_context(resources=resources)
    schemas = extract_schema(context, params={})

    assert schemas and schemas[0].name == "items"

    output = Path(resources["result_sink"].base_path) / "schema" / "tables.json"
    assert output.exists()
    content = json.loads(output.read_text())
    assert content["tables"], "tables.json should include extracted tables"


def test_compute_static_stats_returns_ratios(resources):
    context = build_op_context(resources=resources)
    schemas = extract_schema(context, params={})

    stats = compute_static_stats(context, schemas)
    assert stats[0].row_count == 3
    assert pytest.approx(stats[0].null_ratio["name"], rel=1e-6) == 1 / 3
    assert pytest.approx(stats[0].distinct_ratio["name"], rel=1e-6) == 1 / 3

    output = Path(resources["result_sink"].base_path) / "stats" / "table_stats.json"
    assert output.exists()
