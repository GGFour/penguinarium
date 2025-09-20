from __future__ import annotations

from typing import Any, Dict, List, Optional

from dagster import In, Out, op

from ..resources.datasource import DataSourceResource
from ..schemas.models import ColumnSchema, TableSchema


@op(
    required_resource_keys={"datasource", "result_sink"},
    ins={"params": In(dict, description="Extraction params (optional)")},
    out=Out(List[TableSchema]),
)
def extract_schema(context, params: Optional[Dict[str, Any]]):
    ds: DataSourceResource = context.resources.datasource
    insp = ds.inspector()

    target_schema = params.get("schema") if params else None
    table_schemas: List[TableSchema] = []
    for schema_name, table in ds.get_tables(schema=target_schema):
        columns: List[ColumnSchema] = []
        for column in insp.get_columns(table, schema=schema_name):
            columns.append(
                ColumnSchema(
                    name=column["name"],
                    type=str(column.get("type")),
                    nullable=column.get("nullable"),
                )
            )
        table_schemas.append(TableSchema(schema=schema_name, name=table, columns=columns))

    context.resources.result_sink.write_json(
        "schema", "tables", {"tables": [table.model_dump() for table in table_schemas]}
    )
    return table_schemas
