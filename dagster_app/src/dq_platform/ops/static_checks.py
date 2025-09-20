from __future__ import annotations

from typing import Dict, List

from dagster import In, Out, op
from sqlalchemy import text

from ..resources.datasource import DataSourceResource
from ..schemas.models import TableSchema, TableStats


def _metric_sql(table_fqn: str, column: str) -> str:
    return f"""
    SELECT
      COUNT(*) AS total_rows,
      COUNT("{column}") AS non_nulls,
      COUNT(DISTINCT "{column}") AS distinct_vals
    FROM {table_fqn}
    """


@op(
    required_resource_keys={"datasource", "result_sink"},
    ins={"schemas": In(List[TableSchema])},
    out=Out(List[TableStats]),
)
def compute_static_stats(context, schemas: List[TableSchema]):
    ds: DataSourceResource = context.resources.datasource
    results: List[TableStats] = []

    for table_schema in schemas:
        if table_schema.schema:
            table_fqn = f'"{table_schema.schema}"."{table_schema.name}"'
        else:
            table_fqn = f'"{table_schema.name}"'

        with ds.engine.connect() as conn:
            total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table_fqn}"))
            total_rows = int(total_rows.scalar_one())

        null_ratio: Dict[str, float] = {}
        distinct_ratio: Dict[str, float] = {}

        if total_rows > 0:
            for column in table_schema.columns:
                sql = _metric_sql(table_fqn, column.name)
                with ds.engine.connect() as conn:
                    result = conn.execute(text(sql)).mappings().one()
                non_nulls = int(result["non_nulls"])
                distinct_vals = int(result["distinct_vals"])
                null_ratio[column.name] = (total_rows - non_nulls) / total_rows
                distinct_ratio[column.name] = distinct_vals / total_rows

        stats = TableStats(
            schema=table_schema.schema,
            table=table_schema.name,
            row_count=total_rows,
            null_ratio=null_ratio,
            distinct_ratio=distinct_ratio,
        )
        results.append(stats)

    context.resources.result_sink.write_json(
        "stats", "table_stats", {"stats": [stat.model_dump() for stat in results]}
    )
    return results
