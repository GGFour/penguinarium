"""Metadata extraction helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd


@dataclass
class FieldMetadata:
    name: str
    dtype: str
    non_null_count: int
    null_count: int
    sample_values: List[object]


@dataclass
class TableMetadata:
    name: str
    row_count: int
    fields: List[FieldMetadata]


@dataclass
class RelationMetadata:
    from_table: str
    from_field: str
    to_table: str
    to_field: str
    relation_type: str


@dataclass
class DatasetMetadata:
    generated_at: str
    tables: List[TableMetadata]
    relations: List[RelationMetadata]


def _field_metadata(table_name: str, series: pd.Series) -> FieldMetadata:
    dtype = str(series.dtype)
    non_null = series.notna().sum()
    nulls = int(series.isna().sum())
    # Sample up to five values that are not null.
    samples = series.dropna().head(5).tolist()
    return FieldMetadata(
        name=series.name,
        dtype=dtype,
        non_null_count=int(non_null),
        null_count=nulls,
        sample_values=samples,
    )


def _detect_relations(dataset: Dict[str, pd.DataFrame]) -> Iterable[RelationMetadata]:
    """Detect potential relations based on shared identifier columns."""

    relation_candidates: List[RelationMetadata] = []
    column_index: Dict[str, List[tuple[str, str]]] = {}

    for table_name, df in dataset.items():
        for column in df.columns:
            column_index.setdefault(column.lower(), []).append((table_name, column))

    for normalized_name, references in column_index.items():
        if len(references) < 2:
            continue
        if not (normalized_name.endswith("_id") or normalized_name.startswith("sk_id")):
            continue
        for from_table, from_column in references:
            for to_table, to_column in references:
                if from_table == to_table:
                    continue
                relation_candidates.append(
                    RelationMetadata(
                        from_table=from_table,
                        from_field=from_column,
                        to_table=to_table,
                        to_field=to_column,
                        relation_type="inferred_foreign_key",
                    )
                )

    return relation_candidates


def build_metadata(dataset: Dict[str, pd.DataFrame]) -> DatasetMetadata:
    tables: List[TableMetadata] = []
    for table_name, df in dataset.items():
        fields = [_field_metadata(table_name, df[column]) for column in df.columns]
        tables.append(TableMetadata(name=table_name, row_count=int(df.shape[0]), fields=fields))

    relations = list(_detect_relations(dataset))
    generated_at = datetime.utcnow().isoformat()

    return DatasetMetadata(generated_at=generated_at, tables=tables, relations=relations)
