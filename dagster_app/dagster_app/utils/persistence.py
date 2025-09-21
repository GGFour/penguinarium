"""Helpers for persisting extracted dataset information into PostgreSQL."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Mapping, Sequence, Tuple

import numpy as np
import psycopg2
from psycopg2.extras import Json

from .metadata import DatasetMetadata, RelationMetadata
from .statistics import DatasetStatistics


@dataclass(frozen=True)
class MetadataPersistenceResult:
    """Summary of metadata persistence outcomes."""

    data_source_id: int
    data_source_name: str
    tables_created: int
    fields_created: int
    relations_created: int
    skipped_relations: Tuple[str, ...]


@dataclass(frozen=True)
class StatisticsPersistenceResult:
    """Summary of statistics persistence outcomes."""

    data_source_id: int
    data_source_name: str
    columns_processed: int
    fields_updated: int
    missing_columns: Tuple[str, ...]


VALID_DATA_SOURCE_TYPES = {
    "database",
    "api",
    "file",
    "stream",
    "cloud",
    "other",
}

DEFAULT_DATA_SOURCE_NAME = "Home Credit Dataset"
DEFAULT_DATA_SOURCE_TYPE = "file"


def persist_dataset_metadata(
    metadata: DatasetMetadata,
    *,
    data_source_name: str | None = None,
    data_source_type: str | None = None,
) -> MetadataPersistenceResult:
    """Persist dataset metadata into PostgreSQL tables used by Django."""

    ds_name = _resolve_data_source_name(data_source_name)
    ds_type = _resolve_data_source_type(data_source_type)
    now = datetime.utcnow()

    connection_info_raw: Dict[str, object] = {
        "ingested_by": "dagster_app",
        "last_metadata_refresh_at": metadata.generated_at,
    }
    dataset_dir = os.getenv("DATASET_DIR")
    if dataset_dir:
        connection_info_raw["dataset_dir"] = dataset_dir
    connection_info = _sanitize_json(connection_info_raw)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT data_source_id, connection_info
                FROM pulling_datasource
                WHERE name = %s
                ORDER BY data_source_id
                LIMIT 1
                """,
                (ds_name,),
            )
            row = cur.fetchone()
            if row:
                data_source_id = row[0]
                existing_info = _coerce_json_dict(row[1])
                merged_info = _sanitize_json({**existing_info, **connection_info})
                cur.execute(
                    """
                    UPDATE pulling_datasource
                    SET updated_at = %s,
                        type = %s,
                        connection_info = %s
                    WHERE data_source_id = %s
                    """,
                    (now, ds_type, Json(merged_info), data_source_id),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO pulling_datasource (
                        created_at,
                        updated_at,
                        global_id,
                        is_deleted,
                        name,
                        type,
                        connection_info
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING data_source_id
                    """,
                    (
                        now,
                        now,
                        str(uuid.uuid4()),
                        False,
                        ds_name,
                        ds_type,
                        Json(connection_info),
                    ),
                )
                data_source_id = cur.fetchone()[0]

            # Remove existing tables (cascade deletes fields and relations).
            cur.execute(
                "DELETE FROM pulling_tablemetadata WHERE data_source_id = %s",
                (data_source_id,),
            )

            table_id_lookup: Dict[str, int] = {}
            field_id_lookup: Dict[Tuple[str, str], int] = {}
            tables_created = 0
            fields_created = 0
            for table in metadata.tables:
                table_payload = _sanitize_json(
                    {
                        "row_count": table.row_count,
                        "generated_at": metadata.generated_at,
                        "field_count": len(table.fields),
                    }
                )
                cur.execute(
                    """
                    INSERT INTO pulling_tablemetadata (
                        created_at,
                        updated_at,
                        global_id,
                        is_deleted,
                        data_source_id,
                        name,
                        description,
                        metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING table_metadata_id
                    """,
                    (
                        now,
                        now,
                        str(uuid.uuid4()),
                        False,
                        data_source_id,
                        table.name,
                        None,
                        Json(table_payload),
                    ),
                )
                table_id = cur.fetchone()[0]
                table_id_lookup[table.name] = table_id
                tables_created += 1

                for field in table.fields:
                    dtype = _normalize_field_dtype(field.dtype)
                    field_payload = _sanitize_json(
                        {
                            "non_null_count": field.non_null_count,
                            "null_count": field.null_count,
                            "sample_values": field.sample_values,
                            "original_dtype": field.dtype,
                        }
                    )
                    cur.execute(
                        """
                        INSERT INTO pulling_fieldmetadata (
                            created_at,
                            updated_at,
                            global_id,
                            is_deleted,
                            table_id,
                            name,
                            dtype,
                            metadata
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING field_metadata_id
                        """,
                        (
                            now,
                            now,
                            str(uuid.uuid4()),
                            False,
                            table_id,
                            field.name,
                            dtype,
                            Json(field_payload),
                        ),
                    )
                    field_id = cur.fetchone()[0]
                    field_id_lookup[(table.name, field.name)] = field_id
                    fields_created += 1

            relations_created, skipped_relations = _persist_relations(
                cur, metadata.relations, field_id_lookup, now
            )

    return MetadataPersistenceResult(
        data_source_id=data_source_id,
        data_source_name=ds_name,
        tables_created=tables_created,
        fields_created=fields_created,
        relations_created=relations_created,
        skipped_relations=tuple(skipped_relations),
    )


def persist_dataset_statistics(
    statistics: DatasetStatistics,
    *,
    data_source_name: str | None = None,
) -> StatisticsPersistenceResult:
    """Persist computed statistics into field metadata JSON blobs."""

    ds_name = _resolve_data_source_name(data_source_name)
    now = datetime.utcnow()
    stats_map: Dict[Tuple[str, str], Dict[str, object]] = {}
    for column in statistics.columns:
        column_payload = column.to_payload()
        column_payload["computed_at"] = statistics.generated_at
        stats_map[(column.table, column.column)] = _sanitize_json(column_payload)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT data_source_id, connection_info
                FROM pulling_datasource
                WHERE name = %s
                ORDER BY data_source_id
                LIMIT 1
                """,
                (ds_name,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(
                    f"Data source '{ds_name}' does not exist; run metadata extraction first."
                )
            data_source_id = row[0]
            existing_info = _coerce_json_dict(row[1])
            existing_info["last_statistics_run_at"] = statistics.generated_at
            existing_info["statistics_overview"] = {
                "generated_at": statistics.generated_at,
                "metrics": statistics.metrics,
                "tables": [table.to_payload() for table in statistics.tables],
            }
            existing_info = _sanitize_json(existing_info)
            cur.execute(
                """
                UPDATE pulling_datasource
                SET updated_at = %s,
                    connection_info = %s
                WHERE data_source_id = %s
                """,
                (now, Json(existing_info), data_source_id),
            )

            cur.execute(
                """
                SELECT fm.field_metadata_id, fm.metadata, fm.name, tm.name
                FROM pulling_fieldmetadata AS fm
                JOIN pulling_tablemetadata AS tm
                    ON fm.table_id = tm.table_metadata_id
                WHERE tm.data_source_id = %s
                """,
                (data_source_id,),
            )

            updated_keys = set()
            fields_updated = 0
            for field_id, metadata_payload, field_name, table_name in cur.fetchall():
                key = (table_name, field_name)
                stats_payload = stats_map.get(key)
                if not stats_payload:
                    continue

                existing_metadata = _coerce_json_dict(metadata_payload)
                existing_metadata["statistics"] = stats_payload
                updated_metadata = _sanitize_json(existing_metadata)
                cur.execute(
                    """
                    UPDATE pulling_fieldmetadata
                    SET metadata = %s,
                        updated_at = %s
                    WHERE field_metadata_id = %s
                    """,
                    (Json(updated_metadata), now, field_id),
                )
                updated_keys.add(key)
                fields_updated += 1

    missing_columns = [
        f"{table}.{column}" for (table, column) in stats_map.keys() if (table, column) not in updated_keys
    ]

    return StatisticsPersistenceResult(
        data_source_id=data_source_id,
        data_source_name=ds_name,
        columns_processed=len(statistics.columns),
        fields_updated=fields_updated,
        missing_columns=tuple(missing_columns),
    )


# ---------------------------------------------------------------------------
# Helpers


def _connect():
    params = {
        "host": _env("POSTGRES_HOST", "DAGSTER_POSTGRES_HOST", default="localhost"),
        "port": int(_env("POSTGRES_PORT", "DAGSTER_POSTGRES_PORT", default="5432")),
        "dbname": _env("POSTGRES_DB", "DAGSTER_POSTGRES_DB", default="postgres"),
        "user": _env("POSTGRES_USER", "DAGSTER_POSTGRES_USER", default="postgres"),
        "password": _env(
            "POSTGRES_PASSWORD", "DAGSTER_POSTGRES_PASSWORD", default="postgres"
        ),
    }
    return psycopg2.connect(**params)


def _env(primary: str, secondary: str, *, default: str) -> str:
    return os.getenv(primary) or os.getenv(secondary) or default


def _resolve_data_source_name(name: str | None) -> str:
    candidate = name or os.getenv("DATA_SOURCE_NAME") or DEFAULT_DATA_SOURCE_NAME
    return candidate.strip() or DEFAULT_DATA_SOURCE_NAME


def _resolve_data_source_type(data_source_type: str | None) -> str:
    candidate = (data_source_type or os.getenv("DATA_SOURCE_TYPE") or DEFAULT_DATA_SOURCE_TYPE).lower()
    if candidate not in VALID_DATA_SOURCE_TYPES:
        return "other"
    return candidate


def _sanitize_json(value):  # type: ignore[no-untyped-def]
    if isinstance(value, dict):
        return {k: _sanitize_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_json(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _coerce_json_dict(value) -> Dict[str, object]:  # type: ignore[no-untyped-def]
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(decoded, dict):
            return decoded
        return {}
    if hasattr(value, "items"):
        return dict(value.items())  # type: ignore[no-untyped-call]
    return {}


def _normalize_field_dtype(dtype: str) -> str:
    normalized = dtype.lower()
    if normalized.startswith("int") or normalized.startswith("uint"):
        return "integer"
    if normalized.startswith("float32"):
        return "float"
    if normalized.startswith("float") or normalized.startswith("double"):
        return "double"
    if normalized in {"bool", "boolean"}:
        return "boolean"
    if "timestamp" in normalized or normalized.startswith("datetime"):
        return "datetime"
    if normalized.startswith("date"):
        return "date"
    if normalized in {"object", "string", "category"}:
        return "string"
    if normalized == "text":
        return "text"
    if "uuid" in normalized:
        return "uuid"
    return "other"


def _persist_relations(
    cursor,
    relations: Sequence[RelationMetadata],
    field_lookup: Mapping[Tuple[str, str], int],
    now: datetime,
) -> Tuple[int, List[str]]:
    created = 0
    skipped: List[str] = []
    for relation in relations:
        src_key = (relation.from_table, relation.from_field)
        dst_key = (relation.to_table, relation.to_field)
        src_id = field_lookup.get(src_key)
        dst_id = field_lookup.get(dst_key)
        if src_id is None or dst_id is None:
            skipped.append(f"{relation.from_table}.{relation.from_field}->{relation.to_table}.{relation.to_field}")
            continue

        rel_type = _normalize_relation_type(relation.relation_type)
        cursor.execute(
            """
            INSERT INTO pulling_fieldrelation (
                created_at,
                updated_at,
                global_id,
                is_deleted,
                src_field_id,
                dst_field_id,
                relation_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (now, now, str(uuid.uuid4()), False, src_id, dst_id, rel_type),
        )
        created += 1

    return created, skipped


def _normalize_relation_type(relation_type: str) -> str:
    normalized = relation_type.lower()
    if "foreign" in normalized:
        return "foreign_key"
    if "primary" in normalized:
        return "primary_key"
    if "join" in normalized:
        return "join"
    if "lineage" in normalized:
        return "lineage"
    if "dependency" in normalized:
        return "dependency"
    return "other"

