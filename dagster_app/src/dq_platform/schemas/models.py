from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict


class _BaseModel(BaseModel):
    model_config = ConfigDict(protected_namespaces=())


class ColumnSchema(_BaseModel):
    name: str
    type: str
    nullable: Optional[bool] = None


class TableSchema(_BaseModel):
    schema: Optional[str]
    name: str
    columns: List[ColumnSchema]


class TableStats(_BaseModel):
    schema: Optional[str]
    table: str
    row_count: int
    null_ratio: Dict[str, float]
    distinct_ratio: Dict[str, float]
    sample_taken: Optional[int] = None


class AIFinding(_BaseModel):
    table: str
    severity: str
    issue: str
    details: str


class AgenticAction(_BaseModel):
    step: int
    thought: str
    query: Optional[str] = None
    observation: Optional[str] = None


class AgenticResult(_BaseModel):
    table: str
    actions: List[AgenticAction]
    summary: str
