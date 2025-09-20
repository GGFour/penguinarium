from __future__ import annotations

from typing import List

from dagster import In, Out, op

from ..resources.llm import LLMResource
from ..schemas.models import AIFinding, TableSchema, TableStats


_AI_PROMPT = """You are a data quality reviewer. Given schema and stats, suggest the most likely data quality issues.\nSchema:\n{schema}\nStats:\n{stats}\nReturn concise bullets: [severity] issue - details"""


@op(
    required_resource_keys={"llm", "result_sink"},
    ins={"schemas": In(List[TableSchema]), "stats": In(List[TableStats])},
    out=Out(List[AIFinding]),
)
def ai_review(context, schemas: List[TableSchema], stats: List[TableStats]):
    llm: LLMResource = context.resources.llm
    prompt = _AI_PROMPT.format(
        schema=str([schema.model_dump() for schema in schemas])[:4000],
        stats=str([stat.model_dump() for stat in stats])[:4000],
    )
    raw = llm.complete(prompt)

    findings: List[AIFinding] = []
    for line in (raw or "").splitlines():
        cleaned = line.strip("- ").strip()
        if not cleaned:
            continue
        severity = "info"
        if cleaned.startswith("[") and "]" in cleaned:
            severity = cleaned[1 : cleaned.index("]")]
            cleaned = cleaned[cleaned.index("]") + 1 :].strip()
        if " - " in cleaned:
            issue, details = cleaned.split(" - ", 1)
        else:
            issue, details = cleaned, ""
        findings.append(AIFinding(table="*", severity=severity, issue=issue, details=details))

    context.resources.result_sink.write_json(
        "ai", "findings", {"findings": [finding.model_dump() for finding in findings]}
    )
    return findings
