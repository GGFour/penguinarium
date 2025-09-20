from __future__ import annotations

from typing import Dict, List

from dagster import In, Out, op
from sqlalchemy import text

from ..resources.datasource import DataSourceResource
from ..resources.llm import LLMResource
from ..schemas.models import AgenticAction, AgenticResult, TableSchema, TableStats


_AGENT_SYS = """You are an agentic data quality auditor.\nAt each step:\n1) Decide a hypothesis to test (e.g., foreign-key mismatch, date gaps).\n2) Propose ONE SQL query to test it.\n3) Summarize observation.\nPlan up to {max_steps} steps."""


@op(
    required_resource_keys={"datasource", "llm", "result_sink"},
    ins={"schemas": In(List[TableSchema]), "stats": In(List[TableStats]), "params": In(dict)},
    out=Out(List[AgenticResult]),
)
def agentic_loop(context, schemas: List[TableSchema], stats: List[TableStats], params: Dict[str, object]):
    ds: DataSourceResource = context.resources.datasource
    llm: LLMResource = context.resources.llm
    max_steps = int(params.get("max_steps", 3)) if params else 3
    target_table = params.get("table") if params else None
    target_table = target_table or (schemas[0].name if schemas else "*")

    plan_prompt = (
        _AGENT_SYS.format(max_steps=max_steps)
        + "\nSchema (truncated): "
        + str([schema.model_dump() for schema in schemas])[:3500]
        + "\nStats (truncated): "
        + str([stat.model_dump() for stat in stats])[:1500]
    )

    plan = llm.complete(plan_prompt)
    queries = [
        line
        for line in (plan or "").splitlines()
        if line.strip().upper().startswith("SELECT")
    ][:max_steps]

    actions: List[AgenticAction] = []
    for idx, query in enumerate(queries, start=1):
        observation = ""
        try:
            with ds.engine.connect() as conn:
                rows = conn.execute(text(query)).fetchmany(5)
            observation = f"Top rows: {rows}"
        except Exception as exc:  # pragma: no cover - exercised when query fails
            observation = f"Query failed: {exc}"
        actions.append(AgenticAction(step=idx, thought="Run agentic test", query=query, observation=observation))

    summary_prompt = "Summarize these observations briefly:\n" + "\n".join(action.observation or "" for action in actions)
    summary = llm.complete(summary_prompt) or ""
    result = AgenticResult(table=target_table, actions=actions, summary=summary)

    context.resources.result_sink.write_json("agentic", "result", result.model_dump())
    return [result]
