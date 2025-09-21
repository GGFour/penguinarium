"""Utilities for generating synthetic LLM-style alerts."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Mapping

import pandas as pd


ALERT_SEVERITIES = ["info", "warning", "critical"]
ALERT_STATUS_ACTIVE = "active"
DEFAULT_ALERT_COUNT = 5


@dataclass(frozen=True)
class LlmAlert:
    """Representation of an alert produced by the synthetic LLM pipeline."""

    name: str
    severity: str
    status: str
    details: dict[str, object]
    triggered_at: datetime
    table_name: str | None = None
    field_name: str | None = None


_ISSUE_DESCRIPTIONS = [
    "unexpected distribution shift detected",
    "semantic drift impacting predictive quality",
    "outlier cluster deviates from historical baseline",
    "data freshness below contractual SLO",
    "llm summary conflicts with upstream schema semantics",
    "potential personally identifiable information leakage",
]

_SUGGESTED_ACTIONS = [
    "retrain the downstream model with the latest partition",
    "trigger anomaly review workflow for the owning team",
    "increase monitoring frequency for this slice",
    "backfill the missing records and re-run checks",
    "escalate to data governance council for review",
    "apply semantic normalization to reconcile values",
]


def generate_random_llm_alerts(
    dataset: Mapping[str, pd.DataFrame], *, count: int = DEFAULT_ALERT_COUNT
) -> list[LlmAlert]:
    """Generate a collection of synthetic alerts using dataset context.

    Args:
        dataset: Mapping of table names to pandas DataFrames representing the
            ingested dataset.
        count: Number of alerts to generate. Defaults to ``DEFAULT_ALERT_COUNT``.

    Returns:
        A list of :class:`LlmAlert` objects containing randomized alert payloads.

    Raises:
        ValueError: If the provided dataset does not contain any tables or fields.
    """

    if not dataset:
        raise ValueError("Dataset is empty; cannot generate alerts without context.")

    column_context: list[tuple[str, str, pd.Series]] = []
    for table_name, frame in dataset.items():
        if frame.empty:
            continue
        for column in frame.columns:
            column_context.append((table_name, column, frame[column]))

    if not column_context:
        raise ValueError("Dataset does not include any columns to build alerts from.")

    rng = random.Random()
    alerts: list[LlmAlert] = []
    for _ in range(max(count, 0)):
        table_name, column_name, series = rng.choice(column_context)
        non_null_series = series.dropna()
        sample_value: object | None
        if not non_null_series.empty:
            sample_row = non_null_series.sample(n=1, random_state=rng.randint(0, 2**31 - 1))
            raw_value = sample_row.iloc[0]
            if hasattr(raw_value, "item"):
                sample_value = raw_value.item()  # type: ignore[attr-defined]
            else:
                sample_value = raw_value
        else:
            sample_value = None

        issue = rng.choice(_ISSUE_DESCRIPTIONS)
        suggestion = rng.choice(_SUGGESTED_ACTIONS)
        severity = rng.choices(ALERT_SEVERITIES, weights=[0.45, 0.4, 0.15])[0]
        triggered_at = datetime.utcnow() - timedelta(minutes=rng.randint(0, 6 * 60))

        details: dict[str, object] = {
            "generated_by": "llm_pipeline",
            "confidence": round(rng.uniform(0.55, 0.98), 2),
            "suggested_action": suggestion,
            "sample_value": sample_value,
            "issue_summary": issue,
        }

        alerts.append(
            LlmAlert(
                name=f"LLM alert for {table_name}.{column_name}",
                severity=severity,
                status=ALERT_STATUS_ACTIVE,
                details=details,
                triggered_at=triggered_at,
                table_name=table_name,
                field_name=column_name,
            )
        )

    return alerts


__all__ = ["LlmAlert", "generate_random_llm_alerts", "ALERT_SEVERITIES", "ALERT_STATUS_ACTIVE"]
