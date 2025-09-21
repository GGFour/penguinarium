"""Tests for synthetic LLM alert generation utilities."""

from __future__ import annotations

import pandas as pd
import pytest

from dagster_app.utils.alerts import (
    ALERT_SEVERITIES,
    ALERT_STATUS_ACTIVE,
    generate_random_llm_alerts,
)


def test_generate_random_llm_alerts_returns_expected_count_and_shape():
    dataset = {
        "application_train": pd.DataFrame(
            {
                "AMT_INCOME_TOTAL": [270000, 202500, 135000],
                "NAME_CONTRACT_TYPE": ["Cash loans", "Revolving loans", "Cash loans"],
            }
        )
    }

    alerts = generate_random_llm_alerts(dataset, count=5)

    assert len(alerts) == 5
    for alert in alerts:
        assert alert.status == ALERT_STATUS_ACTIVE
        assert alert.severity in ALERT_SEVERITIES
        assert alert.table_name in dataset
        assert alert.field_name in dataset[alert.table_name].columns
        assert alert.details["generated_by"] == "llm_pipeline"


def test_generate_random_llm_alerts_requires_non_empty_dataset():
    with pytest.raises(ValueError):
        generate_random_llm_alerts({})
