"""Tests for the extended statistical quality checks."""

from __future__ import annotations

import numpy as np
import pandas as pd

from dagster_app.utils.statistics import (
    analyze_column,
    build_alert_candidates,
    compute_statistics,
)


def _checks_by_name(column_statistics):
    return {check.name: check for check in column_statistics.checks}


def test_analyze_column_numeric_outliers_flagged():
    series = pd.Series([1, 1, 1, 1, 5, 100, np.nan])

    stats = analyze_column("tbl", "value", series)
    checks = _checks_by_name(stats)

    assert stats.metrics["total_count"] == len(series)
    assert stats.metrics["null_count"] == 1
    assert stats.metrics["inferred_type"] == "numeric"
    assert stats.issue_summary["failed_checks"] >= 2

    assert "iqr_outliers" in checks
    assert checks["iqr_outliers"].passed is False
    assert checks["iqr_outliers"].metrics["outlier_count"] >= 1
    assert checks["iqr_outliers"].samples
    assert checks["iqr_outliers"].locations["row_ranges"]

    assert checks["null_ratio"].severity == "warning"
    assert checks["null_ratio"].locations["total_count"] == 1


def test_analyze_column_constant_series_detects_issues():
    series = pd.Series([5, 5, 5, 5, 5], dtype=float)

    stats = analyze_column("tbl", "constant", series)
    checks = _checks_by_name(stats)

    assert stats.metrics["inferred_type"] == "numeric"
    assert checks["low_variance"].passed is False
    assert checks["dominant_category"].passed is False
    assert checks["dominant_category"].severity == "critical"
    assert stats.issue_summary["critical_checks"] == 1
    assert checks["skewness"].severity == "info"


def test_categorical_column_records_entropy_and_skips_numeric_checks():
    series = pd.Series(["A", "A", "B", None])

    stats = analyze_column("tbl", "label", series)
    checks = _checks_by_name(stats)

    assert stats.metrics["inferred_type"] == "categorical"
    assert stats.metrics["entropy"] is not None
    assert checks["numeric_profile"].metrics["reason"] == "column does not contain numeric data"
    assert stats.issue_summary["failed_checks"] == 1
    assert checks["null_ratio"].locations["total_count"] == 1


def test_compute_statistics_builds_dataset_summary():
    dataset = {"foo": pd.DataFrame({"value": [1, 2, 3]})}

    stats = compute_statistics(dataset)

    assert stats.generated_at
    assert len(stats.columns) == 1
    column_stats = stats.columns[0]
    assert column_stats.table == "foo"
    assert column_stats.metrics["sum"] == 6.0
    assert stats.tables
    table_summary = stats.tables[0]
    assert table_summary.table == "foo"
    assert table_summary.metrics["cell_count"] == 3
    assert stats.metrics["table_count"] == 1
    assert stats.metrics["columns_with_issues"] == []


def test_null_run_length_captures_long_sequences():
    series = pd.Series([1] + [None] * 8 + list(range(2, 10)))

    stats = analyze_column("tbl", "nullable", series)
    checks = _checks_by_name(stats)

    null_run = checks["null_run_length"]
    assert null_run.passed is False
    assert null_run.locations["total_count"] == 8
    assert null_run.locations["row_ranges"][0]["start"] == 1
    assert stats.metrics["longest_null_run"] == 8


def test_build_alert_candidates_includes_row_context_and_table_summary():
    dataset = {
        "tbl": pd.DataFrame(
            {
                "value": [1, None, None, 5, 7, 1000],
                "other": [10, 11, 12, 13, 14, 15],
            }
        )
    }

    statistics = compute_statistics(dataset)
    alerts = build_alert_candidates(dataset, statistics)

    assert alerts
    null_alert = next(
        alert
        for alert in alerts
        if alert.table == "tbl" and alert.column == "value" and alert.check_name == "null_ratio"
    )
    assert null_alert.severity in {"warning", "critical"}
    assert null_alert.details["locations"]["total_count"] >= 1
    assert null_alert.details["row_context"]
    sample_context = null_alert.details["row_context"][0]["row_samples"][0]
    assert "row_snapshot" in sample_context
    assert isinstance(sample_context["row_snapshot"], dict)

    table_alert = next(alert for alert in alerts if alert.check_name == "table_summary")
    assert table_alert.table == "tbl"
    assert table_alert.column is None
    assert table_alert.details["issue_summary"]["failed_checks"] >= 1
