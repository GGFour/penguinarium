"""Tests for the extended statistical quality checks."""

from __future__ import annotations

import numpy as np
import pandas as pd

from dagster_app.utils.statistics import analyze_column, compute_statistics


def _checks_by_name(column_statistics):
    return {check.name: check for check in column_statistics.checks}


def test_analyze_column_numeric_outliers_flagged():
    series = pd.Series([1, 1, 1, 1, 5, 100, np.nan])

    stats = analyze_column("tbl", "value", series)
    checks = _checks_by_name(stats)

    assert stats.metrics["total_count"] == len(series)
    assert stats.metrics["null_count"] == 1
    assert stats.metrics["inferred_type"] == "numeric"

    assert "iqr_outliers" in checks
    assert checks["iqr_outliers"].passed is False
    assert checks["iqr_outliers"].metrics["outlier_count"] >= 1


def test_analyze_column_constant_series_detects_issues():
    series = pd.Series([5, 5, 5, 5, 5], dtype=float)

    stats = analyze_column("tbl", "constant", series)
    checks = _checks_by_name(stats)

    assert stats.metrics["inferred_type"] == "numeric"
    assert checks["low_variance"].passed is False
    assert checks["dominant_category"].passed is False


def test_categorical_column_records_entropy_and_skips_numeric_checks():
    series = pd.Series(["A", "A", "B", None])

    stats = analyze_column("tbl", "label", series)
    checks = _checks_by_name(stats)

    assert stats.metrics["inferred_type"] == "categorical"
    assert stats.metrics["entropy"] is not None
    assert checks["numeric_profile"].metrics["reason"] == "column does not contain numeric data"


def test_compute_statistics_builds_dataset_summary():
    dataset = {"foo": pd.DataFrame({"value": [1, 2, 3]})}

    stats = compute_statistics(dataset)

    assert stats.generated_at
    assert len(stats.columns) == 1
    column_stats = stats.columns[0]
    assert column_stats.table == "foo"
    assert column_stats.metrics["sum"] == 6.0
