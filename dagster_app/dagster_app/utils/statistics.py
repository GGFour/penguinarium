"""Statistical quality checks for dataset columns."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd


# Thresholds used by the statistical checkers. These values were chosen to
# surface suspicious behaviour without being overly aggressive for typical
# tabular datasets.
NULL_RATIO_WARNING_THRESHOLD = 0.10
NULL_RATIO_CRITICAL_THRESHOLD = 0.50
DISTINCT_RATIO_MIN_THRESHOLD = 0.02
DOMINANT_CATEGORY_THRESHOLD = 0.95
OUTLIER_RATIO_WARNING_THRESHOLD = 0.05
OUTLIER_RATIO_CRITICAL_THRESHOLD = 0.15
SKEWNESS_WARNING_THRESHOLD = 2.0
KURTOSIS_WARNING_THRESHOLD = 3.0
LOW_VARIANCE_THRESHOLD = 1e-9
MAX_ANOMALY_SAMPLES = 20
MAX_ALERT_CONTEXT_ROWS = 5


@dataclass
class ColumnCheckResult:
    """Summary of a single statistical check executed for a column."""

    name: str
    passed: bool
    severity: str
    message: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    samples: List[Dict[str, Any]] = field(default_factory=list)
    locations: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        """Return a serialisable representation of the check result."""

        return {
            "name": self.name,
            "passed": self.passed,
            "severity": self.severity,
            "message": self.message,
            "metrics": self.metrics,
            "samples": self.samples,
            "locations": self.locations,
        }


@dataclass
class ColumnStatistics:
    """Computed statistics and check results for a dataset column."""

    table: str
    column: str
    metrics: Dict[str, Any]
    checks: List[ColumnCheckResult]
    issue_summary: Dict[str, Any]

    def to_payload(self) -> Dict[str, Any]:
        """Return a serialisable representation of the statistics."""

        return {
            "table": self.table,
            "column": self.column,
            "metrics": self.metrics,
            "checks": [check.to_payload() for check in self.checks],
            "issue_summary": self.issue_summary,
        }


@dataclass
class DatasetStatistics:
    generated_at: str
    columns: List[ColumnStatistics]
    tables: List["TableStatistics"]
    metrics: Dict[str, Any]


@dataclass
class TableStatistics:
    table: str
    row_count: int
    column_count: int
    metrics: Dict[str, Any]
    issue_summary: Dict[str, Any]

    def to_payload(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "metrics": self.metrics,
            "issue_summary": self.issue_summary,
        }


@dataclass
class AlertCandidate:
    """Alert extracted from column or table statistics."""

    table: Optional[str]
    column: Optional[str]
    check_name: str
    name: str
    severity: str
    message: str
    details: Dict[str, Any]
    triggered_at: datetime

    def to_payload(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "column": self.column,
            "check_name": self.check_name,
            "name": self.name,
            "severity": self.severity,
            "message": self.message,
            "details": self.details,
            "triggered_at": self.triggered_at.isoformat(),
        }


def _numeric_series(series: pd.Series) -> pd.Series:
    return series.dropna().apply(pd.to_numeric, errors="coerce").dropna()


def _json_safe(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        if pd.isna(value):
            return None
        return pd.Timestamp(value).isoformat()
    if isinstance(value, (pd.Timedelta, np.timedelta64)):
        if pd.isna(value):
            return None
        return str(pd.Timedelta(value))
    if value is None:
        return None
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except TypeError:
        pass
    return value


def _build_position_lookup(index: pd.Index) -> Dict[Any, List[int]]:
    lookup: Dict[Any, List[int]] = {}
    for position, key in enumerate(index.tolist()):
        lookup.setdefault(key, []).append(position)
    return lookup


def _positions_to_ranges(positions: List[int]) -> List[Dict[str, int]]:
    if not positions:
        return []
    sorted_positions = sorted(set(int(pos) for pos in positions))
    ranges: List[Dict[str, int]] = []
    start = prev = sorted_positions[0]
    count = 1
    for pos in sorted_positions[1:]:
        if pos == prev:
            continue
        if pos == prev + 1:
            prev = pos
            count += 1
            continue
        ranges.append({"start": int(start), "end": int(prev), "count": int(count)})
        start = prev = pos
        count = 1
    ranges.append({"start": int(start), "end": int(prev), "count": int(count)})
    return ranges


def _build_anomaly_summary(
    original_series: pd.Series,
    anomaly_indices: List[Any],
    *,
    values_series: pd.Series | None = None,
    max_samples: int = MAX_ANOMALY_SAMPLES,
) -> Dict[str, Any]:
    if not anomaly_indices:
        return {"samples": [], "row_ranges": [], "total_count": 0, "max_consecutive_run": 0}

    lookup = _build_position_lookup(original_series.index)
    samples: List[Dict[str, Any]] = []
    positions: List[int] = []
    for index_value in anomaly_indices:
        candidates = lookup.get(index_value)
        if not candidates:
            continue
        position = candidates.pop(0)
        positions.append(position)
        if len(samples) >= max_samples:
            continue
        if values_series is not None:
            try:
                value = values_series.loc[index_value]
                if isinstance(value, pd.Series):
                    value = value.iloc[0]
            except KeyError:
                value = original_series.iloc[position]
        else:
            value = original_series.iloc[position]
        samples.append(
            {
                "row_number": int(position),
                "row_index": _json_safe(index_value),
                "value": _json_safe(value),
            }
        )

    row_ranges = _positions_to_ranges(positions)
    max_run = max((rng["count"] for rng in row_ranges), default=0)
    return {
        "samples": samples,
        "row_ranges": row_ranges,
        "total_count": int(len(anomaly_indices)),
        "max_consecutive_run": int(max_run),
    }


def _extract_row_context(
    df: pd.DataFrame,
    column: str,
    locations: Dict[str, Any] | None,
    *,
    max_rows: int = MAX_ALERT_CONTEXT_ROWS,
) -> List[Dict[str, Any]]:
    if not locations:
        return []
    row_ranges = locations.get("row_ranges")
    if not row_ranges:
        return []

    contexts: List[Dict[str, Any]] = []
    total_rows = df.shape[0]
    for row_range in row_ranges:
        start = int(row_range.get("start", 0))
        end = int(row_range.get("end", start))
        if start < 0:
            start = 0
        if end < start:
            end = start
        if start >= total_rows:
            continue
        resolved_end = min(end, total_rows - 1)
        limit = min(resolved_end + 1, start + max_rows)
        subset = df.iloc[start:limit]
        samples: List[Dict[str, Any]] = []
        for offset, (idx, row) in enumerate(subset.iterrows()):
            row_dict = {str(col): _json_safe(val) for col, val in row.to_dict().items()}
            samples.append(
                {
                    "row_number": int(start + offset),
                    "row_index": _json_safe(idx),
                    "value": _json_safe(row.get(column)) if column in row else None,
                    "row_snapshot": row_dict,
                }
            )
        contexts.append(
            {
                "range": {
                    "start": int(row_range.get("start", start)),
                    "end": int(row_range.get("end", resolved_end)),
                    "count": int(row_range.get("count", resolved_end - start + 1)),
                },
                "row_samples": samples,
            }
        )

    return contexts


def _longest_true_run(flags: Iterable[bool]) -> tuple[int, List[int]]:
    max_length = 0
    best_range: List[int] = []
    current_start: int | None = None
    for position, value in enumerate(flags):
        if value:
            if current_start is None:
                current_start = position
            current_length = position - current_start + 1
            if current_length > max_length:
                max_length = current_length
                best_range = list(range(current_start, position + 1))
        else:
            current_start = None
    return max_length, best_range


def _build_check(
    name: str,
    *,
    passed: bool,
    severity: str = "warning",
    message: str,
    metrics: Dict[str, Any] | None = None,
    samples: List[Dict[str, Any]] | None = None,
    locations: Dict[str, Any] | None = None,
) -> ColumnCheckResult:
    """Helper to build :class:`ColumnCheckResult` objects with consistent defaults."""

    resolved_severity = "info" if passed else severity
    return ColumnCheckResult(
        name=name,
        passed=passed,
        severity=resolved_severity,
        message=message,
        metrics=metrics or {},
        samples=samples or [],
        locations=locations or {},
    )


def _build_skipped_check(name: str, reason: str) -> ColumnCheckResult:
    return _build_check(
        name,
        passed=True,
        severity="info",
        message=f"Check skipped: {reason}",
        metrics={"reason": reason},
    )


def _summarise_checks(checks: List[ColumnCheckResult]) -> Dict[str, Any]:
    failed = sum(1 for check in checks if not check.passed)
    warning = sum(1 for check in checks if not check.passed and check.severity == "warning")
    critical = sum(1 for check in checks if not check.passed and check.severity == "critical")
    return {
        "total_checks": len(checks),
        "passed_checks": len(checks) - failed,
        "failed_checks": failed,
        "warning_checks": warning,
        "critical_checks": critical,
    }


def _entropy_from_counts(counts: Iterable[int]) -> float | None:
    total = float(sum(counts))
    if total == 0:
        return None
    probabilities = np.array([count / total for count in counts if count > 0], dtype=float)
    if probabilities.size == 0:
        return None
    return float(-(probabilities * np.log2(probabilities)).sum())


def _dominant_value(series: pd.Series) -> tuple[Any | None, float | None]:
    if series.empty:
        return None, None
    counts = series.value_counts(dropna=True)
    if counts.empty:
        return None, None
    top_value = counts.index[0]
    ratio = float(counts.iloc[0] / series.shape[0])
    return top_value, ratio


def analyze_column(table: str, column: str, series: pd.Series) -> ColumnStatistics:
    total_count = int(series.shape[0])
    null_count = int(series.isna().sum())
    non_null_count = total_count - null_count

    metrics: Dict[str, Any] = {
        "total_count": total_count,
        "null_count": null_count,
        "null_ratio": (null_count / total_count) if total_count else None,
        "non_null_count": non_null_count,
    }
    checks: List[ColumnCheckResult] = []

    if total_count == 0:
        metrics["longest_null_run"] = 0
        checks.append(_build_skipped_check("null_ratio", "column has zero rows"))
        checks.append(_build_skipped_check("null_run_length", "column has zero rows"))
    else:
        null_ratio = metrics["null_ratio"] or 0.0
        threshold = NULL_RATIO_WARNING_THRESHOLD
        passed = null_ratio <= threshold
        severity = "warning"
        if null_ratio >= NULL_RATIO_CRITICAL_THRESHOLD:
            severity = "critical"
        summary = (
            _build_anomaly_summary(series, series[series.isna()].index.tolist())
            if not passed
            else None
        )
        message = (
            f"Null ratio {null_ratio:.2%} is within threshold {threshold:.0%}."
            if passed
            else f"Null ratio {null_ratio:.2%} exceeds threshold {threshold:.0%}."
        )
        checks.append(
            _build_check(
                "null_ratio",
                passed=passed,
                severity=severity,
                message=message,
                metrics={
                    "null_ratio": null_ratio,
                    "null_count": null_count,
                    "threshold": threshold,
                    "critical_threshold": NULL_RATIO_CRITICAL_THRESHOLD,
                },
                samples=summary["samples"] if summary else None,
                locations=(
                    {
                        "row_ranges": summary["row_ranges"],
                        "total_count": summary["total_count"],
                        "max_consecutive_run": summary["max_consecutive_run"],
                    }
                    if summary
                    else None
                ),
            )
        )

        longest_null_run, run_positions = _longest_true_run(series.isna().tolist())
        metrics["longest_null_run"] = int(longest_null_run)
        run_threshold = max(5, int(total_count * 0.1))
        critical_run_threshold = max(10, int(total_count * 0.3))
        if longest_null_run == 0:
            checks.append(
                _build_check(
                    "null_run_length",
                    passed=True,
                    severity="info",
                    message="No null streaks detected.",
                    metrics={
                        "longest_run": 0,
                        "threshold": run_threshold,
                        "critical_threshold": critical_run_threshold,
                    },
                )
            )
        else:
            run_indices = [
                series.index[pos]
                for pos in run_positions
                if 0 <= pos < series.shape[0]
            ]
            run_summary = _build_anomaly_summary(series, run_indices)
            passed_run = longest_null_run < run_threshold
            severity = "warning"
            if longest_null_run >= critical_run_threshold:
                severity = "critical"
            message = (
                f"Longest null streak spans {longest_null_run} rows and stays below threshold."
                if passed_run
                else f"Longest null streak spans {longest_null_run} rows exceeding threshold {run_threshold}."
            )
            checks.append(
                _build_check(
                    "null_run_length",
                    passed=passed_run,
                    severity=severity,
                    message=message,
                    metrics={
                        "longest_run": longest_null_run,
                        "threshold": run_threshold,
                        "critical_threshold": critical_run_threshold,
                    },
                    samples=run_summary["samples"],
                    locations={
                        "row_ranges": run_summary["row_ranges"],
                        "total_count": run_summary["total_count"],
                        "max_consecutive_run": run_summary["max_consecutive_run"],
                    },
                )
            )

    if non_null_count:
        non_null_series = series.dropna()
        distinct_count = int(non_null_series.nunique(dropna=True))
        distinct_ratio = float(distinct_count / non_null_count) if non_null_count else None
        duplicate_mask = non_null_series.duplicated(keep=False)
        duplicate_count = int(duplicate_mask.sum())
        metrics.update(
            {
                "distinct_count": distinct_count,
                "distinct_ratio": distinct_ratio,
                "duplicate_count": duplicate_count,
            }
        )

        if distinct_ratio is None:
            checks.append(_build_skipped_check("distinct_ratio", "no non-null values"))
        else:
            unique_coverage = distinct_count == non_null_count
            passed = distinct_ratio >= DISTINCT_RATIO_MIN_THRESHOLD or unique_coverage
            severity = "warning"
            if not passed and distinct_ratio == 0.0 and non_null_count > 1:
                severity = "critical"
            summary = (
                _build_anomaly_summary(
                    series, non_null_series.index[duplicate_mask].tolist()
                )
                if (not passed and duplicate_count)
                else None
            )
            message = (
                f"Distinct ratio {distinct_ratio:.2%} is healthy."
                if passed
                else f"Distinct ratio {distinct_ratio:.2%} falls below {DISTINCT_RATIO_MIN_THRESHOLD:.2%}."
            )
            checks.append(
                _build_check(
                    "distinct_ratio",
                    passed=passed,
                    severity=severity,
                    message=message,
                    metrics={
                        "distinct_ratio": distinct_ratio,
                        "distinct_count": distinct_count,
                        "threshold": DISTINCT_RATIO_MIN_THRESHOLD,
                        "duplicate_count": duplicate_count,
                    },
                    samples=summary["samples"] if summary else None,
                    locations=(
                        {
                            "row_ranges": summary["row_ranges"],
                            "total_count": summary["total_count"],
                            "max_consecutive_run": summary["max_consecutive_run"],
                        }
                        if summary
                        else None
                    ),
                )
            )

        top_value, top_ratio = _dominant_value(non_null_series)
        metrics.update(
            {
                "most_frequent_value": top_value,
                "most_frequent_ratio": top_ratio,
            }
        )
        if top_ratio is None:
            checks.append(_build_skipped_check("dominant_category", "no non-null values"))
        else:
            passed = top_ratio <= DOMINANT_CATEGORY_THRESHOLD
            severity = "warning"
            if not passed and top_ratio >= 0.99:
                severity = "critical"
            summary = (
                _build_anomaly_summary(
                    series, non_null_series[non_null_series == top_value].index.tolist()
                )
                if not passed
                else None
            )
            message = (
                f"Most frequent value ratio {top_ratio:.2%} is acceptable."
                if passed
                else f"Most frequent value ratio {top_ratio:.2%} exceeds {DOMINANT_CATEGORY_THRESHOLD:.0%}."
            )
            checks.append(
                _build_check(
                    "dominant_category",
                    passed=passed,
                    severity=severity,
                    message=message,
                    metrics={
                        "most_frequent_ratio": top_ratio,
                        "most_frequent_value": top_value,
                        "threshold": DOMINANT_CATEGORY_THRESHOLD,
                    },
                    samples=summary["samples"] if summary else None,
                    locations=(
                        {
                            "row_ranges": summary["row_ranges"],
                            "total_count": summary["total_count"],
                            "max_consecutive_run": summary["max_consecutive_run"],
                        }
                        if summary
                        else None
                    ),
                )
            )

        counts = non_null_series.value_counts(dropna=True)
        entropy = _entropy_from_counts(counts.values.tolist())
        metrics["entropy"] = entropy
        if entropy is None or distinct_count <= 1:
            checks.append(
                _build_skipped_check("entropy", "insufficient distinct values to assess entropy")
            )
        else:
            max_entropy = float(np.log2(distinct_count)) if distinct_count > 0 else None
            entropy_ratio = (entropy / max_entropy) if max_entropy else None
            metrics["entropy_ratio"] = entropy_ratio
            if entropy_ratio is None:
                checks.append(_build_skipped_check("entropy", "entropy ratio unavailable"))
            else:
                passed = entropy_ratio >= 0.3
                severity = "warning"
                if not passed and entropy_ratio < 0.1:
                    severity = "critical"
                message = (
                    f"Entropy ratio {entropy_ratio:.2f} is acceptable."
                    if passed
                    else f"Entropy ratio {entropy_ratio:.2f} suggests low variability."
                )
                checks.append(
                    _build_check(
                        "entropy",
                        passed=passed,
                        severity=severity,
                        message=message,
                        metrics={
                            "entropy": entropy,
                            "entropy_ratio": entropy_ratio,
                            "distinct_count": distinct_count,
                            "warning_ratio": 0.3,
                            "critical_ratio": 0.1,
                        },
                    )
                )
    else:
        metrics["distinct_count"] = 0
        metrics["distinct_ratio"] = None
        metrics["most_frequent_value"] = None
        metrics["most_frequent_ratio"] = None
        metrics["entropy"] = None
        metrics["duplicate_count"] = 0
        checks.append(_build_skipped_check("distinct_ratio", "no non-null values"))
        checks.append(_build_skipped_check("dominant_category", "no non-null values"))
        checks.append(_build_skipped_check("entropy", "no non-null values"))

    numeric_series = _numeric_series(series)
    if numeric_series.empty:
        metrics["inferred_type"] = "categorical"
        checks.append(
            _build_skipped_check("numeric_profile", "column does not contain numeric data")
        )
        issue_summary = _summarise_checks(checks)
        return ColumnStatistics(
            table=table,
            column=column,
            metrics=metrics,
            checks=checks,
            issue_summary=issue_summary,
        )

    metrics["inferred_type"] = "numeric"
    metrics.update(
        {
            "sum": float(numeric_series.sum()),
            "mean": float(numeric_series.mean()),
            "min": float(numeric_series.min()),
            "max": float(numeric_series.max()),
            "median": float(numeric_series.median()),
        }
    )

    if numeric_series.size >= 2:
        std = float(numeric_series.std(ddof=0))
        variance = float(numeric_series.var(ddof=0))
    else:
        std = 0.0
        variance = 0.0
    metrics["std_dev"] = std
    metrics["variance"] = variance

    if numeric_series.size:
        q1 = float(numeric_series.quantile(0.25))
        q3 = float(numeric_series.quantile(0.75))
        iqr = float(q3 - q1)
    else:
        q1 = q3 = None
        iqr = None
    metrics.update({"q1": q1, "q3": q3, "iqr": iqr})

    mad = (
        float((numeric_series - metrics["median"]).abs().median())
        if numeric_series.size
        else None
    )
    metrics["mad"] = mad

    if std <= LOW_VARIANCE_THRESHOLD:
        checks.append(
            _build_check(
                "low_variance",
                passed=False,
                severity="warning",
                message="Column exhibits near-zero variance.",
                metrics={"std_dev": std, "threshold": LOW_VARIANCE_THRESHOLD},
            )
        )
    else:
        checks.append(
            _build_check(
                "low_variance",
                passed=True,
                message="Column variance is within expected range.",
                metrics={"std_dev": std, "threshold": LOW_VARIANCE_THRESHOLD},
            )
        )

    if std == 0:
        checks.append(
            _build_skipped_check(
                "z_score_outliers", "standard deviation is zero; z-score detection unavailable"
            )
        )
    else:
        z_scores = ((numeric_series - metrics["mean"]) / std).abs()
        mask = z_scores > 3
        outlier_count = int(mask.sum())
        outlier_ratio = float(outlier_count / numeric_series.size) if numeric_series.size else 0.0
        passed = outlier_ratio <= OUTLIER_RATIO_WARNING_THRESHOLD
        severity = "warning"
        if outlier_ratio >= OUTLIER_RATIO_CRITICAL_THRESHOLD:
            severity = "critical"
        summary = (
            _build_anomaly_summary(series, numeric_series.index[mask].tolist(), values_series=numeric_series)
            if outlier_count
            else None
        )
        message = (
            f"Z-score outlier ratio {outlier_ratio:.2%} is acceptable."
            if passed
            else f"Z-score outlier ratio {outlier_ratio:.2%} exceeds {OUTLIER_RATIO_WARNING_THRESHOLD:.0%}."
        )
        checks.append(
            _build_check(
                "z_score_outliers",
                passed=passed,
                severity=severity,
                message=message,
                metrics={
                    "outlier_count": outlier_count,
                    "outlier_ratio": outlier_ratio,
                    "threshold": OUTLIER_RATIO_WARNING_THRESHOLD,
                    "critical_threshold": OUTLIER_RATIO_CRITICAL_THRESHOLD,
                },
                samples=summary["samples"] if summary else None,
                locations=(
                    {
                        "row_ranges": summary["row_ranges"],
                        "total_count": summary["total_count"],
                        "max_consecutive_run": summary["max_consecutive_run"],
                    }
                    if summary
                    else None
                ),
            )
        )

    if iqr is None or iqr == 0:
        checks.append(
            _build_skipped_check("iqr_outliers", "interquartile range is zero; cannot compute fences")
        )
    else:
        lower_fence = metrics["q1"] - 1.5 * iqr if metrics["q1"] is not None else None
        upper_fence = metrics["q3"] + 1.5 * iqr if metrics["q3"] is not None else None
        mask = pd.Series(False, index=numeric_series.index)
        if lower_fence is not None:
            mask = mask | (numeric_series < lower_fence)
        if upper_fence is not None:
            mask = mask | (numeric_series > upper_fence)
        outlier_count = int(mask.sum())
        outlier_ratio = float(outlier_count / numeric_series.size) if numeric_series.size else 0.0
        passed = outlier_ratio <= OUTLIER_RATIO_WARNING_THRESHOLD
        severity = "warning"
        if outlier_ratio >= OUTLIER_RATIO_CRITICAL_THRESHOLD:
            severity = "critical"
        summary = (
            _build_anomaly_summary(series, numeric_series.index[mask].tolist(), values_series=numeric_series)
            if outlier_count
            else None
        )
        message = (
            f"IQR outlier ratio {outlier_ratio:.2%} is acceptable."
            if passed
            else f"IQR outlier ratio {outlier_ratio:.2%} exceeds {OUTLIER_RATIO_WARNING_THRESHOLD:.0%}."
        )
        checks.append(
            _build_check(
                "iqr_outliers",
                passed=passed,
                severity=severity,
                message=message,
                metrics={
                    "outlier_count": outlier_count,
                    "outlier_ratio": outlier_ratio,
                    "threshold": OUTLIER_RATIO_WARNING_THRESHOLD,
                    "lower_fence": lower_fence,
                    "upper_fence": upper_fence,
                    "critical_threshold": OUTLIER_RATIO_CRITICAL_THRESHOLD,
                },
                samples=summary["samples"] if summary else None,
                locations=(
                    {
                        "row_ranges": summary["row_ranges"],
                        "total_count": summary["total_count"],
                        "max_consecutive_run": summary["max_consecutive_run"],
                    }
                    if summary
                    else None
                ),
            )
        )

    if mad is None or mad == 0:
        checks.append(
            _build_skipped_check(
                "modified_z_score", "median absolute deviation is zero; cannot compute modified z-scores"
            )
        )
    else:
        modified_z_scores = 0.6745 * (numeric_series - metrics["median"]) / mad
        mask = modified_z_scores.abs() > 3.5
        outlier_count = int(mask.sum())
        outlier_ratio = float(outlier_count / numeric_series.size) if numeric_series.size else 0.0
        passed = outlier_ratio <= OUTLIER_RATIO_WARNING_THRESHOLD
        severity = "warning"
        if outlier_ratio >= OUTLIER_RATIO_CRITICAL_THRESHOLD:
            severity = "critical"
        summary = (
            _build_anomaly_summary(series, numeric_series.index[mask].tolist(), values_series=numeric_series)
            if outlier_count
            else None
        )
        message = (
            f"Modified z-score outlier ratio {outlier_ratio:.2%} is acceptable."
            if passed
            else f"Modified z-score outlier ratio {outlier_ratio:.2%} exceeds {OUTLIER_RATIO_WARNING_THRESHOLD:.0%}."
        )
        checks.append(
            _build_check(
                "modified_z_score",
                passed=passed,
                severity=severity,
                message=message,
                metrics={
                    "outlier_count": outlier_count,
                    "outlier_ratio": outlier_ratio,
                    "threshold": OUTLIER_RATIO_WARNING_THRESHOLD,
                    "critical_threshold": OUTLIER_RATIO_CRITICAL_THRESHOLD,
                },
                samples=summary["samples"] if summary else None,
                locations=(
                    {
                        "row_ranges": summary["row_ranges"],
                        "total_count": summary["total_count"],
                        "max_consecutive_run": summary["max_consecutive_run"],
                    }
                    if summary
                    else None
                ),
            )
        )

    if numeric_series.size >= 3:
        skewness_val = numeric_series.skew()
        kurtosis_val = numeric_series.kurtosis()
        metrics["skewness"] = float(skewness_val) if pd.notna(skewness_val) else None
        metrics["kurtosis"] = float(kurtosis_val) if pd.notna(kurtosis_val) else None

        if pd.notna(skewness_val):
            passed_skew = abs(skewness_val) <= SKEWNESS_WARNING_THRESHOLD
            severity = "warning"
            if not passed_skew and abs(skewness_val) >= SKEWNESS_WARNING_THRESHOLD * 2:
                severity = "critical"
            message_skew = (
                f"Skewness {skewness_val:.2f} is within expected bounds."
                if passed_skew
                else f"Skewness {skewness_val:.2f} exceeds ±{SKEWNESS_WARNING_THRESHOLD}."
            )
            checks.append(
                _build_check(
                    "skewness",
                    passed=passed_skew,
                    severity=severity,
                    message=message_skew,
                    metrics={
                        "skewness": float(skewness_val),
                        "threshold": SKEWNESS_WARNING_THRESHOLD,
                        "critical_threshold": SKEWNESS_WARNING_THRESHOLD * 2,
                    },
                )
            )
        else:
            checks.append(
                _build_skipped_check("skewness", "skewness undefined for constant series")
            )

        if pd.notna(kurtosis_val):
            passed_kurt = abs(kurtosis_val) <= KURTOSIS_WARNING_THRESHOLD
            severity = "warning"
            if not passed_kurt and abs(kurtosis_val) >= KURTOSIS_WARNING_THRESHOLD * 2:
                severity = "critical"
            message_kurt = (
                f"Kurtosis {kurtosis_val:.2f} is within expected bounds."
                if passed_kurt
                else f"Kurtosis {kurtosis_val:.2f} exceeds ±{KURTOSIS_WARNING_THRESHOLD}."
            )
            checks.append(
                _build_check(
                    "kurtosis",
                    passed=passed_kurt,
                    severity=severity,
                    message=message_kurt,
                    metrics={
                        "kurtosis": float(kurtosis_val),
                        "threshold": KURTOSIS_WARNING_THRESHOLD,
                        "critical_threshold": KURTOSIS_WARNING_THRESHOLD * 2,
                    },
                )
            )
        else:
            checks.append(
                _build_skipped_check("kurtosis", "kurtosis undefined for constant series")
            )
    else:
        metrics["skewness"] = None
        metrics["kurtosis"] = None
        checks.append(
            _build_skipped_check("skewness", "at least three numeric values required")
        )
        checks.append(
            _build_skipped_check("kurtosis", "at least three numeric values required")
        )

    issue_summary = _summarise_checks(checks)
    return ColumnStatistics(
        table=table,
        column=column,
        metrics=metrics,
        checks=checks,
        issue_summary=issue_summary,
    )


def build_alert_candidates(
    dataset: Dict[str, pd.DataFrame],
    statistics: DatasetStatistics,
    *,
    max_context_rows: int = MAX_ALERT_CONTEXT_ROWS,
) -> List[AlertCandidate]:
    try:
        triggered_at = datetime.fromisoformat(statistics.generated_at)
    except ValueError:
        triggered_at = datetime.utcnow()

    alerts: List[AlertCandidate] = []

    for column_stats in statistics.columns:
        table_name = column_stats.table
        column_name = column_stats.column
        df = dataset.get(table_name)

        for check in column_stats.checks:
            if check.passed:
                continue

            severity = check.severity or "warning"
            alert_name = f"{table_name}.{column_name}::{check.name}"
            row_context: List[Dict[str, Any]] = []
            if (
                df is not None
                and column_name in df.columns
                and check.locations
            ):
                row_context = _extract_row_context(
                    df, column_name, check.locations, max_rows=max_context_rows
                )

            details = {
                "table": table_name,
                "column": column_name,
                "check_name": check.name,
                "message": check.message,
                "metrics": check.metrics,
                "samples": check.samples,
                "locations": check.locations,
                "row_context": row_context,
                "column_metrics": column_stats.metrics,
                "issue_summary": column_stats.issue_summary,
            }

            alerts.append(
                AlertCandidate(
                    table=table_name,
                    column=column_name,
                    check_name=check.name,
                    name=alert_name,
                    severity=severity,
                    message=check.message,
                    details=details,
                    triggered_at=triggered_at,
                )
            )

    for table_stats in statistics.tables:
        failed_checks = int(table_stats.issue_summary.get("failed_checks", 0) or 0)
        warning_checks = int(table_stats.issue_summary.get("warning_checks", 0) or 0)
        critical_checks = int(table_stats.issue_summary.get("critical_checks", 0) or 0)
        if failed_checks == 0:
            continue

        severity = "critical" if critical_checks > 0 else "warning"
        if critical_checks:
            message = (
                f"Table '{table_stats.table}' has {critical_checks} critical and "
                f"{warning_checks} warning column checks failing."
            )
        else:
            message = (
                f"Table '{table_stats.table}' has {warning_checks} warning column checks failing."
            )

        details = {
            "table": table_stats.table,
            "metrics": table_stats.metrics,
            "issue_summary": table_stats.issue_summary,
        }

        alerts.append(
            AlertCandidate(
                table=table_stats.table,
                column=None,
                check_name="table_summary",
                name=f"{table_stats.table}::summary_issues",
                severity=severity,
                message=message,
                details=details,
                triggered_at=triggered_at,
            )
        )

    return alerts


def compute_statistics(dataset: Dict[str, pd.DataFrame]) -> DatasetStatistics:
    column_results: List[ColumnStatistics] = []
    table_registry: Dict[str, Dict[str, Any]] = {}

    for table_name, df in dataset.items():
        row_count = int(df.shape[0])
        column_count = int(len(df.columns))
        table_state = table_registry.setdefault(
            table_name,
            {"row_count": row_count, "column_count": column_count, "columns": []},
        )
        table_state["row_count"] = row_count
        table_state["column_count"] = column_count

        for column in df.columns:
            column_stats = analyze_column(table_name, column, df[column])
            column_results.append(column_stats)
            table_state["columns"].append(column_stats)

    table_summaries: List[TableStatistics] = []
    total_rows = 0
    total_cells = 0
    total_null_cells = 0
    total_failed_checks = 0
    total_warning_checks = 0
    total_critical_checks = 0
    dataset_issue_columns: set[str] = set()
    dataset_critical_columns: set[str] = set()
    max_null_run_overall = 0

    for table_name, info in table_registry.items():
        row_count = int(info.get("row_count", 0))
        column_count = int(info.get("column_count", 0))
        columns: List[ColumnStatistics] = info.get("columns", [])
        cell_count = row_count * column_count
        null_cells = sum(int(col.metrics.get("null_count") or 0) for col in columns)
        failed_checks = sum(int(col.issue_summary.get("failed_checks", 0)) for col in columns)
        warning_checks = sum(int(col.issue_summary.get("warning_checks", 0)) for col in columns)
        critical_checks = sum(int(col.issue_summary.get("critical_checks", 0)) for col in columns)
        issue_columns = {
            col.column
            for col in columns
            if int(col.issue_summary.get("failed_checks", 0)) > 0
        }
        critical_columns = {
            col.column
            for col in columns
            if int(col.issue_summary.get("critical_checks", 0)) > 0
        }
        max_null_run = max(int(col.metrics.get("longest_null_run") or 0) for col in columns) if columns else 0

        total_rows += row_count
        total_cells += cell_count
        total_null_cells += null_cells
        total_failed_checks += failed_checks
        total_warning_checks += warning_checks
        total_critical_checks += critical_checks
        dataset_issue_columns.update(f"{table_name}.{col}" for col in issue_columns)
        dataset_critical_columns.update(f"{table_name}.{col}" for col in critical_columns)
        max_null_run_overall = max(max_null_run_overall, max_null_run)

        table_metrics = {
            "cell_count": cell_count,
            "null_cell_count": null_cells,
            "null_cell_ratio": (null_cells / cell_count) if cell_count else None,
            "failed_check_ratio_per_column": (
                failed_checks / column_count if column_count else None
            ),
            "max_null_run": max_null_run,
        }

        table_issue_summary = {
            "failed_checks": failed_checks,
            "warning_checks": warning_checks,
            "critical_checks": critical_checks,
            "failed_column_count": len(issue_columns),
            "critical_column_count": len(critical_columns),
            "columns_with_issues": sorted(issue_columns),
            "critical_columns": sorted(critical_columns),
        }

        table_summaries.append(
            TableStatistics(
                table=table_name,
                row_count=row_count,
                column_count=column_count,
                metrics=table_metrics,
                issue_summary=table_issue_summary,
            )
        )

    dataset_metrics = {
        "table_count": len(table_registry),
        "column_count": len(column_results),
        "row_count": total_rows,
        "cell_count": total_cells,
        "null_cell_count": total_null_cells,
        "null_cell_ratio": (total_null_cells / total_cells) if total_cells else None,
        "failed_checks": total_failed_checks,
        "warning_checks": total_warning_checks,
        "critical_checks": total_critical_checks,
        "failed_column_count": len(dataset_issue_columns),
        "critical_column_count": len(dataset_critical_columns),
        "columns_with_issues": sorted(dataset_issue_columns),
        "critical_columns": sorted(dataset_critical_columns),
        "max_null_run": max_null_run_overall,
    }

    generated_at = datetime.utcnow().isoformat()
    return DatasetStatistics(
        generated_at=generated_at,
        columns=column_results,
        tables=table_summaries,
        metrics=dataset_metrics,
    )
