"""Statistical quality checks for dataset columns."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List

import numpy as np
import pandas as pd


# Thresholds used by the statistical checkers. These values were chosen to
# surface suspicious behaviour without being overly aggressive for typical
# tabular datasets.
NULL_RATIO_WARNING_THRESHOLD = 0.10
DISTINCT_RATIO_MIN_THRESHOLD = 0.02
DOMINANT_CATEGORY_THRESHOLD = 0.95
OUTLIER_RATIO_WARNING_THRESHOLD = 0.05
SKEWNESS_WARNING_THRESHOLD = 2.0
KURTOSIS_WARNING_THRESHOLD = 3.0
LOW_VARIANCE_THRESHOLD = 1e-9


@dataclass
class ColumnCheckResult:
    """Summary of a single statistical check executed for a column."""

    name: str
    passed: bool
    severity: str
    message: str
    metrics: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        """Return a serialisable representation of the check result."""

        return {
            "name": self.name,
            "passed": self.passed,
            "severity": self.severity,
            "message": self.message,
            "metrics": self.metrics,
        }


@dataclass
class ColumnStatistics:
    """Computed statistics and check results for a dataset column."""

    table: str
    column: str
    metrics: Dict[str, Any]
    checks: List[ColumnCheckResult]

    def to_payload(self) -> Dict[str, Any]:
        """Return a serialisable representation of the statistics."""

        return {
            "metrics": self.metrics,
            "checks": [check.to_payload() for check in self.checks],
        }


@dataclass
class DatasetStatistics:
    generated_at: str
    columns: List[ColumnStatistics]


def _numeric_series(series: pd.Series) -> pd.Series:
    return series.dropna().apply(pd.to_numeric, errors="coerce").dropna()


def _build_check(
    name: str,
    *,
    passed: bool,
    severity: str = "warning",
    message: str,
    metrics: Dict[str, Any] | None = None,
) -> ColumnCheckResult:
    """Helper to build :class:`ColumnCheckResult` objects with consistent defaults."""

    resolved_severity = "info" if passed else severity
    return ColumnCheckResult(
        name=name,
        passed=passed,
        severity=resolved_severity,
        message=message,
        metrics=metrics or {},
    )


def _build_skipped_check(name: str, reason: str) -> ColumnCheckResult:
    return _build_check(
        name,
        passed=True,
        severity="info",
        message=f"Check skipped: {reason}",
        metrics={"reason": reason},
    )


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

    null_ratio = metrics["null_ratio"]
    if null_ratio is None:
        checks.append(_build_skipped_check("null_ratio", "column has zero rows"))
    else:
        threshold = NULL_RATIO_WARNING_THRESHOLD
        passed = null_ratio <= threshold
        message = (
            f"Null ratio {null_ratio:.2%} is within threshold {threshold:.0%}."
            if passed
            else f"Null ratio {null_ratio:.2%} exceeds threshold {threshold:.0%}."
        )
        checks.append(
            _build_check(
                "null_ratio",
                passed=passed,
                message=message,
                metrics={"null_ratio": null_ratio, "threshold": threshold},
            )
        )

    if non_null_count:
        non_null_series = series.dropna()
        distinct_count = int(non_null_series.nunique(dropna=True))
        distinct_ratio = float(distinct_count / non_null_count) if non_null_count else None
        metrics.update(
            {
                "distinct_count": distinct_count,
                "distinct_ratio": distinct_ratio,
            }
        )

        if distinct_ratio is None:
            checks.append(_build_skipped_check("distinct_ratio", "no non-null values"))
        else:
            passed = distinct_ratio >= DISTINCT_RATIO_MIN_THRESHOLD or distinct_count == non_null_count
            message = (
                f"Distinct ratio {distinct_ratio:.2%} is healthy."
                if passed
                else f"Distinct ratio {distinct_ratio:.2%} falls below {DISTINCT_RATIO_MIN_THRESHOLD:.2%}."
            )
            checks.append(
                _build_check(
                    "distinct_ratio",
                    passed=passed,
                    message=message,
                    metrics={
                        "distinct_ratio": distinct_ratio,
                        "distinct_count": distinct_count,
                        "threshold": DISTINCT_RATIO_MIN_THRESHOLD,
                    },
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
            message = (
                f"Most frequent value ratio {top_ratio:.2%} is acceptable."
                if passed
                else f"Most frequent value ratio {top_ratio:.2%} exceeds {DOMINANT_CATEGORY_THRESHOLD:.0%}."
            )
            checks.append(
                _build_check(
                    "dominant_category",
                    passed=passed,
                    message=message,
                    metrics={
                        "most_frequent_ratio": top_ratio,
                        "most_frequent_value": top_value,
                        "threshold": DOMINANT_CATEGORY_THRESHOLD,
                    },
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
                message = (
                    f"Entropy ratio {entropy_ratio:.2f} is acceptable."
                    if passed
                    else f"Entropy ratio {entropy_ratio:.2f} suggests low variability."
                )
                checks.append(
                    _build_check(
                        "entropy",
                        passed=passed,
                        message=message,
                        metrics={
                            "entropy": entropy,
                            "entropy_ratio": entropy_ratio,
                            "distinct_count": distinct_count,
                        },
                    )
                )
    else:
        metrics["distinct_count"] = 0
        metrics["distinct_ratio"] = None
        metrics["most_frequent_value"] = None
        metrics["most_frequent_ratio"] = None
        metrics["entropy"] = None
        checks.append(_build_skipped_check("distinct_ratio", "no non-null values"))
        checks.append(_build_skipped_check("dominant_category", "no non-null values"))
        checks.append(_build_skipped_check("entropy", "no non-null values"))

    numeric_series = _numeric_series(series)
    if numeric_series.empty:
        metrics["inferred_type"] = "categorical"
        checks.append(
            _build_skipped_check("numeric_profile", "column does not contain numeric data")
        )
        return ColumnStatistics(table=table, column=column, metrics=metrics, checks=checks)

    metrics["inferred_type"] = "numeric"
    values = numeric_series.to_numpy()
    metrics.update(
        {
            "sum": float(values.sum()),
            "mean": float(values.mean()),
            "min": float(values.min()),
            "max": float(values.max()),
            "median": float(np.median(values)),
        }
    )

    if values.size >= 2:
        std = float(values.std(ddof=0))
        variance = float(values.var(ddof=0))
    else:
        std = 0.0
        variance = 0.0
    metrics["std_dev"] = std
    metrics["variance"] = variance

    q1, q3 = np.percentile(values, [25, 75]) if values.size else (None, None)
    iqr = float(q3 - q1) if q1 is not None and q3 is not None else None
    metrics.update(
        {
            "q1": float(q1) if q1 is not None else None,
            "q3": float(q3) if q3 is not None else None,
            "iqr": iqr,
        }
    )

    mad = float(np.median(np.abs(values - metrics["median"]))) if values.size else None
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
        z_score_outliers = 0
        checks.append(
            _build_skipped_check(
                "z_score_outliers", "standard deviation is zero; z-score detection unavailable"
            )
        )
    else:
        z_scores = np.abs((values - metrics["mean"]) / std)
        z_score_outliers = int((z_scores > 3).sum())
        outlier_ratio = z_score_outliers / values.size if values.size else 0.0
        passed = outlier_ratio <= OUTLIER_RATIO_WARNING_THRESHOLD
        message = (
            f"Z-score outlier ratio {outlier_ratio:.2%} is acceptable."
            if passed
            else f"Z-score outlier ratio {outlier_ratio:.2%} exceeds {OUTLIER_RATIO_WARNING_THRESHOLD:.0%}."
        )
        checks.append(
            _build_check(
                "z_score_outliers",
                passed=passed,
                message=message,
                metrics={
                    "outlier_count": z_score_outliers,
                    "outlier_ratio": outlier_ratio,
                    "threshold": OUTLIER_RATIO_WARNING_THRESHOLD,
                },
            )
        )

    if iqr is None or iqr == 0:
        iqr_outliers = 0
        checks.append(
            _build_skipped_check("iqr_outliers", "interquartile range is zero; cannot compute fences")
        )
    else:
        lower_fence = metrics["q1"] - 1.5 * iqr if metrics["q1"] is not None else None
        upper_fence = metrics["q3"] + 1.5 * iqr if metrics["q3"] is not None else None
        in_lower = values < lower_fence if lower_fence is not None else np.zeros_like(values, dtype=bool)
        in_upper = values > upper_fence if upper_fence is not None else np.zeros_like(values, dtype=bool)
        iqr_outliers = int(np.logical_or(in_lower, in_upper).sum())
        outlier_ratio = iqr_outliers / values.size if values.size else 0.0
        passed = outlier_ratio <= OUTLIER_RATIO_WARNING_THRESHOLD
        message = (
            f"IQR outlier ratio {outlier_ratio:.2%} is acceptable."
            if passed
            else f"IQR outlier ratio {outlier_ratio:.2%} exceeds {OUTLIER_RATIO_WARNING_THRESHOLD:.0%}."
        )
        checks.append(
            _build_check(
                "iqr_outliers",
                passed=passed,
                message=message,
                metrics={
                    "outlier_count": iqr_outliers,
                    "outlier_ratio": outlier_ratio,
                    "threshold": OUTLIER_RATIO_WARNING_THRESHOLD,
                    "lower_fence": lower_fence,
                    "upper_fence": upper_fence,
                },
            )
        )

    if mad is None or mad == 0:
        checks.append(
            _build_skipped_check(
                "modified_z_score", "median absolute deviation is zero; cannot compute modified z-scores"
            )
        )
    else:
        modified_z_scores = 0.6745 * (values - metrics["median"]) / mad
        mod_z_outliers = int((np.abs(modified_z_scores) > 3.5).sum())
        outlier_ratio = mod_z_outliers / values.size if values.size else 0.0
        passed = outlier_ratio <= OUTLIER_RATIO_WARNING_THRESHOLD
        message = (
            f"Modified z-score outlier ratio {outlier_ratio:.2%} is acceptable."
            if passed
            else f"Modified z-score outlier ratio {outlier_ratio:.2%} exceeds {OUTLIER_RATIO_WARNING_THRESHOLD:.0%}."
        )
        checks.append(
            _build_check(
                "modified_z_score",
                passed=passed,
                message=message,
                metrics={
                    "outlier_count": mod_z_outliers,
                    "outlier_ratio": outlier_ratio,
                    "threshold": OUTLIER_RATIO_WARNING_THRESHOLD,
                },
            )
        )

    if values.size >= 3:
        skewness = float(pd.Series(values).skew())
        kurtosis = float(pd.Series(values).kurtosis())
        metrics["skewness"] = skewness
        metrics["kurtosis"] = kurtosis

        passed_skew = abs(skewness) <= SKEWNESS_WARNING_THRESHOLD
        message_skew = (
            f"Skewness {skewness:.2f} is within expected bounds."
            if passed_skew
            else f"Skewness {skewness:.2f} exceeds ±{SKEWNESS_WARNING_THRESHOLD}."
        )
        checks.append(
            _build_check(
                "skewness",
                passed=passed_skew,
                message=message_skew,
                metrics={"skewness": skewness, "threshold": SKEWNESS_WARNING_THRESHOLD},
            )
        )

        passed_kurt = abs(kurtosis) <= KURTOSIS_WARNING_THRESHOLD
        message_kurt = (
            f"Kurtosis {kurtosis:.2f} is within expected bounds."
            if passed_kurt
            else f"Kurtosis {kurtosis:.2f} exceeds ±{KURTOSIS_WARNING_THRESHOLD}."
        )
        checks.append(
            _build_check(
                "kurtosis",
                passed=passed_kurt,
                message=message_kurt,
                metrics={"kurtosis": kurtosis, "threshold": KURTOSIS_WARNING_THRESHOLD},
            )
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

    return ColumnStatistics(table=table, column=column, metrics=metrics, checks=checks)


def compute_statistics(dataset: Dict[str, pd.DataFrame]) -> DatasetStatistics:
    results: List[ColumnStatistics] = []
    for table_name, df in dataset.items():
        for column in df.columns:
            results.append(analyze_column(table_name, column, df[column]))

    generated_at = datetime.utcnow().isoformat()
    return DatasetStatistics(generated_at=generated_at, columns=results)
