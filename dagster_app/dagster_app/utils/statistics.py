"""Statistical quality checks for dataset columns."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import numpy as np
import pandas as pd


@dataclass
class ColumnStatistics:
    table: str
    column: str
    sum: float | None
    mean: float | None
    std_dev: float | None
    outlier_count: int | None
    total_count: int


@dataclass
class DatasetStatistics:
    generated_at: str
    columns: List[ColumnStatistics]


def _numeric_series(series: pd.Series) -> pd.Series:
    return series.dropna().apply(pd.to_numeric, errors="coerce").dropna()


def analyze_column(table: str, column: str, series: pd.Series) -> ColumnStatistics:
    numeric_series = _numeric_series(series)
    if numeric_series.empty:
        return ColumnStatistics(
            table=table,
            column=column,
            sum=None,
            mean=None,
            std_dev=None,
            outlier_count=None,
            total_count=int(series.shape[0]),
        )

    values = numeric_series.to_numpy()
    column_sum = float(values.sum())
    mean = float(values.mean())
    std = float(values.std(ddof=0))
    if std == 0:
        outliers = 0
    else:
        z_scores = np.abs((values - mean) / std)
        outliers = int((z_scores > 3).sum())

    return ColumnStatistics(
        table=table,
        column=column,
        sum=column_sum,
        mean=mean,
        std_dev=std,
        outlier_count=outliers,
        total_count=int(series.shape[0]),
    )


def compute_statistics(dataset: Dict[str, pd.DataFrame]) -> DatasetStatistics:
    results: List[ColumnStatistics] = []
    for table_name, df in dataset.items():
        for column in df.columns:
            results.append(analyze_column(table_name, column, df[column]))

    generated_at = datetime.utcnow().isoformat()
    return DatasetStatistics(generated_at=generated_at, columns=results)
