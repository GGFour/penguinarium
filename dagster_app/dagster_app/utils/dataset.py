"""Utilities for working with the Home Credit dataset."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

import pandas as pd

DEFAULT_DATASET_DIR = Path(__file__).resolve().parents[2] / "data" / "home_credit"


class DatasetNotFoundError(FileNotFoundError):
    """Raised when the dataset directory does not contain any CSV files."""


def resolve_dataset_dir(path: str | os.PathLike[str] | None = None) -> Path:
    """Resolve the dataset directory from config or environment variables.

    Args:
        path: Optional path provided via op configuration.

    Returns:
        A :class:`pathlib.Path` object pointing to the dataset directory.
    """

    candidate = Path(path or os.getenv("DATASET_DIR", DEFAULT_DATASET_DIR))
    if not candidate.exists():
        raise DatasetNotFoundError(
            f"Dataset directory '{candidate}' does not exist. Configure DATASET_DIR or job config."
        )
    return candidate


def load_dataset(dataset_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load every CSV file in ``dataset_dir`` into a dictionary of DataFrames."""

    dataset: Dict[str, pd.DataFrame] = {}
    for csv_file in sorted(dataset_dir.glob("*.csv")):
        dataset[csv_file.stem] = pd.read_csv(csv_file)

    if not dataset:
        raise DatasetNotFoundError(
            f"No CSV files were found in dataset directory '{dataset_dir}'."
        )

    return dataset
