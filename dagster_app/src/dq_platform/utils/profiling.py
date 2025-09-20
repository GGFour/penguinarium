"""Utility helpers for profile computations (placeholder)."""

from __future__ import annotations

from typing import Iterable


def average(values: Iterable[float]) -> float:
    items = list(values)
    return sum(items) / len(items) if items else 0.0
