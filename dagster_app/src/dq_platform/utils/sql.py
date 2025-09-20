"""Minimal SQL helpers."""

from __future__ import annotations


def quote_identifier(identifier: str) -> str:
    return f'"{identifier}"'
