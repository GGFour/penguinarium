from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from dagster import ConfigurableResource


class ResultSink(ConfigurableResource):
    base_path: str

    def _ensure_dir(self, sub: str) -> Path:
        path = Path(self.base_path) / sub
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_json(self, subdir: str, name: str, payload: Dict[str, Any]):
        path = self._ensure_dir(subdir) / f"{name}.json"
        path.write_text(json.dumps(payload, indent=2))
        return str(path)

    def write_rows(self, subdir: str, name: str, rows: List[Dict[str, Any]]):
        path = self._ensure_dir(subdir) / f"{name}.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
        return str(path)
