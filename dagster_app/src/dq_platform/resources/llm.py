from __future__ import annotations

from typing import Optional

from dagster import ConfigurableResource


class LLMResource(ConfigurableResource):
    provider: Optional[str] = None
    model: Optional[str] = None
    api_key: Optional[str] = None

    def complete(self, prompt: str) -> str:
        if not self.provider or not self.api_key:
            truncated = prompt[:200] if prompt else ""
            return f"[LLM disabled] Prompt received (truncated): {truncated}..."
        return "[LLM call not implemented in skeleton]"
