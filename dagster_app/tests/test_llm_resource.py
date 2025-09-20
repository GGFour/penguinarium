from __future__ import annotations

from dq_platform.resources.llm import LLMResource


def test_llm_resource_returns_placeholder_when_disabled():
    resource = LLMResource()
    result = resource.complete("Tell me something")
    assert "[LLM disabled]" in result
