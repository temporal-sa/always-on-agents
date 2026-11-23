"""ClaudeModel against a mock HTTP transport: request shape and error classification.

No network and no API key: the real Anthropic SDK builds and parses the
requests, and a mock transport plays the API.
"""

from __future__ import annotations

import json

import anthropic
import httpx2
import pytest
from temporalio.exceptions import ApplicationError

from always_on_agents.agent.prompts import BRIEFING_TOOLS, MEMORY_OUTPUT_SCHEMA
from always_on_agents.domain import ModelRequest
from always_on_agents.llm import OPENAI_REASONING, ClaudeModel

TOOL_USE_RESPONSE = {
    "id": "msg_1",
    "type": "message",
    "role": "assistant",
    "model": "claude-opus-5-5",
    "stop_reason": "tool_use",
    "stop_sequence": None,
    "usage": {"input_tokens": 120, "output_tokens": 40},
    "content": [
        {"type": "thinking", "thinking": "", "signature": "sig-abc"},
        {"type": "tool_use", "id": "toolu_1", "name": "read_memory", "input": {}},
    ],
}


def _model(handler) -> ClaudeModel:
    client = anthropic.AsyncAnthropic(
        api_key="test-key",
        max_retries=0,
        http_client=anthropic.DefaultAsyncHttpxClient(transport=httpx2.MockTransport(handler)),
    )
    return ClaudeModel("claude-opus-5-5", client=client)


async def test_request_shape_and_replayable_content():
    seen: dict = {}

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen["body"] = json.loads(request.content)
        seen["beta"] = request.headers.get("anthropic-beta", "")
        return httpx2.Response(200, json=TOOL_USE_RESPONSE)

    response = await _model(handler).complete(
        ModelRequest(
            purpose="briefing",
            system="sys",
            messages=[{"role": "user", "content": "hi"}],
            tools=BRIEFING_TOOLS,
        )
    )
    body = seen["body"]
    assert body["model"] == "claude-opus-5-5"
    assert body["fallbacks"] == "default"
    assert "server-side-fallback-2026-07-01" in seen["beta"]
    assert body["output_config"] == {"effort": "medium"}
    assert "thinking" not in body and "temperature" not in body
    assert [t["name"] for t in body["tools"]] == [t["name"] for t in BRIEFING_TOOLS]

    assert response.stop_reason == "tool_use"
    # Thinking blocks come back with their signature so they can be replayed unchanged.
    assert response.content[0] == {"type": "thinking", "thinking": "", "signature": "sig-abc"}
    assert response.content[1]["name"] == "read_memory"
    assert (response.input_tokens, response.output_tokens) == (120, 40)


async def test_structured_output_schema_is_sent():
    seen: dict = {}

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen["body"] = json.loads(request.content)
        reply = {**TOOL_USE_RESPONSE, "stop_reason": "end_turn", "content": [{"type": "text", "text": '{"notes": []}'}]}
        return httpx2.Response(200, json=reply)

    await _model(handler).complete(
        ModelRequest(
            purpose="memory",
            system="s",
            messages=[{"role": "user", "content": "{}"}],
            output_schema=MEMORY_OUTPUT_SCHEMA,
        )
    )
    assert seen["body"]["output_config"]["format"] == {"type": "json_schema", "schema": MEMORY_OUTPUT_SCHEMA}


@pytest.mark.parametrize(
    ("status", "retryable"),
    [(400, False), (401, False), (404, False), (429, True), (500, True), (529, True)],
)
async def test_errors_are_classified_for_temporal_retries(status, retryable):
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            status,
            headers={"retry-after": "7"},
            json={"type": "error", "error": {"type": "x", "message": "boom"}},
        )

    with pytest.raises(ApplicationError) as err:
        await _model(handler).complete(
            ModelRequest(purpose="briefing", system="s", messages=[{"role": "user", "content": "hi"}])
        )
    assert err.value.non_retryable is (not retryable)
    if status == 429:
        assert err.value.next_retry_delay is not None and err.value.next_retry_delay.total_seconds() == 7


async def test_openai_reasoning_is_not_sent_to_claude():
    """After a provider switch mid-conversation, OpenAI's reasoning blocks are dropped."""
    seen: dict = {}

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen["body"] = json.loads(request.content)
        return httpx2.Response(200, json=TOOL_USE_RESPONSE)

    await _model(handler).complete(
        ModelRequest(
            purpose="session",
            system="s",
            messages=[
                {"role": "user", "content": "hi"},
                {
                    "role": "assistant",
                    "content": [{"type": OPENAI_REASONING, "item": {}}, {"type": "text", "text": "Hey."}],
                },
                {"role": "assistant", "content": [{"type": OPENAI_REASONING, "item": {}}]},
                {"role": "user", "content": "again"},
            ],
        )
    )
    assert seen["body"]["messages"] == [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": [{"type": "text", "text": "Hey."}]},
        {"role": "user", "content": "again"},
    ]
