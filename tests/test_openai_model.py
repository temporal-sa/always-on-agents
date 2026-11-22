"""OpenAIModel against a mock HTTP transport: translation both ways, and errors.

No network and no API key: the real OpenAI SDK builds and parses the requests,
and a mock transport plays the Responses API. The last test runs a whole
two-step agent turn through the adapter, which is where a translation mistake
(a tool call without its result, say) would surface.
"""

from __future__ import annotations

import json

import httpx2
import openai
import pytest
from temporalio.exceptions import ApplicationError

from always_on_agents.agent.prompts import BRIEFING_TOOLS, MEMORY_OUTPUT_SCHEMA
from always_on_agents.agent.runtime import AgentRuntime
from always_on_agents.agent.tools import ToolExecutor
from always_on_agents.domain import ModelRequest
from always_on_agents.llm import OPENAI_REASONING
from always_on_agents.llm_openai import OpenAIModel
from always_on_agents.store import AppStore

REASONING_ITEM = {"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "enc-abc"}


def _response(output: list[dict], **extra) -> dict:
    return {
        "id": "resp_1",
        "object": "response",
        "created_at": 0,
        "model": "gpt-6.2-sol",
        "status": "completed",
        "output": output,
        "usage": {"input_tokens": 120, "output_tokens": 40, "total_tokens": 160},
        **extra,
    }


def _text(text: str) -> dict:
    return {
        "type": "message",
        "id": "msg_1",
        "role": "assistant",
        "status": "completed",
        "content": [{"type": "output_text", "text": text, "annotations": []}],
    }


def _call(call_id: str, name: str, arguments: str = "{}") -> dict:
    return {"type": "function_call", "id": f"fc_{call_id}", "call_id": call_id, "name": name, "arguments": arguments}


def _model(handler) -> OpenAIModel:
    client = openai.AsyncOpenAI(
        api_key="test-key",
        max_retries=0,
        http_client=openai.DefaultAsyncHttpx2Client(transport=httpx2.MockTransport(handler)),
    )
    return OpenAIModel("gpt-6.2-sol", client=client)


def _recording(replies: list[dict]):
    """A handler that records each request body and plays the replies in order."""
    bodies: list[dict] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        bodies.append(json.loads(request.content))
        return httpx2.Response(200, json=replies[len(bodies) - 1])

    return bodies, handler


async def test_request_translates_the_conversation():
    bodies, handler = _recording([_response([_text("ok")])])
    await _model(handler).complete(
        ModelRequest(
            purpose="briefing",
            system="sys",
            tools=BRIEFING_TOOLS,
            messages=[
                {"role": "user", "content": "hi"},
                {
                    "role": "assistant",
                    "content": [
                        {"type": OPENAI_REASONING, "item": REASONING_ITEM},
                        {"type": "thinking", "thinking": "", "signature": "claude-only"},
                        {"type": "text", "text": "Let me look."},
                        {"type": "tool_use", "id": "call_1", "name": "read_memory", "input": {}},
                        {"type": "tool_use", "id": "call_2", "name": "fetch_documents", "input": {"q": "x"}},
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "call_1", "content": "{}"},
                        {"type": "tool_result", "tool_use_id": "call_2", "content": "down", "is_error": True},
                        {"type": "text", "text": "Also, keep it short."},
                    ],
                },
            ],
        )
    )
    body = bodies[0]
    assert body["model"] == "gpt-6.2-sol" and body["instructions"] == "sys"
    assert body["store"] is False and body["include"] == ["reasoning.encrypted_content"]
    assert body["reasoning"] == {"effort": "medium"} and body["max_output_tokens"] == 16000
    assert [t["name"] for t in body["tools"]] == [t["name"] for t in BRIEFING_TOOLS]
    assert body["tools"][0]["parameters"] == BRIEFING_TOOLS[0]["input_schema"]
    # In order; the Claude-only thinking block is dropped, the reasoning item goes back.
    assert body["input"] == [
        {"role": "user", "content": "hi"},
        REASONING_ITEM,
        {"role": "assistant", "content": "Let me look."},
        {"type": "function_call", "call_id": "call_1", "name": "read_memory", "arguments": "{}"},
        {"type": "function_call", "call_id": "call_2", "name": "fetch_documents", "arguments": '{"q": "x"}'},
        {"type": "function_call_output", "call_id": "call_1", "output": "{}"},
        {"type": "function_call_output", "call_id": "call_2", "output": "Error: down"},
        {"role": "user", "content": "Also, keep it short."},
    ]


async def test_response_becomes_claude_style_blocks():
    _, handler = _recording(
        [_response([REASONING_ITEM, _text("Checking."), _call("call_9", "read_memory", '{"a": 1}')])]
    )
    response = await _model(handler).complete(
        ModelRequest(purpose="briefing", system="s", messages=[{"role": "user", "content": "hi"}])
    )
    assert response.stop_reason == "tool_use"
    assert response.content == [
        {"type": OPENAI_REASONING, "item": REASONING_ITEM},
        {"type": "text", "text": "Checking."},
        {"type": "tool_use", "id": "call_9", "name": "read_memory", "input": {"a": 1}},
    ]
    assert (response.model, response.input_tokens, response.output_tokens) == ("gpt-6.2-sol", 120, 40)


async def test_structured_output_schema_is_sent_strict():
    bodies, handler = _recording([_response([_text('{"notes": []}')])])
    await _model(handler).complete(
        ModelRequest(
            purpose="memory",
            system="s",
            messages=[{"role": "user", "content": "{}"}],
            output_schema=MEMORY_OUTPUT_SCHEMA,
        )
    )
    assert bodies[0]["text"]["format"] == {
        "type": "json_schema",
        "name": "output",
        "schema": MEMORY_OUTPUT_SCHEMA,
        "strict": True,
    }


@pytest.mark.parametrize(
    ("reply", "stop_reason"),
    [
        (
            _response([_text("partial")], status="incomplete", incomplete_details={"reason": "max_output_tokens"}),
            "max_tokens",
        ),
        (_response([], status="incomplete", incomplete_details={"reason": "content_filter"}), "refusal"),
        (
            _response([{**_text(""), "content": [{"type": "refusal", "refusal": "I can't help with that."}]}]),
            "refusal",
        ),
        (_response([_text("Done.")]), "end_turn"),
    ],
)
async def test_stop_reasons(reply, stop_reason):
    _, handler = _recording([reply])
    response = await _model(handler).complete(
        ModelRequest(purpose="briefing", system="s", messages=[{"role": "user", "content": "hi"}])
    )
    assert response.stop_reason == stop_reason


@pytest.mark.parametrize(
    ("status", "code", "retryable"),
    [
        (400, None, False),
        (401, None, False),
        (404, None, False),
        (429, None, True),
        (429, "insufficient_quota", False),
        (500, None, True),
    ],
)
async def test_errors_are_classified_for_temporal_retries(status, code, retryable):
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            status,
            headers={"retry-after": "7"},
            json={"error": {"message": "boom", "type": "x", "code": code, "param": None}},
        )

    with pytest.raises(ApplicationError) as err:
        await _model(handler).complete(
            ModelRequest(purpose="briefing", system="s", messages=[{"role": "user", "content": "hi"}])
        )
    assert err.value.non_retryable is (not retryable)
    if retryable and status == 429:
        assert err.value.next_retry_delay is not None and err.value.next_retry_delay.total_seconds() == 7


async def test_a_whole_agent_turn_runs_through_the_adapter(tmp_path):
    bodies, handler = _recording(
        [
            _response([REASONING_ITEM, _call("call_1", "read_memory")]),
            _response([_text("Your briefing is ready.")]),
        ]
    )
    store = AppStore(tmp_path)
    runtime = AgentRuntime(_model(handler), ToolExecutor(store, None, "unused"))
    reply = await runtime.run(
        purpose="briefing",
        system="s",
        tools=BRIEFING_TOOLS,
        messages=[{"role": "user", "content": "Prepare my briefing."}],
        user_id="ada",
        turn_id="turn-1",
    )
    assert reply == "Your briefing is ready."
    assert runtime.tools_called == ["read_memory"]
    # The second request carries the call, its reasoning, and its result, paired by call_id.
    second = bodies[1]["input"]
    assert second[1:3] == [
        REASONING_ITEM,
        {"type": "function_call", "call_id": "call_1", "name": "read_memory", "arguments": "{}"},
    ]
    assert second[3]["type"] == "function_call_output" and second[3]["call_id"] == "call_1"
    assert json.loads(second[3]["output"]) == {"revision": 0, "notes": []}
