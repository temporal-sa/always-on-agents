"""OpenAI models, through the Responses API.

The rest of the app keeps the conversation in one format, Claude's Messages
format: Workflow state stores it, and the agent loop reads ``tool_use`` blocks
from it. This adapter translates at the edge, so nothing else changes when
the provider does:

* Out: the system prompt becomes ``instructions``, tool definitions become
  function tools, and each message becomes Responses API input items. A
  ``tool_use`` block is a ``function_call`` and a ``tool_result`` is a
  ``function_call_output``, matched by the same id.
* Back: output text becomes ``text`` blocks, function calls become
  ``tool_use`` blocks, and the status becomes a Claude-style ``stop_reason``.

Two choices worth knowing:

* ``store=False``. The Workflow already keeps the conversation durably, so
  OpenAI keeps nothing, and every request carries the full history, exactly
  as it does for Claude. Replay and retries work the same way.
* Reasoning items come back encrypted (``reasoning.encrypted_content``) and
  are kept in the conversation as opaque blocks, then sent back on the next
  request. That lets the model keep its reasoning across tool calls even
  though OpenAI stores nothing.
"""

from __future__ import annotations

import json
from typing import Any

from temporalio.exceptions import ApplicationError

from .domain import ModelRequest, ModelResponse
from .llm import OPENAI_REASONING, api_error


# agent(invariant): everything this module returns is Claude Messages shaped (text, tool_use,
# and opaque OPENAI_REASONING blocks). Workflow state and the agent loop depend on that shape.
class OpenAIModel:
    def __init__(self, model_id: str, client: Any = None) -> None:
        import openai

        self._openai = openai
        # Credentials resolve from OPENAI_API_KEY (and OPENAI_BASE_URL, if set).
        self._client = client or openai.AsyncOpenAI(max_retries=0)
        self._model = model_id

    async def complete(self, request: ModelRequest) -> ModelResponse:
        kwargs: dict[str, Any] = {
            "model": self._model,
            "instructions": request.system,
            "input": to_input_items(request.messages),
            "max_output_tokens": request.max_tokens,
            "reasoning": {"effort": request.effort},
            # agent(trade-off): stateless on purpose. The Workflow holds the conversation;
            # don't switch to previous_response_id, which moves it to OpenAI.
            "store": False,
            "include": ["reasoning.encrypted_content"],
        }
        if request.tools:
            kwargs["tools"] = [to_function_tool(t) for t in request.tools]
        if request.output_schema:
            kwargs["text"] = {
                "format": {"type": "json_schema", "name": "output", "schema": request.output_schema, "strict": True}
            }
        try:
            response = await self._client.responses.create(**kwargs)
        except self._openai.APIError as err:
            raise api_error(self._openai, err, "OpenAI API") from err
        return from_response(response)


def to_function_tool(tool: dict[str, Any]) -> dict[str, Any]:
    """A Claude tool definition as an OpenAI function tool."""
    return {
        "type": "function",
        "name": tool["name"],
        "description": tool["description"],
        "parameters": tool["input_schema"],
        # Strict mode requires every property to be required; some of these
        # tools have optional inputs, and the tools validate their own input.
        "strict": False,
    }


def to_input_items(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A Claude Messages conversation as Responses API input items, in order."""
    items: list[dict[str, Any]] = []
    for message in messages:
        role, content = message["role"], message["content"]
        if isinstance(content, str):
            items.append({"role": role, "content": content})
            continue
        for block in content:
            kind = block.get("type")
            if kind == "text":
                items.append({"role": role, "content": block["text"]})
            elif kind == "tool_use":
                items.append(
                    {
                        "type": "function_call",
                        "call_id": block["id"],
                        "name": block["name"],
                        "arguments": json.dumps(block["input"]),
                    }
                )
            elif kind == "tool_result":
                output = block["content"] if isinstance(block["content"], str) else json.dumps(block["content"])
                items.append(
                    {
                        "type": "function_call_output",
                        "call_id": block["tool_use_id"],
                        # The Responses API has no error flag, so say it in the output.
                        "output": f"Error: {output}" if block.get("is_error") else output,
                    }
                )
            elif kind == OPENAI_REASONING:
                items.append(block["item"])
            # Anything else is Claude-only (thinking, fallback markers), left by a
            # provider switch partway through a conversation. OpenAI can't read it.
    return items


def from_response(response: Any) -> ModelResponse:
    """A Responses API result as Claude-style content blocks and stop reason."""
    content: list[dict[str, Any]] = []
    refused = False
    for item in response.output:
        if item.type == "reasoning":
            content.append({"type": OPENAI_REASONING, "item": item.to_dict(mode="json")})
        elif item.type == "message":
            for part in item.content:
                if part.type == "output_text":
                    content.append({"type": "text", "text": part.text})
                elif part.type == "refusal":
                    refused = True
        elif item.type == "function_call":
            try:
                arguments = json.loads(item.arguments or "{}")
            except json.JSONDecodeError as err:
                # Retryable: a new attempt asks the model again.
                raise ApplicationError(
                    f"{item.name} was called with arguments that are not JSON", type="InvalidToolArguments"
                ) from err
            content.append({"type": "tool_use", "id": item.call_id, "name": item.name, "input": arguments})

    incomplete = response.incomplete_details.reason if response.incomplete_details else None
    if incomplete == "max_output_tokens":
        stop_reason = "max_tokens"
    elif refused or incomplete == "content_filter":
        stop_reason = "refusal"
    elif any(block["type"] == "tool_use" for block in content):
        stop_reason = "tool_use"
    else:
        stop_reason = "end_turn"
    usage = response.usage
    return ModelResponse(
        stop_reason=stop_reason,
        content=content,
        model=response.model,
        input_tokens=usage.input_tokens if usage else 0,
        output_tokens=usage.output_tokens if usage else 0,
    )
