"""A minimal in-process agent loop.

This plays the role of an existing agent SDK that owns its own loop. The coarse
integration wraps a whole call to ``AgentRuntime.run`` in one Activity:
Temporal records only the turn's completion, so a failed attempt repeats every
model and tool call inside it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from temporalio.exceptions import ApplicationError

from ..domain import ModelRequest, ToolCall
from ..llm import ModelClient
from .messages import check_stop_reason, final_text, replayable, tool_result_block, tool_uses
from .tools import ToolExecutor


class AgentRuntime:
    def __init__(
        self,
        model: ModelClient,
        tools: ToolExecutor,
        on_step: Callable[[str], None] | None = None,
        max_iterations: int = 12,
    ) -> None:
        self._model = model
        self._tools = tools
        self._on_step = on_step or (lambda _: None)
        self._max_iterations = max_iterations
        self.model_calls = 0
        self.tools_called: list[str] = []

    async def run(
        self,
        *,
        purpose: str,
        system: str,
        tools: list[dict[str, Any]],
        messages: list[dict[str, Any]],
        user_id: str,
        turn_id: str,
    ) -> str:
        for _ in range(self._max_iterations):
            self._on_step(f"model call {self.model_calls + 1}")
            response = await self._model.complete(
                ModelRequest(
                    purpose=purpose,
                    system=system,
                    messages=messages,
                    tools=tools,
                    log_key=f"{turn_id}#{self.model_calls}",
                )
            )
            self.model_calls += 1
            check_stop_reason(response)
            messages.append({"role": "assistant", "content": replayable(response.content)})
            uses = tool_uses(response.content)
            if not uses:
                return final_text(response.content)
            self._on_step("tools: " + ", ".join(u["name"] for u in uses))
            self.tools_called += [u["name"] for u in uses]
            results = await asyncio.gather(
                *(
                    self._tools.execute(
                        ToolCall(
                            name=u["name"], input=u["input"], tool_use_id=u["id"], user_id=user_id, turn_id=turn_id
                        )
                    )
                    for u in uses
                )
            )
            messages.append({"role": "user", "content": [tool_result_block(r) for r in results]})
        raise ApplicationError("Agent exceeded its iteration limit.", type="AgentLoopLimit", non_retryable=True)
