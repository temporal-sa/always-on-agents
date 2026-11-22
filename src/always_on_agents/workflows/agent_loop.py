"""The agent loop in Workflow code: the fine-grained integration.

An agent turn alternates between asking the model what to do and doing it.
Here the loop itself is Workflow code, and every model call and every tool
call is its own Activity, so Temporal records each result in the Workflow's
history. If the worker dies at step five, recovery replays steps one to four
from that history (no new model calls, no repeated tools) and carries on from
there. The model still chooses every next step; Temporal remembers the
choices it already made.

Compare the coarse integration, ``run_briefing_turn`` in
``activities/agent.py``: one Activity wraps the whole loop, so a retry repeats
everything inside it. That is simpler, and fine when a turn is cheap.

What to notice:

* The model's reply is an Activity result. That is what makes the loop
  deterministic on replay, even though the model itself is not.
* Tools that affect other people go through an approval gate that the calling
  Workflow supplies. The briefing Workflow supplies none, so they are refused.
* Independent tool calls run in parallel with ``asyncio.gather``. That is safe
  in Workflow code: Temporal's event loop schedules them deterministically.

See it happen: recovery drills 2 and 3 in the README.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from ..activities.agent import AgentActivities
    from ..agent.messages import check_stop_reason, final_text, replayable, tool_result_block, tool_uses
    from ..agent.prompts import APPROVAL_REQUIRED, DEFAULT_TOOL_TIMEOUT, TOOL_TIMEOUTS
    from ..domain import ModelRequest, ToolCall, ToolResult

MODEL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=10,
)
# Long calls heartbeat in the background, so a dead worker is noticed in
# seconds rather than when the start-to-close timeout expires.
HEARTBEAT_TIMEOUT = timedelta(seconds=30)
TOOL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=30), maximum_attempts=10
)

ApprovalGate = Callable[[ToolCall], Awaitable[ToolResult]]


async def execute_tool(call: ToolCall) -> ToolResult:
    return await workflow.execute_activity_method(
        AgentActivities.run_tool,
        call,
        start_to_close_timeout=timedelta(seconds=TOOL_TIMEOUTS.get(call.name, DEFAULT_TOOL_TIMEOUT)),
        heartbeat_timeout=HEARTBEAT_TIMEOUT,
        retry_policy=TOOL_RETRY,
        summary=f"tool: {call.name}",
    )


class WorkflowAgentLoop:
    def __init__(
        self,
        *,
        purpose: str,
        system: str,
        tools: list[dict[str, Any]],
        user_id: str,
        turn_id: str,
        approval_gate: ApprovalGate | None = None,
        drain_joined: Callable[[], list[dict[str, Any]]] | None = None,
        local_tools: dict[str, ApprovalGate] | None = None,
        max_iterations: int = 12,
    ) -> None:
        self._purpose = purpose
        self._system = system
        self._tools = tools
        self._user_id = user_id
        self._turn_id = turn_id
        self._approval_gate = approval_gate
        self._drain_joined = drain_joined
        # Tools the calling Workflow handles itself (for example, a session's timers).
        self._local_tools = local_tools or {}
        self._max_iterations = max_iterations
        self.model_calls = 0
        self.tools_called: list[str] = []

    async def run(self, messages: list[dict[str, Any]]) -> str:
        for _ in range(self._max_iterations):
            response = await workflow.execute_activity_method(
                AgentActivities.call_model,
                ModelRequest(
                    purpose=self._purpose,
                    system=self._system,
                    messages=messages,
                    tools=self._tools,
                    # agent(invariant): the same call gets the same key on every attempt, which is
                    # how the operation log shows a repeat. agent/runtime.py uses the same format.
                    log_key=f"{self._turn_id}#{self.model_calls}",
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=HEARTBEAT_TIMEOUT,
                retry_policy=MODEL_RETRY,
                summary=f"model call {self.model_calls + 1}",
            )
            self.model_calls += 1
            check_stop_reason(response)
            messages.append({"role": "assistant", "content": replayable(response.content)})
            uses = tool_uses(response.content)
            if not uses:
                return final_text(response.content)
            self.tools_called += [u["name"] for u in uses]
            results = await asyncio.gather(*(self._run_tool(u) for u in uses))
            content = [tool_result_block(r) for r in results]
            if self._drain_joined:
                # Messages that joined this turn while tools ran.
                # agent(constraint): the Claude API needs tool_result blocks first in this
                # message, so joined text is appended after them, never before.
                content += self._drain_joined()
            messages.append({"role": "user", "content": content})
        raise ApplicationError("Agent exceeded its iteration limit.", type="AgentLoopLimit", non_retryable=True)

    async def _run_tool(self, use: dict[str, Any]) -> ToolResult:
        call = ToolCall(
            name=use["name"], input=use["input"], tool_use_id=use["id"], user_id=self._user_id, turn_id=self._turn_id
        )
        if use["name"] in self._local_tools:
            return await self._local_tools[use["name"]](call)
        if use["name"] in APPROVAL_REQUIRED:
            # agent(invariant): an approval-required tool never reaches execute_tool without
            # the gate. With no gate (briefings), it is refused.
            if self._approval_gate is None:
                return ToolResult(call.tool_use_id, f"{call.name} is not available here.", is_error=True)
            return await self._approval_gate(call)
        return await execute_tool(call)
