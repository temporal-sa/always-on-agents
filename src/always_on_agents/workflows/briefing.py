"""A briefing turn as a Workflow, and the research job it can call as a tool."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities.agent import AgentActivities
    from ..activities.research import ResearchActivities
    from ..agent.prompts import BRIEFING_SYSTEM, BRIEFING_TOOLS
    from ..domain import BriefingTurnInput, DeliveryRequest, ResearchInput, ResearchOutcome, TurnOutcome
    from .agent_loop import HEARTBEAT_TIMEOUT, MODEL_RETRY, WorkflowAgentLoop

# Delivery is its own job with its own policy: keep trying, back off to a few
# minutes, and never send twice (the message id is the idempotency key).
DELIVERY_RETRY = RetryPolicy(initial_interval=timedelta(seconds=2), maximum_interval=timedelta(minutes=5))


@workflow.defn
class BriefingTurnWorkflow:
    """One agent turn with fine-grained recovery, then delivery.

    Started directly for an interactive turn, or by a Temporal Schedule for a
    recurring briefing (each scheduled run gets its own Workflow ID).
    """

    def __init__(self) -> None:
        self._stage = "starting"
        self._loop: WorkflowAgentLoop | None = None

    @workflow.run
    async def run(self, request: BriefingTurnInput) -> TurnOutcome:
        turn_id = workflow.info().workflow_id
        self._loop = WorkflowAgentLoop(
            purpose="briefing",
            system=BRIEFING_SYSTEM,
            tools=BRIEFING_TOOLS,
            user_id=request.user_id,
            turn_id=turn_id,
        )
        self._stage = "agent loop"
        reply = await self._loop.run([{"role": "user", "content": request.request}])
        report_id = turn_id if "write_report" in self._loop.tools_called else None

        delivered = False
        if request.deliver:
            self._stage = "delivering"
            delivered = await workflow.execute_activity_method(
                AgentActivities.deliver_message,
                DeliveryRequest(
                    message_id=f"{turn_id}:delivery", user_id=request.user_id, body=reply, report_id=report_id
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=DELIVERY_RETRY,
                summary="deliver briefing",
            )
        self._stage = "done"
        return TurnOutcome(turn_id, report_id, reply, self._loop.model_calls, delivered)

    @workflow.query
    def progress(self) -> dict[str, Any]:
        loop = self._loop
        return {
            "stage": self._stage,
            "model_calls": loop.model_calls if loop else 0,
            "tools_called": loop.tools_called if loop else [],
        }


@workflow.defn
class ResearchWorkflow:
    """A Workflow implementing a long-running tool (a research subagent).

    The calling agent gets a job id back immediately and collects the result
    later; meanwhile this job has its own steps, retries, and history.
    """

    @workflow.run
    async def run(self, request: ResearchInput) -> ResearchOutcome:
        sources = await workflow.execute_activity_method(
            ResearchActivities.gather_sources,
            request,
            start_to_close_timeout=timedelta(minutes=1),
            summary="gather sources",
        )
        summary = await workflow.execute_activity_method(
            ResearchActivities.summarize_sources,
            args=[request, sources],
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=MODEL_RETRY,
            summary="summarize sources",
        )
        return ResearchOutcome(request.question, summary, [s["title"] for s in sources])
