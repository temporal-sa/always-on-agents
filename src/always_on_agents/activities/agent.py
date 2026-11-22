"""Activities for agent turns.

Two integration styles share these:

* Coarse: ``run_briefing_turn`` wraps a whole agent turn. It is the job the
  scheduling helper submits as a Standalone Activity (now or after a delay).
* Fine-grained: ``call_model`` and ``run_tool`` are single operations that a
  Workflow strings together, so Temporal records each result separately.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import activity
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from ..agent.prompts import BRIEFING_SYSTEM, BRIEFING_TOOLS
from ..agent.runtime import AgentRuntime
from ..agent.tools import ToolExecutor
from ..config import Settings
from ..domain import (
    DeliveryRequest,
    InteractionRecord,
    ModelRequest,
    ModelResponse,
    ToolCall,
    ToolResult,
    TurnOutcome,
    WakeUp,
)
from ..llm import ModelClient
from ..store import AppStore
from .heartbeat import heartbeating

# Chat messages feed memory in small batches, so a preference mentioned in
# conversation reaches the next briefing within minutes.
CHAT_MEMORY_BATCH_SIZE = 3
CHAT_MEMORY_MAX_WAIT = timedelta(minutes=5)


class AgentActivities:
    def __init__(self, store: AppStore, model: ModelClient, temporal: Client, settings: Settings) -> None:
        self._store = store
        self._model = model
        self._temporal = temporal
        self._settings = settings

    def _tools(self) -> ToolExecutor:
        # Research jobs run on the same task queue as the turn that started them;
        # scheduled briefings go to the scheduled queue.
        return ToolExecutor(self._store, self._temporal, activity.info().task_queue, self._settings.scheduled_queue)

    # -- coarse: one Activity owns the whole turn ---------------------------

    @activity.defn
    async def run_briefing_turn(self, user_id: str) -> TurnOutcome:
        """Run one complete briefing turn, then deliver it.

        The Activity ID (for example ``briefing-alice-2026-11-23``) names the
        turn, so the report and the delivery are keyed to it and a retry never
        produces a second report or a second email. A retry does repeat the
        model and tool calls, because Temporal records only this Activity's
        completion.
        """
        turn_id = activity.info().activity_id
        progress = {"step": "starting"}
        runtime = AgentRuntime(self._model, self._tools(), on_step=lambda step: progress.update(step=step))
        # Heartbeat in the background, so a slow model call is not mistaken for
        # a dead worker, while a real crash is noticed within the heartbeat timeout.
        async with heartbeating(lambda: progress["step"]):
            reply = await runtime.run(
                purpose="briefing",
                system=BRIEFING_SYSTEM,
                tools=BRIEFING_TOOLS,
                messages=[{"role": "user", "content": "Prepare my weekly briefing."}],
                user_id=user_id,
                turn_id=turn_id,
            )
        report_id = turn_id if "write_report" in runtime.tools_called else None
        delivered = await asyncio.to_thread(
            self._deliver,
            DeliveryRequest(message_id=f"{turn_id}:delivery", user_id=user_id, body=reply, report_id=report_id),
        )
        return TurnOutcome(turn_id, report_id, reply, runtime.model_calls, delivered)

    # -- fine-grained: one Activity per operation ---------------------------

    @activity.defn
    async def call_model(self, request: ModelRequest) -> ModelResponse:
        await asyncio.to_thread(self._store.maybe_fail, "model_call")
        async with heartbeating(lambda: request.log_key):
            response = await self._model.complete(request)
        # Fault point for drills: the expensive call finished, but the worker
        # dies before Temporal records the result.
        await asyncio.to_thread(self._store.maybe_fail, "after_model_call")
        return response

    @activity.defn
    async def run_tool(self, call: ToolCall) -> ToolResult:
        async with heartbeating(lambda: call.name):
            return await self._tools().execute(call)

    @activity.defn
    def deliver_message(self, request: DeliveryRequest) -> bool:
        """Deliver once per message_id. Returns False if it was already delivered."""
        return self._deliver(request)

    # -- the agent's timer tool ---------------------------------------------

    @activity.defn
    async def schedule_wakeup(self, wake: WakeUp, delay_seconds: int) -> bool:
        """Submit the delayed wake-up job through the shared scheduling path."""
        from ..scheduling import schedule_wakeup  # local import: scheduling imports this module

        return await schedule_wakeup(
            self._temporal, wake, timedelta(seconds=delay_seconds), task_queue=self._settings.scheduled_queue
        )

    @activity.defn
    async def wake_session(self, wake: WakeUp) -> str:
        """The delayed job itself: wake the conversation that set the timer.

        If that session has already ended, the note still reaches the user,
        through the outbox instead.
        """
        await asyncio.to_thread(self._store.log_op, "timer:fired", wake.timer_id, activity.info().attempt, wake.note)
        try:
            await self._temporal.get_workflow_handle(wake.session_workflow_id).signal("timer_fired", wake)
            return "woke the session"
        except RPCError as err:
            if err.status != RPCStatusCode.NOT_FOUND:
                raise
        await asyncio.to_thread(
            self._deliver,
            DeliveryRequest(message_id=f"timer:{wake.timer_id}", user_id=wake.user_id, body=f"Reminder: {wake.note}"),
        )
        return "session had ended; delivered to the outbox"

    # -- conversations feed memory ------------------------------------------

    @activity.defn
    async def record_interaction(self, record: InteractionRecord) -> None:
        """Keep what the user said as source material for memory consolidation."""
        from ..app import notify_consolidation  # local import: app imports the Workflows

        await asyncio.to_thread(self._store.record_interaction, record.user_id, record.text, record.interaction_id)
        await notify_consolidation(
            self._temporal,
            self._settings,
            record.user_id,
            record.interaction_id,
            batch_size=CHAT_MEMORY_BATCH_SIZE,
            max_wait=CHAT_MEMORY_MAX_WAIT,
        )

    def _deliver(self, request: DeliveryRequest) -> bool:
        self._store.maybe_fail("deliver")
        body = request.body
        if request.report_id:
            report = self._store.get_report(request.report_id)
            if report:
                body = f"{body}\n\n{report['body']}"
        sent = self._store.deliver(request.message_id, request.user_id, request.channel, body)
        attempt = activity.info().attempt if activity.in_activity() else 1
        self._store.log_op("deliver", request.message_id, attempt, "sent" if sent else "duplicate suppressed")
        return sent
