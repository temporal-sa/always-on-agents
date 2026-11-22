"""Tool implementations shared by both agent integrations.

The coarse integration calls these in-process from inside one Activity; the
fine-grained integration runs each call as its own Activity. Same code, same
effects; only the recovery boundary differs.

Tools that change the outside world are idempotent per turn (``write_report``
upserts under the turn id) or per approval (``share_report`` delivers once per
approval id), so a retried call never doubles its effect.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta

from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import WorkflowIDConflictPolicy

from ..domain import ResearchInput, ResearchOutcome, ToolCall, ToolResult
from ..store import AppStore
from .messages import approval_digest
from .prompts import MAX_DELAY_SECONDS
from .sample_data import DOCUMENTS

RESEARCH_WAIT_SECONDS = 45


class ToolExecutor:
    def __init__(
        self, store: AppStore, temporal: Client | None, research_queue: str, scheduled_queue: str | None = None
    ) -> None:
        self._store = store
        self._temporal = temporal
        self._research_queue = research_queue
        self._scheduled_queue = scheduled_queue

    async def execute(self, call: ToolCall) -> ToolResult:
        handler = getattr(self, f"_tool_{call.name}", None)
        attempt = activity.info().attempt if activity.in_activity() else 1
        await asyncio.to_thread(self._store.log_op, f"tool:{call.name}", call.turn_id, attempt, call.tool_use_id)
        if handler is None:
            return ToolResult(call.tool_use_id, f"Unknown tool {call.name!r}.", is_error=True)
        try:
            return ToolResult(call.tool_use_id, await handler(call))
        except ToolInputError as err:
            # Bad input is the model's to fix, so report it instead of retrying.
            return ToolResult(call.tool_use_id, str(err), is_error=True)

    # -- read-only tools ------------------------------------------------------

    async def _tool_read_memory(self, call: ToolCall) -> str:
        revision, notes = await asyncio.to_thread(self._store.current_memory, call.user_id)
        return json.dumps({"revision": revision, "notes": [n.__dict__ for n in notes]})

    async def _tool_fetch_documents(self, call: ToolCall) -> str:
        return json.dumps(DOCUMENTS)

    # -- scheduling: the same submission path, now or later -------------------

    async def _tool_schedule_briefing(self, call: ToolCall) -> str:
        try:
            delay = int(call.input.get("delay_seconds", 0))
        except (TypeError, ValueError) as err:
            raise ToolInputError("delay_seconds must be an integer") from err
        if not 0 <= delay <= MAX_DELAY_SECONDS:
            raise ToolInputError(f"delay_seconds must be between 0 and {MAX_DELAY_SECONDS}")
        temporal, queue = self._scheduling()
        from ..scheduling import schedule_briefing  # local import: scheduling imports the activities

        # Named after this tool call, so a retried call can't submit a second briefing.
        due = "chat-" + hashlib.sha256(f"{call.turn_id}/{call.tool_use_id}".encode()).hexdigest()[:10]
        submitted = await schedule_briefing(temporal, call.user_id, due, timedelta(seconds=delay), task_queue=queue)
        return json.dumps(
            {
                "briefing_id": f"briefing-{call.user_id}-{due}",
                "starts_in_seconds": delay,
                "already_submitted": not submitted,
                "delivery": "emailed to the user when it is ready",
            }
        )

    # -- the recurring weekly briefing: a Temporal Schedule --------------------
    # Setting the same time twice and cancelling twice are both harmless, so a
    # retried tool call can't leave a second Schedule behind.

    async def _tool_set_weekly_briefing(self, call: ToolCall) -> str:
        from ..scheduling import describe_weekly_briefing, set_weekly_briefing_schedule

        temporal, queue = self._scheduling()
        try:
            await set_weekly_briefing_schedule(
                temporal,
                call.user_id,
                task_queue=queue,
                day=str(call.input.get("day_of_week", "")),
                at=str(call.input.get("time", "")),
                time_zone=str(call.input.get("time_zone") or "UTC"),
            )
        except ValueError as err:
            raise ToolInputError(str(err)) from err
        return json.dumps({"weekly_briefing": await describe_weekly_briefing(temporal, call.user_id)})

    async def _tool_get_weekly_briefing(self, call: ToolCall) -> str:
        from ..scheduling import describe_weekly_briefing

        temporal, _ = self._scheduling()
        return json.dumps({"weekly_briefing": await describe_weekly_briefing(temporal, call.user_id)})

    async def _tool_cancel_weekly_briefing(self, call: ToolCall) -> str:
        from ..scheduling import cancel_weekly_briefing_schedule

        temporal, _ = self._scheduling()
        removed = await cancel_weekly_briefing_schedule(temporal, call.user_id)
        return json.dumps({"cancelled": removed, "weekly_briefing": None})

    def _scheduling(self) -> tuple[Client, str]:
        if self._temporal is None or not self._scheduled_queue:
            raise ToolInputError("scheduling is not available here")
        return self._temporal, self._scheduled_queue

    # -- a Workflow implementing a long-running tool --------------------------

    async def _tool_start_research(self, call: ToolCall) -> str:
        question = str(call.input.get("question", "")).strip()
        if not question:
            raise ToolInputError("question is required")
        if self._temporal is None:
            raise ToolInputError("research is not available in this runtime")
        # A stable ID per tool call, plus USE_EXISTING, means a retried tool call
        # returns the job that is already running instead of starting another.
        job_id = f"research-{call.turn_id}-{call.tool_use_id}"
        await self._temporal.start_workflow(
            "ResearchWorkflow",
            ResearchInput(user_id=call.user_id, question=question),
            id=job_id,
            task_queue=self._research_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        return json.dumps({"job_id": job_id, "status": "started"})

    async def _tool_get_research_result(self, call: ToolCall) -> str:
        job_id = str(call.input.get("job_id", ""))
        if not job_id.startswith("research-") or self._temporal is None:
            raise ToolInputError(f"unknown job_id {job_id!r}")
        handle = self._temporal.get_workflow_handle(job_id, result_type=ResearchOutcome)
        try:
            outcome = await asyncio.wait_for(handle.result(), timeout=RESEARCH_WAIT_SECONDS)
        except TimeoutError:
            return json.dumps({"job_id": job_id, "status": "running"})
        except WorkflowFailureError as err:
            return json.dumps({"job_id": job_id, "status": "failed", "error": str(err.cause)})
        return json.dumps(
            {"job_id": job_id, "status": "completed", "summary": outcome.summary, "sources": outcome.sources}
        )

    # -- tools with external effects -----------------------------------------

    async def _tool_write_report(self, call: ToolCall) -> str:
        title = str(call.input.get("title", "Briefing"))
        markdown = str(call.input.get("markdown", ""))
        if not markdown.strip():
            raise ToolInputError("markdown is required")
        await asyncio.to_thread(self._store.maybe_fail, "write_report")
        # One report per turn: a retry overwrites rather than duplicates.
        report_id = call.turn_id
        await asyncio.to_thread(self._store.upsert_report, report_id, call.user_id, title, markdown)
        return json.dumps({"report_id": report_id})

    async def _tool_share_report(self, call: ToolCall) -> str:
        # Authorization is checked when the action executes, not only when the
        # approval was given: right action, exact parameters, still in time.
        grant = call.approval
        if grant is None:
            raise ToolInputError("share_report requires an approval")
        if grant.digest != approval_digest(call.name, call.input):
            raise ToolInputError("the approval does not cover these parameters")
        if datetime.now(UTC) > datetime.fromisoformat(grant.expires_at):
            raise ToolInputError("the approval expired before the action ran")
        recipient = str(call.input.get("recipient", ""))
        report = await asyncio.to_thread(self._store.latest_report, call.user_id)
        if report is None:
            raise ToolInputError("there is no report to share yet")
        body = f"{call.input.get('note', '')}\n\n{report['body']}".strip()
        # The approval id is the idempotency key: one approval, at most one send.
        sent = await asyncio.to_thread(
            self._store.deliver, f"share:{grant.approval_id}", call.user_id, f"share:{recipient}", body
        )
        return json.dumps({"shared_with": recipient, "report_id": report["report_id"], "duplicate": not sent})


class ToolInputError(ValueError):
    pass
