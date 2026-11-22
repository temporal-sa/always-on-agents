"""One Workflow coordinates a whole session across turns, approvals, and clients.

A simplified version of the patterns in Temporal Agent Kit (experimental:
https://github.com/temporal-community/temporal-agent-kit):

* Accepting a message is separate from finishing the work it asks for. The
  ``send_message`` Update returns as soon as the message is accepted, saying
  whether it started a turn, joined the running turn, or was queued.
* Approval is separate from the action's result. When the agent wants to run a
  tool that affects other people, the session records a pending approval for
  that exact action and those exact parameters, then waits, with a deadline.
  The pending approval survives the client disconnecting and the worker
  restarting. If the deadline passes, the approval expires and cleanup runs.
* Clients follow the session by reading its event log from an offset
  (``events_since``). Disconnecting stops nothing; reconnecting resumes from
  the last event the client saw.

And the session is wired to the rest of the system:

* The agent can wake itself up. Its ``set_timer`` tool submits a delayed job
  through the same scheduling path as briefings (a Standalone Activity with a
  start delay). When the job runs, it signals this session, which runs a turn
  with the agent's note. The session stays open while timers are pending; if
  it has ended anyway, the note goes to the user's outbox.
* What the user says is source material for memory: each message is recorded
  and the user's consolidation Workflow is notified.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from ..activities.agent import AgentActivities
    from ..agent.messages import approval_digest
    from ..agent.prompts import MAX_DELAY_SECONDS, SESSION_SYSTEM, SESSION_TOOLS
    from ..domain import (
        ApprovalDecision,
        ApprovalGrant,
        DeliveryRequest,
        InteractionRecord,
        MessageAccepted,
        PendingApproval,
        PendingTimer,
        SendMessageInput,
        SessionEvent,
        SessionInput,
        SessionState,
        SessionStatus,
        ToolCall,
        ToolResult,
        WakeUp,
    )
    from .agent_loop import TOOL_RETRY, WorkflowAgentLoop, execute_tool
    from .briefing import DELIVERY_RETRY

EVENTS_CARRIED_ACROSS_RUNS = 200
SEEN_IDS_CARRIED_ACROSS_RUNS = 500
WAKE_UP_PREFIX = "[Automated wake-up]"


@workflow.defn
class SessionWorkflow:
    @workflow.init
    def __init__(self, request: SessionInput) -> None:
        state = request.state or SessionState()
        self._request = request
        self._messages = state.messages
        self._events = state.events
        self._next_seq = state.next_seq
        self._seen: dict[str, None] = dict.fromkeys(state.seen_client_message_ids)
        self._queue: list[dict[str, str]] = list(state.queued)
        self._turns = state.turns
        self._joined: list[dict[str, Any]] = []
        self._approvals: dict[str, PendingApproval] = {}
        self._timers: dict[str, PendingTimer] = {t.timer_id: t for t in state.timers}
        self._turn_running = False
        self._closed = False
        # Emitted here, not in run(): a message that arrives with the start is
        # handled before run() begins, and must not become event #1.
        if not self._events:
            self._emit("session_started", f"Session for {request.user_id}.")

    # -- client-facing handlers ---------------------------------------------

    @workflow.update
    def send_message(self, message: SendMessageInput) -> MessageAccepted:
        if message.client_message_id in self._seen:
            return MessageAccepted(message.client_message_id, "duplicate")
        self._seen[message.client_message_id] = None
        if self._turn_running and message.mode == "auto":
            self._joined.append({"type": "text", "text": message.text})
            disposition = "joined"
        else:
            disposition = "started" if not self._turn_running and not self._queue else "queued"
            self._queue.append({"message_id": message.client_message_id, "text": message.text})
        self._emit("message_accepted", message.text, message_id=message.client_message_id, disposition=disposition)
        return MessageAccepted(message.client_message_id, disposition)

    @send_message.validator
    def _validate_message(self, message: SendMessageInput) -> None:
        if self._closed:
            raise ApplicationError("This session is closed.")
        if not message.text.strip():
            raise ApplicationError("Message is empty.")
        if message.mode not in ("auto", "queue"):
            raise ApplicationError("mode must be 'auto' or 'queue'.")

    @workflow.update
    def decide_approval(self, decision: ApprovalDecision) -> str:
        approval = self._approvals[decision.approval_id]
        approval.status = "approved" if decision.approve else "denied"
        self._emit(
            f"approval_{approval.status}", f"{approval.tool} {approval.status}", approval_id=approval.approval_id
        )
        return approval.status

    @decide_approval.validator
    def _validate_decision(self, decision: ApprovalDecision) -> None:
        approval = self._approvals.get(decision.approval_id)
        if approval is None:
            raise ApplicationError(f"Unknown approval {decision.approval_id}.")
        if approval.status != "pending":
            raise ApplicationError(f"Approval is already {approval.status}.")
        if decision.digest != approval.digest:
            raise ApplicationError("Digest does not match: approve exactly the action that was shown.")
        if workflow.now() > datetime.fromisoformat(approval.expires_at):
            raise ApplicationError("Approval has expired.")

    @workflow.signal
    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._emit("session_closed", "Closed by the client.")

    @workflow.signal
    def timer_fired(self, wake: WakeUp) -> None:
        """Sent by the delayed wake-up job. Queues a turn that acts on the agent's note."""
        key = f"timer:{wake.timer_id}"
        if key in self._seen:
            return  # a retried wake-up job
        self._seen[key] = None
        self._timers.pop(wake.timer_id, None)
        self._emit("timer_fired", wake.note, timer_id=wake.timer_id)
        self._queue.append(
            {
                "message_id": key,
                "kind": "timer",
                "text": f"{WAKE_UP_PREFIX} A timer you set has fired. Your note to yourself: {wake.note}\n"
                "Do it now. Your reply is shown to the user.",
            }
        )

    @workflow.query
    def events_since(self, after_seq: int) -> list[SessionEvent]:
        return [e for e in self._events if e.seq > after_seq]

    @workflow.query
    def status(self) -> SessionStatus:
        return SessionStatus(
            session_id=self._request.session_id,
            turn_running=self._turn_running,
            queued_messages=len(self._queue),
            pending_approvals=[a for a in self._approvals.values() if a.status == "pending"],
            last_seq=self._next_seq - 1,
            closed=self._closed,
            pending_timers=list(self._timers.values()),
        )

    # -- main loop -----------------------------------------------------------

    @workflow.run
    async def run(self, request: SessionInput) -> str:
        idle = timedelta(seconds=request.idle_timeout_seconds)
        while True:
            try:
                await workflow.wait_condition(lambda: bool(self._queue) or self._closed, timeout=idle)
            except TimeoutError:
                if self._timers:
                    continue  # idle, but a timer will wake this session: stay open for it
                self._closed = True
                self._emit("session_closed", "Closed after being idle.")
            if not self._queue:
                break  # closed and nothing left to do
            await self._run_turn(self._queue.pop(0))
            if workflow.info().is_continue_as_new_suggested():
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(SessionInput(**{**request.__dict__, "state": self._snapshot()}))
        await workflow.wait_condition(workflow.all_handlers_finished)
        return "closed"

    async def _run_turn(self, queued: dict[str, str]) -> None:
        self._turns += 1
        turn_id = f"{workflow.info().workflow_id}/turn-{self._turns}"
        self._turn_running = True
        self._emit("turn_started", queued["text"], turn_id=turn_id)
        checkpoint = len(self._messages)
        self._messages.append({"role": "user", "content": queued["text"]})
        remember = None
        if queued.get("kind", "user") == "user":
            # Runs alongside the turn; memory never holds up a reply.
            remember = workflow.start_activity_method(
                AgentActivities.record_interaction,
                InteractionRecord(self._request.user_id, turn_id, queued["text"]),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=TOOL_RETRY,
                summary="record for memory",
            )
        loop = WorkflowAgentLoop(
            purpose="session",
            system=SESSION_SYSTEM,
            tools=SESSION_TOOLS,
            user_id=self._request.user_id,
            turn_id=turn_id,
            approval_gate=self._approval_gate,
            drain_joined=self._drain_joined,
            local_tools={"set_timer": self._set_timer},
        )
        try:
            reply = await loop.run(self._messages)
            self._emit("assistant", reply, turn_id=turn_id)
        except (ActivityError, ApplicationError) as err:
            # Keep the conversation valid for the next turn and tell the client.
            del self._messages[checkpoint + 1 :]
            self._messages.append({"role": "assistant", "content": "I could not finish that request."})
            self._emit("turn_failed", str(err.cause if isinstance(err, ActivityError) else err), turn_id=turn_id)
        finally:
            if remember is not None:
                try:
                    await remember
                except ActivityError as err:
                    workflow.logger.warning("could not record %s for memory: %s", turn_id, err.cause)
            self._turn_running = False
            late = self._drain_joined()
            if late:
                # Joined after the turn's last model call: answer them next.
                self._queue.insert(0, {"message_id": f"{turn_id}-late", "text": "\n".join(b["text"] for b in late)})

    # -- timers ---------------------------------------------------------------

    async def _set_timer(self, call: ToolCall) -> ToolResult:
        try:
            delay = int(call.input.get("delay_seconds", 0))
        except (TypeError, ValueError):
            delay = 0
        note = str(call.input.get("note", "")).strip()
        if not 1 <= delay <= MAX_DELAY_SECONDS or not note:
            return ToolResult(
                call.tool_use_id, f"delay_seconds must be 1 to {MAX_DELAY_SECONDS}, and note must not be empty.", True
            )
        # Named after this tool call, so a retry can't create a second timer.
        timer_id = f"{call.turn_id}/{call.tool_use_id}"
        fires_at = (workflow.now() + timedelta(seconds=delay)).isoformat()
        await workflow.execute_activity_method(
            AgentActivities.schedule_wakeup,
            args=[WakeUp(workflow.info().workflow_id, self._request.user_id, timer_id, note), delay],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=TOOL_RETRY,
            summary=f"set timer: {delay}s",
        )
        if f"timer:{timer_id}" not in self._seen:  # it may already have fired
            self._timers[timer_id] = PendingTimer(timer_id, note, fires_at)
        self._emit("timer_set", note, timer_id=timer_id, fires_at=fires_at, delay_seconds=delay)
        return ToolResult(call.tool_use_id, json.dumps({"timer_id": timer_id, "fires_at": fires_at}))

    # -- approvals ------------------------------------------------------------

    async def _approval_gate(self, call: ToolCall) -> ToolResult:
        now = workflow.now()
        expires_at = now + timedelta(seconds=self._request.approval_timeout_seconds)
        approval = PendingApproval(
            approval_id=f"{call.turn_id}/{call.tool_use_id}",
            tool_use_id=call.tool_use_id,
            tool=call.name,
            input=call.input,
            digest=approval_digest(call.name, call.input),
            requested_at=now.isoformat(),
            expires_at=expires_at.isoformat(),
        )
        self._approvals[approval.approval_id] = approval
        self._emit(
            "approval_requested",
            f"Approve {call.name} {json.dumps(call.input, sort_keys=True)}?",
            approval_id=approval.approval_id,
            digest=approval.digest,
            expires_at=approval.expires_at,
            tool=call.name,
            input=call.input,
        )
        try:
            await workflow.wait_condition(lambda: approval.status != "pending", timeout=expires_at - now)
        except TimeoutError:
            approval.status = "expired"
            self._emit("approval_expired", f"{call.name} was not approved in time.", approval_id=approval.approval_id)
            # Cleanup on expiry: tell the user nothing was done.
            await workflow.execute_activity_method(
                AgentActivities.deliver_message,
                DeliveryRequest(
                    message_id=f"{approval.approval_id}:expired",
                    user_id=self._request.user_id,
                    body=f"Your approval to run {call.name} expired, so nothing was sent.",
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=DELIVERY_RETRY,
                summary="notify: approval expired",
            )
            return ToolResult(
                call.tool_use_id, "The user did not approve this before the deadline. Nothing was done.", True
            )
        if approval.status == "denied":
            return ToolResult(call.tool_use_id, "The user declined this action. Nothing was done.", True)
        # Approved: the tool re-checks the grant when it executes.
        call.approval = ApprovalGrant(approval.approval_id, approval.digest, approval.expires_at)
        return await execute_tool(call)

    # -- helpers --------------------------------------------------------------

    def _drain_joined(self) -> list[dict[str, Any]]:
        joined, self._joined = self._joined, []
        return joined

    def _emit(self, kind: str, text: str, **data: Any) -> None:
        self._events.append(SessionEvent(seq=self._next_seq, kind=kind, text=text, data=data))
        self._next_seq += 1

    def _snapshot(self) -> SessionState:
        return SessionState(
            messages=self._messages,
            events=self._events[-EVENTS_CARRIED_ACROSS_RUNS:],
            next_seq=self._next_seq,
            seen_client_message_ids=list(self._seen)[-SEEN_IDS_CARRIED_ACROSS_RUNS:],
            queued=self._queue,
            turns=self._turns,
            timers=list(self._timers.values()),
        )
