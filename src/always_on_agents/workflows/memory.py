"""Memory consolidation: batch new interactions, prepare a revision, publish it.

* New interactions arrive as signals (via signal-with-start on the fixed ID
  ``memory-consolidation-{user_id}``). The Workflow waits durably until either
  ``batch_size`` interactions have arrived or ``max_wait_seconds`` have passed
  since the first one.
* Preparation (one Activity) snapshots the batch, runs the model pass,
  validates the proposal, and saves it.
* Publication (another Activity) commits the saved proposal and the record of
  which interactions it covers in one transaction. If the memory database is
  down, publication retries on its own, from the saved proposal, without a new
  model call. If retries run out, the Workflow asks an operator and waits.

The idea to take away: save the expensive result before the risky step. The
model pass is slow and costs money; the database write is cheap but can fail.
Because the proposal is saved first, a database outage costs retries of the
write, never a second model pass, and the batch ID makes every step safe to
repeat.

See it happen: recovery drill 4 in the README, or the Memory and Operations
pages in the web UI.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from ..activities.memory import MemoryActivities
    from ..domain import (
        ConsolidationInput,
        ConsolidationOutcome,
        OperatorAlert,
        PrepareBatchInput,
        PreparedBatch,
        PublishResult,
    )
    from .agent_loop import HEARTBEAT_TIMEOUT, MODEL_RETRY


def consolidation_workflow_id(user_id: str) -> str:
    return f"memory-consolidation-{user_id}"


# The alert raised when publication runs out of retries. Its subject is the
# batch ID, ``{workflow_id}/{run_id}/batch-N``.
PUBLICATION_FAILED = "memory_publication_failed"


def batch_run(batch_id: str) -> tuple[str, str]:
    """The Workflow ID and run ID that prepared a batch."""
    workflow_id, run_id, _ = batch_id.split("/", 2)
    return workflow_id, run_id


@workflow.defn
class MemoryConsolidationWorkflow:
    @workflow.init
    def __init__(self, request: ConsolidationInput) -> None:
        self._pending = request.pending_count
        self._first_pending_at: datetime | None = None
        self._flush = request.flush_now
        self._operator_retry = False
        self._batches = request.batches_done
        self._published: list[int] = []
        self._status = "starting"

    # -- notifications -------------------------------------------------------

    @workflow.signal
    def interaction_recorded(self, interaction_id: str) -> None:
        self._pending += 1
        if self._first_pending_at is None:
            self._first_pending_at = workflow.now()

    @workflow.signal
    def flush(self) -> None:
        """Process whatever is pending now instead of waiting for the batch to fill."""
        self._flush = True

    @workflow.signal
    def retry_publication(self) -> None:
        """Sent by an operator after fixing whatever made publication fail."""
        self._operator_retry = True

    @workflow.query
    def status(self) -> dict[str, Any]:
        return {
            "status": self._status,
            "pending": self._pending,
            "first_pending_at": self._first_pending_at.isoformat() if self._first_pending_at else None,
            "batches": self._batches,
            "published_revisions": self._published,
        }

    # -- main loop -----------------------------------------------------------

    @workflow.run
    async def run(self, request: ConsolidationInput) -> ConsolidationOutcome:
        if self._pending and self._first_pending_at is None:
            self._first_pending_at = workflow.now()

        while self._pending > 0 or self._flush:
            await self._wait_until_batch_ready(request)

            info = workflow.info()
            # Stable per batch: a retried preparation reloads the same record.
            # agent(constraint): batch_run() and the web alert-retry route parse this format.
            batch_id = f"{info.workflow_id}/{info.run_id}/batch-{self._batches}"
            self._batches += 1
            self._status = "preparing"
            prepared = await workflow.execute_activity_method(
                MemoryActivities.prepare_memory_batch,
                PrepareBatchInput(batch_id=batch_id, user_id=request.user_id),
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=HEARTBEAT_TIMEOUT,
                retry_policy=MODEL_RETRY,
                summary="prepare memory batch",
            )
            self._flush = False
            covered = len(prepared.interaction_ids)
            if covered:
                result = await self._publish(prepared, request)
                if result is None:
                    # Memory changed under this batch. Its interactions are still
                    # unprocessed, so prepare a fresh batch right away.
                    self._flush = True
                    continue
                self._published.append(result.revision)
            # agent(invariant): _pending is only a hint counted from signals. The database's
            # unprocessed interactions are the truth, so an empty batch resets it to zero.
            self._pending = max(0, self._pending - covered) if covered else 0
            self._first_pending_at = workflow.now() if self._pending else None

            if workflow.info().is_continue_as_new_suggested():
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(
                    ConsolidationInput(
                        user_id=request.user_id,
                        batch_size=request.batch_size,
                        max_wait_seconds=request.max_wait_seconds,
                        flush_now=self._flush,
                        max_publish_attempts=request.max_publish_attempts,
                        pending_count=self._pending,
                        batches_done=self._batches,
                    )
                )

        # Nothing pending: finish. The next interaction starts a new run with the
        # same Workflow ID via signal-with-start.
        # agent(trade-off): ending when idle is deliberate (no idle wait, no endless run);
        # app.notify_consolidation restarts it.
        self._status = "idle"
        await workflow.wait_condition(workflow.all_handlers_finished)
        return ConsolidationOutcome(request.user_id, self._published)

    async def _wait_until_batch_ready(self, request: ConsolidationInput) -> None:
        if self._flush or self._pending >= request.batch_size:
            return
        assert self._first_pending_at is not None
        deadline = self._first_pending_at + timedelta(seconds=request.max_wait_seconds)
        remaining = deadline - workflow.now()
        self._status = "batching"
        if remaining > timedelta(0):
            try:
                await workflow.wait_condition(
                    lambda: self._pending >= request.batch_size or self._flush, timeout=remaining
                )
            except TimeoutError:
                pass  # time limit reached: process what we have

    async def _publish(self, prepared: PreparedBatch, request: ConsolidationInput) -> PublishResult | None:
        while True:
            self._status = "publishing"
            try:
                return await workflow.execute_activity_method(
                    MemoryActivities.publish_memory_revision,
                    prepared.batch_id,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=30),
                        maximum_attempts=request.max_publish_attempts,
                        # agent(constraint): matches the class name of store.StaleBatchError. Temporal
                        # types a plain exception by its class name, so renaming it makes stale
                        # batches retry until an operator is paged.
                        non_retryable_error_types=["StaleBatchError"],
                    ),
                    summary="publish memory revision",
                )
            except ActivityError as err:
                cause = err.cause
                if isinstance(cause, ApplicationError) and cause.type == "StaleBatchError":
                    workflow.logger.info("batch %s is stale: %s", prepared.batch_id, cause.message)
                    return None
                # Retries are exhausted. Keep the saved proposal, ask a human,
                # and wait. Nothing is lost while we wait, even across restarts.
                self._status = "waiting_for_operator"
                # agent(invariant): reset before the alert is sent, so a retry signal that
                # arrives while it is being sent is not lost.
                self._operator_retry = False
                await workflow.execute_activity_method(
                    MemoryActivities.notify_operator,
                    OperatorAlert(
                        kind=PUBLICATION_FAILED,
                        subject=prepared.batch_id,
                        detail=f"{cause}. Fix the dependency, then signal retry_publication.",
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    summary="ask an operator for help",
                )
                await workflow.wait_condition(lambda: self._operator_retry)
