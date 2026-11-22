"""Activities for memory consolidation: preparation and publication.

Preparation is the expensive step (a model pass over a batch of interactions).
It saves its proposal durably before returning, which gives publication its
own recovery boundary: if the memory database is down, publication retries
from the saved proposal without paying for another model call.
"""

from __future__ import annotations

import asyncio
import json

from temporalio import activity
from temporalio.exceptions import ApplicationError

from ..agent.messages import check_stop_reason, final_text
from ..agent.prompts import MEMORY_OUTPUT_SCHEMA, MEMORY_SYSTEM
from ..domain import ModelRequest, OperatorAlert, PrepareBatchInput, PreparedBatch, PublishResult
from ..llm import ModelClient
from ..store import AppStore, MemoryNote
from .heartbeat import heartbeating

MAX_NOTES = 50
MAX_NOTE_CHARS = 500


class MemoryActivities:
    def __init__(self, store: AppStore, model: ModelClient) -> None:
        self._store = store
        self._model = model

    @activity.defn
    async def prepare_memory_batch(self, request: PrepareBatchInput) -> PreparedBatch:
        """Create or reload the batch, propose a revision, validate it, and save it."""
        batch = await asyncio.to_thread(
            self._store.create_or_load_batch, request.batch_id, request.user_id, request.max_interactions
        )
        ids: list[str] = batch["interaction_ids"]
        if not ids:
            return PreparedBatch(request.batch_id, request.user_id, [], batch["base_revision"], 0, False)
        if batch["proposal"] is not None:
            # A previous attempt finished the model pass but its completion was
            # never recorded. Reuse the saved proposal instead of paying again.
            await asyncio.to_thread(
                self._store.log_op, "memory:proposal_reused", request.batch_id, activity.info().attempt
            )
            return PreparedBatch(
                request.batch_id, request.user_id, ids, batch["base_revision"], len(batch["proposal"]), True
            )

        _, notes = await asyncio.to_thread(self._store.current_memory, request.user_id)
        interactions = await asyncio.to_thread(self._store.interactions, ids)
        async with heartbeating(lambda: request.batch_id):
            response = await self._model.complete(
                ModelRequest(
                    purpose="memory",
                    system=MEMORY_SYSTEM,
                    messages=[
                        {
                            "role": "user",
                            "content": json.dumps(
                                {
                                    "current_notes": [n.__dict__ for n in notes],
                                    "interactions": [
                                        {"interaction_id": i["interaction_id"], "text": i["text"]} for i in interactions
                                    ],
                                }
                            ),
                        }
                    ],
                    output_schema=MEMORY_OUTPUT_SCHEMA,
                    log_key=request.batch_id,
                )
            )
        check_stop_reason(response)
        proposal = _validate(
            final_text(response.content), allowed_sources=set(ids) | {s for n in notes for s in n.sources}
        )
        await asyncio.to_thread(self._store.save_proposal, request.batch_id, proposal)
        return PreparedBatch(request.batch_id, request.user_id, ids, batch["base_revision"], len(proposal), False)

    @activity.defn
    def publish_memory_revision(self, batch_id: str) -> PublishResult:
        """Commit the saved proposal and the interactions it covers in one transaction."""
        self._store.log_op("memory:publish_attempt", batch_id, activity.info().attempt)
        self._store.maybe_fail("publish_memory")  # e.g. the memory database is unavailable
        revision, covered, already = self._store.publish_batch(batch_id)
        return PublishResult(batch_id, revision, covered, already)

    @activity.defn
    def notify_operator(self, alert: OperatorAlert) -> None:
        """Ask a human to look. A real system would page or open a ticket."""
        self._store.add_alert(alert.kind, alert.subject, alert.detail)
        activity.logger.warning("operator alert: %s %s: %s", alert.kind, alert.subject, alert.detail)


def _validate(raw: str, allowed_sources: set[str]) -> list[MemoryNote]:
    """Reject proposals that are malformed, oversized, or cite unknown sources.

    Raised as a retryable error: a fresh model pass may produce a valid proposal.
    """
    try:
        data = json.loads(raw)
        notes = [MemoryNote(text=str(n["text"]).strip(), sources=[str(s) for s in n["sources"]]) for n in data["notes"]]
    except (json.JSONDecodeError, KeyError, TypeError) as err:
        raise ApplicationError(f"memory proposal is not valid JSON: {err}", type="InvalidProposal") from err
    if len(notes) > MAX_NOTES:
        raise ApplicationError(f"memory proposal has {len(notes)} notes (max {MAX_NOTES})", type="InvalidProposal")
    for note in notes:
        if not note.text or len(note.text) > MAX_NOTE_CHARS:
            raise ApplicationError("memory note is empty or too long", type="InvalidProposal")
        if not note.sources or not set(note.sources) <= allowed_sources:
            raise ApplicationError(f"memory note cites unknown sources: {note.sources}", type="InvalidProposal")
    return notes
