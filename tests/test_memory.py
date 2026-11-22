"""Memory consolidation: batching, saved proposals, publication recovery."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from always_on_agents import app
from always_on_agents.domain import ConsolidationInput, ConsolidationOutcome
from always_on_agents.store import StaleBatchError
from always_on_agents.workflows.memory import MemoryConsolidationWorkflow, consolidation_workflow_id

from .conftest import assert_replays, eventually

FEEDBACK = "Next time, lead with the decisions that need my attention."


def _handle(client, user_id):
    return client.get_workflow_handle_for(MemoryConsolidationWorkflow.run, consolidation_workflow_id(user_id))


async def test_batches_by_count(client, settings, store, workers):
    for i in range(2):
        await app.record_interaction(client, store, settings, "alice", f"note {i}", batch_size=3)
    status = await eventually(lambda: _status_is(client, "alice", "batching"))
    assert status["pending"] == 2
    assert store.current_memory("alice")[0] == 0  # nothing published yet

    await app.record_interaction(client, store, settings, "alice", FEEDBACK, batch_size=3)
    outcome = await _handle(client, "alice").result()
    assert outcome.revisions_published == [1]
    revision, notes = store.current_memory("alice")
    assert revision == 1 and len(notes) == 3
    assert store.unprocessed_count("alice") == 0
    await assert_replays(_handle(client, "alice"))


async def test_batches_by_time(client, settings, store, workers):
    await app.record_interaction(client, store, settings, "bob", FEEDBACK, batch_size=10, max_wait=timedelta(seconds=2))
    outcome = await _handle(client, "bob").result()
    assert outcome.revisions_published == [1]


async def test_publication_retries_from_saved_proposal(client, settings, store, workers):
    # The memory database is down for the first two publication attempts.
    store.set_fault("publish_memory", 2)
    await app.record_interaction(client, store, settings, "carol", FEEDBACK, batch_size=1)
    await _handle(client, "carol").result()

    assert len(store.ops("model_call")) == 1  # the paid model pass ran once
    assert [o["attempt"] for o in store.ops("memory:publish_attempt")] == [1, 2, 3]
    assert store.current_memory("carol")[0] == 1


async def test_exhausted_retries_wait_for_an_operator(client, settings, store, workers):
    store.set_fault("publish_memory", 100)
    store.record_interaction("dave", FEEDBACK, "ix-dave-1")
    handle = await client.start_workflow(
        MemoryConsolidationWorkflow.run,
        ConsolidationInput(user_id="dave", batch_size=1, max_publish_attempts=2),
        id=consolidation_workflow_id("dave"),
        task_queue=settings.background_queue,
        start_signal="interaction_recorded",
        start_signal_args=["ix-dave-1"],
    )
    await eventually(lambda: _status_is(client, "dave", "waiting_for_operator"))
    alerts = await eventually(lambda: asyncio.to_thread(store.alerts))  # raised just after
    assert alerts[0]["kind"] == "memory_publication_failed"
    assert store.current_memory("dave")[0] == 0  # later sessions still read the old revision

    store.clear_faults()  # the operator fixes the database...
    await app.retry_memory_publication(client, "dave")  # ...and tells the Workflow
    outcome: ConsolidationOutcome = await handle.result()
    assert outcome.revisions_published == [1]
    assert len(store.ops("model_call")) == 1
    await assert_replays(handle)


async def test_second_start_is_rejected_while_running(client, settings, store, workers):
    await app.record_interaction(client, store, settings, "erin", "hello", batch_size=5)
    await eventually(lambda: _status_is(client, "erin", "batching"))
    assert not await app.consolidate_now(client, settings, "erin")  # Fail conflict policy
    await _handle(client, "erin").signal(MemoryConsolidationWorkflow.flush)
    assert (await _handle(client, "erin").result()).revisions_published == [1]
    # Now nothing is running, so a direct start is accepted (and finds nothing to do).
    assert await app.consolidate_now(client, settings, "erin")
    assert (await _handle(client, "erin").result()).revisions_published == []


async def test_new_preference_reaches_the_next_briefing(client, settings, store, workers):
    before = await (await app.start_turn(client, settings, "frank", "turn-frank-before")).result()
    await app.record_interaction(client, store, settings, "frank", FEEDBACK, batch_size=1)
    await _handle(client, "frank").result()
    after = await (await app.start_turn(client, settings, "frank", "turn-frank-after")).result()

    def first_section(turn_id):
        return store.get_report(turn_id)["body"].split("## ")[1].splitlines()[0]

    assert first_section(before.turn_id) == "Updates"
    assert first_section(after.turn_id) == "Decisions that need your attention"


def test_stale_batch_is_never_published(store):
    for i in range(2):
        store.record_interaction("gina", f"note {i}", f"ix-{i}")
    first = store.create_or_load_batch("b1", "gina", 10)
    second = store.create_or_load_batch("b2", "gina", 10)  # overlapping snapshot
    assert first["interaction_ids"] == second["interaction_ids"]
    from always_on_agents.store import MemoryNote

    for batch in ("b1", "b2"):
        store.save_proposal(batch, [MemoryNote("n", ["ix-0"])])
    assert store.publish_batch("b1") == (1, 2, False)
    assert store.publish_batch("b1") == (1, 2, True)  # idempotent
    with pytest.raises(StaleBatchError):
        store.publish_batch("b2")


async def _status_is(client, user_id, wanted):
    status = await _handle(client, user_id).query(MemoryConsolidationWorkflow.status)
    return status if status["status"] == wanted else None
