"""Scheduling with Standalone Activities, and the two agent recovery boundaries."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta

import pytest
from temporalio.client import ActivityExecutionStatus

from always_on_agents import app
from always_on_agents.domain import TurnOutcome
from always_on_agents.scheduling import describe_weekly_briefing, schedule_briefing, set_weekly_briefing_schedule

from .conftest import assert_replays, eventually, running_workers
from .fake_model import FakeModel


async def test_now_and_later_share_one_path(client, settings, store, workers):
    submitted_at = time.monotonic()
    assert await schedule_briefing(client, "alice", "now", task_queue=settings.scheduled_queue)
    assert await schedule_briefing(client, "alice", "later", timedelta(seconds=3), task_queue=settings.scheduled_queue)

    now = await client.get_activity_handle("briefing-alice-now", result_type=TurnOutcome).result()
    later = await client.get_activity_handle("briefing-alice-later", result_type=TurnOutcome).result()
    assert time.monotonic() - submitted_at >= 3  # the delayed job waited

    for outcome in (now, later):
        assert outcome.report_id == outcome.turn_id
        assert outcome.delivered
        assert store.get_report(outcome.turn_id) is not None
    assert len(store.outbox("alice")) == 2


async def test_duplicate_submission_is_rejected(client, settings, store, workers):
    assert await schedule_briefing(
        client, "bob", "2026-11-23", timedelta(seconds=1), task_queue=settings.scheduled_queue
    )
    assert not await schedule_briefing(client, "bob", "2026-11-23", task_queue=settings.scheduled_queue)
    await client.get_activity_handle("briefing-bob-2026-11-23").result()
    # A completed briefing can't be resubmitted either (ALLOW_DUPLICATE_FAILED_ONLY).
    assert not await schedule_briefing(client, "bob", "2026-11-23", task_queue=settings.scheduled_queue)
    assert len(store.outbox("bob")) == 1


async def test_delayed_job_waits_for_a_worker_that_was_down(client, settings, store):
    # Submit with no worker running, as if the worker were redeployed overnight.
    await schedule_briefing(client, "carol", "monday", timedelta(seconds=1), task_queue=settings.scheduled_queue)
    handle = client.get_activity_handle("briefing-carol-monday", result_type=TurnOutcome)
    await asyncio.sleep(2)
    assert (await handle.describe()).status == ActivityExecutionStatus.RUNNING  # retained, waiting for a worker
    assert store.ops("model_call") == []

    async with running_workers(client, settings):
        outcome = await handle.result()
    assert outcome.delivered


async def test_coarse_turn_repeats_model_calls_on_retry(client, settings, store, workers):
    # The report write fails once. The whole Activity retries, so every model
    # call before it runs again. Delivery still happens exactly once.
    store.set_fault("write_report", 1)
    await schedule_briefing(client, "dave", "retry", task_queue=settings.scheduled_queue)
    # The helper's retry policy waits 10s before the second attempt.
    outcome = await client.get_activity_handle("briefing-dave-retry", result_type=TurnOutcome).result()
    calls = store.ops("model_call", key_prefix="briefing-dave-retry")
    assert outcome.model_calls == 4
    assert len(calls) == 3 + 4  # first attempt died on its 3rd call's tool; second ran all 4
    assert [c["attempt"] for c in calls] == [1, 1, 1, 2, 2, 2, 2]
    assert len(store.outbox("dave")) == 1


async def test_fine_grained_turn_reuses_recorded_results(client, settings, store, workers):
    # Same failure, Workflow-coordinated loop: only the failed write is retried.
    store.set_fault("write_report", 2)
    handle = await app.start_turn(client, settings, "erin", "turn-erin-1")
    outcome = await handle.result()

    assert outcome.model_calls == 4
    assert len(store.ops("model_call", key_prefix="turn-erin-1")) == 4  # no model call repeated
    writes = store.ops("tool:write_report", key_prefix="turn-erin-1")
    assert [w["attempt"] for w in writes] == [1, 2, 3]
    assert len(store.ops("tool:fetch_documents", key_prefix="turn-erin-1")) == 1
    assert len(store.outbox("erin")) == 1
    await assert_replays(handle)


async def test_turn_can_call_a_workflow_as_a_tool(client, settings, store, workers):
    handle = await app.start_turn(client, settings, "frank", "turn-frank-1")
    outcome = await handle.result()
    research = client.get_workflow_handle("research-turn-frank-1-toolu_01_2_start_research")
    assert (await research.describe()).status.name == "COMPLETED"
    report = store.get_report(outcome.turn_id)
    assert report and "## Research" in report["body"]
    await assert_replays(research)


async def test_scheduled_turn_survives_worker_restart_mid_turn(client, settings, store):
    # Slow the model so the worker can be stopped in the middle of the turn.
    slow = FakeModel(latency_seconds=0.5)
    async with running_workers(client, settings, slow):
        handle = await app.start_turn(client, settings, "gina", "turn-gina-1")
        await eventually(lambda: _model_calls_at_least(store, "turn-gina-1", 2))
    # Worker gone. Start a fresh one: the turn continues from recorded progress.
    async with running_workers(client, settings, slow):
        outcome = await handle.result()
    assert outcome.delivered
    # At most the in-flight call repeats; everything recorded is reused.
    assert len(store.ops("model_call", key_prefix="turn-gina-1")) <= outcome.model_calls + 1
    await assert_replays(handle)


async def _model_calls_at_least(store, key_prefix: str, n: int) -> bool:
    return len(store.ops("model_call", key_prefix=key_prefix)) >= n


async def test_weekly_briefing_rejects_what_it_cannot_schedule(client, settings):
    for bad in ({"day": "someday"}, {"at": "25:00"}, {"at": "7am"}, {"time_zone": "Mars/Olympus_Mons"}):
        with pytest.raises(ValueError):
            await set_weekly_briefing_schedule(client, "kim", task_queue=settings.scheduled_queue, **bad)
    assert await describe_weekly_briefing(client, "kim") is None
