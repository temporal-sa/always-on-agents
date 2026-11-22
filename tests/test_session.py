"""Sessions: accept vs. finish, approvals with deadlines, reconnecting."""

from __future__ import annotations

from datetime import timedelta

import pytest
from temporalio.client import WorkflowUpdateFailedError
from temporalio.service import RPCError

from always_on_agents import app, scheduling

from .conftest import assert_replays, eventually, running_workers

SHARE = "Please share my report with bob@example.com"


async def _pending_approval(client, session_id):
    status = await app.session_status(client, session_id)
    return status.pending_approvals[0] if status.pending_approvals else None


async def _event(client, session_id, kind, after=0):
    events = await app.session_events(client, session_id, after)
    return next((e for e in events if e.kind == kind), None)


async def test_approval_survives_restart_and_executes_once(client, settings, store):
    store.upsert_report("r1", "alice", "Weekly briefing", "# Weekly briefing")
    async with running_workers(client, settings):
        await app.start_session(client, settings, "s1", "alice")
        accepted = await app.send_session_message(client, "s1", "m1", SHARE)
        assert accepted.disposition == "started"
        approval = await eventually(lambda: _pending_approval(client, "s1"))
        assert approval.tool == "share_report" and approval.input["recipient"] == "bob@example.com"

    # Workers restart while approval is pending; the client is "disconnected".
    async with running_workers(client, settings):
        # Reconnect: read events from the start, find the approval request.
        requested = await _event(client, "s1", "approval_requested")
        assert requested.data["approval_id"] == approval.approval_id
        assert await app.decide_approval(client, "s1", approval.approval_id, approval.digest, True) == "approved"
        reply = await eventually(lambda: _event(client, "s1", "assistant"))
        assert "shared" in reply.text

        # The same approval can't be used twice.
        with pytest.raises(WorkflowUpdateFailedError):
            await app.decide_approval(client, "s1", approval.approval_id, approval.digest, True)

    shares = [m for m in store.outbox("alice") if m["channel"] == "share:bob@example.com"]
    assert len(shares) == 1


async def test_approval_must_match_what_was_shown(client, settings, store, workers):
    store.upsert_report("r1", "bob", "Weekly briefing", "# Weekly briefing")
    await app.start_session(client, settings, "s2", "bob")
    await app.send_session_message(client, "s2", "m1", SHARE)
    approval = await eventually(lambda: _pending_approval(client, "s2"))
    with pytest.raises(WorkflowUpdateFailedError):
        await app.decide_approval(client, "s2", approval.approval_id, "not-the-digest", True)
    assert await app.decide_approval(client, "s2", approval.approval_id, approval.digest, False) == "denied"
    reply = await eventually(lambda: _event(client, "s2", "assistant"))
    assert "did not share" in reply.text
    assert store.outbox("bob") == []


async def test_expired_approval_triggers_cleanup(client, settings, store, workers):
    store.upsert_report("r1", "carol", "Weekly briefing", "# Weekly briefing")
    await app.start_session(client, settings, "s3", "carol", approval_timeout=timedelta(seconds=2))
    await app.send_session_message(client, "s3", "m1", SHARE)
    assert await eventually(lambda: _event(client, "s3", "approval_expired"))
    reply = await eventually(lambda: _event(client, "s3", "assistant"))
    assert "did not share" in reply.text
    outbox = store.outbox("carol")
    assert len(outbox) == 1 and "expired" in outbox[0]["body"]  # the cleanup notice, not a share


async def test_messages_join_or_queue_while_a_turn_runs(client, settings, store, workers):
    store.upsert_report("r1", "dave", "Weekly briefing", "# Weekly briefing")
    await app.start_session(client, settings, "s4", "dave")
    await app.send_session_message(client, "s4", "m1", SHARE)
    approval = await eventually(lambda: _pending_approval(client, "s4"))

    # The turn is waiting on approval, so it is still running.
    assert (
        await app.send_session_message(client, "s4", "m2", "also, keep it short", mode="auto")
    ).disposition == "joined"
    assert (
        await app.send_session_message(client, "s4", "m3", "what is next week?", mode="queue")
    ).disposition == "queued"
    assert (
        await app.send_session_message(client, "s4", "m3", "what is next week?", mode="queue")
    ).disposition == "queued"

    await app.decide_approval(client, "s4", approval.approval_id, approval.digest, True)

    async def two_replies():
        events = await app.session_events(client, "s4")
        replies = [e.text for e in events if e.kind == "assistant"]
        return replies if len(replies) >= 2 else None

    replies = await eventually(two_replies)
    assert "shared" in replies[0]
    assert "what is next week?" in replies[1]


async def test_session_closes_and_replays(client, settings, store, workers):
    await app.start_session(client, settings, "s5", "erin")
    await app.send_session_message(client, "s5", "m1", "hello")
    await eventually(lambda: _event(client, "s5", "assistant"))
    handle = app.session_handle(client, "s5")
    await handle.signal("close")
    assert await handle.result() == "closed"
    with pytest.raises((WorkflowUpdateFailedError, RPCError)):  # closed, then completed
        await app.send_session_message(client, "s5", "m2", "anyone there?")
    await assert_replays(handle)


async def _events_of(client, session_id, kind):
    return [e for e in await app.session_events(client, session_id) if e.kind == kind]


async def test_timer_wakes_the_session_later(client, settings, store, workers):
    await app.start_session(client, settings, "s6", "frank")
    await app.send_session_message(client, "s6", "m1", 'Send me "hello" in 2 seconds')

    timer_set = await eventually(lambda: _event(client, "s6", "timer_set"))
    status = await app.session_status(client, "s6")
    assert [t.timer_id for t in status.pending_timers] == [timer_set.data["timer_id"]]

    # The delayed job is a Standalone Activity submitted through the shared scheduling path.
    job = client.get_activity_handle(f"timer-{timer_set.data['timer_id']}")
    assert (await job.describe()).start_delay.total_seconds() == 2

    async def woke_and_replied():
        replies = [e.text for e in await _events_of(client, "s6", "assistant")]
        return replies if "hello" in replies else None

    replies = await eventually(woke_and_replied, timeout=30)
    assert replies[0].startswith("OK, I'll do that")
    assert await _event(client, "s6", "timer_fired")
    assert (await app.session_status(client, "s6")).pending_timers == []


async def test_timer_after_the_session_ends_goes_to_the_outbox(client, settings, store, workers):
    await app.start_session(client, settings, "s7", "gina")
    await app.send_session_message(client, "s7", "m1", 'Send me "standup in 5" in 2 seconds')
    await eventually(lambda: _event(client, "s7", "assistant"))
    await app.session_handle(client, "s7").signal("close")

    async def delivered():
        return [m for m in store.outbox("gina") if m["message_id"].startswith("timer:")]

    reminders = await eventually(delivered, timeout=30)
    assert "standup in 5" in reminders[0]["body"]


async def test_chat_can_schedule_a_briefing(client, settings, store, workers):
    await app.start_session(client, settings, "s8", "hana")
    await app.send_session_message(client, "s8", "m1", "Prepare my briefing in 1 second")
    reply = await eventually(lambda: _event(client, "s8", "assistant"))
    briefing_id = reply.text.split()[2]
    assert briefing_id.startswith("briefing-hana-chat-")

    outcome = await client.get_activity_handle(briefing_id).result()
    assert outcome["delivered"] if isinstance(outcome, dict) else outcome.delivered
    assert any(m["message_id"] == f"{briefing_id}:delivery" for m in store.outbox("hana"))


async def test_what_the_user_says_reaches_memory(client, settings, store, workers):
    await app.start_session(client, settings, "s9", "ivan")
    for i, text in enumerate(["Lead with decisions.", "Bold the due dates.", "Keep it to one page."]):
        await app.send_session_message(client, "s9", f"m{i}", text, mode="queue")

    async def published():
        revision, notes = store.current_memory("ivan")
        return notes if revision >= 1 and len(notes) == 3 else None

    notes = await eventually(published, timeout=30)
    assert {n.text for n in notes} == {
        "User preference: Lead with decisions.",
        "User preference: Bold the due dates.",
        "User preference: Keep it to one page.",
    }


async def test_chat_sets_moves_and_cancels_the_weekly_briefing(client, settings, store, workers):
    await app.start_session(client, settings, "s10", "jo")

    async def reply_to(message_id, text):
        seen = await app.session_events(client, "s10", 0)
        after = seen[-1].seq if seen else 0
        await app.send_session_message(client, "s10", message_id, text)
        return (await eventually(lambda: _event(client, "s10", "assistant", after))).text

    reply = await reply_to("m1", "Send me my briefing every Monday at 8am America/New_York")
    assert reply == "Your weekly briefing runs Mondays at 08:00 (America/New_York)."

    # Asking again with a new time moves the same Schedule rather than adding one.
    reply = await reply_to("m2", "Actually, every Friday at 4:30pm")
    assert reply == "Your weekly briefing runs Fridays at 16:30 (UTC)."
    weekly = await scheduling.describe_weekly_briefing(client, "jo")
    assert weekly["schedule_id"] == "weekly-briefing-jo" and weekly["when"] == "Fridays at 16:30"
    assert weekly["next_runs"]

    assert await reply_to("m3", "When is my weekly briefing?") == "Your weekly briefing runs Fridays at 16:30 (UTC)."
    assert await reply_to("m4", "Cancel my weekly briefing") == "Your weekly briefing is cancelled."
    assert await scheduling.describe_weekly_briefing(client, "jo") is None
