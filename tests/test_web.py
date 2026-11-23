"""The web API, end to end against a dev server and workers (with the test model double).

The Briefings and Memory views are read-only, so these tests start work the way
the assistant, the CLI, or a Schedule would, then check what the API shows.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from always_on_agents import app as agent_app
from always_on_agents import scheduling
from always_on_agents.domain import ConsolidationInput
from always_on_agents.web.server import create_app
from always_on_agents.workflows.memory import MemoryConsolidationWorkflow, consolidation_workflow_id

from .conftest import eventually


@pytest.fixture
async def http(client, settings, workers):
    app = create_app(client=client, settings=settings)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
        yield http


async def _json(http, method, url, **kwargs):
    response = await http.request(method, url, **kwargs)
    assert response.status_code == 200, response.text
    return response.json()


def _sse(text: str) -> list[tuple[str | None, str, dict]]:
    """Parse an SSE body into (id, event, data) tuples, skipping comments."""
    out = []
    for block in text.strip().split("\n\n"):
        fields = dict(line.split(": ", 1) for line in block.splitlines() if ": " in line and not line.startswith(":"))
        if "data" in fields:
            out.append((fields.get("id"), fields.get("event", "message"), json.loads(fields["data"])))
    return out


async def test_page_and_config(http):
    page = await http.get("/")
    assert page.status_code == 200 and "Briefing Assistant" in page.text
    config = await _json(http, "GET", "/api/config")
    assert (config["model_provider"], config["model"]) == ("anthropic", "claude-opus-5-5")


async def test_briefings_view(http, client, settings, store):
    # The assistant and the weekly Schedule start briefings; the view only reads them.
    turn_id = "turn-web-alice-1"
    await (await agent_app.start_turn(client, settings, "web-alice", turn_id)).result()
    assert await scheduling.schedule_briefing(client, "web-alice", "now1", task_queue=settings.scheduled_queue)

    async def job_done():
        jobs = await _json(http, "GET", "/api/users/web-alice/scheduled")
        return jobs if jobs and jobs[0]["status"] == "COMPLETED" else None

    jobs = await eventually(job_done)
    assert jobs[0]["activity_id"] == "briefing-web-alice-now1"
    assert jobs[0]["result"]["report_id"] == "briefing-web-alice-now1"

    reports = await _json(http, "GET", "/api/users/web-alice/reports")
    assert {r["report_id"] for r in reports} == {turn_id, "briefing-web-alice-now1"}
    body = await _json(http, "GET", f"/api/reports/{turn_id}")
    assert body["body"].startswith("# Weekly briefing")
    store.upsert_report("session-web-alice-s1/turn-3", "web-alice", "From chat", "# From chat")
    assert (await _json(http, "GET", "/api/reports/session-web-alice-s1%2Fturn-3"))["title"] == "From chat"
    assert len(await _json(http, "GET", "/api/users/web-alice/outbox")) == 2
    runs = await _json(http, "GET", "/api/users/web-alice/turns")
    assert [r["turn_id"] for r in runs] == [turn_id]


async def test_weekly_schedule_view(http, client, settings):
    assert (await _json(http, "GET", "/api/users/web-bob/weekly"))["exists"] is False
    await scheduling.set_weekly_briefing_schedule(
        client, "web-bob", task_queue=settings.scheduled_queue, day="tue", at="06:45", time_zone="Europe/Berlin"
    )
    weekly = await _json(http, "GET", "/api/users/web-bob/weekly")
    assert weekly["exists"] and weekly["when"] == "Tuesdays at 06:45" and weekly["time_zone"] == "Europe/Berlin"
    assert len(weekly["next_runs"]) == 3
    await scheduling.cancel_weekly_briefing_schedule(client, "web-bob")
    assert (await _json(http, "GET", "/api/users/web-bob/weekly"))["exists"] is False


async def test_briefings_and_memory_are_read_only(http):
    for path in ("turns", "scheduled", "weekly", "weekly/trigger", "feedback", "consolidate", "retry-publication"):
        response = await http.post(f"/api/users/web-x/{path}", json={})
        assert response.status_code in (404, 405), path


async def test_session_approval_and_stream_resume(http, store):
    store.upsert_report("r1", "web-carol", "Weekly briefing", "# Weekly briefing")
    session = await _json(http, "POST", "/api/sessions", json={"user": "web-carol", "session_id": "web-carol-s1"})
    assert session["workflow_id"] == "session-web-carol-s1"

    accepted = await _json(
        http,
        "POST",
        "/api/sessions/web-carol-s1/messages",
        json={"text": "Share my report with dana@example.com", "client_message_id": "m1"},
    )
    assert accepted["disposition"] == "started"

    async def pending():
        status = await _json(http, "GET", "/api/sessions/web-carol-s1/status")
        return status["pending_approvals"][0] if status["pending_approvals"] else None

    approval = await eventually(pending)
    wrong = await http.post(
        "/api/sessions/web-carol-s1/approvals",
        json={"approval_id": approval["approval_id"], "digest": "nope", "approve": True},
    )
    assert wrong.status_code == 409 and "Digest" in wrong.json()["detail"]
    decided = await _json(
        http,
        "POST",
        "/api/sessions/web-carol-s1/approvals",
        json={"approval_id": approval["approval_id"], "digest": approval["digest"], "approve": True},
    )
    assert decided == {"status": "approved"}

    async def replied():
        status = await _json(http, "GET", "/api/sessions/web-carol-s1/status")
        return not status["turn_running"] and not status["pending_approvals"]

    await eventually(replied)
    await _json(http, "POST", "/api/sessions/web-carol-s1/close")

    # The stream ends once the session is closed and idle, so it can be read whole.
    full = _sse((await asyncio.wait_for(http.get("/api/sessions/web-carol-s1/events"), 15)).text)
    session_events = [(int(i), d) for i, e, d in full if e == "session"]
    kinds = [d["kind"] for _, d in session_events]
    assert kinds[:4] == ["session_started", "message_accepted", "turn_started", "approval_requested"]
    assert "approval_approved" in kinds and "assistant" in kinds and kinds[-1] == "session_closed"
    requested = next(d for _, d in session_events if d["kind"] == "approval_requested")
    assert requested["data"]["tool"] == "share_report"
    assert requested["data"]["input"]["recipient"] == "dana@example.com"

    # A reconnecting browser sends Last-Event-ID and gets only what it missed.
    resumed = _sse(
        (await asyncio.wait_for(http.get("/api/sessions/web-carol-s1/events", headers={"Last-Event-ID": "4"}), 15)).text
    )
    assert [int(i) for i, e, _ in resumed if e == "session"] == [s for s, _ in session_events if s > 4]

    shares = [m for m in await _json(http, "GET", "/api/users/web-carol/outbox") if m["channel"].startswith("share:")]
    assert len(shares) == 1


async def test_memory_view(http, client, store, settings):
    await agent_app.record_interaction(client, store, settings, "web-dave", "Lead with decisions.", batch_size=1)

    async def published():
        memory = await _json(http, "GET", "/api/users/web-dave/memory")
        return memory if memory["revision"] == 1 else None

    memory = await eventually(published)
    assert memory["notes"][0]["text"] == "User preference: Lead with decisions."
    assert memory["workflow"]["status"] == "COMPLETED"


async def test_stuck_publication_is_retried_from_its_alert(http, client, store, settings):
    store.set_fault("publish_memory", 100)
    store.record_interaction("web-gus", "Keep it short.", "ix-web-gus-1")
    handle = await client.start_workflow(
        MemoryConsolidationWorkflow.run,
        ConsolidationInput(user_id="web-gus", batch_size=1, max_publish_attempts=1),
        id=consolidation_workflow_id("web-gus"),
        task_queue=settings.background_queue,
        start_signal="interaction_recorded",
        start_signal_args=["ix-web-gus-1"],
    )

    async def retryable():
        alerts = await _json(http, "GET", "/api/alerts")
        return alerts[0] if alerts and alerts[0]["can_retry"] else None

    alert = await eventually(retryable)
    assert alert["kind"] == "memory_publication_failed"
    store.clear_faults()  # the operator fixes the database, then retries from the alert
    assert await _json(http, "POST", f"/api/alerts/{alert['id']}/retry-publication") == {"signaled": True}
    assert (await handle.result()).revisions_published == [1]
    assert (await _json(http, "GET", "/api/alerts"))[0]["can_retry"] is False


async def test_workspace_through_the_api(http):
    assert (await _json(http, "GET", "/api/pool"))["running"] is False
    await _json(http, "POST", "/api/pool", json={"target_ready": 1, "tick_seconds": 1})
    await _json(http, "POST", "/api/workspaces/web-erin-ws/open", json={"idle_seconds": 5})

    async def active():
        ws = await _json(http, "GET", "/api/workspaces/web-erin-ws")
        return ws if ws.get("stage") == "active" else None

    await eventually(active)
    await _json(http, "POST", "/api/workspaces/web-erin-ws/files", json={"name": "notes.md", "text": "hello"})
    ws = await _json(http, "GET", "/api/workspaces/web-erin-ws")
    assert ws["files"] == [{"name": "notes.md", "text": "hello"}]
    await _json(http, "POST", "/api/workspaces/web-erin-ws/finish")

    async def released():
        ws = await _json(http, "GET", "/api/workspaces/web-erin-ws")
        return ws if ws["stage"] == "released" else None

    assert (await eventually(released))["latest_snapshot"]


async def test_operations_and_validation(http):
    faults = await _json(http, "POST", "/api/faults", json={"point": "write_report", "count": 2, "mode": "error"})
    assert faults == [{"name": "write_report", "remaining": 2, "mode": "error"}]
    assert await _json(http, "DELETE", "/api/faults") == []
    assert isinstance(await _json(http, "GET", "/api/ops?limit=5"), list)

    assert (await http.get("/api/users/bad%20user/reports")).status_code == 400
    assert (await http.post("/api/faults", json={"point": "x", "mode": "explode"})).status_code == 400
    bad_file = await http.post("/api/workspaces/ws/files", json={"name": "../escape", "text": "x"})
    assert bad_file.status_code == 400
    assert (await http.get("/api/reports/does-not-exist")).status_code == 404
    assert (await http.post("/api/alerts/999/retry-publication")).status_code == 404
