"""Web frontend for the briefing assistant.

A small FastAPI server over the same helpers the CLI uses (``app.py``), plus
a static single-page UI. The server is stateless: everything it shows comes
from Temporal (Workflow queries, Activity and Schedule descriptions) or the
application's own store.

The Briefings and Memory views are read-only. You act through the assistant
(a session Workflow), which schedules briefings and feeds memory itself;
operator actions, such as retrying a stuck memory publication, live with the
alerts in Operations.

The session view streams events with Server-Sent Events. Each event's SSE id
is the session's event sequence number, so when a browser reconnects it sends
``Last-Event-ID`` and the stream resumes exactly where it left off. That is the
article's reconnection pattern: closing the page stops nothing in the session.

    uv run agent-web            # http://127.0.0.1:8000

This is a local development tool: it binds to localhost and has no auth.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import os
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowUpdateFailedError
from temporalio.service import RPCError, RPCStatusCode

from .. import app as agent_app
from .. import scheduling
from ..config import Settings, load_dotenv
from ..domain import PoolConfig, TurnOutcome
from ..store import AppStore
from ..worker import connect
from ..workflows.memory import PUBLICATION_FAILED, MemoryConsolidationWorkflow, batch_run, consolidation_workflow_id
from ..workflows.workspace import WorkspaceLeaseWorkflow, WorkspacePoolWorkflow, lease_workflow_id

STATIC_DIR = Path(__file__).parent / "static"
SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
SAFE_FILE = re.compile(r"^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}$")
STREAM_POLL_SECONDS = 0.5


class RevalidatingStaticFiles(StaticFiles):
    """Static files the browser re-checks on every load (cheap, thanks to ETags),
    so an edited page is never shadowed by a heuristically cached copy."""

    def file_response(self, *args: Any, **kwargs: Any) -> Any:
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "no-cache"
        return response


# -- request bodies ----------------------------------------------------------------


class SessionRequest(BaseModel):
    user: str
    session_id: str | None = None
    approval_timeout_seconds: int = Field(900, ge=10)


class MessageRequest(BaseModel):
    text: str = Field(min_length=1, max_length=4000)
    mode: str = "auto"
    client_message_id: str | None = None


class ApprovalRequest(BaseModel):
    approval_id: str
    digest: str
    approve: bool


class PoolRequest(BaseModel):
    target_ready: int = Field(2, ge=0, le=10)
    tick_seconds: int = Field(10, ge=1, le=600)


class OpenWorkspaceRequest(BaseModel):
    idle_seconds: int = Field(60, ge=5, le=86400)


class FileRequest(BaseModel):
    name: str
    text: str = Field(max_length=100_000)


class FaultRequest(BaseModel):
    point: str
    count: int = Field(1, ge=1, le=1000)
    mode: str = "error"


# -- helpers -----------------------------------------------------------------------------


def _check_id(value: str, what: str = "id") -> str:
    if not SAFE_ID.match(value):
        raise HTTPException(400, f"{what} must be 1-48 letters, digits, '-' or '_'")
    return value


def _plain(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    return value


def _iso(value: Any) -> str | None:
    return value.isoformat() if value else None


async def _not_found_ok(coro: Any) -> Any:
    try:
        return await coro
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            return None
        raise


# -- app ---------------------------------------------------------------------------------


def create_app(client: Client | None = None, settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    store = AppStore(settings.data_dir)
    holder: dict[str, Client | None] = {"client": client}

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if holder["client"] is None:
            holder["client"] = await connect()
        yield

    api = FastAPI(title="Briefing assistant", lifespan=lifespan)

    def tc() -> Client:
        assert holder["client"] is not None
        return holder["client"]

    @api.exception_handler(RPCError)
    async def _rpc_error(_: Request, err: RPCError) -> JSONResponse:
        code = 404 if err.status == RPCStatusCode.NOT_FOUND else 502
        return JSONResponse({"detail": err.message}, status_code=code)

    @api.exception_handler(WorkflowUpdateFailedError)
    async def _update_failed(_: Request, err: WorkflowUpdateFailedError) -> JSONResponse:
        cause = getattr(err.cause, "message", None) or str(err.cause or err)
        return JSONResponse({"detail": cause}, status_code=409)

    # -- config ------------------------------------------------------------------

    @api.get("/api/config")
    async def config() -> dict[str, Any]:
        return {
            "claude_model": settings.claude_model,
            "namespace": tc().namespace,
            "temporal_ui": os.environ.get("TEMPORAL_UI_URL", "http://localhost:8233").rstrip("/"),
            "queues": list(settings.all_queues),
            "pool_workflow_id": settings.pool_workflow_id,
        }

    # -- briefings (read-only) -----------------------------------------------------

    @api.get("/api/users/{user}/turns")
    async def turns(user: str) -> list[dict[str, Any]]:
        _check_id(user, "user")
        found = []
        async for wf in tc().list_workflows("WorkflowType = 'BriefingTurnWorkflow'", limit=200):
            if wf.id.startswith((f"turn-{user}-", f"briefing-run-{user}-")):
                found.append(
                    {"turn_id": wf.id, "status": wf.status.name if wf.status else None, "started": _iso(wf.start_time)}
                )
        return sorted(found, key=lambda t: t["started"] or "", reverse=True)[:15]

    @api.get("/api/users/{user}/scheduled")
    async def scheduled(user: str) -> list[dict[str, Any]]:
        _check_id(user, "user")
        items = []
        async for act in tc().list_activities("ActivityType = 'run_briefing_turn'", limit=200):
            if act.activity_id.startswith(f"briefing-{user}-"):
                items.append(act)
        items = sorted(items, key=lambda a: a.schedule_time or 0, reverse=True)[:10]

        async def describe(act: Any) -> dict[str, Any]:
            handle = tc().get_activity_handle(act.activity_id, result_type=TurnOutcome)
            d = await handle.describe()
            entry = {
                "activity_id": d.activity_id,
                "status": d.status.name,
                "run_state": d.run_state.name if d.run_state else None,
                "attempt": d.attempt,
                "scheduled": _iso(d.schedule_time),
                "start_delay_seconds": d.start_delay.total_seconds() if d.start_delay else 0,
                "closed": _iso(d.close_time),
            }
            if d.status.name == "COMPLETED":
                entry["result"] = _plain(await handle.result())
            return entry

        return list(await asyncio.gather(*(describe(a) for a in items)))

    @api.get("/api/users/{user}/weekly")
    async def weekly(user: str) -> dict[str, Any]:
        found = await scheduling.describe_weekly_briefing(tc(), _check_id(user, "user"))
        return {"exists": False} if found is None else {"exists": True, **found}

    # -- reports and outbox -------------------------------------------------------------

    @api.get("/api/users/{user}/reports")
    async def reports(user: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(store.reports, _check_id(user, "user"))

    # Report IDs from chat sessions contain a slash (``session-x/turn-3``).
    @api.get("/api/reports/{report_id:path}")
    async def report(report_id: str) -> dict[str, Any]:
        found = await asyncio.to_thread(store.get_report, report_id)
        if found is None:
            raise HTTPException(404, "no such report")
        return found

    @api.get("/api/users/{user}/outbox")
    async def outbox(user: str) -> list[dict[str, Any]]:
        messages = await asyncio.to_thread(store.outbox, _check_id(user, "user"))
        return messages[::-1][:30]

    # -- memory (read-only) --------------------------------------------------------

    @api.get("/api/users/{user}/memory")
    async def memory(user: str) -> dict[str, Any]:
        _check_id(user, "user")
        revision, notes = await asyncio.to_thread(store.current_memory, user)
        unprocessed = await asyncio.to_thread(store.unprocessed_count, user)
        handle = tc().get_workflow_handle(consolidation_workflow_id(user))
        desc = await _not_found_ok(handle.describe())
        workflow = None
        if desc is not None:
            workflow = {"status": desc.status.name, "run_id": desc.run_id}
            if desc.status == WorkflowExecutionStatus.RUNNING:
                workflow["state"] = await handle.query(MemoryConsolidationWorkflow.status)
        return {
            "revision": revision,
            "notes": [n.__dict__ for n in notes],
            "unprocessed": unprocessed,
            "workflow": workflow,
            "workflow_id": consolidation_workflow_id(user),
        }

    # -- sessions ----------------------------------------------------------------------

    @api.post("/api/sessions")
    async def start_session(body: SessionRequest) -> dict[str, str]:
        user = _check_id(body.user, "user")
        session_id = _check_id(body.session_id or f"{user}-{uuid.uuid4().hex[:6]}", "session_id")
        await agent_app.start_session(
            tc(), settings, session_id, user, approval_timeout=timedelta(seconds=body.approval_timeout_seconds)
        )
        return {"session_id": session_id, "workflow_id": f"session-{session_id}"}

    @api.get("/api/sessions/{session_id}/status")
    async def session_status(session_id: str) -> dict[str, Any]:
        return _plain(await agent_app.session_status(tc(), _check_id(session_id, "session_id")))

    @api.post("/api/sessions/{session_id}/messages")
    async def send_message(session_id: str, body: MessageRequest) -> dict[str, Any]:
        _check_id(session_id, "session_id")
        if body.mode not in ("auto", "queue"):
            raise HTTPException(400, "mode must be 'auto' or 'queue'")
        message_id = body.client_message_id or uuid.uuid4().hex[:12]
        accepted = await agent_app.send_session_message(tc(), session_id, message_id, body.text, body.mode)
        return _plain(accepted)

    @api.post("/api/sessions/{session_id}/approvals")
    async def decide(session_id: str, body: ApprovalRequest) -> dict[str, str]:
        _check_id(session_id, "session_id")
        status = await agent_app.decide_approval(tc(), session_id, body.approval_id, body.digest, body.approve)
        return {"status": status}

    @api.post("/api/sessions/{session_id}/close")
    async def close_session(session_id: str) -> dict[str, bool]:
        await agent_app.session_handle(tc(), _check_id(session_id, "session_id")).signal("close")
        return {"closed": True}

    @api.get("/api/sessions/{session_id}/events")
    async def session_events(session_id: str, request: Request, since: int = 0) -> StreamingResponse:
        _check_id(session_id, "session_id")
        last_seen = request.headers.get("last-event-id")
        after = int(last_seen) if last_seen and last_seen.isdigit() else since

        async def stream() -> AsyncIterator[str]:
            cursor = after
            last_status: str | None = None
            yield "retry: 1500\n\n"
            while not await request.is_disconnected():
                try:
                    events = await agent_app.session_events(tc(), session_id, cursor)
                    status = _plain(await agent_app.session_status(tc(), session_id))
                except RPCError as err:
                    if err.status == RPCStatusCode.NOT_FOUND:
                        yield f"event: gone\ndata: {json.dumps({'detail': err.message})}\n\n"
                        return
                    # Usually no worker is up to answer the query. The session is
                    # fine; tell the page and keep trying.
                    yield f"event: unavailable\ndata: {json.dumps({'detail': err.message})}\n\n"
                    last_status = None  # re-send status once queries work again
                    await asyncio.sleep(2)
                    continue
                for event in events:
                    cursor = event.seq
                    yield f"id: {event.seq}\nevent: session\ndata: {json.dumps(_plain(event))}\n\n"
                encoded = json.dumps(status, sort_keys=True)
                if encoded != last_status:
                    last_status = encoded
                    yield f"event: status\ndata: {encoded}\n\n"
                elif not events:
                    yield ": keep-alive\n\n"
                if status["closed"] and not status["turn_running"] and not events:
                    return
                await asyncio.sleep(STREAM_POLL_SECONDS)

        return StreamingResponse(
            stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        )

    # -- workspaces -------------------------------------------------------------------

    @api.get("/api/pool")
    async def pool() -> dict[str, Any]:
        handle = tc().get_workflow_handle(settings.pool_workflow_id)
        desc = await _not_found_ok(handle.describe())
        if desc is None or desc.status != WorkflowExecutionStatus.RUNNING:
            return {"running": False}
        status = _plain(await handle.query(WorkspacePoolWorkflow.status))
        return {"running": True, "run_id": desc.run_id, **status}

    @api.post("/api/pool")
    async def start_pool(body: PoolRequest) -> dict[str, Any]:
        config = PoolConfig(target_ready=body.target_ready, tick_seconds=body.tick_seconds)
        await agent_app.ensure_pool(tc(), settings, config)
        return {"started": True}

    @api.get("/api/workspaces/{workspace_id}")
    async def workspace(workspace_id: str) -> dict[str, Any]:
        _check_id(workspace_id, "workspace_id")
        handle = tc().get_workflow_handle(lease_workflow_id(workspace_id))
        desc = await _not_found_ok(handle.describe())
        snapshot = await asyncio.to_thread(store.latest_snapshot, workspace_id)
        if desc is None:
            return {"exists": False, "latest_snapshot": snapshot}
        info = await handle.query(WorkspaceLeaseWorkflow.info)
        files: list[dict[str, Any]] = []
        if info.get("vm_id") and info["stage"] == "active":
            root = store.vm_dir(info["vm_id"])
            files = [
                {"name": p.name, "text": p.read_text(errors="replace")[:4000]}
                for p in sorted(root.glob("*"))
                if p.is_file()
            ]
        return {
            "exists": True,
            "status": desc.status.name,
            "stage": info["stage"],
            "vm_id": info.get("vm_id"),
            "files": files,
            "latest_snapshot": snapshot,
            "workflow_id": lease_workflow_id(workspace_id),
        }

    @api.post("/api/workspaces/{workspace_id}/open")
    async def open_workspace(workspace_id: str, body: OpenWorkspaceRequest) -> dict[str, bool]:
        _check_id(workspace_id, "workspace_id")
        await agent_app.open_workspace(tc(), settings, workspace_id, idle_timeout=timedelta(seconds=body.idle_seconds))
        return {"opened": True}

    @api.post("/api/workspaces/{workspace_id}/files")
    async def write_file(workspace_id: str, body: FileRequest) -> dict[str, str]:
        _check_id(workspace_id, "workspace_id")
        if not SAFE_FILE.match(body.name):
            raise HTTPException(400, "file name must be a plain name like notes.md")
        try:
            await agent_app.write_workspace_file(tc(), store, workspace_id, body.name, body.text)
        except RuntimeError as err:
            raise HTTPException(409, str(err)) from err
        return {"written": body.name}

    @api.post("/api/workspaces/{workspace_id}/finish")
    async def finish_workspace(workspace_id: str) -> dict[str, bool]:
        _check_id(workspace_id, "workspace_id")
        await tc().get_workflow_handle(lease_workflow_id(workspace_id)).signal(WorkspaceLeaseWorkflow.finish)
        return {"finishing": True}

    # -- operations ----------------------------------------------------------------------

    @api.get("/api/ops")
    async def ops(limit: int = 80) -> list[dict[str, Any]]:
        return await asyncio.to_thread(store.ops, None, None, min(max(limit, 1), 500))

    @api.get("/api/faults")
    async def faults() -> list[dict[str, Any]]:
        return await asyncio.to_thread(store.faults)

    @api.post("/api/faults")
    async def set_fault(body: FaultRequest) -> list[dict[str, Any]]:
        if not SAFE_FILE.match(body.point) or body.mode not in ("error", "crash"):
            raise HTTPException(400, "bad fault point or mode")
        await asyncio.to_thread(store.set_fault, body.point, body.count, body.mode)
        return await faults()

    @api.delete("/api/faults")
    async def clear_faults() -> list[dict[str, Any]]:
        await asyncio.to_thread(store.clear_faults)
        return []

    async def _waiting_for_operator(workflow_id: str, run_id: str) -> bool:
        handle = tc().get_workflow_handle(workflow_id)
        desc = await _not_found_ok(handle.describe())
        if desc is None or desc.status != WorkflowExecutionStatus.RUNNING or desc.run_id != run_id:
            return False
        state = await handle.query(MemoryConsolidationWorkflow.status)
        return state["status"] == "waiting_for_operator"

    @api.get("/api/alerts")
    async def alerts() -> list[dict[str, Any]]:
        found = (await asyncio.to_thread(store.alerts))[::-1]
        # A failed memory publication can be retried from its alert while the
        # run that raised it is still waiting for an operator.
        runs = list({batch_run(a["subject"]) for a in found if a["kind"] == PUBLICATION_FAILED})
        waiting = dict(zip(runs, await asyncio.gather(*(_waiting_for_operator(*r) for r in runs)), strict=True))
        for alert in found:
            alert["can_retry"] = alert["kind"] == PUBLICATION_FAILED and waiting[batch_run(alert["subject"])]
        return found

    @api.post("/api/alerts/{alert_id}/retry-publication")
    async def retry_publication(alert_id: int) -> dict[str, bool]:
        """The operator fixed the dependency; tell the waiting Workflow to publish again."""
        alert = next((a for a in await asyncio.to_thread(store.alerts) if a["id"] == alert_id), None)
        if alert is None or alert["kind"] != PUBLICATION_FAILED:
            raise HTTPException(404, "no such memory publication alert")
        workflow_id, _ = batch_run(alert["subject"])
        await tc().get_workflow_handle(workflow_id).signal(MemoryConsolidationWorkflow.retry_publication)
        return {"signaled": True}

    api.mount("/", RevalidatingStaticFiles(directory=STATIC_DIR, html=True), name="static")
    return api


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Web frontend for the briefing assistant")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    import uvicorn

    uvicorn.run(create_app(), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
