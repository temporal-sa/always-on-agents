"""Command line for demos and recovery drills.

uv run agent --help
uv run agent <command> --help
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio.client import Client

from . import app, scheduling
from .config import Settings, load_dotenv
from .domain import PoolConfig, TurnOutcome
from .store import AppStore
from .worker import connect
from .workflows.memory import MemoryConsolidationWorkflow, consolidation_workflow_id
from .workflows.workspace import WorkspaceLeaseWorkflow, WorkspacePoolWorkflow, lease_workflow_id


def duration(text: str) -> timedelta:
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", text.strip())
    if not match:
        raise argparse.ArgumentTypeError("use a duration like 30s, 5m, 2h, or 1d")
    units = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
    return timedelta(**{units[match.group(2)]: float(match.group(1))})


def show(value: Any) -> None:
    if hasattr(value, "__dataclass_fields__"):
        value = value.__dict__
    print(json.dumps(value, indent=2, default=lambda o: getattr(o, "__dict__", str(o))))


# -- commands -------------------------------------------------------------------


async def cmd_schedule(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    if a.at:
        due_at = datetime.fromisoformat(a.at)
        due_at = due_at if due_at.tzinfo else due_at.replace(tzinfo=UTC)
        ok = await scheduling.schedule_briefing_at(c, a.user, due_at, task_queue=s.scheduled_queue)
        activity_id = f"briefing-{a.user}-{due_at.isoformat(timespec='minutes')}"
    else:
        delay = a.delay or timedelta(0)
        due = a.due or (datetime.now(UTC) + delay).strftime("%Y-%m-%dT%H%M%S")
        ok = await scheduling.schedule_briefing(c, a.user, due, delay, task_queue=s.scheduled_queue)
        activity_id = f"briefing-{a.user}-{due}"
    print(("submitted " if ok else "already submitted: ") + activity_id)
    print(f"watch it:  uv run agent job {activity_id}")


async def cmd_inbox(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    ok = await scheduling.handle_inbox_event(c, a.user, a.event_id, task_queue=s.scheduled_queue)
    print(("submitted " if ok else "already submitted: ") + f"briefing-{a.user}-event-{a.event_id}")


async def cmd_job(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    handle = c.get_activity_handle(a.activity_id, result_type=TurnOutcome)
    d = await handle.describe()
    print(f"{d.activity_id}: {d.status.name} ({d.run_state.name if d.run_state else '-'}), attempt {d.attempt}")
    print(f"  scheduled {d.schedule_time}, start delay {d.start_delay}, task queue {d.task_queue}")
    if a.wait:
        show(await handle.result())


async def cmd_turn(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    turn_id = a.id or f"turn-{a.user}-{uuid.uuid4().hex[:6]}"
    handle = await app.start_turn(c, s, a.user, turn_id, deliver=not a.no_deliver)
    print(f"started {turn_id}; waiting for the result (Ctrl-C does not stop the turn)")
    show(await handle.result())


async def cmd_weekly(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    if a.cancel:
        removed = await scheduling.cancel_weekly_briefing_schedule(c, a.user)
        print("weekly briefing cancelled" if removed else "no weekly briefing to cancel")
        return
    schedule_id = await scheduling.set_weekly_briefing_schedule(
        c, a.user, task_queue=s.scheduled_queue, day=a.day, at=a.at, time_zone=a.tz
    )
    show(await scheduling.describe_weekly_briefing(c, a.user))
    if a.trigger:
        await c.get_schedule_handle(schedule_id).trigger()
        print("triggered one run now")


async def cmd_feedback(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    interaction_id = await app.record_interaction(
        c, st, s, a.user, a.text, batch_size=a.batch_size, max_wait=a.max_wait
    )
    print(f"recorded {interaction_id}; notified {consolidation_workflow_id(a.user)}")


async def cmd_consolidate(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    if await app.consolidate_now(c, s, a.user):
        print("started a consolidation run")
    else:
        print("rejected: a consolidation run is already in progress for this user (Fail conflict policy)")


async def cmd_nightly(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    print(await scheduling.ensure_nightly_memory_schedule(c, a.user, task_queue=s.background_queue, cron=a.cron))


async def cmd_memory(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    revision, notes = st.current_memory(a.user)
    print(f"memory revision {revision}; {st.unprocessed_count(a.user)} unprocessed interactions")
    for note in notes:
        print(f"  - {note.text}  (sources: {', '.join(note.sources)})")
    try:
        status = await c.get_workflow_handle(consolidation_workflow_id(a.user)).query(
            MemoryConsolidationWorkflow.status
        )
        print("consolidation workflow:", json.dumps(status))
    except Exception:
        print("consolidation workflow: not running")


async def cmd_retry_publication(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    await app.retry_memory_publication(c, a.user)
    print("signaled retry_publication")


async def cmd_pool(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    if a.action == "start":
        config = PoolConfig(
            target_ready=a.target, max_vms=a.max_vms, max_creates_per_window=a.rate, tick_seconds=a.tick
        )
        await app.ensure_pool(c, s, config)
        print(f"pool controller {s.pool_workflow_id} running")
    show(await c.get_workflow_handle(s.pool_workflow_id).query(WorkspacePoolWorkflow.status))


async def cmd_workspace(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    handle = c.get_workflow_handle(lease_workflow_id(a.workspace))
    if a.action == "open":
        await app.open_workspace(c, s, a.workspace, idle_timeout=a.idle)
        print(f"lease {lease_workflow_id(a.workspace)} started (saves and releases after {a.idle} idle)")
    elif a.action == "write":
        path = await app.write_workspace_file(c, st, a.workspace, a.name, a.text)
        print(f"wrote {path}")
    elif a.action == "finish":
        await handle.signal(WorkspaceLeaseWorkflow.finish)
        print("asked the lease to save and release now")
    show(await handle.query(WorkspaceLeaseWorkflow.info))


async def cmd_vm(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    if a.action == "leak":
        vm_id, _ = st.provision_vm(f"leaked-{uuid.uuid4().hex[:6]}")
        print(f"created {vm_id} outside the pool; the controller should destroy it on its next tick")
    elif a.action == "break":
        st.mark_vm_unhealthy(a.vm_id)
        print(f"{a.vm_id} will fail its next health check")
    print("running VMs:", ", ".join(st.running_vms()) or "none")


async def cmd_session(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    if a.action == "start":
        await app.start_session(c, s, a.session, a.user, approval_timeout=a.approval_timeout)
        print(f"session-{a.session} running")
    elif a.action == "send":
        accepted = await app.send_session_message(c, a.session, a.id or uuid.uuid4().hex[:8], a.text, a.mode)
        print(f"accepted {accepted.message_id}: {accepted.disposition}")
    elif a.action == "follow":
        await _follow(c, a.session, a.since, a.once)
    elif a.action == "approve":
        await _approve(c, a.session, a.approval_id, approve=not a.deny)
    elif a.action == "close":
        await app.session_handle(c, a.session).signal("close")
        print("close requested")


async def _follow(c: Client, session_id: str, since: int, once: bool) -> None:
    print(f"following session-{session_id} from event {since} (Ctrl-C disconnects; the session keeps going)")
    while True:
        for event in await app.session_events(c, session_id, since):
            extra = f"  {json.dumps(event.data)}" if event.data else ""
            print(f"[{event.seq:>3}] {event.kind:<18} {event.text}{extra}")
            since = event.seq
        status = await app.session_status(c, session_id)
        if once or status.closed:
            print(f"(resume with --since {since})")
            return
        await asyncio.sleep(0.5)


async def _approve(c: Client, session_id: str, approval_id: str | None, approve: bool) -> None:
    pending = (await app.session_status(c, session_id)).pending_approvals
    if approval_id:
        pending = [p for p in pending if p.approval_id == approval_id]
    if len(pending) != 1:
        print("pending approvals:", [p.approval_id for p in pending] or "none")
        return
    p = pending[0]
    print(
        f"{'approving' if approve else 'denying'} {p.tool} {json.dumps(p.input)} (digest {p.digest}, expires {p.expires_at})"
    )
    print("result:", await app.decide_approval(c, session_id, p.approval_id, p.digest, approve))


async def cmd_fault(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    if a.clear:
        st.clear_faults()
    elif a.point:
        st.set_fault(a.point, a.count, a.mode)
    show(st.faults())


async def cmd_ops(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    for op in st.ops(a.op, a.key):
        print(f"{op['at']}  {op['op']:<24} attempt {op['attempt']}  {op['key']}  {op['detail']}")


async def cmd_outbox(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    for m in st.outbox(a.user):
        print(f"--- {m['message_id']} -> {m['user_id']} via {m['channel']} at {m['created_at']}\n{m['body']}\n")


async def cmd_report(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    report = st.get_report(a.report_id) if a.report_id else st.latest_report(a.user)
    print(report["body"] if report else "no report")


async def cmd_alerts(c: Client, s: Settings, st: AppStore, a: argparse.Namespace) -> None:
    for alert in st.alerts():
        print(f"{alert['created_at']}  {alert['kind']}  {alert['subject']}\n  {alert['detail']}")


# -- parser -----------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="agent", description="Briefing assistant: Temporal reference implementation")
    sub = p.add_subparsers(dest="command", required=True)

    def cmd(name: str, fn: Any, help: str) -> argparse.ArgumentParser:
        sp = sub.add_parser(name, help=help, description=help)
        sp.set_defaults(fn=fn)
        return sp

    sp = cmd("schedule", cmd_schedule, "Submit a briefing turn as a Standalone Activity, now or after a delay.")
    sp.add_argument("user")
    when = sp.add_mutually_exclusive_group()
    when.add_argument("--in", dest="delay", type=duration, help="start delay, e.g. 30s or 2h")
    when.add_argument("--at", help="wall-clock time, e.g. 2026-11-23T07:00:00+00:00")
    sp.add_argument("--due", help="label that names this briefing (becomes part of the Activity ID)")

    sp = cmd("inbox", cmd_inbox, "Handle an inbox event that needs a turn now (same path, zero delay).")
    sp.add_argument("user")
    sp.add_argument("event_id")

    sp = cmd("job", cmd_job, "Describe a scheduled Standalone Activity.")
    sp.add_argument("activity_id")
    sp.add_argument("--wait", action="store_true", help="wait for and print the result")

    sp = cmd("turn", cmd_turn, "Run an interactive briefing turn as a Workflow (fine-grained recovery).")
    sp.add_argument("user")
    sp.add_argument("--id", help="Workflow ID for the turn")
    sp.add_argument("--no-deliver", action="store_true")

    sp = cmd("weekly", cmd_weekly, "Set (or move) the user's weekly briefing, a Temporal Schedule.")
    sp.add_argument("user")
    sp.add_argument("--day", default="mon", help="mon, tue, ... sun")
    sp.add_argument("--at", default="07:00", help="24-hour time, HH:MM")
    sp.add_argument("--tz", default="UTC", help="IANA time zone, e.g. America/New_York")
    sp.add_argument("--trigger", action="store_true", help="also run once now")
    sp.add_argument("--cancel", action="store_true", help="remove the weekly briefing instead")

    sp = cmd("feedback", cmd_feedback, "Record an interaction and notify the user's memory consolidation Workflow.")
    sp.add_argument("user")
    sp.add_argument("text")
    sp.add_argument("--batch-size", type=int, default=5)
    sp.add_argument("--max-wait", type=duration, default=timedelta(hours=1))

    sp = cmd("consolidate", cmd_consolidate, "Start consolidation now (rejected if one is running).")
    sp.add_argument("user")

    sp = cmd("nightly", cmd_nightly, "Create a nightly consolidation Schedule (Skip overlap policy).")
    sp.add_argument("user")
    sp.add_argument("--cron", default="0 3 * * *")

    sp = cmd("memory", cmd_memory, "Show the user's current memory revision and consolidation status.")
    sp.add_argument("user")

    sp = cmd("retry-publication", cmd_retry_publication, "Operator: tell a stuck consolidation to retry publishing.")
    sp.add_argument("user")

    sp = cmd("pool", cmd_pool, "Start or inspect the workspace hot-pool controller.")
    sp.add_argument("action", choices=["start", "status"])
    sp.add_argument("--target", type=int, default=2, help="VMs to keep ready")
    sp.add_argument("--max-vms", type=int, default=6)
    sp.add_argument("--rate", type=int, default=3, help="max VM creates per minute")
    sp.add_argument("--tick", type=int, default=5, help="seconds between reconcile passes")

    sp = cmd("workspace", cmd_workspace, "Open, use, finish, or inspect a workspace lease.")
    sp.add_argument("action", choices=["open", "write", "finish", "status"])
    sp.add_argument("workspace")
    sp.add_argument("name", nargs="?", default="notes.md")
    sp.add_argument("text", nargs="?", default="")
    sp.add_argument("--idle", type=duration, default=timedelta(seconds=30), help="idle time before save-and-release")

    sp = cmd("vm", cmd_vm, "Drills against the fake compute provider.")
    sp.add_argument("action", choices=["leak", "break", "list"])
    sp.add_argument("vm_id", nargs="?")

    sp = cmd("session", cmd_session, "Session with approvals: start, send, follow, approve, close.")
    sp.add_argument("action", choices=["start", "send", "follow", "approve", "close"])
    sp.add_argument("session")
    sp.add_argument("arg", nargs="?", help="start: user id; send: message text")
    sp.add_argument("--mode", default="auto", choices=["auto", "queue"])
    sp.add_argument("--id", help="client message id (makes a resend idempotent)")
    sp.add_argument("--since", type=int, default=0, help="follow: resume after this event number")
    sp.add_argument("--once", action="store_true", help="follow: print and exit")
    sp.add_argument("--approval-id")
    sp.add_argument("--deny", action="store_true")
    sp.add_argument("--approval-timeout", type=duration, default=timedelta(minutes=15))

    sp = cmd("fault", cmd_fault, "Inject faults: make the next COUNT calls at POINT fail or crash the worker.")
    sp.add_argument("point", nargs="?", help="model_call, after_model_call, write_report, deliver, publish_memory, ...")
    sp.add_argument("count", nargs="?", type=int, default=1)
    sp.add_argument("--mode", choices=["error", "crash"], default="error")
    sp.add_argument("--clear", action="store_true")

    sp = cmd("ops", cmd_ops, "Show the operation log (what ran, which attempt).")
    sp.add_argument("--op")
    sp.add_argument("--key", help="key prefix, e.g. a turn id")

    sp = cmd("outbox", cmd_outbox, "Show delivered messages.")
    sp.add_argument("user", nargs="?")

    sp = cmd("report", cmd_report, "Print a report.")
    sp.add_argument("--user")
    sp.add_argument("--id", dest="report_id")

    cmd("alerts", cmd_alerts, "Show operator alerts.")
    return p


def main() -> None:
    load_dotenv()
    args = build_parser().parse_args()
    if args.command == "session":
        if args.action == "start":
            args.user = args.arg
        args.text = args.arg
    settings = Settings.from_env()
    store = AppStore(settings.data_dir)

    async def run() -> None:
        client = await connect()
        await args.fn(client, settings, store, args)

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\ndisconnected")


if __name__ == "__main__":
    main()
