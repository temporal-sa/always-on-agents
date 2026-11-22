"""Submit work the same way, now or later.

``schedule_briefing`` is the helper from the article. The application submits
the job, its arguments, and a start delay; Temporal retains it durably through
the wait, even if the submitting process exits, and then dispatches it to a
worker. A delay of zero makes it available immediately, so the inbox handler
and the agent's timer tool share one submission path and the application needs
no timer table or due-work poller.

For recurring work, a Temporal Schedule starts a Workflow on a calendar. The
assistant manages the user's weekly briefing Schedule from chat.

Which to use: a Standalone Activity for one job at one time (a briefing
"in an hour", a reminder), and a Schedule for "every Monday at 8". Both
replace things an application would otherwise build itself: a table of due
jobs, a poller, and the recovery logic for when the poller was down.

See it happen: recovery drill 1 in the README (the worker is stopped through
the due time, and the job still runs once).
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleCalendarSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleUpdate,
)
from temporalio.common import ActivityIDReusePolicy, RetryPolicy
from temporalio.exceptions import ActivityAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from .activities.agent import AgentActivities
from .domain import BriefingTurnInput, ConsolidationInput, WakeUp
from .workflows.briefing import BriefingTurnWorkflow
from .workflows.memory import MemoryConsolidationWorkflow

run_briefing_turn = AgentActivities.run_briefing_turn
wake_session = AgentActivities.wake_session


async def schedule_briefing(
    client: Client,
    user_id: str,
    due: str,
    delay: timedelta = timedelta(0),
    *,
    task_queue: str = "agent-scheduled",
) -> bool:
    """Submit the briefing turn for ``due``. Returns False if it is already submitted.

    The ID names one particular briefing. While it is pending or running,
    another submission with the same ID is rejected, so a retried inbox event
    or a double-clicked button cannot start a second run. After it completes,
    ALLOW_DUPLICATE_FAILED_ONLY still rejects a resubmission, while a failed
    briefing can be submitted again.
    """
    try:
        await client.start_activity(
            run_briefing_turn,  # starts the next agent turn
            args=[user_id],
            id=f"briefing-{user_id}-{due}",
            task_queue=task_queue,
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(seconds=30),  # notice a dead worker quickly
            start_delay=delay,
            id_reuse_policy=ActivityIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
            # agent(trade-off): each attempt repeats the whole turn, model calls included,
            # so attempts are capped here, unlike the fine-grained loop's per-call retries.
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=10), maximum_attempts=5),
        )
    except ActivityAlreadyStartedError:
        return False
    return True


async def schedule_briefing_at(
    client: Client, user_id: str, due_at: datetime, *, task_queue: str = "agent-scheduled"
) -> bool:
    """Schedule for a wall-clock time (for example, next Monday 07:00)."""
    delay = max(timedelta(0), due_at - datetime.now(due_at.tzinfo))
    return await schedule_briefing(client, user_id, due_at.isoformat(timespec="minutes"), delay, task_queue=task_queue)


async def schedule_wakeup(client: Client, wake: WakeUp, delay: timedelta, *, task_queue: str) -> bool:
    """The agent's timer tool: the same submission path, with a different job.

    When the delay elapses, a worker runs ``wake_session``, which wakes the
    conversation that set the timer. Returns False if this timer was already
    submitted (a retried tool call).
    """
    try:
        await client.start_activity(
            wake_session,
            args=[wake],
            id=f"timer-{wake.timer_id}",
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=30),
            start_delay=delay,
            retry_policy=RetryPolicy(maximum_interval=timedelta(minutes=1)),
        )
    except ActivityAlreadyStartedError:
        return False
    return True


async def handle_inbox_event(
    client: Client, user_id: str, event_id: str, *, task_queue: str = "agent-scheduled"
) -> bool:
    """An inbox event that needs an agent turn now: same helper, no delay."""
    return await schedule_briefing(client, user_id, due=f"event-{event_id}", task_queue=task_queue)


# agent(constraint): Temporal numbers the days of the week from Sunday = 0.
WEEKDAYS = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")
DAY_NAMES = dict(
    zip(WEEKDAYS, ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"), strict=True)
)
TIME_OF_DAY = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")


def weekly_briefing_schedule_id(user_id: str) -> str:
    return f"weekly-briefing-{user_id}"


async def set_weekly_briefing_schedule(
    client: Client,
    user_id: str,
    *,
    task_queue: str,
    day: str = "mon",
    at: str = "07:00",
    time_zone: str = "UTC",
) -> str:
    """Create the user's weekly briefing Schedule, or move it to a new day and time.

    A Schedule owns the recurrence; each run is its own Workflow, and delivery
    is a separate Activity with its own retries. Setting the same time twice
    changes nothing, so a retried call is safe. Raises ``ValueError`` for a day,
    time, or time zone it can't use.
    """
    day = day.strip().lower()[:3]
    if day not in WEEKDAYS:
        raise ValueError(f"day must be one of {', '.join(WEEKDAYS)}")
    match = TIME_OF_DAY.match(at.strip())
    if not match:
        raise ValueError("time must be 24-hour HH:MM, for example 07:30")
    hour, minute = int(match.group(1)), int(match.group(2))
    try:
        ZoneInfo(time_zone)
    except (ZoneInfoNotFoundError, ValueError) as err:
        raise ValueError(f"unknown time zone {time_zone!r}; use an IANA name like America/New_York") from err

    schedule_id = weekly_briefing_schedule_id(user_id)
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            BriefingTurnWorkflow.run,
            BriefingTurnInput(user_id=user_id),
            id=f"briefing-run-{user_id}",  # Temporal appends the scheduled time
            task_queue=task_queue,
        ),
        spec=ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    day_of_week=[ScheduleRange(WEEKDAYS.index(day))],
                    hour=[ScheduleRange(hour)],
                    minute=[ScheduleRange(minute)],
                    # agent(constraint): describe_weekly_briefing reads this comment back. The
                    # server rewrites cron_expressions into calendars and drops the cron text,
                    # so don't switch to cron here.
                    comment=f"{DAY_NAMES[day]}s at {hour:02d}:{minute:02d}",
                )
            ],
            time_zone_name=time_zone,
        ),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )
    try:
        await client.create_schedule(schedule_id, schedule)
    except ScheduleAlreadyRunningError:
        await client.get_schedule_handle(schedule_id).update(lambda _: ScheduleUpdate(schedule=schedule))
    return schedule_id


async def describe_weekly_briefing(client: Client, user_id: str) -> dict[str, Any] | None:
    """When the user's weekly briefing runs, or None if there isn't one."""
    try:
        desc = await client.get_schedule_handle(weekly_briefing_schedule_id(user_id)).describe()
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            return None
        raise
    spec = desc.schedule.spec
    return {
        "schedule_id": desc.id,
        "when": "; ".join(c.comment for c in spec.calendars if c.comment) or None,
        "time_zone": spec.time_zone_name or "UTC",
        "next_runs": [t.isoformat() for t in desc.info.next_action_times[:3]],
        "runs_so_far": desc.info.num_actions,
        "paused": desc.schedule.state.paused,
    }


async def cancel_weekly_briefing_schedule(client: Client, user_id: str) -> bool:
    """Delete the user's weekly briefing Schedule. Returns False if there was none.
    Past runs, reports, and deliveries are kept."""
    try:
        await client.get_schedule_handle(weekly_briefing_schedule_id(user_id)).delete()
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            return False
        raise
    return True


async def ensure_nightly_memory_schedule(
    client: Client, user_id: str, *, task_queue: str, cron: str = "0 3 * * *", time_zone: str = "UTC"
) -> str:
    """Periodic consolidation. The default Skip overlap policy skips a run while
    the previous run from this Schedule is still in progress."""
    schedule_id = f"nightly-memory-{user_id}"
    try:
        await client.create_schedule(
            schedule_id,
            Schedule(
                action=ScheduleActionStartWorkflow(
                    MemoryConsolidationWorkflow.run,
                    ConsolidationInput(user_id=user_id, flush_now=True),
                    # agent(trade-off): a Schedule appends the run time to this id, so nightly runs
                    # can't share the per-user consolidation id and may overlap it. That is safe:
                    # store.publish_batch raises StaleBatchError for the later publish.
                    id=f"memory-nightly-{user_id}",
                    task_queue=task_queue,
                ),
                spec=ScheduleSpec(cron_expressions=[cron], time_zone_name=time_zone),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        )
    except ScheduleAlreadyRunningError:
        pass
    return schedule_id
