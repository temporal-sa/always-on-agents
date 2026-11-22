"""Background heartbeating for Activities that wait on slow calls.

A model call can take minutes, so a start-to-close timeout has to be long. On
its own, that means a worker that dies mid-call is noticed only when the
timeout expires. Heartbeating every few seconds with a short heartbeat timeout
lets Temporal notice a dead worker within seconds and retry elsewhere.

The heartbeat runs as a background task because the slow part is one awaited
call: there is no loop in which to heartbeat between steps. The details
(for example, which model call is in progress) show up in the Temporal UI
under the pending Activity.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from temporalio import activity

# agent(constraint): heartbeats only matter when the caller sets a heartbeat_timeout, and
# this interval must stay well under it (30 s everywhere here). Set one when you add a caller.
HEARTBEAT_EVERY_SECONDS = 5.0


@asynccontextmanager
async def heartbeating(details: Callable[[], str] = lambda: "") -> AsyncIterator[None]:
    async def beat() -> None:
        while True:
            activity.heartbeat(details())
            await asyncio.sleep(HEARTBEAT_EVERY_SECONDS)

    task = asyncio.create_task(beat())
    try:
        yield
    finally:
        task.cancel()
