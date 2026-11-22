"""Shared test setup: a real Temporal dev server, real workers, a fake model.

The tests run the same Workflows and Activities as the app, against a local
Temporal server, so what they exercise is real durable execution: timers,
retries, signals, Updates, Continue-As-New. Only the model is replaced, by
``fake_model.FakeModel``, so runs are free and repeatable.

Two helpers carry most of the recovery tests:

* ``running_workers`` is a context manager. Leaving the block stops the
  workers, and entering a new one starts fresh ones: a worker restart in
  two lines. Whatever was in flight resumes in the new workers.
* ``assert_replays`` replays a finished Workflow's history against the
  current code. If the code would make different decisions on replay (the
  non-determinism Temporal forbids), it raises.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import shutil
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from temporalio.client import Client, WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer

from always_on_agents.config import Settings
from always_on_agents.llm import ModelClient
from always_on_agents.store import AppStore
from always_on_agents.worker import WORKFLOWS, build_workers

from .fake_model import FakeModel


@pytest.fixture(scope="session")
async def env() -> AsyncIterator[WorkflowEnvironment]:
    # Reuse the installed Temporal CLI if present; otherwise the SDK downloads one.
    cli = shutil.which("temporal")
    environment = await (
        WorkflowEnvironment.start_local(dev_server_existing_path=cli) if cli else WorkflowEnvironment.start_local()
    )
    yield environment
    await environment.shutdown()


@pytest.fixture
def client(env: WorkflowEnvironment) -> Client:
    return env.client


@pytest.fixture
def settings(tmp_path) -> Settings:
    # agent(invariant): tests share one dev server. Unique queues and pool id per test keep one
    # test's worker from running another test's tasks.
    suffix = uuid.uuid4().hex[:8]
    return Settings(
        interactive_queue=f"interactive-{suffix}",
        scheduled_queue=f"scheduled-{suffix}",
        background_queue=f"background-{suffix}",
        pool_workflow_id=f"pool-{suffix}",
        data_dir=tmp_path,
    )


@pytest.fixture
def store(settings: Settings) -> AppStore:
    return AppStore(settings.data_dir)


@contextlib.asynccontextmanager
async def running_workers(client: Client, settings: Settings, model: ModelClient | None = None) -> AsyncIterator[None]:
    """Run workers for all queues; leaving the block stops them (a 'restart').

    Tests use a deterministic test double instead of Claude, so they are free
    and repeatable. The application itself always runs on Claude.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        async with contextlib.AsyncExitStack() as stack:
            for worker in build_workers(client, settings, settings.all_queues, executor, model=model or FakeModel()):
                await stack.enter_async_context(worker)
            yield


@pytest.fixture
async def workers(client: Client, settings: Settings) -> AsyncIterator[None]:
    async with running_workers(client, settings):
        yield


async def eventually(check: Callable[[], Awaitable[Any]], timeout: float = 20.0, interval: float = 0.2) -> Any:
    """Poll until ``check`` returns a truthy value."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            value = await check()
            if value:
                return value
        except Exception:
            if time.monotonic() > deadline:
                raise
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(interval)


async def assert_replays(handle: WorkflowHandle) -> None:
    """Replay the recorded history against current code: catches non-determinism."""
    await Replayer(workflows=WORKFLOWS).replay_workflow(await handle.fetch_history())
