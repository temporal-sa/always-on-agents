"""Worker process: polls the interactive, scheduled, and background task queues.

Run one process for everything during development, or one per queue in
production so each can scale on its own:

    uv run agent-worker                        # all three queues
    uv run agent-worker --queue agent-scheduled

A worker is just a process that polls task queues and runs whatever Workflow
and Activity tasks it finds. It holds no durable state: stop it at any point
and start it again (or start several), and Temporal hands out the remaining
work. That is what the recovery drills exercise.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import logging
from collections.abc import Sequence

from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from .activities.agent import AgentActivities
from .activities.memory import MemoryActivities
from .activities.research import ResearchActivities
from .activities.workspace import WorkspaceActivities
from .config import Settings, load_dotenv
from .llm import ModelClient, build_model, verify_model_access
from .store import AppStore
from .workflows.briefing import BriefingTurnWorkflow, ResearchWorkflow
from .workflows.memory import MemoryConsolidationWorkflow
from .workflows.session import SessionWorkflow
from .workflows.workspace import WorkspaceLeaseWorkflow, WorkspacePoolWorkflow

WORKFLOWS = [
    BriefingTurnWorkflow,
    ResearchWorkflow,
    MemoryConsolidationWorkflow,
    WorkspacePoolWorkflow,
    WorkspaceLeaseWorkflow,
    SessionWorkflow,
]


async def connect() -> Client:
    """Connect using Temporal's standard env vars / TOML profile (works for Temporal Cloud too)."""
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    return await Client.connect(**config)


def build_workers(
    client: Client,
    settings: Settings,
    queues: Sequence[str],
    executor: concurrent.futures.Executor,
    *,
    model: ModelClient | None = None,
) -> list[Worker]:
    """Workers for the given queues. ``model`` lets tests substitute a test double for the model."""
    store = AppStore(settings.data_dir)
    model = build_model(settings, store, model)
    agent = AgentActivities(store, model, client, settings)
    research = ResearchActivities(store, model)
    memory = MemoryActivities(store, model)
    workspace = WorkspaceActivities(store, client, settings.pool_workflow_id)
    activities = [
        agent.run_briefing_turn,
        agent.call_model,
        agent.run_tool,
        agent.deliver_message,
        agent.schedule_wakeup,
        agent.wake_session,
        agent.record_interaction,
        research.gather_sources,
        research.summarize_sources,
        memory.prepare_memory_batch,
        memory.publish_memory_revision,
        memory.notify_operator,
        workspace.provision_vm,
        workspace.check_vm_health,
        workspace.destroy_vm,
        workspace.recycle_vm,
        workspace.find_orphans,
        workspace.claim_vm,
        workspace.snapshot_workspace,
        workspace.restore_workspace,
    ]
    # agent(trade-off): every queue registers everything, so the task_queue a caller picks
    # decides where work runs. Splitting queues across processes needs no code change.
    return [
        Worker(client, task_queue=queue, workflows=WORKFLOWS, activities=activities, activity_executor=executor)
        for queue in queues
    ]


async def run(queues: Sequence[str]) -> None:
    settings = Settings.from_env()
    model_name = await verify_model_access(settings)
    client = await connect()
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        workers = build_workers(client, settings, queues, executor)
        logging.info("worker polling %s (model: %s, data: %s)", ", ".join(queues), model_name, settings.data_dir)
        await asyncio.gather(*(w.run() for w in workers))


def main() -> None:
    load_dotenv()
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--queue", action="append", choices=settings.all_queues, help="queue to poll (repeatable; default: all)"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(args.queue or settings.all_queues))


if __name__ == "__main__":
    main()
