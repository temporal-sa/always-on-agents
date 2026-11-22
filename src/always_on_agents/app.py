"""Application-side entry points: what an API server or chat backend would call.

These are thin. They record what the application owns (its database rows) and
hand the rest to Temporal: start or signal a Workflow, send an Update, read a
Query. None of them waits for the work itself to finish.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from .config import Settings
from .domain import (
    ApprovalDecision,
    BriefingTurnInput,
    ConsolidationInput,
    LeaseInput,
    MessageAccepted,
    PoolConfig,
    PoolInput,
    SendMessageInput,
    SessionEvent,
    SessionInput,
    SessionStatus,
    TurnOutcome,
)
from .store import AppStore
from .workflows.briefing import BriefingTurnWorkflow
from .workflows.memory import MemoryConsolidationWorkflow, consolidation_workflow_id
from .workflows.session import SessionWorkflow
from .workflows.workspace import WorkspaceLeaseWorkflow, WorkspacePoolWorkflow, lease_workflow_id

# -- interactive turns --------------------------------------------------------


async def start_turn(
    client: Client, settings: Settings, user_id: str, turn_id: str, *, deliver: bool = True
) -> WorkflowHandle[BriefingTurnWorkflow, TurnOutcome]:
    return await client.start_workflow(
        BriefingTurnWorkflow.run,
        BriefingTurnInput(user_id=user_id, deliver=deliver),
        id=turn_id,
        task_queue=settings.interactive_queue,
    )


# -- memory --------------------------------------------------------------------


async def record_interaction(
    client: Client,
    store: AppStore,
    settings: Settings,
    user_id: str,
    text: str,
    *,
    batch_size: int = 5,
    max_wait: timedelta = timedelta(hours=1),
) -> str:
    """Save the interaction (the application's source of truth), then notify
    the user's consolidation Workflow, starting it if it is not running.

    If the notification fails, the interaction is still unprocessed in the
    database, and the next batch picks it up.
    """
    interaction_id = store.record_interaction(user_id, text)
    await notify_consolidation(client, settings, user_id, interaction_id, batch_size=batch_size, max_wait=max_wait)
    return interaction_id


async def notify_consolidation(
    client: Client,
    settings: Settings,
    user_id: str,
    interaction_id: str,
    *,
    batch_size: int = 5,
    max_wait: timedelta = timedelta(hours=1),
) -> None:
    """Tell the user's consolidation Workflow about a saved interaction (signal-with-start)."""
    await client.start_workflow(
        MemoryConsolidationWorkflow.run,
        ConsolidationInput(user_id=user_id, batch_size=batch_size, max_wait_seconds=int(max_wait.total_seconds())),
        id=consolidation_workflow_id(user_id),
        task_queue=settings.background_queue,
        start_signal="interaction_recorded",
        start_signal_args=[interaction_id],
    )


async def consolidate_now(client: Client, settings: Settings, user_id: str) -> bool:
    """Start a consolidation run for whatever is pending.

    Uses the fixed per-user Workflow ID with the Fail conflict policy, so a
    second start is rejected while one is running. Returns False in that case
    (the running Workflow will reach the pending interactions itself).
    """
    try:
        await client.start_workflow(
            MemoryConsolidationWorkflow.run,
            ConsolidationInput(user_id=user_id, flush_now=True),
            id=consolidation_workflow_id(user_id),
            task_queue=settings.background_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )
    except WorkflowAlreadyStartedError:
        return False
    return True


async def retry_memory_publication(client: Client, user_id: str) -> None:
    await client.get_workflow_handle(consolidation_workflow_id(user_id)).signal(
        MemoryConsolidationWorkflow.retry_publication
    )


# -- workspaces ------------------------------------------------------------------


async def ensure_pool(client: Client, settings: Settings, config: PoolConfig | None = None) -> None:
    await client.start_workflow(
        WorkspacePoolWorkflow.run,
        PoolInput(config=config or PoolConfig()),
        id=settings.pool_workflow_id,
        task_queue=settings.background_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )


async def open_workspace(
    client: Client, settings: Settings, workspace_id: str, *, idle_timeout: timedelta = timedelta(minutes=5)
) -> None:
    await client.start_workflow(
        WorkspaceLeaseWorkflow.run,
        LeaseInput(
            workspace_id=workspace_id,
            idle_timeout_seconds=int(idle_timeout.total_seconds()),
            pool_workflow_id=settings.pool_workflow_id,
        ),
        id=lease_workflow_id(workspace_id),
        task_queue=settings.background_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )


async def write_workspace_file(client: Client, store: AppStore, workspace_id: str, name: str, text: str) -> str:
    """Simulate the agent editing a scratch file in its workspace."""
    handle = client.get_workflow_handle(lease_workflow_id(workspace_id))
    info = await handle.query(WorkspaceLeaseWorkflow.info)
    if info["stage"] != "active":
        raise RuntimeError(f"workspace {workspace_id} is {info['stage']}, not active")
    path = store.vm_dir(info["vm_id"]) / name
    path.write_text(text)
    await handle.signal(WorkspaceLeaseWorkflow.touch)
    return str(path)


# -- sessions ----------------------------------------------------------------------


def session_handle(client: Client, session_id: str) -> WorkflowHandle[SessionWorkflow, str]:
    return client.get_workflow_handle_for(SessionWorkflow.run, f"session-{session_id}")


async def start_session(
    client: Client,
    settings: Settings,
    session_id: str,
    user_id: str,
    *,
    approval_timeout: timedelta = timedelta(minutes=15),
    idle_timeout: timedelta = timedelta(hours=1),
) -> None:
    await client.start_workflow(
        SessionWorkflow.run,
        SessionInput(
            session_id=session_id,
            user_id=user_id,
            idle_timeout_seconds=int(idle_timeout.total_seconds()),
            approval_timeout_seconds=int(approval_timeout.total_seconds()),
        ),
        id=f"session-{session_id}",
        task_queue=settings.interactive_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )


async def send_session_message(
    client: Client, session_id: str, client_message_id: str, text: str, mode: str = "auto"
) -> MessageAccepted:
    # The Update returns once the message is accepted, not when the work is done.
    return await session_handle(client, session_id).execute_update(
        SessionWorkflow.send_message, SendMessageInput(client_message_id, text, mode), id=client_message_id
    )


async def session_events(client: Client, session_id: str, after_seq: int = 0) -> list[SessionEvent]:
    return await session_handle(client, session_id).query(SessionWorkflow.events_since, after_seq)


async def session_status(client: Client, session_id: str) -> SessionStatus:
    return await session_handle(client, session_id).query(SessionWorkflow.status)


async def decide_approval(client: Client, session_id: str, approval_id: str, digest: str, approve: bool) -> str:
    return await session_handle(client, session_id).execute_update(
        SessionWorkflow.decide_approval, ApprovalDecision(approval_id, approve, digest)
    )
