"""Data shapes passed between clients, Workflows, and Activities.

Kept free of I/O and heavy imports so Workflow modules can import it through
the sandbox cheaply. Everything here is a plain dataclass, which Temporal's
default data converter serializes to JSON.

These shapes are also a contract with history. A Workflow that is waiting
(a session, a Schedule, a paused memory batch) will be handed the same JSON
after you deploy new code, so changes here have to be backward compatible.
"""

# agent(constraint): payloads are JSON by field name. Add fields with defaults; don't rename
# or remove fields that running Workflows or Schedules carry.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Model calls
# ---------------------------------------------------------------------------


@dataclass
class ModelRequest:
    purpose: str  # "briefing" | "session" | "research" | "memory"
    system: str
    messages: list[dict[str, Any]]
    tools: list[dict[str, Any]] = field(default_factory=list)
    output_schema: dict[str, Any] | None = None
    max_tokens: int = 16000
    effort: str = "medium"
    # Recorded in the operation log so drills can show which calls repeated.
    log_key: str = ""


@dataclass
class ModelResponse:
    stop_reason: str
    content: list[dict[str, Any]]
    model: str
    input_tokens: int = 0
    output_tokens: int = 0


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@dataclass
class ApprovalGrant:
    """What a human approved: one action, with exact parameters, until a deadline."""

    approval_id: str
    digest: str
    expires_at: str  # ISO-8601, UTC


@dataclass
class ToolCall:
    name: str
    input: dict[str, Any]
    tool_use_id: str
    user_id: str
    turn_id: str
    approval: ApprovalGrant | None = None


@dataclass
class ToolResult:
    tool_use_id: str
    content: str
    is_error: bool = False


# ---------------------------------------------------------------------------
# Briefing turns
# ---------------------------------------------------------------------------


@dataclass
class BriefingTurnInput:
    user_id: str
    request: str = "Prepare my weekly briefing."
    deliver: bool = True


@dataclass
class TurnOutcome:
    turn_id: str
    report_id: str | None
    reply: str
    model_calls: int
    delivered: bool


@dataclass
class DeliveryRequest:
    message_id: str  # idempotency key: a retried delivery never sends twice
    user_id: str
    body: str
    channel: str = "email"
    report_id: str | None = None  # appended to the message when set


# ---------------------------------------------------------------------------
# Workflow-as-tool: research subagent
# ---------------------------------------------------------------------------


@dataclass
class ResearchInput:
    user_id: str
    question: str


@dataclass
class ResearchOutcome:
    question: str
    summary: str
    sources: list[str]


# ---------------------------------------------------------------------------
# Memory consolidation
# ---------------------------------------------------------------------------


@dataclass
class ConsolidationInput:
    user_id: str
    # Batch when either condition is met: this many new interactions, or this
    # long since the first unprocessed one arrived.
    batch_size: int = 5
    max_wait_seconds: int = 3600
    # Process whatever is pending right away (event- or Schedule-started runs).
    flush_now: bool = False
    # Publication retries before the Workflow asks an operator for help.
    max_publish_attempts: int = 8
    # Arrivals carried across Continue-As-New.
    pending_count: int = 0
    batches_done: int = 0


@dataclass
class PrepareBatchInput:
    batch_id: str
    user_id: str
    max_interactions: int = 50


@dataclass
class PreparedBatch:
    batch_id: str
    user_id: str
    interaction_ids: list[str]
    base_revision: int
    note_count: int
    reused_saved_proposal: bool


@dataclass
class PublishResult:
    batch_id: str
    revision: int
    covered_interactions: int
    already_published: bool


@dataclass
class OperatorAlert:
    kind: str
    subject: str
    detail: str


@dataclass
class ConsolidationOutcome:
    user_id: str
    revisions_published: list[int]


# ---------------------------------------------------------------------------
# Workspace hot pool and save-and-release
# ---------------------------------------------------------------------------


@dataclass
class PoolConfig:
    target_ready: int = 2
    max_vms: int = 6
    # Protect the provisioning service: at most this many creates per window.
    max_creates_per_window: int = 3
    rate_window_seconds: int = 60
    tick_seconds: int = 30
    # Continue-As-New after this many ticks even if the server has not
    # suggested it yet, to keep each run's history small.
    max_ticks_per_run: int = 500


@dataclass
class VmRecord:
    vm_id: str
    status: str  # "ready" | "claimed"
    owner_workspace: str | None = None
    owner_workflow_id: str | None = None


@dataclass
class PoolState:
    vms: dict[str, VmRecord] = field(default_factory=dict)
    recent_creates: list[str] = field(default_factory=list)  # ISO timestamps
    provision_seq: int = 0


@dataclass
class PoolInput:
    config: PoolConfig = field(default_factory=PoolConfig)
    state: PoolState | None = None


@dataclass
class PoolStatus:
    ready: list[str]
    claimed: dict[str, str]  # vm_id -> workspace_id
    waiting_claims: int
    creates_in_window: int


@dataclass
class ClaimRequest:
    workspace_id: str
    owner_workflow_id: str


@dataclass
class ReleaseRequest:
    workspace_id: str
    vm_id: str


@dataclass
class ProvisionRequest:
    request_id: str  # provider deduplicates on this, so a retried create returns the same VM


@dataclass
class OrphanCheck:
    # vm_id -> owner workflow id
    claims: dict[str, str]
    known_vm_ids: list[str]


@dataclass
class OrphanReport:
    orphaned_claims: list[str]  # vm_ids whose owner is gone
    unknown_vms: list[str]  # VMs at the provider the pool never recorded


@dataclass
class LeaseInput:
    workspace_id: str
    idle_timeout_seconds: int = 300
    pool_workflow_id: str = "workspace-pool"


@dataclass
class LeaseOutcome:
    workspace_id: str
    vm_id: str
    restored_from: str | None
    snapshot_id: str | None
    files_saved: int


@dataclass
class SnapshotRequest:
    workspace_id: str
    vm_id: str
    snapshot_id: str


@dataclass
class SnapshotResult:
    snapshot_id: str
    files: int


@dataclass
class RestoreRequest:
    workspace_id: str
    vm_id: str


# ---------------------------------------------------------------------------
# Sessions and approvals
# ---------------------------------------------------------------------------


@dataclass
class SessionEvent:
    seq: int
    kind: str  # message_accepted | turn_started | assistant | approval_requested | ...
    text: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class PendingApproval:
    approval_id: str
    tool_use_id: str
    tool: str
    input: dict[str, Any]
    digest: str
    requested_at: str
    expires_at: str
    status: str = "pending"  # pending | approved | denied | expired


@dataclass
class PendingTimer:
    timer_id: str
    note: str
    fires_at: str  # ISO-8601, UTC


@dataclass
class WakeUp:
    """The delayed job behind the agent's timer tool: wake this session with this note."""

    session_workflow_id: str
    user_id: str
    timer_id: str
    note: str


@dataclass
class InteractionRecord:
    """Something the user said, kept as source material for memory consolidation."""

    user_id: str
    interaction_id: str
    text: str


@dataclass
class SessionState:
    """Everything a session carries across Continue-As-New."""

    messages: list[dict[str, Any]] = field(default_factory=list)
    events: list[SessionEvent] = field(default_factory=list)
    next_seq: int = 1
    seen_client_message_ids: list[str] = field(default_factory=list)
    queued: list[dict[str, str]] = field(default_factory=list)  # {"message_id", "text", "kind"}
    turns: int = 0
    timers: list[PendingTimer] = field(default_factory=list)


@dataclass
class SessionInput:
    session_id: str
    user_id: str
    idle_timeout_seconds: int = 3600
    approval_timeout_seconds: int = 900
    state: SessionState | None = None


@dataclass
class SendMessageInput:
    client_message_id: str  # makes retries of the same send idempotent
    text: str
    # "auto": start a turn if idle, otherwise join the running turn.
    # "queue": always wait for the next turn.
    mode: str = "auto"


@dataclass
class MessageAccepted:
    message_id: str
    disposition: str  # "started" | "joined" | "queued" | "duplicate"


@dataclass
class ApprovalDecision:
    approval_id: str
    approve: bool
    # The client echoes the digest it showed the human, so an approval can
    # only ever authorize the exact parameters that were displayed.
    digest: str


@dataclass
class SessionStatus:
    session_id: str
    turn_running: bool
    queued_messages: int
    pending_approvals: list[PendingApproval]
    last_seq: int
    closed: bool
    pending_timers: list[PendingTimer] = field(default_factory=list)
