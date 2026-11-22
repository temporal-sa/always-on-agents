"""Workspace management between turns.

``WorkspacePoolWorkflow`` is a long-lived controller for a hot pool of sandbox
VMs: it keeps some ready, hands them out on claim, health-checks them, reclaims
VMs whose owner is gone, destroys VMs it never recorded, and rate-limits
creation. It carries its state into fresh runs with Continue-As-New.

``WorkspaceLeaseWorkflow`` holds one workspace's VM. Its lifetime is separate
from the agent's: it restores saved files when work arrives, waits while the
agent works, and when the workspace goes idle it saves the files, records
that, and only then releases the VM. If a worker fails after the save is
recorded, recovery continues with the release.

Temporal coordinates; the application still calls the compute and storage
APIs, and a waiting Workflow does not itself suspend a VM or stop its charges.

Why the split: an agent's sandbox is expensive to keep and slow to create.
The pool trades a few idle VMs for instant starts. The lease makes sure that
giving a VM back never loses work: the save is a recorded step, so a crash
between "saved" and "released" resumes at the release, not before the save.

What to notice:

* The pool is a control loop: reconcile, then sleep until a tick or a wake-up
  from a handler. Every provider call in it is an Activity with retries.
* A claim is an Update, so the caller gets a VM id back, or waits in line.
* Orphan cleanup covers both directions: VMs claimed by a lease that no longer
  runs, and VMs the provider has that the pool never recorded.

See it happen: recovery drill 6 in the README, or the Workspace page in the web UI.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities.workspace import WorkspaceActivities
    from ..domain import (
        ClaimRequest,
        LeaseInput,
        LeaseOutcome,
        OrphanCheck,
        PoolInput,
        PoolState,
        PoolStatus,
        ProvisionRequest,
        ReleaseRequest,
        RestoreRequest,
        SnapshotRequest,
        VmRecord,
    )

PROVIDER_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=30))


def lease_workflow_id(workspace_id: str) -> str:
    return f"workspace-{workspace_id}"


@workflow.defn
class WorkspacePoolWorkflow:
    @workflow.init
    def __init__(self, request: PoolInput) -> None:
        self._config = request.config
        self._state = request.state or PoolState()
        self._claim_queue: list[str] = []  # workspace ids waiting for a VM, FIFO
        self._wake = False
        self._ticks = 0

    # -- handlers ------------------------------------------------------------

    @workflow.update
    async def claim(self, request: ClaimRequest) -> str:
        """Hand a ready VM to a workspace, waiting in line if none is ready."""
        existing = self._owned_by(request.workspace_id)
        if existing:
            return existing.vm_id  # idempotent: this workspace already holds one
        if request.workspace_id in self._claim_queue:
            await workflow.wait_condition(lambda: self._owned_by(request.workspace_id) is not None)
            return self._owned_by(request.workspace_id).vm_id  # type: ignore[union-attr]
        self._claim_queue.append(request.workspace_id)
        self._wake = True
        await workflow.wait_condition(lambda: self._claim_queue[0] == request.workspace_id and bool(self._ready()))
        self._claim_queue.pop(0)
        vm = self._ready()[0]
        vm.status, vm.owner_workspace, vm.owner_workflow_id = "claimed", request.workspace_id, request.owner_workflow_id
        self._wake = True  # replenish the ready pool
        return vm.vm_id

    @workflow.signal
    def release(self, request: ReleaseRequest) -> None:
        # agent(invariant): only the current owner can release. A late release from an old
        # lease must not free a VM that another workspace now holds.
        vm = self._state.vms.get(request.vm_id)
        if vm and vm.owner_workspace == request.workspace_id:
            vm.status = "releasing"
            self._wake = True

    @workflow.query
    def status(self) -> PoolStatus:
        window = self._window_start()
        return PoolStatus(
            ready=[v.vm_id for v in self._ready()],
            claimed={v.vm_id: v.owner_workspace or "" for v in self._state.vms.values() if v.status == "claimed"},
            waiting_claims=len(self._claim_queue),
            creates_in_window=sum(1 for t in self._state.recent_creates if datetime.fromisoformat(t) > window),
        )

    # -- control loop --------------------------------------------------------

    @workflow.run
    async def run(self, request: PoolInput) -> None:
        while True:
            await self._reconcile()
            self._ticks += 1
            due = self._ticks >= self._config.max_ticks_per_run or workflow.info().is_continue_as_new_suggested()
            # agent(invariant): never Continue-As-New while claims wait. Waiting for
            # all_handlers_finished would deadlock: a claim finishes only when this loop
            # provisions a VM.
            if due and not self._claim_queue:
                # Carry state into a fresh run so history never grows without bound.
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(PoolInput(config=self._config, state=self._state))
            self._wake = False
            try:
                await workflow.wait_condition(lambda: self._wake, timeout=timedelta(seconds=self._config.tick_seconds))
            except TimeoutError:
                pass

    async def _reconcile(self) -> None:
        vms = self._state.vms

        # 1. Recycle VMs that workspaces released.
        for vm in [v for v in vms.values() if v.status == "releasing"]:
            await self._activity(WorkspaceActivities.recycle_vm, vm.vm_id, "recycle VM")
            vm.status, vm.owner_workspace, vm.owner_workflow_id = "ready", None, None

        # 2. Health checks: replace VMs that fail.
        if vms:
            unhealthy = await self._activity(WorkspaceActivities.check_vm_health, sorted(vms), "health check")
            for vm_id in unhealthy:
                await self._activity(WorkspaceActivities.destroy_vm, vm_id, "destroy unhealthy VM")
                vms.pop(vm_id, None)

        # 3. Clean up resources left without an owner.
        claims = {v.vm_id: v.owner_workflow_id or "" for v in vms.values() if v.status == "claimed"}
        report = await self._activity(
            WorkspaceActivities.find_orphans, OrphanCheck(claims=claims, known_vm_ids=sorted(vms)), "find orphans"
        )
        for vm_id in report.orphaned_claims:
            vm = vms.get(vm_id)
            # The VM may have been released and claimed again while we checked.
            # agent(invariant): compare the owner too, or a fresh claim gets reclaimed.
            if vm is None or vm.status != "claimed" or vm.owner_workflow_id != claims[vm_id]:
                continue
            workflow.logger.info("reclaiming %s: its owner is gone", vm_id)
            await self._activity(WorkspaceActivities.recycle_vm, vm_id, "reclaim orphaned VM")
            vm.status, vm.owner_workspace, vm.owner_workflow_id = "ready", None, None
        for vm_id in report.unknown_vms:
            workflow.logger.info("destroying %s: the pool never recorded it", vm_id)
            await self._activity(WorkspaceActivities.destroy_vm, vm_id, "destroy untracked VM")

        # 4. Replenish the ready pool, within capacity and the creation rate limit.
        window = self._window_start()
        self._state.recent_creates = [t for t in self._state.recent_creates if datetime.fromisoformat(t) > window]
        wanted = self._config.target_ready + len(self._claim_queue) - len(self._ready())
        capacity = self._config.max_vms - len(vms)
        budget = self._config.max_creates_per_window - len(self._state.recent_creates)
        to_create = max(0, min(wanted, capacity, budget))
        if to_create:
            # agent(invariant): request ids come from provision_seq, which is carried across
            # Continue-As-New. The provider deduplicates on them, so a retried provision
            # returns the same VM.
            request_ids = []
            for _ in range(to_create):
                self._state.provision_seq += 1
                request_ids.append(f"{workflow.info().workflow_id}/vm-{self._state.provision_seq}")
            created = await asyncio.gather(
                *(
                    self._activity(WorkspaceActivities.provision_vm, ProvisionRequest(r), "provision VM")
                    for r in request_ids
                )
            )
            for vm_id in created:
                vms[vm_id] = VmRecord(vm_id=vm_id, status="ready")
                self._state.recent_creates.append(workflow.now().isoformat())

    # -- helpers -------------------------------------------------------------

    def _ready(self) -> list[VmRecord]:
        return sorted((v for v in self._state.vms.values() if v.status == "ready"), key=lambda v: v.vm_id)

    def _owned_by(self, workspace_id: str) -> VmRecord | None:
        return next(
            (v for v in self._state.vms.values() if v.status == "claimed" and v.owner_workspace == workspace_id), None
        )

    def _window_start(self) -> datetime:
        return workflow.now() - timedelta(seconds=self._config.rate_window_seconds)

    async def _activity(self, fn: Any, arg: Any, summary: str) -> Any:
        return await workflow.execute_activity_method(
            fn, arg, start_to_close_timeout=timedelta(minutes=2), retry_policy=PROVIDER_RETRY, summary=summary
        )


@workflow.defn
class WorkspaceLeaseWorkflow:
    def __init__(self) -> None:
        self._stage = "claiming"
        self._vm_id: str | None = None
        self._touched = False
        self._done = False

    @workflow.signal
    def touch(self) -> None:
        """The agent used the workspace; push the idle deadline out."""
        self._touched = True

    @workflow.signal
    def finish(self) -> None:
        """Save and release now instead of waiting for the idle timeout."""
        self._done = True

    @workflow.query
    def info(self) -> dict[str, Any]:
        return {"stage": self._stage, "vm_id": self._vm_id}

    @workflow.run
    async def run(self, request: LeaseInput) -> LeaseOutcome:
        me = workflow.info().workflow_id
        vm_id = await workflow.execute_activity_method(
            WorkspaceActivities.claim_vm,
            ClaimRequest(workspace_id=request.workspace_id, owner_workflow_id=me),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=PROVIDER_RETRY,
            summary="claim VM from pool",
        )
        self._vm_id = vm_id

        # Work is arriving: put the saved files back before the agent uses them.
        self._stage = "restoring"
        restored_from = await workflow.execute_activity_method(
            WorkspaceActivities.restore_workspace,
            RestoreRequest(workspace_id=request.workspace_id, vm_id=vm_id),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=PROVIDER_RETRY,
            summary="restore workspace files",
        )

        self._stage = "active"
        idle = timedelta(seconds=request.idle_timeout_seconds)
        while not self._done:
            self._touched = False
            try:
                await workflow.wait_condition(lambda: self._touched or self._done, timeout=idle)
            except TimeoutError:
                break  # idle long enough

        # Save first, and let Temporal record it. Only then release the VM.
        # agent(invariant): keep this order. Releasing first lets the pool wipe the VM
        # before its files are saved.
        self._stage = "saving"
        snapshot_id = f"{request.workspace_id}-{workflow.info().run_id[:8]}"
        saved = await workflow.execute_activity_method(
            WorkspaceActivities.snapshot_workspace,
            SnapshotRequest(workspace_id=request.workspace_id, vm_id=vm_id, snapshot_id=snapshot_id),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=PROVIDER_RETRY,
            summary="save workspace files",
        )

        self._stage = "releasing"
        pool = workflow.get_external_workflow_handle(request.pool_workflow_id)
        await pool.signal(WorkspacePoolWorkflow.release, ReleaseRequest(workspace_id=request.workspace_id, vm_id=vm_id))
        self._stage = "released"
        return LeaseOutcome(request.workspace_id, vm_id, restored_from, saved.snapshot_id, saved.files)
