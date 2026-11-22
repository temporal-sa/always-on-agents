"""Activities for the workspace control plane.

Temporal coordinates these operations; the application still calls the compute
and storage APIs. Here the "provider" is the fake one in ``store.py``.
"""

from __future__ import annotations

from temporalio import activity
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from ..domain import (
    ClaimRequest,
    OrphanCheck,
    OrphanReport,
    ProvisionRequest,
    RestoreRequest,
    SnapshotRequest,
    SnapshotResult,
)
from ..store import AppStore


class WorkspaceActivities:
    def __init__(self, store: AppStore, temporal: Client, pool_workflow_id: str) -> None:
        self._store = store
        self._temporal = temporal
        self._pool_workflow_id = pool_workflow_id

    # -- compute provider ---------------------------------------------------

    @activity.defn
    def provision_vm(self, request: ProvisionRequest) -> str:
        self._store.maybe_fail("provision_vm")
        vm_id, created = self._store.provision_vm(request.request_id)
        self._store.log_op("vm:provision", vm_id, activity.info().attempt, "created" if created else "existing")
        return vm_id

    @activity.defn
    def check_vm_health(self, vm_ids: list[str]) -> list[str]:
        """Return the VMs that failed their health check."""
        return [vm_id for vm_id in vm_ids if not self._store.vm_healthy(vm_id)]

    @activity.defn
    def destroy_vm(self, vm_id: str) -> None:
        self._store.destroy_vm(vm_id)
        self._store.log_op("vm:destroy", vm_id, activity.info().attempt)

    @activity.defn
    def recycle_vm(self, vm_id: str) -> None:
        """Wipe a released VM so the next workspace starts clean."""
        self._store.wipe_vm(vm_id)
        self._store.log_op("vm:recycle", vm_id, activity.info().attempt)

    @activity.defn
    async def find_orphans(self, check: OrphanCheck) -> OrphanReport:
        """Find claims whose owner is gone, and VMs the pool never recorded.

        The second case covers a controller that crashed between the provider
        creating a VM and the pool recording it.
        """
        orphaned = []
        for vm_id, owner in check.claims.items():
            try:
                desc = await self._temporal.get_workflow_handle(owner).describe()
                if desc.status != WorkflowExecutionStatus.RUNNING:
                    orphaned.append(vm_id)
            except RPCError as err:
                if err.status == RPCStatusCode.NOT_FOUND:
                    orphaned.append(vm_id)
                else:
                    raise
        running = set(self._store.running_vms())
        unknown = sorted(running - set(check.known_vm_ids))
        return OrphanReport(orphaned_claims=orphaned, unknown_vms=unknown)

    # -- used by a workspace lease -----------------------------------------

    @activity.defn
    async def claim_vm(self, request: ClaimRequest) -> str:
        """Claim a ready VM from the pool (waits in the pool's queue if none is ready)."""
        pool = self._temporal.get_workflow_handle(self._pool_workflow_id)
        return await pool.execute_update("claim", request, result_type=str)

    @activity.defn
    def snapshot_workspace(self, request: SnapshotRequest) -> SnapshotResult:
        self._store.maybe_fail("snapshot_workspace")
        files = self._store.snapshot_workspace(request.workspace_id, request.vm_id, request.snapshot_id)
        self._store.log_op("workspace:snapshot", request.snapshot_id, activity.info().attempt, f"{files} files")
        return SnapshotResult(request.snapshot_id, files)

    @activity.defn
    def restore_workspace(self, request: RestoreRequest) -> str | None:
        snapshot_id = self._store.restore_workspace(request.workspace_id, request.vm_id)
        self._store.log_op("workspace:restore", request.workspace_id, activity.info().attempt, snapshot_id or "fresh")
        return snapshot_id
