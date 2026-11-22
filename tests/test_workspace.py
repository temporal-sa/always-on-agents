"""Hot pool controller and save-and-release."""

from __future__ import annotations

from datetime import timedelta

from always_on_agents import app
from always_on_agents.domain import PoolConfig
from always_on_agents.workflows.workspace import WorkspaceLeaseWorkflow, WorkspacePoolWorkflow, lease_workflow_id

from .conftest import assert_replays, eventually

FAST = PoolConfig(target_ready=2, max_vms=4, max_creates_per_window=4, rate_window_seconds=60, tick_seconds=1)


def _pool(client, settings):
    return client.get_workflow_handle_for(WorkspacePoolWorkflow.run, settings.pool_workflow_id)


async def _ready(client, settings, n):
    status = await _pool(client, settings).query(WorkspacePoolWorkflow.status)
    return status if len(status.ready) >= n else None


async def test_save_then_release_then_restore(client, settings, store, workers):
    await app.ensure_pool(client, settings, FAST)
    await eventually(lambda: _ready(client, settings, 2))

    await app.open_workspace(client, settings, "ws1", idle_timeout=timedelta(seconds=2))
    lease = client.get_workflow_handle_for(WorkspaceLeaseWorkflow.run, lease_workflow_id("ws1"))
    await eventually(lambda: _stage(lease, "active"))
    await app.write_workspace_file(client, store, "ws1", "notes.md", "draft v1")

    first = await lease.result()  # goes idle, saves, then releases
    assert first.files_saved == 1 and first.restored_from is None
    assert len(store.ops("workspace:snapshot")) == 1

    # The released VM is wiped and returned to the ready pool.
    async def back_in_pool():
        status = await _pool(client, settings).query(WorkspacePoolWorkflow.status)
        return first.vm_id in status.ready

    await eventually(back_in_pool)
    assert not (store.vm_dir(first.vm_id) / "notes.md").exists()

    # Work returns: a new lease restores the saved files before the agent uses them.
    await app.open_workspace(client, settings, "ws1", idle_timeout=timedelta(seconds=1))
    lease2 = client.get_workflow_handle_for(WorkspaceLeaseWorkflow.run, lease_workflow_id("ws1"))
    info = await eventually(lambda: _stage(lease2, "active"))
    assert (store.vm_dir(info["vm_id"]) / "notes.md").read_text() == "draft v1"
    second = await lease2.result()
    assert second.restored_from == first.snapshot_id
    await assert_replays(lease)


async def test_orphaned_claim_is_reclaimed(client, settings, store, workers):
    await app.ensure_pool(client, settings, FAST)
    await app.open_workspace(client, settings, "ws2", idle_timeout=timedelta(minutes=10))
    lease = client.get_workflow_handle(lease_workflow_id("ws2"))
    info = await eventually(lambda: _stage(lease, "active"))
    await lease.terminate("simulated crash of the owner")

    async def reclaimed():
        status = await _pool(client, settings).query(WorkspacePoolWorkflow.status)
        return info["vm_id"] in status.ready

    await eventually(reclaimed)


async def test_untracked_and_unhealthy_vms_are_destroyed(client, settings, store, workers):
    await app.ensure_pool(client, settings, FAST)
    status = await eventually(lambda: _ready(client, settings, 2))
    rogue, _ = store.provision_vm("created-outside-the-pool")
    sick = status.ready[0]
    store.mark_vm_unhealthy(sick)

    async def cleaned():
        running = store.running_vms()
        now = await _pool(client, settings).query(WorkspacePoolWorkflow.status)
        return rogue not in running and sick not in running and len(now.ready) == 2

    await eventually(cleaned)


async def test_creation_is_rate_limited(client, settings, store, workers):
    config = PoolConfig(target_ready=4, max_vms=6, max_creates_per_window=2, rate_window_seconds=60, tick_seconds=1)
    await app.ensure_pool(client, settings, config)
    status = await eventually(lambda: _ready(client, settings, 2))
    assert status.creates_in_window == 2
    assert len(store.running_vms()) == 2  # target is 4, but only 2 creates per minute


async def test_controller_continues_as_new_with_its_state(client, settings, store, workers):
    config = PoolConfig(target_ready=2, max_vms=4, max_creates_per_window=4, tick_seconds=1, max_ticks_per_run=3)
    await app.ensure_pool(client, settings, config)
    first_run = (await _pool(client, settings).describe()).run_id
    before = await eventually(lambda: _ready(client, settings, 2))

    async def new_run():
        return (await _pool(client, settings).describe()).run_id != first_run

    await eventually(new_run)
    after = await _pool(client, settings).query(WorkspacePoolWorkflow.status)
    assert after.ready == before.ready  # same VMs, carried across runs
    assert len(store.running_vms()) == 2
    await assert_replays(_pool(client, settings))  # replays the open run's history so far


async def _stage(handle, wanted):
    info = await handle.query(WorkspaceLeaseWorkflow.info)
    return info if info["stage"] == wanted else None
