"""The application's own systems, simulated with SQLite and local folders.

Temporal coordinates work; it does not replace the application's database,
compute provider, or storage. This module stands in for those so the reference
implementation runs on a laptop:

* memory store (interactions, memory revisions, consolidation batches)
* reports and an outbox for delivered messages
* a fake compute provider (VMs are folders) and snapshot storage
* an operation log, so recovery drills can show exactly which operations ran
  (and which repeated) after a crash
* fault injection, so drills can make a dependency fail or a worker crash at a
  precise point
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS ops (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    op TEXT NOT NULL, key TEXT NOT NULL, attempt INTEGER NOT NULL,
    detail TEXT NOT NULL DEFAULT '', at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS faults (
    name TEXT PRIMARY KEY, remaining INTEGER NOT NULL, mode TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS interactions (
    interaction_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, text TEXT NOT NULL,
    created_at TEXT NOT NULL, batch_id TEXT
);
CREATE TABLE IF NOT EXISTS memory_revisions (
    user_id TEXT NOT NULL, revision INTEGER NOT NULL, notes_json TEXT NOT NULL,
    batch_id TEXT NOT NULL, created_at TEXT NOT NULL,
    PRIMARY KEY (user_id, revision)
);
CREATE TABLE IF NOT EXISTS memory_batches (
    batch_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, interaction_ids_json TEXT NOT NULL,
    base_revision INTEGER NOT NULL, proposal_json TEXT, status TEXT NOT NULL,
    published_revision INTEGER, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, title TEXT NOT NULL,
    body TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    message_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, channel TEXT NOT NULL,
    body TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS operator_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, subject TEXT NOT NULL,
    detail TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS vms (
    vm_id TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, status TEXT NOT NULL,
    healthy INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, vm_id TEXT NOT NULL,
    files INTEGER NOT NULL, created_at TEXT NOT NULL
);
"""


class InjectedFault(RuntimeError):
    """Raised by ``maybe_fail`` to simulate a dependency outage."""


# agent(constraint): workflows/memory.py names this class as a string in its retry policy.
class StaleBatchError(RuntimeError):
    """The memory changed since a batch was prepared; its proposal can't be published."""


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


@dataclass
class MemoryNote:
    text: str
    sources: list[str]


class AppStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "agent.db"
        with self._conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)

    # -- plumbing -----------------------------------------------------------

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """A write transaction that takes the database lock up front."""
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    # -- operation log ------------------------------------------------------

    def log_op(self, op: str, key: str, attempt: int = 1, detail: str = "") -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO ops (op, key, attempt, detail, at) VALUES (?, ?, ?, ?, ?)",
                (op, key, attempt, detail, now_iso()),
            )

    def ops(
        self, op: str | None = None, key_prefix: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Operations in the order they ran. With ``limit``, only the most recent ones."""
        sql, args = "SELECT id, op, key, attempt, detail, at FROM ops WHERE 1=1", []
        if op:
            sql += " AND op = ?"
            args.append(op)
        if key_prefix:
            sql += " AND key LIKE ?"
            args.append(key_prefix + "%")
        sql += " ORDER BY id DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        with self._conn() as conn:
            return [dict(r) for r in conn.execute(sql, args)][::-1]

    # -- fault injection ----------------------------------------------------

    def set_fault(self, name: str, count: int, mode: str = "error") -> None:
        """Make the next ``count`` calls at fault point ``name`` fail.

        mode="error" raises (a retryable dependency outage); mode="crash" kills
        the worker process on the spot, which is how drills simulate a host
        dying at an exact point.
        """
        if mode not in ("error", "crash"):
            raise ValueError("mode must be 'error' or 'crash'")
        with self._tx() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO faults (name, remaining, mode) VALUES (?, ?, ?)",
                (name, count, mode),
            )

    def clear_faults(self) -> None:
        with self._tx() as conn:
            conn.execute("DELETE FROM faults")

    def faults(self) -> list[dict[str, Any]]:
        with self._conn() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM faults WHERE remaining > 0")]

    # agent(constraint): the point names are listed in FAULT_POINTS (web/static/app.js) and the
    # README drills. Add or rename them in all three places.
    def maybe_fail(self, point: str) -> None:
        with self._tx() as conn:
            row = conn.execute(
                "SELECT remaining, mode FROM faults WHERE name = ? AND remaining > 0", (point,)
            ).fetchone()
            if row is None:
                return
            conn.execute("UPDATE faults SET remaining = remaining - 1 WHERE name = ?", (point,))
            mode = row["mode"]
        if mode == "crash":
            print(f"[fault] simulated worker crash at {point!r}", flush=True)
            os._exit(42)
        raise InjectedFault(f"injected fault at {point!r}")

    # -- interactions and memory -------------------------------------------

    def record_interaction(self, user_id: str, text: str, interaction_id: str | None = None) -> str:
        interaction_id = interaction_id or f"ix-{uuid.uuid4().hex[:12]}"
        with self._tx() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO interactions (interaction_id, user_id, text, created_at) VALUES (?, ?, ?, ?)",
                (interaction_id, user_id, text, now_iso()),
            )
        return interaction_id

    def unprocessed_count(self, user_id: str) -> int:
        with self._conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM interactions WHERE user_id = ? AND batch_id IS NULL",
                (user_id,),
            ).fetchone()[0]

    def current_memory(self, user_id: str) -> tuple[int, list[MemoryNote]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT revision, notes_json FROM memory_revisions WHERE user_id = ? ORDER BY revision DESC LIMIT 1",
                (user_id,),
            ).fetchone()
        if row is None:
            return 0, []
        return row["revision"], [MemoryNote(**n) for n in json.loads(row["notes_json"])]

    def create_or_load_batch(self, batch_id: str, user_id: str, max_interactions: int) -> dict[str, Any]:
        """Create a batch record under a stable ID, or reload it on retry.

        The record pins a fixed snapshot of the unprocessed interactions, so a
        retried preparation works on exactly the same inputs; anything that
        arrives later stays unprocessed for a future batch.
        """
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM memory_batches WHERE batch_id = ?", (batch_id,)).fetchone()
            if row is None:
                ids = [
                    r[0]
                    for r in conn.execute(
                        "SELECT interaction_id FROM interactions WHERE user_id = ? AND batch_id IS NULL"
                        " ORDER BY created_at, interaction_id LIMIT ?",
                        (user_id, max_interactions),
                    )
                ]
                base = conn.execute(
                    "SELECT COALESCE(MAX(revision), 0) FROM memory_revisions WHERE user_id = ?",
                    (user_id,),
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO memory_batches (batch_id, user_id, interaction_ids_json, base_revision,"
                    " status, created_at) VALUES (?, ?, ?, ?, 'prepared', ?)",
                    (batch_id, user_id, json.dumps(ids), base, now_iso()),
                )
                row = conn.execute("SELECT * FROM memory_batches WHERE batch_id = ?", (batch_id,)).fetchone()
        batch = dict(row)
        batch["interaction_ids"] = json.loads(batch.pop("interaction_ids_json"))
        batch["proposal"] = json.loads(batch["proposal_json"]) if batch.pop("proposal_json") else None
        return batch

    def interactions(self, ids: list[str]) -> list[dict[str, Any]]:
        if not ids:
            return []
        marks = ",".join("?" * len(ids))
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT interaction_id, text, created_at FROM interactions WHERE interaction_id IN ({marks})"
                " ORDER BY created_at, interaction_id",
                ids,
            )
            return [dict(r) for r in rows]

    def save_proposal(self, batch_id: str, notes: list[MemoryNote]) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE memory_batches SET proposal_json = ?, status = 'proposed' WHERE batch_id = ?"
                " AND status = 'prepared'",
                (json.dumps([n.__dict__ for n in notes]), batch_id),
            )

    def publish_batch(self, batch_id: str) -> tuple[int, int, bool]:
        """Commit a saved proposal and the interactions it covers in one transaction.

        Returns (revision, covered_interactions, already_published). Idempotent:
        publishing the same batch twice returns the original revision.
        """
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM memory_batches WHERE batch_id = ?", (batch_id,)).fetchone()
            if row is None or row["proposal_json"] is None:
                raise ValueError(f"batch {batch_id} has no saved proposal")
            ids = json.loads(row["interaction_ids_json"])
            if row["status"] == "published":
                return row["published_revision"], len(ids), True
            current = conn.execute(
                "SELECT COALESCE(MAX(revision), 0) FROM memory_revisions WHERE user_id = ?",
                (row["user_id"],),
            ).fetchone()[0]
            marks = ",".join("?" * len(ids)) or "''"
            taken = conn.execute(
                f"SELECT COUNT(*) FROM interactions WHERE interaction_id IN ({marks}) AND batch_id IS NOT NULL",
                ids,
            ).fetchone()[0]
            if current != row["base_revision"] or taken:
                raise StaleBatchError(
                    f"memory for {row['user_id']} is at revision {current}, batch was prepared"
                    f" against {row['base_revision']}"
                )
            revision = current + 1
            conn.execute(
                "INSERT INTO memory_revisions (user_id, revision, notes_json, batch_id, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (row["user_id"], revision, row["proposal_json"], batch_id, now_iso()),
            )
            conn.execute(f"UPDATE interactions SET batch_id = ? WHERE interaction_id IN ({marks})", [batch_id, *ids])
            conn.execute(
                "UPDATE memory_batches SET status = 'published', published_revision = ? WHERE batch_id = ?",
                (revision, batch_id),
            )
            return revision, len(ids), False

    def add_alert(self, kind: str, subject: str, detail: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO operator_alerts (kind, subject, detail, created_at) VALUES (?, ?, ?, ?)",
                (kind, subject, detail, now_iso()),
            )

    def alerts(self) -> list[dict[str, Any]]:
        with self._conn() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM operator_alerts ORDER BY id")]

    # -- reports and outbox -------------------------------------------------

    def upsert_report(self, report_id: str, user_id: str, title: str, body: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO reports (report_id, user_id, title, body, updated_at) VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(report_id) DO UPDATE SET title = excluded.title, body = excluded.body,"
                " updated_at = excluded.updated_at",
                (report_id, user_id, title, body, now_iso()),
            )

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM reports WHERE report_id = ?", (report_id,)).fetchone()
            return dict(row) if row else None

    def latest_report(self, user_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM reports WHERE user_id = ? ORDER BY updated_at DESC LIMIT 1", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    def reports(self, user_id: str, limit: int = 20) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT report_id, title, updated_at FROM reports WHERE user_id = ? ORDER BY updated_at DESC LIMIT ?",
                (user_id, limit),
            )
            return [dict(r) for r in rows]

    def deliver(self, message_id: str, user_id: str, channel: str, body: str) -> bool:
        """Insert into the outbox once per message_id. Returns False for a repeat."""
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO outbox (message_id, user_id, channel, body, created_at) VALUES (?, ?, ?, ?, ?)",
                (message_id, user_id, channel, body, now_iso()),
            )
            return cur.rowcount == 1

    def outbox(self, user_id: str | None = None) -> list[dict[str, Any]]:
        sql, args = "SELECT * FROM outbox", []
        if user_id:
            sql, args = sql + " WHERE user_id = ?", [user_id]
        with self._conn() as conn:
            return [dict(r) for r in conn.execute(sql + " ORDER BY created_at", args)]

    # -- fake compute provider ---------------------------------------------

    def vm_dir(self, vm_id: str) -> Path:
        return self.data_dir / "vms" / vm_id / "workspace"

    def provision_vm(self, request_id: str) -> tuple[str, bool]:
        """Create a VM, deduplicated on request_id. Returns (vm_id, created)."""
        vm_id = "vm-" + hashlib.sha256(request_id.encode()).hexdigest()[:10]
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO vms (vm_id, request_id, status, created_at) VALUES (?, ?, 'running', ?)",
                (vm_id, request_id, now_iso()),
            )
            created = cur.rowcount == 1
        self.vm_dir(vm_id).mkdir(parents=True, exist_ok=True)
        return vm_id, created

    def running_vms(self) -> list[str]:
        with self._conn() as conn:
            return [r[0] for r in conn.execute("SELECT vm_id FROM vms WHERE status = 'running' ORDER BY vm_id")]

    def vm_healthy(self, vm_id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute("SELECT status, healthy FROM vms WHERE vm_id = ?", (vm_id,)).fetchone()
        return bool(row and row["status"] == "running" and row["healthy"])

    def mark_vm_unhealthy(self, vm_id: str) -> None:
        with self._tx() as conn:
            conn.execute("UPDATE vms SET healthy = 0 WHERE vm_id = ?", (vm_id,))

    def destroy_vm(self, vm_id: str) -> None:
        with self._tx() as conn:
            conn.execute("UPDATE vms SET status = 'destroyed' WHERE vm_id = ?", (vm_id,))
        shutil.rmtree(self.vm_dir(vm_id).parent, ignore_errors=True)

    def wipe_vm(self, vm_id: str) -> None:
        shutil.rmtree(self.vm_dir(vm_id), ignore_errors=True)
        self.vm_dir(vm_id).mkdir(parents=True, exist_ok=True)

    # -- snapshot storage ---------------------------------------------------

    def snapshot_dir(self, workspace_id: str, snapshot_id: str) -> Path:
        return self.data_dir / "snapshots" / workspace_id / snapshot_id

    def snapshot_workspace(self, workspace_id: str, vm_id: str, snapshot_id: str) -> int:
        """Copy the VM's workspace to durable storage. Idempotent per snapshot_id."""
        with self._conn() as conn:
            row = conn.execute("SELECT files FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)).fetchone()
        if row:
            return row["files"]
        target = self.snapshot_dir(workspace_id, snapshot_id)
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(self.vm_dir(vm_id), target)
        files = sum(1 for p in target.rglob("*") if p.is_file())
        with self._tx() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO snapshots (snapshot_id, workspace_id, vm_id, files, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (snapshot_id, workspace_id, vm_id, files, now_iso()),
            )
        return files

    def latest_snapshot(self, workspace_id: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT snapshot_id FROM snapshots WHERE workspace_id = ? ORDER BY created_at DESC LIMIT 1",
                (workspace_id,),
            ).fetchone()
        return row["snapshot_id"] if row else None

    def restore_workspace(self, workspace_id: str, vm_id: str) -> str | None:
        snapshot_id = self.latest_snapshot(workspace_id)
        self.wipe_vm(vm_id)
        if snapshot_id:
            shutil.copytree(self.snapshot_dir(workspace_id, snapshot_id), self.vm_dir(vm_id), dirs_exist_ok=True)
        return snapshot_id
