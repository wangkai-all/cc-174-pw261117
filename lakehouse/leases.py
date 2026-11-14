"""Writer leases: time-bounded proofs that an attempted commit is still live."""

from __future__ import annotations

from typing import Any

from .config import DEFAULT_LEASE_TTL
from .db import Store, event, new_id
from .util import epoch, now_iso, pid_alive


def acquire_lease(
    store: Store,
    table_id: str,
    owner: str,
    task_id: str | None,
    ttl: float = DEFAULT_LEASE_TTL,
) -> dict[str, Any]:
    lease_id = new_id("lease")
    now_ts = epoch()
    with store.write_txn() as conn:
        conn.execute(
            """INSERT INTO leases(id, table_id, task_id, owner, kind, state, ttl_seconds,
                                  expires_at, heartbeat_at, created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (lease_id, table_id, task_id, owner, "writer", "active", ttl,
             now_ts + ttl, now_ts, now_iso()),
        )
        event(conn, "info", f"lease {lease_id} acquired for {owner}", {"task_id": task_id})
    return get_lease(store, lease_id)


def get_lease(store: Store, lease_id: str) -> dict[str, Any] | None:
    with store.read() as conn:
        row = store.one(conn, "SELECT * FROM leases WHERE id=?", (lease_id,))
        return dict(row) if row else None


def heartbeat(store: Store, lease_id: str, ttl: float | None = None) -> dict[str, Any] | None:
    now_ts = epoch()
    with store.write_txn() as conn:
        row = store.one(conn, "SELECT * FROM leases WHERE id=?", (lease_id,))
        if not row:
            return None
        ttl = ttl if ttl is not None else float(row["ttl_seconds"])
        if row["state"] != "active":
            event(conn, "warn", f"heartbeat rejected: lease {lease_id} is {row['state']}")
            return dict(row)
        conn.execute(
            "UPDATE leases SET heartbeat_at=?, expires_at=? WHERE id=?",
            (now_ts, now_ts + ttl, lease_id),
        )
        event(conn, "debug", f"lease {lease_id} heartbeat", {"expires_in": ttl})
    return get_lease(store, lease_id)


def release_lease(store: Store, lease_id: str, reason: str = "released") -> None:
    with store.write_txn() as conn:
        conn.execute(
            """UPDATE leases SET state=?, ended_at=?, end_reason=?
               WHERE id=? AND state='active'""",
            (reason, now_iso(), reason, lease_id),
        )


def lease_is_live(lease: dict[str, Any] | None, now_ts: float | None = None) -> bool:
    if not lease:
        return False
    now_ts = now_ts if now_ts is not None else epoch()
    if lease["state"] != "active":
        return False
    return float(lease["expires_at"]) > now_ts


def lease_blocks_reclaim(lease: dict[str, Any] | None, now_ts: float | None = None) -> bool:
    """True while a writer may still legitimately reference its files.

    A lease whose TTL elapsed is still held when its owner process is alive: the
    writer may be paused mid-commit, so only recovery may retire it.  A lease
    whose process is gone and whose TTL elapsed is no longer authoritative; GC
    surfaces it as an expired lease instead of silently treating it as live.
    """
    if not lease or lease["state"] != "active":
        return False
    now_ts = now_ts if now_ts is not None else epoch()
    if float(lease["expires_at"]) > now_ts:
        return True
    return pid_alive(_owner_pid(lease["owner"]))


def expire_stale_leases(store: Store) -> list[dict[str, Any]]:
    """Mark active leases whose deadline passed and whose owner PID is gone."""
    changed: list[dict[str, Any]] = []
    now_ts = epoch()
    with store.write_txn() as conn:
        rows = conn.execute("SELECT * FROM leases WHERE state='active'").fetchall()
        for row in rows:
            if float(row["expires_at"]) > now_ts:
                continue
            owner_pid = _owner_pid(row["owner"])
            if pid_alive(owner_pid):
                # The process is alive but stopped heartbeating; leave it for the
                # demo, otherwise GC could race a live (paused) writer.
                continue
            conn.execute(
                """UPDATE leases SET state='expired', ended_at=?, end_reason=?
                   WHERE id=?""",
                (now_iso(), "ttl elapsed and owner process is gone", row["id"]),
            )
            changed.append(dict(row))
            event(conn, "warn", f"lease {row['id']} expired", {"owner": row["owner"]})
    return changed


def _owner_pid(owner: str) -> int | None:
    """Owner strings look like ``pid:1234:host``."""
    if owner.startswith("pid:"):
        parts = owner.split(":")
        if len(parts) > 1 and parts[1].isdigit():
            return int(parts[1])
    return None


def lease_rows(store: Store, table_id: str) -> list[dict[str, Any]]:
    with store.read() as conn:
        rows = conn.execute(
            "SELECT * FROM leases WHERE table_id=? ORDER BY created_at DESC", (table_id,)
        ).fetchall()
    return [dict(r) for r in rows]
