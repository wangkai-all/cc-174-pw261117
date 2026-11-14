"""Reader references: pinned versions that GC must not delete underneath."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .db import Store, event, new_id
from .util import dumps, epoch, iso, loads, now_iso

DEFAULT_READER_TTL = 120.0


def _manifest_file_ids(conn, table_id: str, version_id: str | None) -> tuple[str | None, int | None, list[str]]:
    if not version_id:
        return None, None, []
    row = conn.execute(
        "SELECT * FROM versions WHERE id=? AND table_id=?", (version_id, table_id)
    ).fetchone()
    if not row:
        return version_id, None, []
    manifest = loads(row["manifest_json"], {}) or {}
    return version_id, int(row["seq"]), [f["file_id"] for f in manifest.get("files", [])]


def open_reader(
    store: Store,
    table_id: str,
    version_id: str | None,
    label: str,
    ttl: float = DEFAULT_READER_TTL,
) -> dict[str, Any]:
    """Pin the given version (default: latest committed at open time)."""
    with store.write_txn() as conn:
        if version_id is None:
            latest = store.latest_version(conn, table_id)
            version_id = latest["id"] if latest else None
        resolved, seq, file_ids = _manifest_file_ids(conn, table_id, version_id)
        now_ts = epoch()
        reader_id = new_id("reader")
        conn.execute(
            """INSERT INTO readers(id, table_id, version_id, version_seq, file_ids_json,
                                   label, state, ttl_seconds, opened_at, expires_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (reader_id, table_id, resolved, seq, dumps(file_ids), label, "open",
             ttl, now_iso(), now_ts + ttl),
        )
        event(conn, "info", f"reader {reader_id} pinned",
              {"label": label, "version": resolved, "seq": seq})
    return get_reader(store, reader_id)


def _reader_dict(row: Any) -> dict[str, Any]:
    record = dict(row)
    record["file_ids"] = loads(record.pop("file_ids_json", None), []) or []
    return record


def get_reader(store: Store, reader_id: str) -> dict[str, Any] | None:
    with store.read() as conn:
        row = store.one(conn, "SELECT * FROM readers WHERE id=?", (reader_id,))
        return _reader_dict(row) if row else None


def close_reader(store: Store, reader_id: str) -> dict[str, Any] | None:
    with store.write_txn() as conn:
        row = store.one(conn, "SELECT * FROM readers WHERE id=?", (reader_id,))
        if not row:
            return None
        if row["state"] == "open":
            conn.execute(
                "UPDATE readers SET state='closed', closed_at=? WHERE id=?",
                (now_iso(), reader_id),
            )
            event(conn, "info", f"reader {reader_id} closed")
    return get_reader(store, reader_id)


def expire_readers(store: Store) -> list[str]:
    now_ts = epoch()
    expired: list[str] = []
    with store.write_txn() as conn:
        rows = conn.execute("SELECT * FROM readers WHERE state='open'").fetchall()
        for row in rows:
            if float(row["expires_at"]) <= now_ts:
                conn.execute(
                    "UPDATE readers SET state='expired', closed_at=? WHERE id=?",
                    (now_iso(), row["id"]),
                )
                expired.append(row["id"])
        if expired:
            event(conn, "warn", f"readers expired: {', '.join(expired)}")
    return expired


def active_pins(store: Store, table_id: str) -> dict[str, Any]:
    """File references currently protected by a live (unexpired) reader."""
    now_ts = epoch()
    pinned_files: set[str] = set()
    pinned_versions: set[str] = set()
    readers: list[dict[str, Any]] = []
    with store.read() as conn:
        rows = conn.execute(
            "SELECT * FROM readers WHERE table_id=? AND state='open' ORDER BY opened_at",
            (table_id,),
        ).fetchall()
    for row in rows:
        record = dict(row)
        live = float(record["expires_at"]) > now_ts
        record["live"] = live
        # An open row past its TTL no longer pinpoints anything; report the
        # effective state so the UI does not claim the reference is still live.
        record["effective_state"] = "open" if live else "expired"
        record["remaining_seconds"] = max(0.0, round(float(record["expires_at"]) - now_ts, 1))
        record["file_ids"] = loads(record["file_ids_json"], []) or []
        record["expires_at_iso"] = iso(
            datetime.fromtimestamp(float(record["expires_at"]), timezone.utc)
        )
        readers.append(record)
        if live:
            pinned_files.update(record["file_ids"])
            if record["version_id"]:
                pinned_versions.add(record["version_id"])
    return {
        "readers": readers,
        "pinned_files": sorted(pinned_files),
        "pinned_versions": sorted(pinned_versions),
    }
