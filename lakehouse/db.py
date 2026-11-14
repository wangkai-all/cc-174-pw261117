"""SQLite metadata store: schema, connections and small helpers.

All durable state of the lakehouse lives here: tables, committed versions
(full file manifests), data file records, writer leases, commit intents,
reader references, GC runs, recovery runs and consistency verifications.
The data files themselves are Parquet; no object store is involved.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS tables (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    location TEXT NOT NULL,
    schema_json TEXT NOT NULL,
    partition_columns_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS versions (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL REFERENCES tables(id),
    seq INTEGER NOT NULL,
    parent_id TEXT,
    kind TEXT NOT NULL,
    actor TEXT NOT NULL,
    message TEXT NOT NULL,
    manifest_json TEXT NOT NULL,
    stats_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    retained INTEGER NOT NULL DEFAULT 1,
    expired_at TEXT,
    UNIQUE(table_id, seq)
);

CREATE TABLE IF NOT EXISTS files (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL REFERENCES tables(id),
    rel_path TEXT NOT NULL,
    staging_rel_path TEXT,
    partition_key TEXT NOT NULL,
    partitions_json TEXT NOT NULL,
    rows INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    state TEXT NOT NULL,              -- staging|pending|active|superseded|orphaned|reclaiming|reclaimed
    owner_lease_id TEXT,
    owner_task_id TEXT,
    note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    committed_at TEXT,
    reverted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_table_state ON files(table_id, state);

CREATE TABLE IF NOT EXISTS commit_intents (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    base_version_id TEXT NOT NULL,
    base_seq INTEGER NOT NULL,
    op_json TEXT NOT NULL,
    file_ids_json TEXT NOT NULL,
    state TEXT NOT NULL,              -- pending|committing|committed|conflicted|aborted
    attempts INTEGER NOT NULL DEFAULT 0,
    recomputes INTEGER NOT NULL DEFAULT 0,
    result_version_id TEXT,
    result_seq INTEGER,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    task_id TEXT,
    owner TEXT NOT NULL,
    kind TEXT NOT NULL,               -- writer
    state TEXT NOT NULL,              -- active|released|crashed|expired
    ttl_seconds REAL NOT NULL,
    expires_at REAL NOT NULL,
    heartbeat_at REAL NOT NULL,
    created_at TEXT NOT NULL,
    ended_at TEXT,
    end_reason TEXT
);

CREATE TABLE IF NOT EXISTS readers (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    version_id TEXT,
    version_seq INTEGER,
    file_ids_json TEXT NOT NULL,
    label TEXT NOT NULL,
    state TEXT NOT NULL,              -- open|closed|expired
    ttl_seconds REAL NOT NULL,
    opened_at TEXT NOT NULL,
    expires_at REAL NOT NULL,
    closed_at TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    name TEXT NOT NULL,
    op_json TEXT NOT NULL,
    crash_point TEXT NOT NULL,
    state TEXT NOT NULL,              -- preparing|prepared|committing|committed|conflicted|crashed|failed|cancelled
    phase TEXT NOT NULL,
    lease_id TEXT,
    intent_id TEXT,
    base_version_id TEXT,
    base_seq INTEGER,
    result_version_id TEXT,
    result_seq INTEGER,
    recomputes INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    log_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gc_runs (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    reclaimed_json TEXT NOT NULL,
    skipped_json TEXT NOT NULL,
    verification_before_json TEXT,
    verification_after_json TEXT
);

CREATE TABLE IF NOT EXISTS recovery_runs (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    verification_before_json TEXT,
    verification_after_json TEXT
);

CREATE TABLE IF NOT EXISTS verifications (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    created_at TEXT NOT NULL,
    ok INTEGER NOT NULL,
    result_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    detail_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_id ON events(id DESC);
"""


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Store:
    def __init__(self, path: str):
        self.path = os.path.abspath(path)

    def connect(self) -> sqlite3.Connection:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        conn = sqlite3.connect(
            self.path, timeout=30.0, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def write_txn(self) -> Iterator[sqlite3.Connection]:
        """BEGIN IMMEDIATE serializes metadata commits across writers."""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def one(self, conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Any:
        return conn.execute(sql, params).fetchone()

    def all(self, conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list:
        return conn.execute(sql, params).fetchall()

    def latest_version(self, conn: sqlite3.Connection, table_id: str) -> Any:
        return conn.execute(
            "SELECT * FROM versions WHERE table_id=? ORDER BY seq DESC LIMIT 1",
            (table_id,),
        ).fetchone()


def event(conn: sqlite3.Connection, level: str, message: str, detail: Any = None) -> None:
    from .util import dumps, now_iso

    conn.execute(
        "INSERT INTO events(ts, level, message, detail_json) VALUES(?,?,?,?)",
        (now_iso(), level, message, dumps(detail) if detail is not None else None),
    )


def log_event(store: Store, level: str, message: str, detail: Any = None) -> None:
    with store.write_txn() as conn:
        event(conn, level, message, detail)
