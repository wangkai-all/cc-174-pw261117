"""Commit protocol, crash recovery, orphan reclamation and verification.

Design in one paragraph
-----------------------
A writer takes a lease, reads the current table version, writes Parquet into a
private staging directory and records a *commit intent* that points at those
files.  Committing is optimistic: the final metadata transaction re-reads the
latest version.  Appends and replacements of disjoint partitions simply rebase
onto the newer version; a replacement that overlaps a partition changed since
the writer's read is recomputed from the new base instead of being written over
it.  Files only become visible through an immutable version manifest, so a
crash before that transaction leaves files that no commit record references.
Reclamation therefore inspects commit records, live writer leases and reader
pins for each candidate before deleting anything.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

from . import data as data_mod
from . import leases as lease_mod
from . import query as query_mod
from . import readers as reader_mod
from .config import DEFAULT_LEASE_TTL, MAX_COMMIT_ATTEMPTS, ROW_CAP
from .db import Store, event, new_id
from .util import dumps, epoch, loads, now_iso, pid_alive, tail

DEFAULT_PARTITIONS = ["us", "eu", "apac"]

CRASH_POINTS = {
    "none": "不注入崩溃",
    "after_write": "崩溃点 ①：Parquet 已写完，提交意图未记录",
    "after_intent": "崩溃点 ②：提交意图已记录，元数据未提交",
    "after_promote": "崩溃点 ③：文件已移到正式路径，元数据未提交",
    "after_commit": "崩溃点 ④：版本已提交，任务收尾前崩溃",
}


class CrashInjected(Exception):
    def __init__(self, point: str):
        super().__init__(f"injected crash at {point}")
        self.point = point


class RetryCommit(Exception):
    """Raised when the metadata version moved between read and write."""


def _rel(table_root: str, absolute: str) -> str:
    return os.path.relpath(absolute, table_root).replace(os.sep, "/")


def _abs(table_root: str, rel: str) -> str:
    return os.path.join(table_root, *rel.split("/"))


class Engine:
    def __init__(self, data_dir: str, db_path: str, lease_ttl: float = DEFAULT_LEASE_TTL):
        self.store = Store(db_path)
        self.data_dir = os.path.abspath(data_dir)
        self.lease_ttl = lease_ttl
        self._write_lock = threading.Lock()
        self.store.init()
        os.makedirs(self.data_dir, exist_ok=True)

    # ================================================================== tables
    def list_tables(self) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            rows = conn.execute("SELECT * FROM tables ORDER BY created_at").fetchall()
        return [self._table_dict(r) for r in rows]

    def _table_dict(self, row: Any) -> dict[str, Any]:
        record = dict(row)
        record["schema"] = loads(record.pop("schema_json"), [])
        record["partition_columns"] = loads(record.pop("partition_columns_json"), [])
        return record

    def get_table(self, name: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = self.store.one(conn, "SELECT * FROM tables WHERE name=?", (name,))
        if not row:
            raise KeyError(f"unknown table {name!r}")
        return self._table_dict(row)

    def create_table(self, name: str, partitions: list[str] | None = None) -> dict[str, Any]:
        partitions = partitions or DEFAULT_PARTITIONS
        table_id = new_id("tbl")
        location = os.path.join(self.data_dir, "tables", name)
        os.makedirs(os.path.join(location, "data"), exist_ok=True)
        os.makedirs(os.path.join(location, "_staging"), exist_ok=True)
        schema = [{"name": f.name, "type": str(f.type)} for f in data_mod.SCHEMA]
        with self.store.write_txn() as conn:
            conn.execute(
                """INSERT INTO tables(id, name, location, schema_json,
                                      partition_columns_json, created_at)
                   VALUES(?,?,?,?,?,?)""",
                (table_id, name, location, dumps(schema),
                 dumps(data_mod.PARTITION_COLUMNS), now_iso()),
            )
            self._seed_bootstrap(conn, table_id, location, partitions)
            event(conn, "info", f"table {name} created",
                  {"table_id": table_id, "partitions": partitions})
        return self.get_table(name)

    def _seed_bootstrap(self, conn: sqlite3.Connection, table_id: str,
                        location: str, partitions: list[str]) -> None:
        entries: list[dict[str, Any]] = []
        created = datetime.now(timezone.utc)
        for part in partitions:
            file_id = new_id("file")
            rel = f"data/region={part}/part-{file_id}.parquet"
            rows = data_mod.generated_rows(
                f"seed:{part}", "seed", "bootstrap", part, 4, 0, created
            )
            abs_path = _abs(location, rel)
            row_count, size = data_mod.write_parquet(abs_path, rows)
            sha = data_mod.file_sha256(abs_path)
            part_key, parts = data_mod.canonical_partitions({"region": part})
            conn.execute(
                """INSERT INTO files(id, table_id, rel_path, staging_rel_path,
                       partition_key, partitions_json, rows, bytes, sha256, state,
                       owner_lease_id, owner_task_id, created_at, updated_at,
                       committed_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (file_id, table_id, rel, None, part_key, dumps(parts), row_count,
                 size, sha, "active", None, None, now_iso(), now_iso(), now_iso()),
            )
            entries.append(
                {
                    "file_id": file_id, "path": rel, "partition_key": part_key,
                    "partitions": parts, "rows": row_count, "bytes": size,
                    "sha256": sha, "writer": "bootstrap",
                }
            )
        manifest = {"files": entries, "touched": list(partitions), "op": "bootstrap",
                    "task_id": None}
        stats = {"rows": sum(e["rows"] for e in entries), "files": len(entries),
                 "partitions": list(partitions), "touched": list(partitions),
                 "op": "bootstrap", "task_id": None, "recomputes": 0, "base_seq": 0,
                 "attempt": 1}
        conn.execute(
            """INSERT INTO versions(id, table_id, seq, parent_id, kind, actor,
                   message, manifest_json, stats_json, created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (new_id("ver"), table_id, 1, None, "bootstrap", "system",
             "bootstrap seed data", dumps(manifest), dumps(stats), now_iso()),
        )

    # ================================================================== tasks
    def prepare_task(self, table_name: str, task_name: str, op: dict[str, Any],
                     crash_point: str = "none", actor: str = "demo") -> dict[str, Any]:
        if crash_point not in CRASH_POINTS:
            raise ValueError(f"unknown crash point {crash_point!r}")
        table = self.get_table(table_name)
        table_id = table["id"]
        location = table["location"]
        op = self._normalise_op(op)
        task_id = new_id("task")
        lease = lease_mod.acquire_lease(
            self.store, table_id, f"pid:{os.getpid()}:web", task_id, self.lease_ttl
        )
        with self.store.write_txn() as conn:
            conn.execute(
                """INSERT INTO tasks(id, table_id, name, op_json, crash_point, state,
                       phase, lease_id, log_json, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (task_id, table_id, task_name, dumps(op), crash_point, "preparing",
                 "acquire_lease", lease["id"], dumps([]), now_iso(), now_iso()),
            )
            latest = self.store.latest_version(conn, table_id)
        self._log_task(task_id, "info",
                       f"lease {lease['id']} 已获取；读取当前表版本 seq {latest['seq']}")
        self._log_task(task_id, "info",
                       f"op {op['type']} partitions={op['partitions']} rows={op['rows']}")

        node = "after_write"
        try:
            written = self._write_staged_files(table, task_id, op, latest)
            self._log_task(
                task_id, "info",
                "已写入临时文件： " + ", ".join(f"{w.staging_rel_path} "
                                                f"({w.rows} 行)" for w in written),
            )
            if crash_point == "after_write":
                raise CrashInjected("after_write")
            node = "after_intent"
            intent_id = new_id("intent")
            with self.store.write_txn() as conn:
                conn.execute(
                    """INSERT INTO commit_intents(id, table_id, task_id, actor,
                           base_version_id, base_seq, op_json, file_ids_json, state,
                           created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (intent_id, table_id, task_id, actor, latest["id"], latest["seq"],
                     dumps(op), dumps([w.file_id for w in written]), "pending",
                     now_iso(), now_iso()),
                )
                conn.execute(
                    """UPDATE tasks SET state='prepared', phase='prepared',
                           intent_id=?, base_version_id=?, base_seq=?, updated_at=?
                       WHERE id=?""",
                    (intent_id, latest["id"], latest["seq"], now_iso(), task_id),
                )
                event(conn, "info", f"task {task_id} prepared on seq {latest['seq']}",
                      {"intent_id": intent_id})
            self._log_task(task_id, "info",
                           f"提交意图 {intent_id} 已记录（pending），"
                           f"基线 seq {latest['seq']}")
            if crash_point == "after_intent":
                raise CrashInjected("after_intent")
            self._log_task(task_id, "info", "任务已准备好，等待提交")
        except CrashInjected as crash:
            self._mark_crashed(task_id, crash.point, "prepare")
            self._log_task(task_id, "error", f"⚡ 注入崩溃：{crash.point}")
        except Exception as exc:
            with self.store.write_txn() as conn:
                conn.execute(
                    "UPDATE tasks SET state='failed', phase='prepare', error=?, "
                    "updated_at=? WHERE id=?",
                    (str(exc), now_iso(), task_id),
                )
                event(conn, "error", f"task {task_id} failed in prepare",
                      {"error": str(exc)})
            self._log_task(task_id, "error", f"准备失败：{exc}")
        return self.get_task(task_id)

    def _normalise_op(self, op: dict[str, Any]) -> dict[str, Any]:
        op_type = str(op.get("type", "append")).lower()
        if op_type not in ("append", "replace"):
            raise ValueError("op type must be append or replace")
        partitions = op.get("partitions") or [op.get("partition") or "us"]
        partitions = [str(p) for p in partitions if str(p)]
        if not partitions:
            partitions = ["us"]
        rows = int(op.get("rows", 3))
        if rows < 0 or rows > ROW_CAP:
            raise ValueError(f"rows must be between 0 and {ROW_CAP}")
        return {"type": op_type, "partitions": sorted(dict.fromkeys(partitions)),
                "rows": rows, "tag": str(op.get("tag", "A"))}

    def _write_staged_files(self, table: dict[str, Any], task_id: str,
                            op: dict[str, Any], base_version: Any) -> list[data_mod.WrittenFile]:
        location = table["location"]
        files: list[data_mod.WrittenFile] = []
        base_rows_by_partition = (
            self._partition_rows(location, loads(base_version["manifest_json"], {}))
            if op["type"] == "replace" else {}
        )
        created = datetime.now(timezone.utc)
        lease_id = self._task_lease_id(task_id)
        for part in op["partitions"]:
            file_id = new_id("file")
            partition_key, partitions = data_mod.canonical_partitions({"region": part})
            rel = f"data/region={part}/part-{file_id}.parquet"
            staging_rel = f"_staging/{task_id}/part-{file_id}.parquet"
            if op["type"] == "append":
                rows = data_mod.generated_rows(
                    task_id, op["tag"], "writer", part, op["rows"],
                    int(base_version["seq"]), created,
                )
            else:
                rows = data_mod.derive_rows(
                    base_rows_by_partition.get(part, []), op["tag"], "writer",
                    int(base_version["seq"]),
                )
            staging_abs = _abs(location, staging_rel)
            row_count, size = data_mod.write_parquet(staging_abs, rows)
            sha = data_mod.file_sha256(staging_abs)
            with self.store.write_txn() as conn:
                conn.execute(
                    """INSERT INTO files(id, table_id, rel_path, staging_rel_path,
                           partition_key, partitions_json, rows, bytes, sha256, state,
                           owner_lease_id, owner_task_id, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (file_id, table["id"], rel, staging_rel, partition_key,
                     dumps(partitions), row_count, size, sha, "staging",
                     lease_id, task_id, now_iso(), now_iso()),
                )
            files.append(
                data_mod.WrittenFile(
                    file_id=file_id, partition_key=partition_key, partitions=partitions,
                    staging_rel_path=staging_rel, rel_path=rel, rows=row_count,
                    bytes=size, sha256=sha,
                )
            )
        return files

    def _partition_rows(self, location: str,
                        manifest: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[str]] = {}
        for entry in manifest.get("files", []):
            grouped.setdefault(entry["partitions"]["region"], []).append(
                _abs(location, entry["path"])
            )
        out: dict[str, list[dict[str, Any]]] = {}
        for part, paths in grouped.items():
            out[part] = data_mod.read_parquet_rows(paths)
        return out

    # ================================================================ commits
    def commit_task(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if task["state"] != "prepared":
            raise ValueError(f"任务 {task_id} 当前状态为 {task['state']}，不能提交")
        intent = self._intent(task["intent_id"])
        table = self._table_by_id(task["table_id"])
        location = table["location"]
        op = loads(intent["op_json"], {})
        lease = lease_mod.get_lease(self.store, task["lease_id"])
        if not lease_mod.lease_is_live(lease):
            raise ValueError("写入租约已失效，请先运行恢复")

        self._log_task(task_id, "info", "开始提交：将临时文件提升到正式路径")
        with self.store.write_txn() as conn:
            conn.execute(
                "UPDATE commit_intents SET state='committing', attempts=attempts+1, "
                "updated_at=? WHERE id=?",
                (now_iso(), intent["id"]),
            )
            conn.execute(
                "UPDATE tasks SET state='committing', phase='promote', updated_at=? "
                "WHERE id=?",
                (now_iso(), task_id),
            )
        self._promote_files(location, loads(intent["file_ids_json"], []))
        self._log_task(task_id, "info", "临时文件已移动到正式数据路径")

        if task["crash_point"] == "after_promote":
            self._mark_crashed(task_id, "after_promote", "commit")
            self._log_task(task_id, "error", "⚡ 注入崩溃：after_promote")
            return self.get_task(task_id)

        try:
            result = self._commit_with_retries(task, intent, table, op)
        except CrashInjected as crash:
            self._mark_crashed(task_id, crash.point, "commit")
            self._log_task(task_id, "error", f"⚡ 注入崩溃：{crash.point}")
            return self.get_task(task_id)
        except Exception as exc:
            with self.store.write_txn() as conn:
                conn.execute(
                    "UPDATE tasks SET state='failed', phase='commit', error=?, "
                    "updated_at=? WHERE id=?",
                    (str(exc), now_iso(), task_id),
                )
                conn.execute(
                    "UPDATE commit_intents SET state='aborted', error=?, updated_at=? "
                    "WHERE id=?",
                    (str(exc), now_iso(), intent["id"]),
                )
                event(conn, "error", f"task {task_id} commit failed",
                      {"error": str(exc)})
            lease_mod.release_lease(self.store, lease["id"], "released")
            self._log_task(task_id, "error", f"提交失败：{exc}")
            return self.get_task(task_id)

        with self.store.write_txn() as conn:
            conn.execute(
                """UPDATE tasks SET state='committed', phase='done',
                       result_version_id=?, result_seq=?, recomputes=?, updated_at=?
                   WHERE id=?""",
                (result["id"], result["seq"], result["recomputes"], now_iso(),
                 task_id),
            )
            event(conn, "info", f"task {task_id} committed seq {result['seq']}",
                  {"version_id": result["id"]})
        lease_mod.release_lease(self.store, lease["id"], "released")
        self._log_task(task_id, "info",
                       f"✅ 已提交版本 seq {result['seq']}（共 {result['rows']} 行）")
        return self.get_task(task_id)

    def commit_many(self, task_ids: list[str], order: str = "sequential") -> dict[str, Any]:
        """Commit several prepared tasks; ``concurrent`` races them in threads.

        Tasks that are no longer ``prepared`` (already committed, crashed,
        aborted) are reported as ``skipped`` instead of failing the batch.
        """
        results: dict[str, Any] = {}
        errors: dict[str, str] = {}
        skipped: dict[str, str] = {}

        def run(tid: str) -> None:
            try:
                self.commit_task(tid)
            except Exception as exc:  # keep batch endpoint alive
                errors[tid] = str(exc)

        runnable: list[str] = []
        for tid in task_ids:
            try:
                task = self.get_task(tid)
            except KeyError:
                skipped[tid] = "任务不存在"
                continue
            if task["state"] != "prepared":
                skipped[tid] = f"任务状态为 {task['state']}，已跳过"
                continue
            runnable.append(tid)

        if order == "concurrent":
            threads = [threading.Thread(target=run, args=(t,), daemon=True)
                       for t in runnable]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        else:
            for t in runnable:
                run(t)
        for tid in task_ids:
            if tid in skipped:
                continue
            try:
                results[tid] = self.get_task(tid)
            except KeyError:
                results[tid] = {"id": tid, "state": "missing"}
        return {"tasks": results, "errors": errors, "skipped": skipped}

    def _promote_files(self, location: str, file_ids: list[str]) -> None:
        for file_id in file_ids:
            with self.store.read() as conn:
                row = self.store.one(conn, "SELECT * FROM files WHERE id=?", (file_id,))
            if not row:
                continue
            if row["staging_rel_path"]:
                src = _abs(location, row["staging_rel_path"])
                dst = _abs(location, row["rel_path"])
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                if os.path.exists(src):
                    os.replace(src, dst)
                with self.store.write_txn() as conn:
                    conn.execute(
                        "UPDATE files SET staging_rel_path=NULL, state='pending', "
                        "updated_at=? WHERE id=?",
                        (now_iso(), file_id),
                    )

    def _commit_with_retries(self, task: Any, intent: Any, table: dict[str, Any],
                             op: dict[str, Any]) -> dict[str, Any]:
        table_id = table["id"]
        base_seq = int(intent["base_seq"])
        base_version_id = intent["base_version_id"]
        recomputes = int(intent["recomputes"])
        file_ids = loads(intent["file_ids_json"], [])

        for attempt in range(1, MAX_COMMIT_ATTEMPTS + 1):
            with self.store.read() as conn:
                latest = self.store.latest_version(conn, table_id)
            if int(latest["seq"]) != base_seq:
                changed = self._changed_partitions(table_id, base_seq)
                overlap = sorted(set(changed) & set(op["partitions"]))
                if op["type"] == "replace" and overlap:
                    recomputes += 1
                    self._log_task(
                        task["id"], "warn",
                        f"冲突：seq {base_seq}→{latest['seq']} 期间分区 {overlap} "
                        "被其他写入者更新；按新基线重算，拒绝直接覆盖",
                    )
                    self._recompute_files(task, table, op, latest, file_ids)
                else:
                    self._log_task(
                        task["id"], "info",
                        f"重试合并：基线 seq {base_seq}→{latest['seq']}，"
                        f"期间变更分区 {changed}，本任务分区 {op['partitions']} "
                        f"不冲突，合并到新版本",
                    )
                base_seq = int(latest["seq"])
                base_version_id = latest["id"]
                with self.store.write_txn() as conn:
                    conn.execute(
                        """UPDATE commit_intents SET base_version_id=?, base_seq=?,
                               attempts=?, recomputes=?, updated_at=? WHERE id=?""",
                        (base_version_id, base_seq, attempt, recomputes,
                         now_iso(), intent["id"]),
                    )
                continue

            try:
                version = self._commit_txn(
                    task, intent, table, op, base_seq, base_version_id, file_ids,
                    recomputes, attempt,
                )
            except RetryCommit:
                self._log_task(task["id"], "warn",
                               "提交事务发现元数据已移动，重新读取版本后重试")
                continue
            return version

        raise RuntimeError(
            f"提交在 {MAX_COMMIT_ATTEMPTS} 次尝试后仍未达成（竞争过激）"
        )

    def _commit_txn(self, task: Any, intent: Any, table: dict[str, Any],
                    op: dict[str, Any], base_seq: int, base_version_id: str,
                    file_ids: list[str], recomputes: int, attempt: int) -> dict[str, Any]:
        table_id = table["id"]
        with self.store.write_txn() as conn:
            latest = self.store.latest_version(conn, table_id)
            if int(latest["seq"]) != base_seq or latest["id"] != base_version_id:
                raise RetryCommit()
            prev_manifest = loads(latest["manifest_json"], {}) or {"files": []}
            op_parts = set(op["partitions"])
            kept: list[dict[str, Any]] = []
            dropped: list[dict[str, Any]] = []
            for entry in prev_manifest.get("files", []):
                if op["type"] == "replace" and entry["partitions"].get("region") in op_parts:
                    dropped.append(entry)
                else:
                    kept.append(entry)
            new_entries: list[dict[str, Any]] = []
            for file_id in file_ids:
                row = self.store.one(conn, "SELECT * FROM files WHERE id=?", (file_id,))
                if not row:
                    raise RuntimeError(f"待提交文件 {file_id} 已不存在")
                if row["state"] not in ("pending", "staging"):
                    raise RuntimeError(f"待提交文件 {file_id} 状态异常：{row['state']}")
                new_entries.append(
                    {
                        "file_id": row["id"], "path": row["rel_path"],
                        "partition_key": row["partition_key"],
                        "partitions": loads(row["partitions_json"], {}),
                        "rows": row["rows"], "bytes": row["bytes"],
                        "sha256": row["sha256"], "writer": task["name"],
                    }
                )
            manifest_files = kept + new_entries
            total_rows = sum(int(f["rows"]) for f in manifest_files)
            manifest = {"files": manifest_files, "touched": sorted(op_parts),
                        "op": op["type"], "task_id": task["id"]}
            seq = int(latest["seq"]) + 1
            version_id = new_id("ver")
            stats = {
                "rows": total_rows, "files": len(manifest_files),
                "partitions": sorted({f["partitions"].get("region")
                                      for f in manifest_files}),
                "touched": sorted(op_parts), "op": op["type"],
                "task_id": task["id"], "recomputes": recomputes,
                "base_seq": base_seq, "attempt": attempt,
            }
            now_ts = now_iso()
            conn.execute(
                """INSERT INTO versions(id, table_id, seq, parent_id, kind, actor,
                       message, manifest_json, stats_json, created_at, retained)
                   VALUES(?,?,?,?,?,?,?,?,?,?,1)""",
                (version_id, table_id, seq, latest["id"], op["type"], task["name"],
                 f"{op['type']} {', '.join(op['partitions'])} via {task['name']}",
                 dumps(manifest), dumps(stats), now_ts),
            )
            for entry in dropped:
                conn.execute(
                    "UPDATE files SET state='superseded', updated_at=? "
                    "WHERE id=? AND state='active'",
                    (now_ts, entry["file_id"]),
                )
            for entry in new_entries:
                conn.execute(
                    "UPDATE files SET state='active', staging_rel_path=NULL, "
                    "committed_at=?, updated_at=? WHERE id=?",
                    (now_ts, now_ts, entry["file_id"]),
                )
            conn.execute(
                """UPDATE commit_intents SET state='committed', result_version_id=?,
                       result_seq=?, recomputes=?, attempts=?, updated_at=?
                   WHERE id=?""",
                (version_id, seq, recomputes, attempt, now_ts, intent["id"]),
            )
            conn.execute(
                """UPDATE tasks SET phase='metadata_committed', recomputes=?,
                       updated_at=? WHERE id=?""",
                (recomputes, now_ts, task["id"]),
            )
            event(conn, "info", f"version seq {seq} committed",
                  {"task": task["id"], "rows": total_rows, "recomputes": recomputes,
                   "attempt": attempt})
        if task["crash_point"] == "after_commit":
            raise CrashInjected("after_commit")
        return {"id": version_id, "seq": seq, "rows": total_rows,
                "recomputes": recomputes}

    def _recompute_files(self, task: Any, table: dict[str, Any],
                         op: dict[str, Any], latest: Any, file_ids: list[str]) -> None:
        location = table["location"]
        base_rows = self._partition_rows(location, loads(latest["manifest_json"], {}))
        with self.store.read() as conn:
            rows_by_id = {
                fid: dict(self.store.one(conn, "SELECT * FROM files WHERE id=?", (fid,)))
                for fid in file_ids
            }
        for file_id, row in rows_by_id.items():
            if not row:
                continue
            part = (loads(row["partitions_json"], {}) or {}).get("region")
            derived = data_mod.derive_rows(
                base_rows.get(part, []), op["tag"], "writer", int(latest["seq"])
            )
            target_rel = row["staging_rel_path"] or row["rel_path"]
            target_abs = _abs(location, target_rel)
            row_count, size = data_mod.write_parquet(target_abs, derived)
            sha = data_mod.file_sha256(target_abs)
            with self.store.write_txn() as conn:
                conn.execute(
                    """UPDATE files SET rows=?, bytes=?, sha256=?, note=?,
                           updated_at=? WHERE id=?""",
                    (row_count, size, sha, f"recomputed on base seq {latest['seq']}",
                     now_iso(), file_id),
                )
            self._log_task(
                task["id"], "warn",
                f"重算 {row['rel_path']}：基于 seq {latest['seq']}，"
                f"{row_count} 行，sha {sha[:12]}",
            )

    def _changed_partitions(self, table_id: str, since_seq: int) -> list[str]:
        with self.store.read() as conn:
            rows = conn.execute(
                "SELECT stats_json FROM versions WHERE table_id=? AND seq>? ORDER BY seq",
                (table_id, since_seq),
            ).fetchall()
        changed: list[str] = []
        for row in rows:
            stats = loads(row["stats_json"], {}) or {}
            changed.extend(stats.get("touched", []))
        return sorted(set(changed))

    # ============================================================== recovery
    def recover(self, table_name: str) -> dict[str, Any]:
        table = self.get_table(table_name)
        table_id = table["id"]
        before = self.verify(table_name, phase="before_recovery")
        actions: list[dict[str, Any]] = []
        with self.store.write_txn() as conn:
            tasks = conn.execute(
                """SELECT * FROM tasks WHERE table_id=?
                   AND state IN ('crashed','committing') ORDER BY created_at""",
                (table_id,),
            ).fetchall()
            for task_row in tasks:
                task = dict(task_row)
                intent = None
                if task["intent_id"]:
                    irow = conn.execute(
                        "SELECT * FROM commit_intents WHERE id=?", (task["intent_id"],)
                    ).fetchone()
                    intent = dict(irow) if irow else None
                if intent and intent["state"] == "committed" and intent["result_version_id"]:
                    conn.execute(
                        """UPDATE tasks SET state='committed', phase='recovered',
                               result_version_id=?, result_seq=?, updated_at=?
                           WHERE id=?""",
                        (intent["result_version_id"], intent["result_seq"],
                         now_iso(), task["id"]),
                    )
                    actions.append({
                        "task_id": task["id"], "action": "finalized_committed_task",
                        "detail": f"seq {intent['result_seq']} 的元数据已持久化；"
                                  "任务与租约被重新对齐，无需回滚",
                    })
                elif intent and intent["state"] in ("pending", "committing"):
                    conn.execute(
                        """UPDATE commit_intents SET state='aborted',
                               error=?, updated_at=? WHERE id=?""",
                        ("recovery: 写入者在元数据提交前崩溃", now_iso(), intent["id"]),
                    )
                    file_ids = loads(intent["file_ids_json"], [])
                    for file_id in file_ids:
                        conn.execute(
                            """UPDATE files SET state='orphaned', updated_at=?,
                                   note=? WHERE id=? AND state IN ('staging','pending')""",
                            (now_iso(), "recovery: 无提交记录，标记为孤儿", file_id),
                        )
                    conn.execute(
                        """UPDATE tasks SET state='aborted', phase='recovered',
                               error=?, updated_at=? WHERE id=?""",
                        ("未提交的意图已中止；文件转为孤儿", now_iso(), task["id"]),
                    )
                    actions.append({
                        "task_id": task["id"], "action": "aborted_uncommitted_intent",
                        "detail": f"提交意图 {intent['id']} 没有对应版本；"
                                  f"{len(file_ids)} 个文件标记为孤儿",
                    })
                else:
                    conn.execute(
                        """UPDATE tasks SET state='failed', phase='recovered',
                               error=coalesce(error, '崩溃发生在提交意图之前'),
                               updated_at=? WHERE id=?""",
                        (now_iso(), task["id"]),
                    )
                    actions.append({
                        "task_id": task["id"], "action": "marked_failed",
                        "detail": "没有提交意图，无需回滚；临时文件将在回收时清理",
                    })
                if task["lease_id"]:
                    conn.execute(
                        """UPDATE leases SET state='crashed', ended_at=?, end_reason=?
                           WHERE id=? AND state='active'""",
                        (now_iso(), "recovery: 写入者崩溃", task["lease_id"]),
                    )
                    actions.append({
                        "task_id": task["id"], "action": "retired_crashed_lease",
                        "detail": f"租约 {task['lease_id']} 已退役，不再阻止回收",
                    })

            # Orphan sweep: files that no manifest and no live intent reference
            # become explicit reclamation candidates.
            versioned = self._all_versioned_file_ids(conn, table_id)
            live_intent_files = self._live_intent_file_ids(conn)
            candidates = conn.execute(
                "SELECT id FROM files WHERE table_id=? AND state IN ('staging','pending')",
                (table_id,),
            ).fetchall()
            marked = 0
            for cand in candidates:
                if cand["id"] in versioned or cand["id"] in live_intent_files:
                    continue
                conn.execute(
                    """UPDATE files SET state='orphaned', updated_at=?,
                           note='orphan sweep during recovery' WHERE id=?""",
                    (now_iso(), cand["id"]),
                )
                marked += 1
            if marked:
                actions.append({
                    "task_id": None, "action": "orphan_sweep",
                    "detail": f"{marked} 个无引用文件被标记为孤儿",
                })

            now_ts = epoch()
            expired = 0
            for row in conn.execute(
                "SELECT * FROM leases WHERE table_id=? AND state='active'", (table_id,)
            ).fetchall():
                if float(row["expires_at"]) <= now_ts and not pid_alive(
                    self._pid_from_owner(row["owner"])
                ):
                    conn.execute(
                        """UPDATE leases SET state='expired', ended_at=?, end_reason=?
                           WHERE id=?""",
                        (now_iso(), "TTL 到期且进程已不在", row["id"]),
                    )
                    expired += 1
            if expired:
                actions.append({"task_id": None, "action": "expired_stale_leases",
                                "detail": f"{expired} 个租约因 TTL 到期被标记"})
            run_id = new_id("recovery")
            conn.execute(
                """INSERT INTO recovery_runs(id, table_id, created_at, actions_json,
                       verification_before_json) VALUES(?,?,?,?,?)""",
                (run_id, table_id, now_iso(), dumps(actions), dumps(before)),
            )
            event(conn, "warn", f"recovery {run_id} completed",
                  {"actions": len(actions)})
        after = self.verify(table_name, phase="after_recovery")
        with self.store.write_txn() as conn:
            conn.execute(
                "UPDATE recovery_runs SET verification_after_json=? WHERE id=?",
                (dumps(after), run_id),
            )
        return {"run_id": run_id, "actions": actions, "before": before,
                "after": after,
                "consistent": bool(before.get("ok") and after.get("ok"))}

    @staticmethod
    def _pid_from_owner(owner: str) -> int | None:
        parts = owner.split(":")
        if len(parts) > 1 and parts[0] == "pid" and parts[1].isdigit():
            return int(parts[1])
        return None

    def _all_versioned_file_ids(self, conn: sqlite3.Connection,
                                table_id: str, retained_only: bool = False) -> set[str]:
        sql = "SELECT manifest_json FROM versions WHERE table_id=?"
        if retained_only:
            sql += " AND retained=1"
        ids: set[str] = set()
        for row in conn.execute(sql, (table_id,)).fetchall():
            manifest = loads(row["manifest_json"], {}) or {}
            for entry in manifest.get("files", []):
                ids.add(entry["file_id"])
        return ids

    def _live_intent_file_ids(self, conn: sqlite3.Connection) -> set[str]:
        ids: set[str] = set()
        for row in conn.execute(
            "SELECT file_ids_json FROM commit_intents WHERE state IN ('pending','committing')"
        ).fetchall():
            ids.update(loads(row["file_ids_json"], []) or [])
        return ids

    # ================================================================== GC
    def _expire_version_plan(self, conn: sqlite3.Connection, table_id: str,
                             retain_versions: int | None) -> tuple[list[dict[str, Any]],
                                                                   list[dict[str, Any]]]:
        if retain_versions is None:
            return [], []
        rows = conn.execute(
            "SELECT id, seq FROM versions WHERE table_id=? AND retained=1 ORDER BY seq",
            (table_id,),
        ).fetchall()
        if not rows:
            return [], []
        pinned = set(reader_mod.active_pins(self.store, table_id)["pinned_versions"])
        cutoff = int(rows[-1]["seq"]) - max(0, int(retain_versions))
        to_expire: list[dict[str, Any]] = []
        pinned_out: list[dict[str, Any]] = []
        for row in rows:
            if int(row["seq"]) > cutoff:
                continue
            (pinned_out if row["id"] in pinned else to_expire).append(dict(row))
        return to_expire, pinned_out

    def gc_scan(self, table_name: str, retain_versions: int | None = None) -> dict[str, Any]:
        table = self.get_table(table_name)
        table_id = table["id"]
        location = table["location"]
        now_ts = epoch()
        with self.store.read() as conn:
            pins = reader_mod.active_pins(self.store, table_id)
            to_expire, pinned_versions = self._expire_version_plan(
                conn, table_id, retain_versions)
            # Policy candidates include pinned versions: those stay readable, but
            # their files are reported as held by a reader rather than by the
            # retention window, so the three protection reasons stay visible.
            policy_candidates = to_expire + pinned_versions
            expired_ids = ({v["id"] for v in policy_candidates}
                           if retain_versions is not None else set())
            retained_ids: set[str] = set()
            for row in conn.execute(
                "SELECT id, retained, manifest_json FROM versions WHERE table_id=?",
                (table_id,),
            ).fetchall():
                if int(row["retained"]) == 1 and row["id"] not in expired_ids:
                    manifest = loads(row["manifest_json"], {}) or {}
                    retained_ids.update(e["file_id"] for e in manifest.get("files", []))
            live_intents = self._live_intent_file_ids(conn)
            live_leases = {
                r["id"]: dict(r)
                for r in conn.execute(
                    "SELECT * FROM leases WHERE table_id=? AND state='active'", (table_id,)
                ).fetchall()
                if lease_mod.lease_blocks_reclaim(dict(r), now_ts)
            }
            readers = pins["readers"]
            file_rows = conn.execute(
                """SELECT * FROM files WHERE table_id=? AND state IN
                   ('staging','pending','superseded','orphaned')
                   ORDER BY created_at""",
                (table_id,),
            ).fetchall()
            known_paths = {
                r["rel_path"] for r in conn.execute(
                    "SELECT rel_path FROM files WHERE table_id=?", (table_id,)
                ).fetchall()
            } | {
                r["staging_rel_path"] for r in conn.execute(
                    "SELECT staging_rel_path FROM files WHERE table_id=?", (table_id,)
                ).fetchall() if r["staging_rel_path"]
            }
        pinned_files = set(pins["pinned_files"])
        candidates: list[dict[str, Any]] = []
        for row in file_rows:
            file_row = dict(row)
            entry = {
                "kind": "registered", "file_id": file_row["id"],
                "path": file_row["rel_path"],
                "staging_path": file_row["staging_rel_path"],
                "state": file_row["state"], "rows": file_row["rows"],
                "bytes": file_row["bytes"],
                "owner_task_id": file_row["owner_task_id"],
                "owner_lease_id": file_row["owner_lease_id"],
                "note": file_row["note"],
                "exists": self._file_exists(location, file_row),
            }
            # Ordering matters for the UI: report the most specific reason that
            # keeps a file alive.  A live reader pin is called out even when the
            # version would also be kept by the retention window.
            if file_row["id"] in live_intents:
                entry.update(protected=True, reason="pending_intent",
                             detail="某个仍有效的提交意图持有该文件")
            elif file_row["owner_lease_id"] in live_leases:
                entry.update(protected=True, reason="active_lease",
                             detail=f"写入租约 {file_row['owner_lease_id']} 仍然活跃")
            elif file_row["id"] in pinned_files:
                entry.update(protected=True, reason="reader_pin",
                             detail="有活跃读者引用了该文件")
            elif file_row["id"] in retained_ids:
                entry.update(protected=True, reason="commit_record",
                             detail="被一个保留中的已提交版本清单引用")
            else:
                entry.update(protected=False, reason="reclaimable",
                             detail="无提交记录、活跃租约或读者引用")
            candidates.append(entry)

        for rel in self._disk_files(location):
            if rel in known_paths:
                continue
            abs_path = _abs(location, rel)
            entry = {
                "kind": "unregistered", "file_id": None, "path": rel,
                "staging_path": None, "state": "unregistered", "rows": None,
                "bytes": os.path.getsize(abs_path), "owner_task_id": None,
                "owner_lease_id": None, "note": None, "exists": True,
                "protected": False, "reason": "reclaimable",
                "detail": "磁盘上存在但没有任何元数据记录的文件",
            }
            owner_task = self._task_from_staging_path(rel)
            if owner_task:
                entry["owner_task_id"] = owner_task
                lease_id = self._task_lease_id(owner_task)
                lease = live_leases.get(lease_id)
                task_state = self._task_state(owner_task)
                if lease and task_state not in ("crashed", "aborted", "failed", "committed"):
                    entry.update(protected=True, reason="active_lease",
                                 detail=f"正在写入的任务 {owner_task} 持有租约 {lease_id}")
                else:
                    entry["detail"] = f"任务 {owner_task} 的残留临时文件"
            candidates.append(entry)
        reclaimable = [c for c in candidates if not c["protected"]]
        protected = [c for c in candidates if c["protected"]]
        return {
            "table": table_name, "candidates": candidates,
            "reclaimable": reclaimable, "protected": protected,
            "reclaimable_bytes": sum(int(c.get("bytes") or 0) for c in reclaimable),
            "retain_versions": retain_versions,
            "versions_to_expire": to_expire,
            "versions_pinned": pinned_versions,
            "leases": [dict(r) for r in live_leases.values()],
            "readers": readers,
            "checked_at": now_iso(),
        }

    def _task_from_staging_path(self, rel: str) -> str | None:
        parts = rel.split("/")
        if len(parts) >= 3 and parts[0] == "_staging":
            return parts[1]
        return None

    def _task_state(self, task_id: str) -> str | None:
        with self.store.read() as conn:
            row = self.store.one(conn, "SELECT state FROM tasks WHERE id=?", (task_id,))
        return row["state"] if row else None

    def _file_exists(self, location: str, row: Any) -> bool:
        if row["staging_rel_path"] and os.path.exists(_abs(location, row["staging_rel_path"])):
            return True
        return os.path.exists(_abs(location, row["rel_path"]))

    def _disk_files(self, location: str) -> list[str]:
        found: list[str] = []
        for root, dirs, files in os.walk(location):
            dirs.sort()
            for name in sorted(files):
                if name.endswith(".parquet"):
                    found.append(_rel(location, os.path.join(root, name)))
        return found

    def gc_reclaim(self, table_name: str, retain_versions: int | None = None,
                   reason_note: str = "") -> dict[str, Any]:
        table = self.get_table(table_name)
        table_id = table["id"]
        location = table["location"]
        scan = self.gc_scan(table_name, retain_versions)
        before = self.verify(table_name, phase="before_gc", retain_versions=retain_versions)
        if not before.get("ok"):
            # Deleting anything while a committed version is already broken would
            # destroy the evidence needed to diagnose it.
            return {
                "ok": False,
                "error": "回收前一致性核对未通过，拒绝删除任何文件",
                "before": before,
                "reclaimed": [],
                "skipped": [],
                "consistent": False,
            }
        reclaimed: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        expired_versions: list[dict[str, Any]] = []
        with self.store.write_txn() as conn:
            # Re-check every protection inside the write transaction so a lease
            # renewal or reader pin that just landed cannot be raced.
            for version in scan["versions_to_expire"]:
                conn.execute(
                    "UPDATE versions SET retained=0, expired_at=? WHERE id=?",
                    (now_iso(), version["id"]),
                )
                expired_versions.append(version)
            retained_ids = self._all_versioned_file_ids(conn, table_id, retained_only=True)
            live_intents = self._live_intent_file_ids(conn)
            now_ts = epoch()
            live_leases = {
                r["id"] for r in conn.execute(
                    "SELECT * FROM leases WHERE table_id=? AND state='active'", (table_id,)
                ).fetchall()
                if lease_mod.lease_blocks_reclaim(dict(r), now_ts)
            }
            pinned = set(reader_mod.active_pins(self.store, table_id)["pinned_files"])
            for cand in scan["candidates"]:
                if cand["protected"]:
                    skipped.append(cand)
                    continue
                file_id = cand["file_id"]
                if file_id:
                    row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
                    if not row:
                        skipped.append(cand)
                        continue
                    if row["state"] in ("active",):
                        skipped.append({**cand, "protected": True, "reason": "commit_record",
                                        "detail": "文件在回收事务期间变为活跃"})
                        continue
                    if file_id in retained_ids:
                        skipped.append({**cand, "protected": True, "reason": "commit_record",
                                        "detail": "回收期间该文件出现在保留版本清单中"})
                        continue
                    if file_id in live_intents:
                        skipped.append({**cand, "protected": True, "reason": "pending_intent",
                                        "detail": "回收期间提交意图变为有效"})
                        continue
                    if row["owner_lease_id"] in live_leases:
                        skipped.append({**cand, "protected": True, "reason": "active_lease",
                                        "detail": "回收期间租约被续期"})
                        continue
                    if file_id in pinned:
                        skipped.append({**cand, "protected": True, "reason": "reader_pin",
                                        "detail": "回收期间读者引用该文件"})
                        continue
                removed_count = 0
                delete_error = None
                for rel in (cand["path"], cand.get("staging_path")):
                    if not rel:
                        continue
                    abs_path = _abs(location, rel)
                    try:
                        if os.path.exists(abs_path):
                            os.remove(abs_path)
                            removed_count += 1
                    except OSError as exc:
                        delete_error = str(exc)
                if delete_error:
                    skipped.append({**cand, "protected": False,
                                    "reason": "delete_failed", "detail": delete_error})
                    continue
                if file_id:
                    conn.execute(
                        """UPDATE files SET state='reclaimed', staging_rel_path=NULL,
                               note=?, updated_at=? WHERE id=?""",
                        (reason_note or "reclaimed by GC", now_iso(), file_id),
                    )
                    self._remove_empty_staging_dirs(location, cand.get("staging_path"))
                reclaimed.append({**cand, "deleted_paths": removed_count})
            run_id = new_id("gc")
            conn.execute(
                """INSERT INTO gc_runs(id, table_id, created_at, reclaimed_json,
                       skipped_json, verification_before_json) VALUES(?,?,?,?,?,?)""",
                (run_id, table_id, now_iso(), dumps(reclaimed), dumps(skipped),
                 dumps(before)),
            )
            event(conn, "warn", f"GC {run_id} reclaimed {len(reclaimed)} file(s)",
                  {"skipped": len(skipped),
                   "bytes": sum(int(c.get("bytes") or 0) for c in reclaimed),
                   "expired_versions": [v["seq"] for v in expired_versions]})
        after = self.verify(table_name, phase="after_gc", retain_versions=retain_versions)
        with self.store.write_txn() as conn:
            conn.execute(
                "UPDATE gc_runs SET verification_after_json=? WHERE id=?",
                (dumps(after), run_id),
            )
        return {
            "ok": True, "run_id": run_id, "reclaimed": reclaimed, "skipped": skipped,
            "expired_versions": expired_versions, "before": before, "after": after,
            "consistent": bool(before.get("ok") and after.get("ok")),
            "pending_after": self.gc_scan(table_name, retain_versions),
        }

    @staticmethod
    def _remove_empty_staging_dirs(location: str, staging_rel: str | None) -> None:
        if not staging_rel:
            return
        path = os.path.dirname(_abs(location, staging_rel))
        try:
            if os.path.isdir(path) and not os.listdir(path):
                os.rmdir(path)
        except OSError:
            pass

    # ========================================================= verification
    def verify(self, table_name: str, phase: str = "manual",
               retain_versions: int | None = None) -> dict[str, Any]:
        table = self.get_table(table_name)
        table_id = table["id"]
        location = table["location"]
        checks: list[dict[str, Any]] = []

        def check(name: str, ok: bool, detail: Any = None) -> None:
            checks.append({"name": name, "ok": bool(ok), "detail": detail})

        with self.store.read() as conn:
            integrity = conn.execute("PRAGMA quick_check").fetchone()[0]
            check("sqlite_integrity", integrity == "ok", integrity)
            versions = [
                dict(r) for r in conn.execute(
                    "SELECT * FROM versions WHERE table_id=? ORDER BY seq", (table_id,)
                ).fetchall()
            ]
            file_rows = {
                r["id"]: dict(r)
                for r in conn.execute(
                    "SELECT * FROM files WHERE table_id=?", (table_id,)
                ).fetchall()
            }
        seqs = [int(v["seq"]) for v in versions]
        check("version_sequence", seqs == list(range(1, len(seqs) + 1)), {"seqs": seqs})
        parent_ok = all(
            (i == 0 and versions[i]["parent_id"] is None)
            or (i > 0 and versions[i]["parent_id"] == versions[i - 1]["id"])
            for i in range(len(versions))
        )
        check("version_parent_chain", parent_ok)
        if not versions:
            check("has_version", False, "表没有已提交版本")
            return self._finish_verification(table_id, phase, checks, 0, 0)

        retained_versions = [v for v in versions if int(v["retained"]) == 1]
        expired_versions = [v for v in versions if int(v["retained"]) == 0]
        check("has_retained_version", bool(retained_versions),
              f"{len(retained_versions)} 个保留版本，{len(expired_versions)} 个已修剪")

        manifest_ok = True
        problems: list[dict[str, Any]] = []
        missing: list[dict[str, Any]] = []
        fingerprints: dict[str, str] = {}
        refs_checked = 0
        for version in retained_versions:
            manifest = loads(version["manifest_json"], {}) or {}
            stats = loads(version["stats_json"], {}) or {}
            entries = manifest.get("files", [])
            refs_checked += len(entries)
            expected = int(stats.get("rows", 0))
            if sum(int(e["rows"]) for e in entries) != expected:
                manifest_ok = False
                problems.append({"seq": version["seq"], "problem": "行数与清单不一致",
                                 "manifest_rows": sum(int(e["rows"]) for e in entries),
                                 "stats_rows": expected})
            paths: list[str] = []
            for entry in entries:
                abs_path = _abs(location, entry["path"])
                if not os.path.isfile(abs_path):
                    missing.append({"seq": version["seq"], "path": entry["path"]})
                    continue
                actual = os.path.getsize(abs_path)
                if actual != int(entry["bytes"]):
                    problems.append({"seq": version["seq"], "path": entry["path"],
                                     "problem": "文件大小与清单不一致",
                                     "manifest_bytes": entry["bytes"], "disk_bytes": actual})
                    manifest_ok = False
                record = file_rows.get(entry["file_id"])
                if not record:
                    problems.append({"seq": version["seq"], "path": entry["path"],
                                     "problem": "清单文件没有元数据行"})
                    manifest_ok = False
                elif record["sha256"] != entry["sha256"]:
                    problems.append({"seq": version["seq"], "path": entry["path"],
                                     "problem": "清单与文件行的 sha256 不一致"})
                    manifest_ok = False
                paths.append(abs_path)
            if not missing and paths:
                try:
                    fingerprints[str(version["seq"])] = query_mod.fingerprint(paths)
                except FileNotFoundError:
                    pass
        check("retained_manifest_files_present", not missing, missing[:10])
        check("retained_manifest_sizes_and_hashes", manifest_ok, problems[:10])
        query_failures: list[dict[str, Any]] = []
        for version in retained_versions:
            manifest = loads(version["manifest_json"], {}) or {}
            paths = [_abs(location, e["path"]) for e in manifest.get("files", [])]
            stats = loads(version["stats_json"], {}) or {}
            result = query_mod.query_version(paths, int(stats.get("rows", 0)),
                                             sample_limit=0)
            if not result.get("ok") or not result.get("rows_match"):
                query_failures.append({
                    "seq": version["seq"], "error": result.get("error"),
                    "rows": result.get("rows"), "expected": result.get("expected_rows"),
                })
        check("every_retained_version_queryable", not query_failures, query_failures[:10])

        latest = retained_versions[-1] if retained_versions else None
        latest_manifest = loads(latest["manifest_json"], {}) if latest else {"files": []}
        latest_ids = {f["file_id"] for f in (latest_manifest or {}).get("files", [])}
        active_ids = {fid for fid, row in file_rows.items() if row["state"] == "active"}
        check("active_files_match_latest_manifest", active_ids == latest_ids,
              {"active_without_manifest": sorted(active_ids - latest_ids)[:10],
               "manifest_without_active": sorted(latest_ids - active_ids)[:10]})
        with self.store.read() as conn:
            orphaned = conn.execute(
                "SELECT count(*) FROM files WHERE table_id=? AND state='orphaned'",
                (table_id,),
            ).fetchone()[0]
        checks.append({"name": "orphans_pending_gc", "ok": True,
                       "detail": {"orphaned_files": orphaned,
                                  "note": "孤儿文件不影响任何已提交版本"}})
        return self._finish_verification(
            table_id, phase, checks, len(retained_versions), refs_checked,
            fingerprints, {"retained": len(retained_versions),
                           "expired": len(expired_versions)},
        )

    def _finish_verification(self, table_id: str, phase: str,
                             checks: list[dict[str, Any]], version_count: int,
                             file_count: int, fingerprints: dict[str, str] | None = None,
                             version_counts: dict[str, int] | None = None,
                             ) -> dict[str, Any]:
        ok = all(c["ok"] for c in checks)
        result = {
            "ok": ok, "phase": phase, "checked_at": now_iso(),
            "versions_checked": version_count, "file_references_checked": file_count,
            "checks": checks, "fingerprints": fingerprints or {},
            "version_counts": version_counts or {},
        }
        with self.store.write_txn() as conn:
            conn.execute(
                """INSERT INTO verifications(id, table_id, phase, created_at, ok,
                       result_json) VALUES(?,?,?,?,?,?)""",
                (new_id("verify"), table_id, phase, now_iso(), 1 if ok else 0,
                 dumps(result)),
            )
        return result

    # ================================================================ reads
    def query_all_versions(self, table_name: str) -> dict[str, Any]:
        table = self.get_table(table_name)
        with self.store.read() as conn:
            versions = conn.execute(
                "SELECT seq FROM versions WHERE table_id=? ORDER BY seq", (table["id"],)
            ).fetchall()
        results = []
        for version in versions:
            seq = int(version["seq"])
            try:
                result = self.query_version(table_name, seq, sample_limit=0)
            except KeyError as exc:  # pragma: no cover - defensive
                result = {"ok": False, "seq": seq, "error": str(exc)}
            results.append(
                {
                    "seq": seq,
                    "ok": result.get("ok", False),
                    "error": result.get("error"),
                    "rows": result.get("rows", 0),
                    "expected_rows": result.get("expected_rows"),
                    "rows_match": result.get("rows_match", False),
                    "file_count": result.get("file_count", 0),
                    "amount_sum": result.get("amount_sum"),
                    "by_region": result.get("by_region", []),
                }
            )
        return {"table": table_name, "versions": results}

    def query_version(self, table_name: str, seq: int, sample_limit: int = 25) -> dict[str, Any]:
        table = self.get_table(table_name)
        with self.store.read() as conn:
            version = self.store.one(
                conn, "SELECT * FROM versions WHERE table_id=? AND seq=?",
                (table["id"], seq),
            )
        if not version:
            raise KeyError(f"no version seq {seq}")
        if int(version["retained"]) == 0:
            return {
                "ok": False, "error": "version_pruned", "seq": seq,
                "version_id": version["id"], "rows": 0, "sample": [],
                "by_region": [], "files": [], "stats": {}, "message": "该版本已被保留策略修剪",
            }
        manifest = loads(version["manifest_json"], {}) or {}
        stats = loads(version["stats_json"], {}) or {}
        paths = [_abs(table["location"], e["path"]) for e in manifest.get("files", [])]
        result = query_mod.query_version(paths, int(stats.get("rows", 0)), sample_limit)
        result.update({
            "seq": seq, "version_id": version["id"], "kind": version["kind"],
            "message": version["message"], "actor": version["actor"],
            "created_at": version["created_at"], "stats": stats,
            "manifest": manifest, "files": manifest.get("files", []),
        })
        return result

    # ================================================================ state
    def task_dict(self, row: Any) -> dict[str, Any]:
        record = dict(row)
        record["op"] = loads(record.pop("op_json"), {})
        record["log"] = loads(record.pop("log_json"), [])
        return record

    def get_task(self, task_id: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = self.store.one(conn, "SELECT * FROM tasks WHERE id=?", (task_id,))
        if not row:
            raise KeyError(f"unknown task {task_id}")
        return self.task_dict(row)

    def _intent(self, intent_id: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = self.store.one(conn, "SELECT * FROM commit_intents WHERE id=?",
                                 (intent_id,))
        if not row:
            raise KeyError(f"unknown commit intent {intent_id}")
        return dict(row)

    def _table_by_id(self, table_id: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = self.store.one(conn, "SELECT * FROM tables WHERE id=?", (table_id,))
        if not row:
            raise KeyError(f"unknown table id {table_id}")
        return self._table_dict(row)

    def _task_lease_id(self, task_id: str) -> str | None:
        with self.store.read() as conn:
            row = self.store.one(conn, "SELECT lease_id FROM tasks WHERE id=?", (task_id,))
        return row["lease_id"] if row else None

    def _log_task(self, task_id: str, level: str, message: str) -> None:
        with self.store.write_txn() as conn:
            row = conn.execute("SELECT log_json FROM tasks WHERE id=?",
                               (task_id,)).fetchone()
            if not row:
                return
            log = loads(row["log_json"], []) or []
            log.append({"ts": now_iso(), "level": level, "message": tail(message)})
            conn.execute("UPDATE tasks SET log_json=?, updated_at=? WHERE id=?",
                         (dumps(log[-200:]), now_iso(), task_id))

    def _mark_crashed(self, task_id: str, point: str, phase: str) -> None:
        with self.store.write_txn() as conn:
            conn.execute(
                """UPDATE tasks SET state='crashed', phase=?, error=?, updated_at=?
                   WHERE id=?""",
                (phase, f"injected crash: {point}", now_iso(), task_id),
            )
            conn.execute(
                "UPDATE commit_intents SET error=?, updated_at=? WHERE task_id=? "
                "AND state NOT IN ('committed','aborted')",
                (f"crashed at {point}", now_iso(), task_id),
            )
            event(conn, "error", f"task {task_id} crashed at {point}", {"phase": phase})

    def table_state(self, table_name: str, retain_versions: int | None = None) -> dict[str, Any]:
        table = self.get_table(table_name)
        table_id = table["id"]
        with self.store.read() as conn:
            versions = [dict(r) for r in conn.execute(
                """SELECT id, table_id, seq, parent_id, kind, actor, message,
                          stats_json, created_at, retained, expired_at
                   FROM versions WHERE table_id=? ORDER BY seq DESC""",
                (table_id,)).fetchall()]
            for v in versions:
                v["stats"] = loads(v.pop("stats_json"), {})
            tasks = [self.task_dict(r) for r in conn.execute(
                "SELECT * FROM tasks WHERE table_id=? ORDER BY created_at DESC LIMIT 40",
                (table_id,)).fetchall()]
            intents = [dict(r) for r in conn.execute(
                """SELECT * FROM commit_intents WHERE table_id=?
                   ORDER BY created_at DESC LIMIT 40""",
                (table_id,)).fetchall()]
            for i in intents:
                i["op"] = loads(i.pop("op_json"), {})
                i["file_ids"] = loads(i.pop("file_ids_json"), [])
            files = [dict(r) for r in conn.execute(
                "SELECT * FROM files WHERE table_id=? ORDER BY created_at",
                (table_id,)).fetchall()]
            for f in files:
                f["partitions"] = loads(f.pop("partitions_json"), {})
            leases = [dict(r) for r in conn.execute(
                "SELECT * FROM leases WHERE table_id=? ORDER BY created_at DESC LIMIT 40",
                (table_id,)).fetchall()]
            events = [dict(r) for r in conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT 100").fetchall()]
            for e in events:
                e["detail"] = loads(e.pop("detail_json"), None)
            runs = [dict(r) for r in conn.execute(
                """SELECT id, table_id, created_at, actions_json,
                          verification_before_json, verification_after_json
                   FROM recovery_runs WHERE table_id=? ORDER BY created_at DESC LIMIT 10""",
                (table_id,)).fetchall()]
            for r in runs:
                r["actions"] = loads(r.pop("actions_json"), [])
                r["before"] = loads(r.pop("verification_before_json"), None)
                r["after"] = loads(r.pop("verification_after_json"), None)
            gc_runs = [dict(r) for r in conn.execute(
                """SELECT id, table_id, created_at, reclaimed_json, skipped_json,
                          verification_before_json, verification_after_json
                   FROM gc_runs WHERE table_id=? ORDER BY created_at DESC LIMIT 10""",
                (table_id,)).fetchall()]
            for g in gc_runs:
                g["reclaimed"] = loads(g.pop("reclaimed_json"), [])
                g["skipped"] = loads(g.pop("skipped_json"), [])
                g["reclaimed_count"] = len(g["reclaimed"])
                g["skipped_count"] = len(g["skipped"])
                g["before"] = loads(g.pop("verification_before_json"), None)
                g["after"] = loads(g.pop("verification_after_json"), None)
            latest_verify = conn.execute(
                """SELECT * FROM verifications WHERE table_id=?
                   ORDER BY created_at DESC LIMIT 1""",
                (table_id,)).fetchone()
            refs: dict[str, list[int]] = {}
            for row in conn.execute(
                "SELECT seq, manifest_json FROM versions WHERE table_id=?", (table_id,)
            ).fetchall():
                for entry in (loads(row["manifest_json"], {}) or {}).get("files", []):
                    refs.setdefault(entry["file_id"], []).append(int(row["seq"]))
            latest_version_row = conn.execute(
                "SELECT * FROM versions WHERE table_id=? ORDER BY seq DESC LIMIT 1",
                (table_id,),
            ).fetchone()
        readers = reader_mod.active_pins(self.store, table_id)
        for f in files:
            f["referenced_by"] = sorted(refs.get(f["id"], []))
        latest_files: list[dict[str, Any]] = []
        if latest_version_row:
            manifest = loads(latest_version_row["manifest_json"], {}) or {}
            latest_files = [
                {**entry, "referenced_by": sorted(refs.get(entry["file_id"], []))}
                for entry in manifest.get("files", [])
            ]
        gc_pending = self.gc_scan(table_name, retain_versions)
        verification = loads(latest_verify["result_json"], None) if latest_verify else None
        return {
            "table": table, "versions": versions, "tasks": tasks, "intents": intents,
            "files": files, "latest_files": latest_files, "leases": leases,
            "readers": readers["readers"], "events": events,
            "recovery_runs": runs, "gc_runs": gc_runs, "gc_pending": gc_pending,
            "last_verification": verification,
            "crash_points": CRASH_POINTS,
            "lease_ttl": self.lease_ttl,
        }

    # ================================================================ reset
    def reset_all(self) -> None:
        with self.store.read() as conn:
            tables = [dict(r) for r in conn.execute("SELECT * FROM tables").fetchall()]
        for table in tables:
            shutil.rmtree(table["location"], ignore_errors=True)
        with self.store.write_txn() as conn:
            for table in ("events", "verifications", "gc_runs", "recovery_runs",
                          "tasks", "readers", "leases", "commit_intents", "files",
                          "versions", "tables"):
                conn.execute(f"DELETE FROM {table}")
            event(conn, "warn", "all lakehouse state reset")
        os.makedirs(self.data_dir, exist_ok=True)
