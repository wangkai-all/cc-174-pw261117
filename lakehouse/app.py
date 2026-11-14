"""FastAPI application exposing the lakehouse demo.

``create_app(engine)`` builds a self-contained app so tests can point it at a
temporary data directory; the module-level ``app`` uses the environment
configuration and is what ``uvicorn lakehouse.app:app`` loads.
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import config, leases as lease_mod, readers as reader_mod
from .engine import CRASH_POINTS, Engine

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
DEMO_TABLE = "events"
DEMO_PARTITIONS = ["us", "eu", "apac"]


class TableIn(BaseModel):
    name: str = DEMO_TABLE
    partitions: list[str] = Field(default_factory=lambda: list(DEMO_PARTITIONS))


class TaskIn(BaseModel):
    table: str = DEMO_TABLE
    name: str
    op: dict[str, Any]
    crash_point: str = "none"
    actor: str = "demo"


class CommitBatchIn(BaseModel):
    table: str = DEMO_TABLE
    task_ids: list[str]
    order: str = "sequential"


class TableActionIn(BaseModel):
    table: str = DEMO_TABLE
    retain_versions: int | None = None


class ReaderIn(BaseModel):
    table: str = DEMO_TABLE
    version_seq: int | None = None
    label: str = "reader"
    ttl: float = 120.0


class HeartbeatIn(BaseModel):
    ttl: float | None = None


def _fail(exc: Exception) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc).strip("'\""))
    if isinstance(exc, ValueError):
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


def create_app(engine: Engine, ensure_demo: bool = True) -> FastAPI:
    app = FastAPI(title="Local Lakehouse Lab", version="0.1.0")
    app.state.engine = engine

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "data_dir": engine.data_dir, "db": engine.store.path,
                "lease_ttl": engine.lease_ttl}

    @app.get("/api/tables")
    def list_tables() -> dict[str, Any]:
        return {"tables": engine.list_tables(), "demo_table": DEMO_TABLE}

    @app.post("/api/tables")
    def create_table(body: TableIn) -> dict[str, Any]:
        if any(t["name"] == body.name for t in engine.list_tables()):
            return {"created": False, "table": engine.get_table(body.name)}
        try:
            table = engine.create_table(body.name, body.partitions)
        except Exception as exc:
            raise _fail(exc) from exc
        return {"created": True, "table": table}

    @app.post("/api/demo/reset")
    def reset_demo(body: TableIn | None = None) -> dict[str, Any]:
        engine.reset_all()
        partitions = (body.partitions if body else None) or list(DEMO_PARTITIONS)
        return {"ok": True, "table": engine.create_table(DEMO_TABLE, partitions)}

    @app.get("/api/state")
    def state(
        table: str = DEMO_TABLE,
        retain_versions: int | None = Query(default=None, ge=0, le=100),
    ) -> dict[str, Any]:
        try:
            payload = engine.table_state(table, retain_versions)
        except Exception as exc:
            raise _fail(exc) from exc
        payload["crash_points"] = CRASH_POINTS
        return payload

    @app.post("/api/tasks")
    def prepare_task(body: TaskIn) -> dict[str, Any]:
        try:
            return engine.prepare_task(
                body.table, body.name, body.op, body.crash_point, body.actor
            )
        except Exception as exc:
            raise _fail(exc) from exc

    @app.post("/api/tasks/{task_id}/commit")
    def commit_task(task_id: str) -> dict[str, Any]:
        try:
            return engine.commit_task(task_id)
        except Exception as exc:
            raise _fail(exc) from exc

    @app.post("/api/commits")
    def commit_batch(body: CommitBatchIn) -> dict[str, Any]:
        if body.order not in ("sequential", "concurrent"):
            raise HTTPException(status_code=400,
                                detail="order 必须是 sequential 或 concurrent")
        try:
            return engine.commit_many(body.task_ids, body.order)
        except Exception as exc:
            raise _fail(exc) from exc

    @app.post("/api/recover")
    def recover(body: TableActionIn) -> dict[str, Any]:
        try:
            return engine.recover(body.table)
        except Exception as exc:
            raise _fail(exc) from exc

    @app.post("/api/gc/scan")
    def gc_scan(body: TableActionIn) -> dict[str, Any]:
        try:
            return engine.gc_scan(body.table, body.retain_versions)
        except Exception as exc:
            raise _fail(exc) from exc

    @app.post("/api/gc/reclaim")
    def gc_reclaim(body: TableActionIn) -> dict[str, Any]:
        try:
            return engine.gc_reclaim(body.table, body.retain_versions)
        except Exception as exc:
            raise _fail(exc) from exc

    @app.post("/api/verify")
    def verify(body: TableActionIn) -> dict[str, Any]:
        try:
            return engine.verify(body.table)
        except Exception as exc:
            raise _fail(exc) from exc

    @app.get("/api/query")
    def query_version(
        table: str = DEMO_TABLE,
        seq: int = Query(ge=1),
        limit: int = Query(default=25, ge=0, le=200),
    ) -> dict[str, Any]:
        try:
            return engine.query_version(table, seq, limit)
        except Exception as exc:
            raise _fail(exc) from exc

    @app.get("/api/query-all")
    def query_all(table: str = DEMO_TABLE) -> dict[str, Any]:
        try:
            return engine.query_all_versions(table)
        except Exception as exc:
            raise _fail(exc) from exc

    @app.post("/api/readers")
    def open_reader(body: ReaderIn) -> dict[str, Any]:
        table = engine.get_table(body.table)
        version_id = None
        if body.version_seq is not None:
            with engine.store.read() as conn:
                row = engine.store.one(
                    conn, "SELECT id FROM versions WHERE table_id=? AND seq=?",
                    (table["id"], body.version_seq),
                )
            if not row:
                raise HTTPException(status_code=404,
                                    detail=f"版本 seq {body.version_seq} 不存在")
            version_id = row["id"]
        return reader_mod.open_reader(engine.store, table["id"], version_id,
                                      body.label, body.ttl)

    @app.post("/api/readers/{reader_id}/close")
    def close_reader(reader_id: str) -> dict[str, Any]:
        record = reader_mod.close_reader(engine.store, reader_id)
        if not record:
            raise HTTPException(status_code=404, detail="reader not found")
        return record

    @app.post("/api/leases/{lease_id}/heartbeat")
    def heartbeat(lease_id: str, body: HeartbeatIn) -> dict[str, Any]:
        record = lease_mod.heartbeat(engine.store, lease_id, body.ttl)
        if not record:
            raise HTTPException(status_code=404, detail="lease not found")
        return record

    @app.get("/api/intents")
    def intents(table: str = DEMO_TABLE) -> dict[str, Any]:
        payload = engine.table_state(table)
        return {"intents": payload["intents"], "tasks": payload["tasks"],
                "files": payload["files"]}

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    if ensure_demo and not any(t["name"] == DEMO_TABLE for t in engine.list_tables()):
        engine.create_table(DEMO_TABLE, list(DEMO_PARTITIONS))
    return app


def default_engine() -> Engine:
    return Engine(
        data_dir=os.environ.get("LAKE_DATA_DIR", config.DEFAULT_DATA_DIR),
        db_path=os.environ.get("LAKE_DB_PATH", config.DEFAULT_DB_PATH),
        lease_ttl=float(os.environ.get("LAKE_LEASE_TTL", config.DEFAULT_LEASE_TTL)),
    )


engine = default_engine()
app = create_app(engine, ensure_demo=os.environ.get("LAKE_AUTO_INIT", "1") == "1")
