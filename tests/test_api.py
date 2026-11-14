import os

from fastapi.testclient import TestClient

from lakehouse.app import create_app
from lakehouse.engine import Engine


def make_client(tmp_path):
    engine = Engine(str(tmp_path / "data"), str(tmp_path / "meta.sqlite3"), lease_ttl=30)
    app = create_app(engine, ensure_demo=True)
    return TestClient(app), engine


def test_index_and_assets_served(tmp_path):
    client, _ = make_client(tmp_path)
    assert client.get("/").status_code == 200
    assert "本地湖表实验台" in client.get("/").text
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/static/styles.css").status_code == 200


def test_full_demo_flow_over_http(tmp_path):
    client, engine = make_client(tmp_path)
    state = client.get("/api/state").json()
    assert state["table"]["name"] == "events"
    assert len(state["versions"]) == 1
    assert state["crash_points"]["after_write"]

    a = client.post("/api/tasks", json={
        "name": "A", "op": {"type": "append", "partitions": ["us"], "rows": 2, "tag": "A"},
    }).json()
    b = client.post("/api/tasks", json={
        "name": "B", "op": {"type": "replace", "partitions": ["us"], "rows": 2, "tag": "B"},
    }).json()
    assert a["state"] == b["state"] == "prepared"

    batch = client.post("/api/commits", json={
        "task_ids": [a["id"], b["id"]], "order": "sequential",
    }).json()
    assert batch["errors"] == {}
    assert batch["tasks"][b["id"]]["state"] == "committed"
    assert batch["tasks"][b["id"]]["recomputes"] == 1

    latest = engine.table_state("events")["versions"][0]["seq"]
    queried = client.get(f"/api/query?seq={latest}").json()
    assert queried["ok"] and queried["rows_match"]

    all_versions = client.get("/api/query-all").json()["versions"]
    assert all(v["ok"] and v["rows_match"] for v in all_versions)

    verified = client.post("/api/verify", json={}).json()
    assert verified["ok"]

    scan = client.post("/api/gc/scan", json={}).json()
    assert scan["reclaimable"] == []
    reclaim = client.post("/api/gc/reclaim", json={}).json()
    assert reclaim["consistent"]


def test_http_crash_recover_and_gc(tmp_path):
    client, engine = make_client(tmp_path)
    task = client.post("/api/tasks", json={
        "name": "crash", "op": {"type": "append", "partitions": ["eu"], "rows": 2},
        "crash_point": "after_intent",
    }).json()
    assert task["state"] == "crashed"

    scan = client.post("/api/gc/scan", json={}).json()
    assert any(c["reason"] == "pending_intent" for c in scan["protected"])

    recovery = client.post("/api/recover", json={}).json()
    assert recovery["consistent"]
    assert "aborted_uncommitted_intent" in [a["action"] for a in recovery["actions"]]

    reclaim = client.post("/api/gc/reclaim", json={}).json()
    assert reclaim["consistent"]
    assert reclaim["reclaimed"]

    verified = client.post("/api/verify", json={}).json()
    assert verified["ok"]
    assert engine.query_version("events", 1)["rows"] == 12


def test_http_reader_pin_protects_files(tmp_path):
    client, engine = make_client(tmp_path)
    reader = client.post("/api/readers", json={
        "version_seq": 1, "label": "pin", "ttl": 120,
    }).json()
    assert reader["version_seq"] == 1

    task = client.post("/api/tasks", json={
        "name": "R", "op": {"type": "replace", "partitions": ["us"], "rows": 2},
    }).json()
    client.post(f"/api/tasks/{task['id']}/commit")
    scan = client.post("/api/gc/scan", json={}).json()
    assert any(c["reason"] == "reader_pin" for c in scan["protected"])

    client.post(f"/api/readers/{reader['id']}/close")
    reclaim = client.post("/api/gc/reclaim", json={}).json()
    assert reclaim["consistent"]
    assert engine.query_version("events", 1)["ok"]


def test_http_validation_errors(tmp_path):
    client, _ = make_client(tmp_path)
    assert client.post("/api/tasks", json={
        "name": "X", "op": {"type": "delete", "partitions": ["us"], "rows": 1},
    }).status_code == 400
    assert client.post("/api/commits", json={
        "task_ids": [], "order": "random",
    }).status_code == 400
    assert client.get("/api/query?seq=99").status_code == 404
    assert client.get("/api/query?seq=0").status_code == 422
    assert client.post("/api/readers", json={"version_seq": 42}).status_code == 404


def test_http_reset_restores_bootstrap(tmp_path):
    client, engine = make_client(tmp_path)
    task = client.post("/api/tasks", json={
        "name": "A", "op": {"type": "append", "partitions": ["us"], "rows": 2},
    }).json()
    client.post(f"/api/tasks/{task['id']}/commit")
    assert len(engine.table_state("events")["versions"]) == 2
    client.post("/api/demo/reset", json={})
    assert len(engine.table_state("events")["versions"]) == 1
    assert engine.query_version("events", 1)["rows"] == 12
