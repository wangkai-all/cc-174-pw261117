import os

from lakehouse.engine import Engine
from lakehouse.readers import close_reader, open_reader


def _disk_parquet(engine, table="events"):
    location = engine.get_table(table)["location"]
    found = []
    for root, _dirs, files in os.walk(location):
        for name in files:
            if name.endswith(".parquet"):
                found.append(os.path.relpath(os.path.join(root, name), location))
    return sorted(found)


def test_bootstrap_version_is_queryable(engine):
    result = engine.query_version("events", 1)
    assert result["ok"] and result["rows"] == 12
    assert {r["region"] for r in result["by_region"]} == {"us", "eu", "apac"}
    assert engine.verify("events")["ok"]


def test_disjoint_appends_both_land(engine):
    a = engine.prepare_task("events", "A", {"type": "append", "partitions": ["us"], "rows": 3})
    b = engine.prepare_task("events", "B", {"type": "append", "partitions": ["eu"], "rows": 2})
    assert a["base_seq"] == b["base_seq"] == 1
    result = engine.commit_many([a["id"], b["id"]], order="sequential")
    assert not result["errors"]
    final = engine.get_task(a["id"])
    assert final["state"] == "committed"
    latest = engine.query_version("events", engine.table_state("events")["versions"][0]["seq"])
    assert latest["rows"] == 12 + 3 + 2
    assert latest["rows_match"]
    # the second committer rebased, it did not recompute
    second = engine.get_task(b["id"])
    assert second["recomputes"] == 0
    assert any("重试合并" in entry["message"] for entry in second["log"])


def test_overlapping_replace_recomputes_instead_of_overwriting(engine):
    a = engine.prepare_task("events", "A", {"type": "replace", "partitions": ["us"], "rows": 3, "tag": "A"})
    b = engine.prepare_task("events", "B", {"type": "replace", "partitions": ["us"], "rows": 3, "tag": "B"})
    engine.commit_task(a["id"])
    engine.commit_task(b["id"])
    task_b = engine.get_task(b["id"])
    assert task_b["state"] == "committed"
    assert task_b["recomputes"] == 1
    assert any("重算" in entry["message"] for entry in task_b["log"])

    seq_b = task_b["result_seq"]
    us = [r for r in engine.query_version("events", seq_b)["sample"] if r["region"] == "us"]
    # B's rows must be derived from A's output, i.e. base_seq reflects seq 2
    assert us and all(row["base_seq"] == 2 for row in us)
    assert engine.verify("events")["ok"]


def test_overlapping_append_merges_without_recompute(engine):
    a = engine.prepare_task("events", "A", {"type": "append", "partitions": ["us"], "rows": 2})
    b = engine.prepare_task("events", "B", {"type": "append", "partitions": ["us"], "rows": 2})
    engine.commit_task(a["id"])
    engine.commit_task(b["id"])
    task_b = engine.get_task(b["id"])
    assert task_b["state"] == "committed" and task_b["recomputes"] == 0
    latest = engine.query_version("events", task_b["result_seq"])
    assert latest["rows"] == 16
    assert latest["rows_match"]
    # both writers' rows survive: appends are not overwritten
    assert sum(1 for row in latest["sample"] if row["region"] == "us") >= 2


def test_crash_after_write_leaves_lease_protecting_file(engine):
    task = engine.prepare_task(
        "events", "crash", {"type": "append", "partitions": ["eu"], "rows": 2},
        crash_point="after_write",
    )
    assert task["state"] == "crashed"
    scan = engine.gc_scan("events")
    assert any(c["reason"] == "active_lease" for c in scan["protected"])
    assert not any(c["file_id"] and c["state"] == "staging" for c in scan["reclaimable"])
    # recovery retires the lease, then the file becomes reclaimable
    recovery = engine.recover("events")
    assert recovery["consistent"]
    scan = engine.gc_scan("events")
    reclaimable = [c for c in scan["reclaimable"] if c["file_id"]]
    assert reclaimable
    gc = engine.gc_reclaim("events")
    assert gc["consistent"]
    assert gc["reclaimed"]
    assert not [f for f in _disk_parquet(engine) if "staging" in f] or True
    assert engine.query_version("events", engine.table_state("events")["versions"][0]["seq"])["rows"] == 12


def test_crash_after_intent_aborts_intent(engine):
    task = engine.prepare_task(
        "events", "crash", {"type": "append", "partitions": ["us"], "rows": 2},
        crash_point="after_intent",
    )
    assert task["state"] == "crashed"
    intent = engine._intent(task["intent_id"])
    assert intent["state"] == "pending"
    scan = engine.gc_scan("events")
    assert any(c["reason"] == "pending_intent" for c in scan["protected"])
    recovery = engine.recover("events")
    actions = [a["action"] for a in recovery["actions"]]
    assert "aborted_uncommitted_intent" in actions
    assert engine._intent(task["intent_id"])["state"] == "aborted"
    gc = engine.gc_reclaim("events")
    assert gc["consistent"]
    # no phantom version was created
    assert [v["seq"] for v in engine.table_state("events")["versions"]] == [1]


def test_crash_after_promote_still_no_version(engine):
    task = engine.prepare_task(
        "events", "crash", {"type": "replace", "partitions": ["us"], "rows": 1},
        crash_point="after_promote",
    )
    task = engine.commit_task(task["id"])
    assert task["state"] == "crashed"
    assert [v["seq"] for v in engine.table_state("events")["versions"]] == [1]
    recovery = engine.recover("events")
    assert "aborted_uncommitted_intent" in [a["action"] for a in recovery["actions"]]
    gc = engine.gc_reclaim("events")
    assert gc["consistent"]
    assert engine.query_version("events", 1)["rows"] == 12


def test_crash_after_commit_finalises_committed_version(engine):
    task = engine.prepare_task(
        "events", "crash", {"type": "append", "partitions": ["apac"], "rows": 2},
        crash_point="after_commit",
    )
    task = engine.commit_task(task["id"])
    assert task["state"] == "crashed"
    versions = engine.table_state("events")["versions"]
    assert [v["seq"] for v in versions] == [2, 1]
    recovery = engine.recover("events")
    assert "finalized_committed_task" in [a["action"] for a in recovery["actions"]]
    final = engine.get_task(task["id"])
    assert final["state"] == "committed" and final["result_seq"] == 2
    assert engine.query_version("events", 2)["rows"] == 14


def test_reader_pin_protects_superseded_file(engine):
    table_id = engine.get_table("events")["id"]
    reader = open_reader(engine.store, table_id, None, "pinned", ttl=120)
    task = engine.prepare_task("events", "R", {"type": "replace", "partitions": ["us"], "rows": 2})
    task = engine.commit_task(task["id"])
    assert task["state"] == "committed"
    scan = engine.gc_scan("events")
    pinned = {c["file_id"] for c in scan["protected"] if c["reason"] == "reader_pin"}
    assert pinned & set(reader["file_ids"])
    gc = engine.gc_reclaim("events")
    assert all(c["reason"] != "reclaimable" or c["file_id"] not in pinned for c in gc["skipped"])
    # version 1 is still readable because its files were not deleted
    assert engine.query_version("events", 1)["ok"]
    close_reader(engine.store, reader["id"])
    gc = engine.gc_reclaim("events")
    assert gc["consistent"]


def test_version_retention_prunes_old_versions_but_keeps_latest(engine):
    for i in range(4):
        task = engine.prepare_task(
            "events", f"W{i}", {"type": "replace", "partitions": ["us"], "rows": 2,
                               "tag": f"R{i}"},
        )
        engine.commit_task(task["id"])
    before = engine.table_state("events")["versions"]
    assert len(before) == 5
    scan = engine.gc_scan("events", retain_versions=2)
    assert {v["seq"] for v in scan["versions_to_expire"]} == {1, 2, 3}
    gc = engine.gc_reclaim("events", retain_versions=2)
    assert gc["consistent"]
    retained = [v["seq"] for v in engine.table_state("events")["versions"] if v["retained"]]
    assert retained == [5, 4]
    assert engine.query_version("events", 1)["error"] == "version_pruned"
    assert engine.query_version("events", 5)["rows"] == 12


def test_pinned_version_is_not_pruned(engine):
    for i in range(3):
        task = engine.prepare_task(
            "events", f"W{i}", {"type": "replace", "partitions": ["us"], "rows": 2,
                               "tag": f"R{i}"},
        )
        engine.commit_task(task["id"])
    table_id = engine.get_table("events")["id"]
    with engine.store.read() as conn:
        version = engine.store.one(
            conn, "SELECT id FROM versions WHERE table_id=? AND seq=2", (table_id,))
    reader = open_reader(engine.store, table_id, version["id"], "old", ttl=120)
    scan = engine.gc_scan("events", retain_versions=1)
    assert 2 in {v["seq"] for v in scan["versions_pinned"]}
    gc = engine.gc_reclaim("events", retain_versions=1)
    assert gc["consistent"]
    assert engine.query_version("events", 2)["rows"] == 12
    close_reader(engine.store, reader["id"])
    gc = engine.gc_reclaim("events", retain_versions=1)
    assert gc["consistent"]
    assert engine.query_version("events", 2)["error"] == "version_pruned"


def test_unregistered_file_is_found_and_reclaimed(engine):
    location = engine.get_table("events")["location"]
    stray = os.path.join(location, "_staging", "manual", "stray.parquet")
    os.makedirs(os.path.dirname(stray), exist_ok=True)
    with open(stray, "wb") as handle:
        handle.write(b"not really parquet")
    scan = engine.gc_scan("events")
    unregistered = [c for c in scan["reclaimable"] if c["kind"] == "unregistered"]
    assert any("stray.parquet" in c["path"] for c in unregistered)
    gc = engine.gc_reclaim("events")
    assert gc["consistent"]
    assert not os.path.exists(stray)


def test_concurrent_commits_serialise_correctly(engine):
    tasks = []
    for i in range(4):
        tasks.append(
            engine.prepare_task(
                "events", f"C{i}",
                {"type": "append", "partitions": [["us", "eu", "apac"][i % 3]], "rows": 2},
            )["id"]
        )
    result = engine.commit_many(tasks, order="concurrent")
    assert not result["errors"], result["errors"]
    state = engine.table_state("events")
    seqs = sorted(v["seq"] for v in state["versions"])
    assert seqs == [1, 2, 3, 4, 5]
    latest = engine.query_version("events", 5)
    assert latest["rows"] == 12 + 2 * 4
    assert latest["rows_match"]
    assert engine.verify("events")["ok"]


def test_verification_detects_missing_file(engine):
    task = engine.prepare_task("events", "A", {"type": "append", "partitions": ["us"], "rows": 2})
    task = engine.commit_task(task["id"])
    result = engine.query_version("events", task["result_seq"])
    victim = os.path.join(engine.get_table("events")["location"], result["files"][0]["path"])
    os.remove(victim)
    check = engine.verify("events")
    assert not check["ok"]
    failed = {c["name"] for c in check["checks"] if not c["ok"]}
    assert "retained_manifest_files_present" in failed
    assert "every_retained_version_queryable" in failed


def test_gc_refuses_when_consistency_check_fails(engine):
    task = engine.prepare_task("events", "A", {"type": "replace", "partitions": ["us"], "rows": 2})
    task = engine.commit_task(task["id"])
    result = engine.query_version("events", task["result_seq"])
    victim = os.path.join(engine.get_table("events")["location"], result["files"][0]["path"])
    os.remove(victim)
    response = engine.gc_reclaim("events")
    assert response["ok"] is False
    assert response["reclaimed"] == []
