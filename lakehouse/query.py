"""DuckDB-powered reads over the Parquet files of a committed version."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

import duckdb

_SAMPLE_QUERY = """
SELECT id, region, amount, ts, note, writer, base_seq
FROM read_parquet(?)
ORDER BY region, id
LIMIT ?
"""

_STATS_QUERY = """
SELECT count(*) AS rows,
       count(DISTINCT id) AS distinct_ids,
       coalesce(round(sum(amount), 6), 0) AS amount_sum,
       coalesce(min(id), 0) AS min_id,
       coalesce(max(id), 0) AS max_id,
       coalesce(min(base_seq), 0) AS min_base_seq,
       coalesce(max(base_seq), 0) AS max_base_seq
FROM read_parquet(?)
"""

_BY_REGION_QUERY = """
SELECT region,
       count(*) AS rows,
       round(sum(amount), 6) AS amount_sum
FROM read_parquet(?)
GROUP BY region
ORDER BY region
"""

_FINGERPRINT_QUERY = """
SELECT region, id, amount, note
FROM read_parquet(?)
ORDER BY region, id, note
"""


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("SET threads=2")
    return con


def _one_dict(cursor) -> dict[str, Any]:
    columns = [d[0] for d in cursor.description]
    row = cursor.fetchone()
    return dict(zip(columns, row)) if row else {}


def _many_dicts(cursor) -> list[dict[str, Any]]:
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def query_version(
    paths: list[str],
    expected_rows: int,
    sample_limit: int = 25,
) -> dict[str, Any]:
    """Run real DuckDB queries over the physical files of one version."""
    missing = [p for p in paths if not os.path.isfile(p)]
    if missing:
        return {
            "ok": False,
            "error": "missing_files",
            "missing": missing,
            "rows": 0,
            "sample": [],
            "by_region": [],
        }
    if not paths:
        return {
            "ok": True,
            "rows": 0,
            "expected_rows": expected_rows,
            "distinct_ids": 0,
            "amount_sum": 0,
            "sample": [],
            "by_region": [],
            "rows_match": expected_rows == 0,
        }
    con = _connect()
    try:
        stats = _one_dict(con.execute(_STATS_QUERY, [paths]))
        by_region = _many_dicts(con.execute(_BY_REGION_QUERY, [paths]))
        sample = _many_dicts(con.execute(_SAMPLE_QUERY, [paths, sample_limit]))
    finally:
        con.close()
    return {
        "ok": True,
        "rows": int(stats["rows"]),
        "expected_rows": expected_rows,
        "rows_match": int(stats["rows"]) == int(expected_rows),
        "distinct_ids": int(stats["distinct_ids"]),
        "amount_sum": float(stats["amount_sum"]),
        "min_id": int(stats["min_id"]),
        "max_id": int(stats["max_id"]),
        "base_seq_range": [int(stats["min_base_seq"]), int(stats["max_base_seq"])],
        "by_region": by_region,
        "sample": sample,
        "file_count": len(paths),
    }


def fingerprint(paths: list[str]) -> str:
    """Content fingerprint over all rows, independent of file layout."""
    if not paths or any(not os.path.isfile(p) for p in paths):
        raise FileNotFoundError("fingerprint needs existing files")
    con = _connect()
    try:
        rows = con.execute(_FINGERPRINT_QUERY, [paths]).fetchall()
    finally:
        con.close()
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, sort_keys=True).encode())
    return digest.hexdigest()[:16]


def physical_scan(paths: list[str]) -> dict[str, Any]:
    """Read parquet footers only; used to cross-check manifest row counts."""
    import pyarrow.parquet as pq

    out: dict[str, Any] = {"files": [], "errors": []}
    for path in paths:
        try:
            meta = pq.ParquetFile(path).metadata
            out["files"].append(
                {
                    "path": path,
                    "rows": meta.num_rows,
                    "row_groups": meta.num_row_groups,
                    "bytes": os.path.getsize(path),
                }
            )
        except Exception as exc:  # pragma: no cover - defensive
            out["errors"].append({"path": path, "error": str(exc)})
    return out
