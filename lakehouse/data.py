"""Parquet data files and deterministic row generation for the demo table."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("region", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("ts", pa.string()),
        pa.field("note", pa.string()),
        pa.field("writer", pa.string()),
        pa.field("base_seq", pa.int64()),
    ]
)
PARTITION_COLUMNS = ["region"]
PARTITION_FIELD = "region"
ROW_CAP = 5000


@dataclass
class WrittenFile:
    file_id: str
    partition_key: str
    partitions: dict[str, Any]
    staging_rel_path: str
    rel_path: str
    rows: int
    bytes: int
    sha256: str
    row_data: list[dict[str, Any]] = field(default_factory=list)


def canonical_partitions(values: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    parts = {k: values[k] for k in sorted(values)}
    key = "/".join(f"{k}={v}" for k, v in parts.items())
    return key, parts


def partition_key_from_row(row: dict[str, Any]) -> str:
    return canonical_partitions({k: row[k] for k in PARTITION_COLUMNS})[0]


def _iso_plus(base: datetime, seconds: int) -> str:
    return (base + timedelta(seconds=seconds)).astimezone(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def _stable_int(text: str, bits: int = 31) -> int:
    return int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big") % (2 ** bits)


def generated_rows(
    task_id: str,
    tag: str,
    writer: str,
    partition: str,
    count: int,
    base_seq: int,
    created_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for i in range(count):
        seed = _stable_int(f"{task_id}:{partition}:{i}")
        rows.append(
            {
                "id": seed,
                "region": partition,
                "amount": round(1 + seed % 100000 / 1000.0, 3),
                "ts": _iso_plus(created_at, i),
                "note": f"{tag} {partition} #{i}",
                "writer": writer,
                "base_seq": base_seq,
            }
        )
    return rows


def derive_rows(
    rows: list[dict[str, Any]],
    tag: str,
    writer: str,
    base_seq: int,
) -> list[dict[str, Any]]:
    """A base-dependent transform; the output changes if the base changes."""
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "id": row["id"],
                "region": row["region"],
                "amount": round(float(row["amount"]) * 1.07 + 0.5, 3),
                "ts": row["ts"],
                "note": f"{row['note']} | derive {tag}@{base_seq}",
                "writer": writer,
                "base_seq": base_seq,
            }
        )
    return result


def read_parquet_rows(paths: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not os.path.isfile(path):
            raise FileNotFoundError(path)
        table = pq.read_table(path, columns=columns)
        rows.extend(table.to_pylist())
    return rows


def write_parquet(path: str, rows: list[dict[str, Any]]) -> tuple[int, int]:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path, compression="snappy")
    return table.num_rows, os.path.getsize(path)


def file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
