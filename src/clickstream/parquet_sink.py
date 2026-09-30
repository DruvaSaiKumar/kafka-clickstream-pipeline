"""Writes completed sessions to date-partitioned Parquet, the durable analytical output."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([
    ("user_id", pa.string()),
    ("session_id", pa.string()),
    ("start_ts", pa.timestamp("us", tz="UTC")),
    ("end_ts", pa.timestamp("us", tz="UTC")),
    ("duration_seconds", pa.float64()),
    ("event_count", pa.int32()),
    ("unique_urls", pa.int32()),
    ("event_type_counts", pa.string()),  # JSON-encoded: counts vary per session, not fixed columns
    ("has_purchase", pa.bool_()),
    ("purchase_value_total", pa.float64()),
])  # fmt: skip


def write_sessions(records: list[dict], out_dir: str | Path) -> list[Path]:
    """Partitions `records` by the session's end date (a batch can span midnight) and writes one
    new file per day. Returns the paths written; an empty list if `records` is empty."""
    by_day: dict[str, list[dict]] = {}
    for r in records:
        by_day.setdefault(r["end_ts"][:10], []).append(r)

    paths = []
    for day, day_records in by_day.items():
        columns = {name: [] for name in SCHEMA.names}
        for r in day_records:
            for name in SCHEMA.names:
                value = r[name]
                if name in ("start_ts", "end_ts"):
                    value = datetime.fromisoformat(value)  # pyarrow needs datetimes, not ISO strings
                elif name == "event_type_counts":
                    value = json.dumps(value)
                columns[name].append(value)
        table = pa.table(columns, schema=SCHEMA)

        target_dir = Path(out_dir) / f"dt={day}"
        target_dir.mkdir(parents=True, exist_ok=True)
        existing = list(target_dir.glob("part-*.parquet"))
        path = target_dir / f"part-{len(existing)}.parquet"
        pq.write_table(table, path, compression="snappy")
        paths.append(path)
    return paths
