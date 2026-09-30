"""End-to-end demo against a real, already-running Kafka broker.

    docker compose up -d          # or point --bootstrap at any broker you already have
    python -m scripts.run_demo

Generates synthetic events with a known set of injected defects, produces them to
clickstream.raw, runs the consumer to completion, and prints a report. Exits 1 if the counts
don't match what was injected (so this doubles as a smoke test), 0 otherwise.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from clickstream import DLQ_TOPIC, ENRICHED_TOPIC, GROUP_ID, RAW_TOPIC, SESSIONS_TOPIC  # noqa: E402
from clickstream.admin import consumer_lag, ensure_topics  # noqa: E402
from clickstream.consumer import run as run_consumer  # noqa: E402
from clickstream.generator import generate_events  # noqa: E402
from clickstream.producer import produce_lines  # noqa: E402

EARLIEST = datetime(2024, 1, 1, tzinfo=timezone.utc)
NOW = datetime(2024, 6, 2, tzinfo=timezone.utc)  # fixed "now": keeps the demo reproducible


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--out-dir", default="data/sessions")
    parser.add_argument("--users", type=int, default=150)
    parser.add_argument("--group-id", default=GROUP_ID)
    args = parser.parse_args()

    ensure_topics(args.bootstrap, {RAW_TOPIC: 3, DLQ_TOPIC: 1, ENRICHED_TOPIC: 3, SESSIONS_TOPIC: 1})

    lines, injected = generate_events(n_users=args.users)
    print(f"Generated {len(lines)} lines. Injected defects: {injected}")

    delivery = produce_lines(args.bootstrap, lines)
    print(f"Produced: {delivery}")

    stats = run_consumer(args.bootstrap, args.out_dir, EARLIEST, NOW, group_id=args.group_id)
    print(f"\nConsumed:            {stats.consumed}")
    print(f"Valid (incl. dupes): {stats.valid}")
    print(f"Duplicates detected: {stats.duplicates}")
    print(f"Rejected to DLQ:     {stats.rejected}")
    for reason, n in sorted(stats.rejected_by_reason.items()):
        print(f"  {n:>3}  {reason}")
    print(f"Sessions completed:  {stats.sessions_completed}")
    print(f"Parquet files:       {stats.parquet_files}")

    lag = consumer_lag(args.bootstrap, args.group_id, [RAW_TOPIC])
    print(f"Consumer lag:        {lag}")

    expected_rejected = sum(v for k, v in injected.items() if k != "duplicate_events")
    ok = (
        stats.rejected == expected_rejected
        and stats.duplicates == injected["duplicate_events"]
        and lag[RAW_TOPIC] == 0
    )
    print("\nRESULT:", "PASS" if ok else "FAIL")
    if not ok:
        print(f"expected rejected={expected_rejected}, duplicates={injected['duplicate_events']}, lag=0")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
