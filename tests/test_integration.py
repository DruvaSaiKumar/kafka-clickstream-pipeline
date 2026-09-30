"""Runs the full pipeline against a real, live Kafka broker: produce, consume, sessionize, sink.

Each test gets its own topic names and consumer group (suffixed with a uuid) so repeated runs
against the same long-lived broker never see each other's data or offsets.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer

from clickstream.admin import consumer_lag, ensure_topics
from clickstream.consumer import run as run_consumer
from clickstream.generator import generate_events
from clickstream.producer import produce_lines

from .conftest import requires_kafka

EARLIEST = datetime(2024, 1, 1, tzinfo=timezone.utc)
NOW = datetime(2024, 6, 2, tzinfo=timezone.utc)


def _topics(run_id: str) -> dict[str, str]:
    return {
        "raw": f"test.raw.{run_id}",
        "dlq": f"test.dlq.{run_id}",
        "enriched": f"test.enriched.{run_id}",
        "sessions": f"test.sessions.{run_id}",
    }


def _read_all(bootstrap: str, topic: str, timeout: float = 10.0) -> list[dict]:
    """Read every message currently on `topic` from the beginning, using a throwaway group."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap, "group.id": f"reader-{uuid.uuid4()}",
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
    })  # fmt: skip
    consumer.subscribe([topic])
    records, empty_polls = [], 0
    try:
        while empty_polls < 5:
            msg = consumer.poll(timeout / 5)
            if msg is None:
                empty_polls += 1
                continue
            empty_polls = 0
            if msg.error():
                continue
            records.append(json.loads(msg.value()))
        return records
    finally:
        consumer.close()


@requires_kafka
def test_full_pipeline_against_a_live_broker(bootstrap, tmp_path):
    run_id = uuid.uuid4().hex[:8]
    topics = _topics(run_id)
    group_id = f"group-{run_id}"
    ensure_topics(
        bootstrap, {topics["raw"]: 3, topics["dlq"]: 1, topics["enriched"]: 3, topics["sessions"]: 1}
    )

    lines, injected = generate_events(n_users=60, seed=99)
    delivery = produce_lines(bootstrap, lines, topic=topics["raw"])
    assert delivery == {"sent": len(lines), "delivered": len(lines)}

    stats = run_consumer(
        bootstrap, str(tmp_path / "sessions"), EARLIEST, NOW, group_id=group_id,
        raw_topic=topics["raw"], dlq_topic=topics["dlq"],
        enriched_topic=topics["enriched"], sessions_topic=topics["sessions"],
    )  # fmt: skip

    expected_rejected = sum(v for k, v in injected.items() if k != "duplicate_events")
    assert stats.consumed == len(lines)
    assert stats.rejected == expected_rejected
    assert stats.duplicates == injected["duplicate_events"]
    assert sum(stats.rejected_by_reason.values()) == expected_rejected

    # Every rejected record actually landed on the DLQ topic, with its reason attached.
    dlq_records = _read_all(bootstrap, topics["dlq"])
    assert len(dlq_records) == expected_rejected
    assert {r["reason"] for r in dlq_records} == set(stats.rejected_by_reason)

    # Every consumed, non-duplicate valid record produced exactly one enriched record.
    enriched_records = _read_all(bootstrap, topics["enriched"])
    assert len(enriched_records) == stats.valid
    assert sum(1 for r in enriched_records if r["is_duplicate"]) == stats.duplicates

    # Sessions landed on the topic and in Parquet, and agree with each other.
    session_records = _read_all(bootstrap, topics["sessions"])
    assert len(session_records) == stats.sessions_completed
    assert stats.parquet_files, "expected at least one Parquet file to be written"
    total_parquet_rows = 0
    for path in stats.parquet_files:
        import pyarrow.parquet as pq

        total_parquet_rows += pq.read_table(path).num_rows
    assert total_parquet_rows == stats.sessions_completed

    # The consumer committed offsets for everything it read: no lag left behind.
    lag = consumer_lag(bootstrap, group_id, [topics["raw"]])
    assert lag[topics["raw"]] == 0


@requires_kafka
def test_rerunning_ensure_topics_is_a_no_op(bootstrap):
    run_id = uuid.uuid4().hex[:8]
    name = f"test.idempotent.{run_id}"
    ensure_topics(bootstrap, {name: 1})
    ensure_topics(bootstrap, {name: 1})  # must not raise on "already exists"


@requires_kafka
def test_producer_raises_on_an_unreachable_broker():
    import pytest

    with pytest.raises(Exception, match="(?i)transport|resolve|timed out|fail"):
        produce_lines("localhost:1", ["{}"], topic="unreachable", message_timeout_ms=2_000)
