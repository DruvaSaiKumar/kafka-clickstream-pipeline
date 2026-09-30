"""Consumes raw events, validates and deduplicates them, sessionizes valid ones in real time,
and emits enriched events, completed sessions, and a Parquet sink.

Delivery semantics: offsets are committed synchronously after each message is fully handled
(routed to the DLQ, or turned into an enriched record and folded into the session store). That is
at-least-once, not exactly-once: a crash between finishing the work and the commit replays the
message. The in-memory `seen_ids` de-dup guard makes a replay of a *previously committed* message
harmless if it recurs later in the same process, but it is not persisted, so it does not survive a
restart. A production version would persist de-dup state (or rely on idempotent downstream writes)
instead of relying on this process's memory.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from . import DLQ_TOPIC, ENRICHED_TOPIC, GROUP_ID, RAW_TOPIC, SESSIONS_TOPIC
from .parquet_sink import write_sessions
from .schema import ValidationError, parse_event
from .windowing import DEFAULT_GAP, SessionStore

logger = logging.getLogger(__name__)


@dataclass
class RunStats:
    consumed: int = 0
    valid: int = 0
    duplicates: int = 0
    rejected: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=dict)
    sessions_completed: int = 0
    parquet_files: list[str] = field(default_factory=list)


def _dlq_record(raw_bytes: bytes, reason: str) -> bytes:
    return json.dumps(
        {"reason": reason, "raw": raw_bytes.decode("utf-8", errors="replace")[:2000]}
    ).encode()


def run(
    bootstrap: str,
    out_dir: str,
    earliest: datetime,
    now: datetime,
    idle_polls: int = 5,
    poll_timeout: float = 1.0,
    session_gap=DEFAULT_GAP,
    group_id: str = GROUP_ID,
    raw_topic: str = RAW_TOPIC,
    dlq_topic: str = DLQ_TOPIC,
    enriched_topic: str = ENRICHED_TOPIC,
    sessions_topic: str = SESSIONS_TOPIC,
) -> RunStats:
    """Consume clickstream.raw until `idle_polls` consecutive polls return nothing (the topic has
    caught up), then close every still-open session. Suited to a bounded run against a finite,
    already-produced input, which is exactly the demo and the integration tests; a long-running
    deployment would instead call `store.expire_idle()` on a timer and never reach "idle".

    Partitions are statically assigned rather than subscribed to. This is one sessionizer instance
    per topic, not a group of interchangeable consumers, so there is nothing to dynamically
    rebalance; `assign()` also skips Kafka's ~3s initial-consumer-group-rebalance delay, which
    otherwise races against `idle_polls` and can make a short bounded run look empty. Offsets are
    still committed under `group_id`, so `lag_monitor.py` reports progress the normal way.
    """
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })  # fmt: skip
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(raw_topic, timeout=15)
    partitions = metadata.topics[raw_topic].partitions
    consumer.assign([TopicPartition(raw_topic, p) for p in partitions])

    stats = RunStats()
    store = SessionStore(gap=session_gap)
    seen_ids: set[str] = set()
    session_buffer: list[dict] = []
    empty_polls = 0
    # Separate, more generous budget for transient broker/coordinator errors: the first time a
    # brand-new consumer group commits against a just-started broker, the internal
    # __consumer_offsets topic and its coordinator can take longer to settle than a real "we've
    # caught up on data" idle gap should ever have to wait. This must not count against
    # `idle_polls`, or a slow-but-recovering broker looks identical to "no more data".
    retriable_errors_left = 30

    def emit(topic: str, value: bytes, key: bytes | None = None) -> None:
        producer.produce(topic, value=value, key=key)
        producer.poll(0)

    def close_session(session) -> None:
        stats.sessions_completed += 1
        record = session.to_record()
        emit(sessions_topic, json.dumps(record).encode(), key=session.user_id.encode())
        session_buffer.append(record)

    try:
        while empty_polls < idle_polls:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                if msg.error().retriable() or msg.error().code() == KafkaError.NOT_COORDINATOR:
                    if retriable_errors_left <= 0:
                        raise RuntimeError(f"consumer error persisted past retry budget: {msg.error()}")
                    retriable_errors_left -= 1
                    logger.warning("retriable consumer error, will retry: %s", msg.error())
                    continue  # not an empty poll: does not count toward idle_polls
                raise RuntimeError(f"consumer error: {msg.error()}")
            empty_polls = 0

            stats.consumed += 1
            raw_bytes = msg.value()
            try:
                raw = json.loads(raw_bytes)
            except json.JSONDecodeError:
                stats.rejected += 1
                stats.rejected_by_reason["invalid JSON"] = (
                    stats.rejected_by_reason.get("invalid JSON", 0) + 1
                )
                emit(dlq_topic, _dlq_record(raw_bytes, "invalid JSON"), key=msg.key())
                consumer.commit(msg, asynchronous=False)
                continue

            try:
                event = parse_event(raw, earliest, now)
            except ValidationError as exc:
                stats.rejected += 1
                reason = str(exc)
                stats.rejected_by_reason[reason] = stats.rejected_by_reason.get(reason, 0) + 1
                emit(dlq_topic, _dlq_record(raw_bytes, reason), key=msg.key())
                consumer.commit(msg, asynchronous=False)
                continue

            is_duplicate = event.event_id in seen_ids
            if is_duplicate:
                stats.duplicates += 1
            else:
                seen_ids.add(event.event_id)
                closed = store.update(event)
                if closed is not None:
                    close_session(closed)
            stats.valid += 1

            current_count = None
            if not is_duplicate:
                open_session = store.current_session(event.user_id)
                current_count = open_session.event_count if open_session else None
            enriched = {
                "event_id": event.event_id, "user_id": event.user_id, "session_id": event.session_id,
                "event_type": event.event_type, "event_ts": event.event_ts.isoformat(),
                "is_duplicate": is_duplicate, "session_event_count": current_count,
            }  # fmt: skip
            emit(enriched_topic, json.dumps(enriched).encode(), key=msg.key())
            consumer.commit(msg, asynchronous=False)

        for session in store.close_all():
            close_session(session)

        producer.flush(timeout=30)
        if session_buffer:
            stats.parquet_files = [str(p) for p in write_sessions(session_buffer, out_dir)]
        return stats
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run(
        bootstrap="localhost:9092",
        out_dir="data/sessions",
        earliest=datetime(2024, 1, 1, tzinfo=timezone.utc),
        now=datetime.now(timezone.utc),
    )
    print(result)
