"""Topic management, cluster warm-up, and consumer-lag reporting."""

from __future__ import annotations

import time
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topics(bootstrap: str, topics: dict[str, int], timeout: float = 30.0) -> None:
    """Create each topic (name -> partition count) if it does not already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=timeout).topics
    missing = {name: parts for name, parts in topics.items() if name not in existing}
    if not missing:
        return
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=parts, replication_factor=1) for name, parts in missing.items()]
    )
    for future in futures.values():
        try:
            future.result(timeout=timeout)
        except Exception as exc:  # noqa: BLE001 - surface every topic error, including "already exists"
            if "already exists" not in str(exc).lower():
                raise


def warmup_cluster(bootstrap: str, timeout: float = 120.0) -> None:
    """Force Kafka's internal topics (`__consumer_offsets`, `__transaction_state`) to be created
    and their coordinators elected, by actually running one produce-idempotent + consume + commit
    cycle through to completion, retrying through the "coordinator load in progress" /
    "not coordinator" errors that are expected the first time a fresh broker is used for either.

    Call this once, before any timing-sensitive consumer loop runs (`consumer.run`'s own idle
    detection is not meant to absorb an unbounded one-time cluster bootstrap cost). How long this
    takes varies with the environment - a few seconds locally, sometimes closer to a minute on a
    shared CI runner - which is exactly why it is a generous, retried loop rather than a fixed wait.
    """
    topic = f"__warmup_{uuid.uuid4().hex[:8]}"
    ensure_topics(bootstrap, {topic: 1}, timeout=timeout)
    deadline = time.time() + timeout
    last_exc: Exception | None = None

    while time.time() < deadline:
        producer = Producer({
            "bootstrap.servers": bootstrap, "enable.idempotence": True,
            "message.timeout.ms": int(timeout * 1000),
        })  # fmt: skip
        consumer = Consumer({
            "bootstrap.servers": bootstrap, "group.id": f"warmup-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False, "auto.offset.reset": "earliest",
        })  # fmt: skip
        try:
            producer.produce(topic, value=b"warmup")
            producer.flush(timeout=timeout)
            consumer.assign([TopicPartition(topic, 0, 0)])
            msg = None
            read_deadline = time.time() + min(15.0, deadline - time.time())
            while msg is None and time.time() < read_deadline:
                msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                consumer.commit(msg, asynchronous=False)
                return  # both coordinators are up: a produce and a commit both succeeded
        except Exception as exc:  # noqa: BLE001 - any of the above failing just means "not ready yet"
            last_exc = exc
        finally:
            consumer.close()
        time.sleep(2)

    raise RuntimeError(f"Kafka internal topics did not become ready within {timeout}s: {last_exc}")


def consumer_lag(
    bootstrap: str, group_id: str, topics: list[str], timeout: float = 15.0
) -> dict[str, int]:
    """Committed-offset lag per topic, summed across partitions. 0 means fully caught up."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap, "group.id": group_id,
        "enable.auto.commit": False,
    })  # fmt: skip
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        metadata = admin.list_topics(timeout=timeout)
        lag: dict[str, int] = {}
        for topic in topics:
            parts = list(metadata.topics[topic].partitions) if topic in metadata.topics else []
            tps = [TopicPartition(topic, p) for p in parts]
            committed = consumer.committed(tps, timeout=timeout)
            total = 0
            for tp in committed:
                low, high = consumer.get_watermark_offsets(tp, timeout=timeout, cached=False)
                current = tp.offset if tp.offset is not None and tp.offset >= 0 else low
                total += max(0, high - current)
            lag[topic] = total
        return lag
    finally:
        consumer.close()
