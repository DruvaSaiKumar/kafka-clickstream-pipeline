"""Topic management and consumer-lag reporting."""

from __future__ import annotations

from confluent_kafka import TopicPartition
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


def consumer_lag(
    bootstrap: str, group_id: str, topics: list[str], timeout: float = 15.0
) -> dict[str, int]:
    """Committed-offset lag per topic, summed across partitions. 0 means fully caught up."""
    from confluent_kafka import Consumer

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
