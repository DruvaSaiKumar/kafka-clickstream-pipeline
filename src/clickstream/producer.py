"""Publishes raw clickstream lines to Kafka. Does not validate: that is the consumer's job,
matching how a real edge/collector service works (accept everything, validate downstream)."""

from __future__ import annotations

import json
import logging

from confluent_kafka import Producer

from . import RAW_TOPIC

logger = logging.getLogger(__name__)


def _key_for(line: str) -> bytes | None:
    """Partition by user_id when the line parses, so one user's events stay in order on one
    partition. A line that isn't even valid JSON has no key; the consumer routes it to the DLQ."""
    try:
        raw = json.loads(line)
    except json.JSONDecodeError:
        return None
    user_id = raw.get("user_id") if isinstance(raw, dict) else None
    return user_id.encode("utf-8") if isinstance(user_id, str) else None


def produce_lines(
    bootstrap: str, lines: list[str], topic: str = RAW_TOPIC, message_timeout_ms: int = 10_000
) -> dict[str, int]:
    """Send every line; returns delivery counts. Raises if any message is ultimately not
    acknowledged by the broker (a producer that silently drops data is worse than one that fails).

    `message.timeout.ms` is set explicitly (10s, well under librdkafka's 5-minute default) so an
    unreachable broker is reported quickly instead of only after a long flush() wait.
    """
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "message.timeout.ms": message_timeout_ms,
    })  # fmt: skip
    delivered, failed = 0, []

    def on_delivery(err, msg):
        nonlocal delivered
        if err is not None:
            failed.append(str(err))
        else:
            delivered += 1

    for line in lines:
        producer.produce(topic, value=line.encode("utf-8"), key=_key_for(line), on_delivery=on_delivery)
        producer.poll(0)
    still_in_flight = producer.flush(timeout=message_timeout_ms / 1000 + 30)

    if failed or still_in_flight:
        raise RuntimeError(
            f"{len(failed)} message(s) failed delivery, {still_in_flight} still in flight: {failed[:5]}"
        )
    return {"sent": len(lines), "delivered": delivered}
