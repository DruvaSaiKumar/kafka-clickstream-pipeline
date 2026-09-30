import os

import pytest
from confluent_kafka.admin import AdminClient

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def _broker_reachable() -> bool:
    try:
        metadata = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=5)
        return metadata is not None
    except Exception:
        return False


requires_kafka = pytest.mark.skipif(
    not _broker_reachable(),
    reason=f"no Kafka broker reachable at {BOOTSTRAP} (set KAFKA_BOOTSTRAP, or `docker compose up -d`)",
)


@pytest.fixture
def bootstrap():
    return BOOTSTRAP
