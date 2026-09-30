import os

import pytest
from confluent_kafka.admin import AdminClient

from clickstream.admin import warmup_cluster

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


@pytest.fixture(scope="session", autouse=True)
def _warm_up_cluster_once():
    """Pay the one-time cost of Kafka's internal topics settling before any timed test runs,
    instead of leaving each test's own retry budget to absorb an unpredictable cold-start delay."""
    if _broker_reachable():
        warmup_cluster(BOOTSTRAP)


@pytest.fixture
def bootstrap():
    return BOOTSTRAP
