"""Real-time clickstream sessionization on Kafka."""

__version__ = "0.1.0"

RAW_TOPIC = "clickstream.raw"
DLQ_TOPIC = "clickstream.dlq"
ENRICHED_TOPIC = "clickstream.enriched"
SESSIONS_TOPIC = "clickstream.sessions"
GROUP_ID = "clickstream-sessionizer"
