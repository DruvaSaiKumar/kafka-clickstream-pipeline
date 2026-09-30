"""Session windowing: groups a user's events into sessions with an inactivity gap.

Pure Python, no Kafka dependency, so the windowing rules are unit-tested directly instead of only
through a live-broker integration test. This mirrors what a Kafka Streams session window does, but
implemented explicitly here so the mechanics (why a session closes, when it flushes) are visible
rather than hidden inside a framework.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

DEFAULT_GAP = timedelta(minutes=30)


@dataclass
class Session:
    user_id: str
    session_id: str
    start_ts: datetime
    last_ts: datetime
    event_count: int = 0
    urls: set[str] = field(default_factory=set)
    event_type_counts: dict[str, int] = field(default_factory=dict)
    has_purchase: bool = False
    purchase_value_total: float = 0.0

    def add(self, event) -> None:
        self.last_ts = max(self.last_ts, event.event_ts)
        self.event_count += 1
        self.urls.add(event.url)
        self.event_type_counts[event.event_type] = self.event_type_counts.get(event.event_type, 0) + 1
        if event.event_type == "PURCHASE":
            self.has_purchase = True
            self.purchase_value_total += event.value

    def to_record(self) -> dict:
        return {
            "user_id": self.user_id,
            "session_id": self.session_id,
            "start_ts": self.start_ts.isoformat(),
            "end_ts": self.last_ts.isoformat(),
            "duration_seconds": (self.last_ts - self.start_ts).total_seconds(),
            "event_count": self.event_count,
            "unique_urls": len(self.urls),
            "event_type_counts": dict(self.event_type_counts),
            "has_purchase": self.has_purchase,
            "purchase_value_total": round(self.purchase_value_total, 2),
        }


class SessionStore:
    """One open session per user_id. `update()` and `expire_idle()` are the only ways a session
    closes, so every closed session is observed exactly once by whichever caller drives them."""

    def __init__(self, gap: timedelta = DEFAULT_GAP):
        self.gap = gap
        self._open: dict[str, Session] = {}

    @property
    def open_count(self) -> int:
        return len(self._open)

    def current_session(self, user_id: str) -> Session | None:
        return self._open.get(user_id)

    def update(self, event) -> Session | None:
        """Feed one valid event. Returns the just-closed prior session, if this event starts a new
        one (a different session_id from the client, or the inactivity gap was exceeded)."""
        current = self._open.get(event.user_id)
        closed = None
        if current is not None and (
            current.session_id != event.session_id or event.event_ts - current.last_ts > self.gap
        ):
            closed = current
            current = None
        if current is None:
            current = Session(event.user_id, event.session_id, event.event_ts, event.event_ts)
            self._open[event.user_id] = current
        current.add(event)
        return closed

    def expire_idle(self, now: datetime) -> list[Session]:
        """Close every session whose last event is older than `gap` relative to `now`. A real
        streaming job calls this on a timer (a punctuation); the demo calls it once at the end of
        a bounded run so no session is left dangling when results are checked."""
        expired = [s for s in self._open.values() if now - s.last_ts > self.gap]
        for session in expired:
            del self._open[session.user_id]
        return expired

    def close_all(self) -> list[Session]:
        sessions = list(self._open.values())
        self._open.clear()
        return sessions
