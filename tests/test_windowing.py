from datetime import datetime, timedelta, timezone

from clickstream.windowing import SessionStore

T0 = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)


class E:
    """Minimal stand-in for a ClickEvent: SessionStore only reads these attributes."""

    def __init__(self, user_id, session_id, event_type, url, value, ts):
        self.user_id, self.session_id, self.event_type = user_id, session_id, event_type
        self.url, self.value, self.event_ts = url, value, ts


def click(user="u1", session="s1", minute=0, event_type="CLICK", url="/a", value=None):
    return E(user, session, event_type, url, value, T0 + timedelta(minutes=minute))


def test_events_in_the_same_session_accumulate():
    store = SessionStore(gap=timedelta(minutes=30))
    store.update(click(minute=0, url="/a"))
    store.update(click(minute=5, url="/b"))
    closed = store.update(click(minute=10, url="/a"))  # revisits /a
    assert closed is None
    s = store.current_session("u1")
    assert s.event_count == 3 and len(s.urls) == 2


def test_inactivity_gap_closes_the_session_on_the_next_event():
    store = SessionStore(gap=timedelta(minutes=30))
    store.update(click(minute=0))
    store.update(click(minute=10))
    closed = store.update(click(minute=45))  # 35 min after the last event: gap exceeded
    assert closed is not None and closed.event_count == 2
    assert store.current_session("u1").event_count == 1  # the new event started a fresh session


def test_a_new_session_id_from_the_client_closes_the_old_one_even_within_the_gap():
    store = SessionStore(gap=timedelta(minutes=30))
    store.update(click(session="s1", minute=0))
    closed = store.update(click(session="s2", minute=1))
    assert closed is not None and closed.session_id == "s1"
    assert store.current_session("u1").session_id == "s2"


def test_expire_idle_closes_only_sessions_past_the_gap():
    store = SessionStore(gap=timedelta(minutes=30))
    store.update(click(user="u1", minute=0))
    store.update(click(user="u2", minute=50))
    expired = store.expire_idle(now=T0 + timedelta(minutes=60))
    assert [s.user_id for s in expired] == ["u1"]
    assert store.open_count == 1 and store.current_session("u2") is not None


def test_close_all_returns_and_clears_every_open_session():
    store = SessionStore()
    store.update(click(user="u1"))
    store.update(click(user="u2"))
    closed = store.close_all()
    assert {s.user_id for s in closed} == {"u1", "u2"}
    assert store.open_count == 0


def test_purchase_totals_and_event_type_counts():
    store = SessionStore()
    store.update(click(event_type="PAGE_VIEW", value=None))
    store.update(click(minute=1, event_type="PURCHASE", value=19.99))
    closed = store.update(click(minute=90, event_type="PAGE_VIEW", value=None))  # forces a close
    assert closed.has_purchase is True
    assert closed.purchase_value_total == 19.99
    assert closed.event_type_counts == {"PAGE_VIEW": 1, "PURCHASE": 1}


def test_to_record_is_json_ready():
    store = SessionStore()
    store.update(click())
    record = store.close_all()[0].to_record()
    assert record["duration_seconds"] == 0.0
    assert isinstance(record["start_ts"], str) and isinstance(record["end_ts"], str)
