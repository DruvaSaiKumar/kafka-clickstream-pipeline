from datetime import datetime, timezone

import pytest

from clickstream.schema import ValidationError, parse_event

EARLIEST = datetime(2024, 1, 1, tzinfo=timezone.utc)
NOW = datetime(2024, 7, 1, 12, 0, tzinfo=timezone.utc)


def good(**overrides):
    event = {
        "event_id": "e1", "user_id": "u1", "session_id": "s1", "event_type": "CLICK",
        "url": "/product/1", "referrer": "/home", "device": "DESKTOP", "value": None,
        "event_ts": "2024-06-15T10:30:00Z",
    }  # fmt: skip
    event.update(overrides)
    return event


def test_valid_event():
    e = parse_event(good(), EARLIEST, NOW)
    assert e.event_ts == datetime(2024, 6, 15, 10, 30, tzinfo=timezone.utc)
    assert e.value is None


def test_offset_timestamps_are_converted_to_utc():
    e = parse_event(good(event_ts="2024-06-15T05:30:00-05:00"), EARLIEST, NOW)
    assert e.event_ts == datetime(2024, 6, 15, 10, 30, tzinfo=timezone.utc)


def test_purchase_requires_a_non_negative_value():
    e = parse_event(good(event_type="PURCHASE", value=19.99), EARLIEST, NOW)
    assert e.value == 19.99


@pytest.mark.parametrize(
    ("overrides", "fragment"),
    [
        ({"event_id": ""}, "event_id"),
        ({"user_id": None}, "user_id"),
        ({"session_id": 5}, "session_id"),
        ({"event_type": "SCROLL"}, "event_type"),
        ({"device": "SMART_TV"}, "device"),
        ({"url": "product/1"}, "absolute path"),
        ({"referrer": "home"}, "referrer"),
        ({"event_ts": None}, "event_ts"),
        ({"event_ts": "not-a-time"}, "ISO-8601"),
        ({"event_ts": "2024-06-15T10:30:00"}, "UTC offset"),
        ({"event_ts": "2023-01-01T00:00:00Z"}, "before"),
        ({"event_ts": "2024-07-05T00:00:00Z"}, "future"),
        ({"event_type": "PURCHASE", "value": None}, "required"),
        ({"event_type": "PURCHASE", "value": -5}, "negative"),
        ({"event_type": "PURCHASE", "value": 999_999}, "exceeds"),
        ({"event_type": "PURCHASE", "value": "abc"}, "must be a number"),
        ({"value": 5.0}, "must be absent"),
    ],
)
def test_rejections(overrides, fragment):
    with pytest.raises(ValidationError, match=fragment):
        parse_event(good(**overrides), EARLIEST, NOW)


def test_non_object_and_missing_field():
    with pytest.raises(ValidationError, match="not a JSON object"):
        parse_event([1, 2], EARLIEST, NOW)
    raw = good()
    del raw["url"]
    with pytest.raises(ValidationError, match="url"):
        parse_event(raw, EARLIEST, NOW)
