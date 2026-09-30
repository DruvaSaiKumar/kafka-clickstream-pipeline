"""Clickstream event contract and validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

EVENT_TYPES = frozenset({"PAGE_VIEW", "CLICK", "ADD_TO_CART", "PURCHASE", "ERROR"})
DEVICES = frozenset({"DESKTOP", "MOBILE", "TABLET"})
MAX_VALUE = 100_000.0


class ValidationError(ValueError):
    """A record breaks the contract; the message becomes the DLQ reason."""


@dataclass(frozen=True)
class ClickEvent:
    event_id: str
    user_id: str
    session_id: str
    event_type: str
    url: str
    referrer: str | None
    device: str
    value: float | None  # purchase amount; only set (and only valid) for PURCHASE events
    event_ts: datetime  # timezone-aware, UTC


def _required_str(raw: dict, field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field} is missing or not a non-empty string")
    return value.strip()


def _timestamp(value: object, earliest: datetime, now: datetime) -> datetime:
    if not isinstance(value, str):
        raise ValidationError("event_ts is missing or not a string")
    try:
        ts = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        raise ValidationError("event_ts is not an ISO-8601 timestamp") from None
    if ts.tzinfo is None:
        raise ValidationError("event_ts has no UTC offset")
    ts = ts.astimezone(timezone.utc)
    if ts < earliest:
        raise ValidationError(f"event_ts is before {earliest.isoformat()}")
    if ts > now + timedelta(days=1):
        raise ValidationError("event_ts is more than a day in the future")
    return ts


def _value(raw: object, event_type: str) -> float | None:
    if event_type == "PURCHASE":
        if raw is None or isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ValidationError("value is required and must be a number for PURCHASE events")
        value = float(raw)
        if value < 0:
            raise ValidationError("value must not be negative")
        if value > MAX_VALUE:
            raise ValidationError(f"value exceeds the {MAX_VALUE:g} cap")
        return value
    if raw is not None:
        raise ValidationError(f"value must be absent for {event_type} events")
    return None


def parse_event(raw: object, earliest: datetime, now: datetime) -> ClickEvent:
    if not isinstance(raw, dict):
        raise ValidationError("record is not a JSON object")
    event_type = _required_str(raw, "event_type")
    if event_type not in EVENT_TYPES:
        raise ValidationError(f"event_type {event_type!r} is not one of {sorted(EVENT_TYPES)}")
    device = _required_str(raw, "device")
    if device not in DEVICES:
        raise ValidationError(f"device {device!r} is not one of {sorted(DEVICES)}")
    url = _required_str(raw, "url")
    if not url.startswith("/"):
        raise ValidationError("url must be an absolute path starting with '/'")
    referrer = raw.get("referrer")
    if referrer is not None and (not isinstance(referrer, str) or not referrer.startswith("/")):
        raise ValidationError("referrer must be null or an absolute path starting with '/'")
    return ClickEvent(
        event_id=_required_str(raw, "event_id"),
        user_id=_required_str(raw, "user_id"),
        session_id=_required_str(raw, "session_id"),
        event_type=event_type,
        url=url,
        referrer=referrer,
        device=device,
        value=_value(raw.get("value"), event_type),
        event_ts=_timestamp(raw.get("event_ts"), earliest, now),
    )
