"""Synthetic clickstream events with a known set of injected problems (seeded, deterministic).

Nothing here comes from any real site or company. Events are generated per simulated user as a
short browsing session: a handful of page views, maybe a click or a cart add, occasionally a
purchase. A known number of defects and duplicates are injected into disjoint slices of the
stream, so a downstream run has exact, checkable results.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

PAGES = ["/home", "/search", "/product/1", "/product/2", "/product/3", "/cart", "/checkout", "/help"]
DEVICES = ["DESKTOP", "MOBILE", "TABLET"]
START = datetime(2024, 6, 1, 9, 0, tzinfo=timezone.utc)

# Defects injected into the stream (disjoint slices of events, so each count is exact).
DEFECTS = {
    "missing_user_id": 8,
    "bad_event_type": 6,
    "bad_device": 5,
    "naive_timestamp": 6,
    "purchase_missing_value": 7,
    "negative_value": 4,
    "value_on_non_purchase": 5,
    "not_json": 4,
    "duplicate_events": 10,
}


def _user_session(rng: random.Random, user_n: int, start: datetime) -> list[dict]:
    """One simulated browsing session for one user: 3-8 events a few seconds to minutes apart."""
    user_id, session_id = f"U{user_n:05d}", str(uuid.uuid4())
    n = rng.randint(3, 8)
    events, ts, purchased = [], start, False
    for i in range(n):
        ts = ts + timedelta(seconds=rng.randint(5, 240))
        if i == n - 1 and rng.random() < 0.3:
            event_type, value, purchased = "PURCHASE", round(rng.uniform(9.99, 249.99), 2), True
        elif rng.random() < 0.15 and not purchased:
            event_type, value = "ADD_TO_CART", None
        elif rng.random() < 0.05:
            event_type, value = "ERROR", None
        else:
            event_type, value = ("PAGE_VIEW", None) if i == 0 else ("CLICK", None)
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "url": rng.choice(PAGES),
            "referrer": rng.choice(PAGES) if i > 0 else None,
            "device": rng.choice(DEVICES),
            "value": value,
            "event_ts": ts.isoformat().replace("+00:00", "Z"),
        })  # fmt: skip
    return events


def generate_events(n_users: int = 150, seed: int = 21) -> tuple[list[str], dict[str, int]]:
    """Return (JSON-encoded lines in send order, injected defect counts).

    A line may be malformed JSON (the `not_json` defect); every other line is a JSON object that
    may or may not pass schema validation, matching what a producer would actually put on the wire.
    """
    rng = random.Random(seed)
    events: list[dict] = []
    cursor = START
    for u in range(1, n_users + 1):
        cursor = cursor + timedelta(seconds=rng.randint(0, 20))
        events += _user_session(rng, u, cursor)

    purchase_idx = [i for i, e in enumerate(events) if e["event_type"] == "PURCHASE"]
    non_purchase_idx = [i for i, e in enumerate(events) if e["event_type"] != "PURCHASE"]

    non_purchase_need = (DEFECTS["missing_user_id"] + DEFECTS["bad_event_type"]
                         + DEFECTS["bad_device"] + DEFECTS["naive_timestamp"]
                         + DEFECTS["value_on_non_purchase"])  # fmt: skip
    non_purchase_sample = rng.sample(non_purchase_idx, non_purchase_need)
    purchase_sample = rng.sample(
        purchase_idx, DEFECTS["purchase_missing_value"] + DEFECTS["negative_value"]
    )
    mutated = set(non_purchase_sample) | set(purchase_sample)
    non_purchase_pool, purchase_pool = iter(non_purchase_sample), iter(purchase_sample)

    for _ in range(DEFECTS["missing_user_id"]):
        del events[next(non_purchase_pool)]["user_id"]
    for _ in range(DEFECTS["bad_event_type"]):
        events[next(non_purchase_pool)]["event_type"] = "SCROLL"
    for _ in range(DEFECTS["bad_device"]):
        events[next(non_purchase_pool)]["device"] = "SMART_TV"
    for _ in range(DEFECTS["naive_timestamp"]):
        e = events[next(non_purchase_pool)]
        e["event_ts"] = e["event_ts"].rstrip("Z")  # drop the UTC marker -> naive, no offset
    for _ in range(DEFECTS["value_on_non_purchase"]):
        events[next(non_purchase_pool)]["value"] = round(rng.uniform(1, 50), 2)

    for _ in range(DEFECTS["purchase_missing_value"]):
        del events[next(purchase_pool)]["value"]
    for _ in range(DEFECTS["negative_value"]):
        e = events[next(purchase_pool)]
        e["value"] = -abs(e["value"])

    # Duplicates always copy an untouched, valid record: this defect tests event_id dedup on
    # otherwise-good data, not interaction with the other defects.
    clean_idx = [i for i in range(len(events)) if i not in mutated]
    duplicate_pool = rng.sample(clean_idx, DEFECTS["duplicate_events"])
    duplicates = [dict(events[i]) for i in duplicate_pool]

    lines = [json.dumps(e) for e in events] + [json.dumps(e) for e in duplicates]
    lines += ["{not valid json" for _ in range(DEFECTS["not_json"])]
    rng.shuffle(lines)
    return lines, dict(DEFECTS)
