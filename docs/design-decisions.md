# Design notes

**confluent-kafka, not kafka-python.** It wraps librdkafka, the same client library most production
Kafka deployments actually run, rather than a pure-Python reimplementation of the wire protocol. The
delivery-report callback, idempotent producer flag, and manual offset commits used here all map
directly onto real deployment concerns.

**Static partition assignment, not `subscribe()`.** This is one sessionizer process per topic, not a
pool of interchangeable consumers sharing work, so there is no group membership to rebalance.
`assign()` also sidesteps Kafka's initial consumer-group-rebalance delay, which otherwise races
against the idle-detection loop below and can make a short bounded run look like it received nothing.
Offsets are still committed under a `group_id`, so `lag_monitor.py` and standard consumer-group
tooling still work. The trade-off: if you wanted several sessionizer instances splitting the
partitions of one topic, this design would need to go back to `subscribe()`.

**At-least-once, not exactly-once.** Offsets commit synchronously after a message is fully handled
(routed to the DLQ, or folded into the session store and emitted downstream), so a crash between
finishing the work and the commit replays that message. An in-memory `seen_ids` set makes a
replayed *valid* message harmless — it is counted as a duplicate rather than double-processed — but
that set is not persisted. A restart forgets it, so a replay across a restart could in principle be
processed twice. A production version would persist de-dup state (or size the downstream writes to
be idempotent on their own) instead of relying on this process's memory.

**A retry budget for coordinator errors, separate from "caught up."** The consumer decides it has
caught up on a topic after several consecutive empty polls. The very first time a brand-new
consumer group commits against a freshly started broker, Kafka's internal `__consumer_offsets`
topic and its coordinator can take a few seconds to settle, which shows up as a `NOT_COORDINATOR`
error on that first commit attempt. Counting that toward "caught up" was an actual bug during
development (see below) — the fix keeps a separate, more generous retry budget for retriable broker
errors, so a slow-but-recovering coordinator is not mistaken for "no more data."

**Session windowing is hand-rolled, not Kafka Streams or Faust.** `windowing.py` is plain Python with
no Kafka import at all, so the rule for when a session closes (a client-side session id change, or
an inactivity gap) is directly unit-tested rather than exercised only through a live-broker test.
The real cost of this choice: state lives in the consumer process's memory, with no fault-tolerant
state store behind it. A crash loses every open session. A production version at meaningful scale
would want Kafka Streams' session windows (or an equivalent with a changelog-backed state store)
specifically for that durability, not for the windowing logic itself, which is not complicated.

**A Parquet sink alongside the Kafka topics.** Completed sessions land on `clickstream.sessions` for
any downstream Kafka consumer, and also as partitioned Parquet, so there is a durable, queryable
output that does not depend on Kafka's retention window. This mirrors the curated-layer pattern in
the other portfolio projects.

**Why the demo force-closes every open session at the end.** A live deployment relies purely on the
inactivity gap (via a periodic `expire_idle()` call) to decide a session is over. The demo and the
integration tests run against a bounded, already-fully-produced input, so waiting out a real
30-minute gap isn't practical; they call `close_all()` once the consumer has caught up, so every
session in the finite input is accounted for in the results.

## A real bug this caught

Early on, `run()` used `consumer.subscribe()` and treated every consumer error (including a
transient `NOT_COORDINATOR` right after a topic's first-ever use) the same as "no more messages,"
so the very first integration test run against a freshly started broker reported zero consumed
messages — the coordinator hiccup ran out the idle-poll budget before real data ever arrived. Fixed
by switching to static assignment and giving retriable broker errors their own retry budget,
separate from idle detection (both described above). Re-running the exact same test against a
freshly reformatted, cold broker afterward passed cleanly.

## What is not verified

- Single broker, single partition-set, replication factor 1 throughout. Nothing here has been run
  against a multi-broker cluster, so leader failover and replication are untested.
- No test restarts the consumer mid-run to check what actually happens to in-flight de-dup state
  across a crash; the at-least-once/in-memory-dedup trade-off above is reasoned about, not measured.
- No throughput or load testing. Volumes here are small (a few hundred to a few thousand events).
- Local verification on the development machine used a raw Kafka (KRaft) binary directly, since
  Docker was not available there. `docker-compose.yml` is the documented path for a reader, and CI
  runs against a real Kafka service container — both are exercised, just not on that one machine.
