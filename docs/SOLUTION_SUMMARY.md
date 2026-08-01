# Solution Summary — Foreground-only concurrency at streaming scale

**Team Nirad · SonyLIV track**

**The problem.** An open app is not a watching viewer. Counting paused,
backgrounded and silent sessions inflates the audience, and ad-load, capacity
and content decisions inherit the error.

**What the data said.** We measured before designing, and four findings drove
everything. Heartbeat cadence is 40s, not the 60s the data dictionary claims
(p90 = p95 = 40.0s). `pause`/`resume` are not event types — they hide inside
`event_type='VideoHeartbeat'` as the `event` sub-field, so filtering on
`event_type` silently counts all paused time as watching. Decisively, **a
foreground pause keeps emitting heartbeats: 15,660 of 19,060 (82%), median 6
beats within 120s** — so "heartbeat means watching" is false. And state
signals do not balance (`pause ≠ resume` in 65% of sessions), so transitions
must be collapsed, never paired.

**The model.** The three exclusions the problem names are two independent
signals, and the answer is their intersection:

`active = intent_playing AND client_alive`

`intent_playing` is toggled only by explicit transitions. `client_alive` is
false during event silence beyond 120s (3× cadence; p99 gap is 96.4s, only
0.894% exceed it). A single state machine gets one case wrong whichever way
you build it: closing on a heartbeat gap needs an explicit `resume` to reopen,
which undercounts a network drop mid-playback; opening on heartbeats
overcounts every foreground pause.

**How ClickHouse is used.** Interval derivation runs entirely in ClickHouse as
array algebra over per-session event sequences — `arraySort`, `arraySplit`,
`arrayFilter`, `arrayResize`. `raw_events` is ordered
`(video_session_id, event_timestamp_ms)` so each session is physically
contiguous, and partitioned by session-start date so no session splits across
partitions.

The serving layer stores **minute deltas**, not a minute grid: two rows per
interval regardless of duration, 31,521 rows against the 145,821 a per-minute
explosion needs. Hourly checkpoints store absolute concurrency at each hour
boundary, so a range query reads one checkpoint plus the deltas since — cost
proportional to the range, not to retention. Ordering was chosen by
measurement: our first sort key put `minute` last and a one-hour query still
read every row, because a predicate on a trailing key column cannot prune
granules. `minute` now leads, with a `PROJECTION` preserving dimension-first
access. Peak cannot be pre-aggregated — it is a max over a running total, not
additive across dimensions.

Open sessions live in a **hot tier** computed at read time from a
`ReplacingMergeTree`, bounded by concurrency rather than retention. A late
heartbeat costs one replaced row; nothing is rebuilt.

**Verification.** With the answer key private, we built an independent Python
oracle and require three paths to agree on every query. It compares every
interval, not aggregates: 35,902 of 35,902 identical. It caught five real
bugs, including a Cloud-only failure where a node-local dictionary cache made
`video_type='live'` answer 0 instead of 469 while reporting itself healthy — a
single-node server cannot reproduce it.

**Result.** Peak concurrency 3,090 versus 3,743 naive — **17.4% of reported
audience removed**, and 26.6% for live content on Android phones.

ClickStack traces the pipeline into ClickHouse. Everything runs on ClickHouse
Cloud (Mumbai).

*(492 words)*
