# Foreground-only concurrency at streaming scale

Team Nirad · Click-a-thon 2026 · SonyLIV track

This problem is won on trade-off thinking, so this document is the argument,
not the feature list. Every number below was measured on the provided dataset
(905,558 events / 10,866 sessions / 2026-07-14 → 2026-07-26 UTC).

---

## 1. What the data actually says

We did not design from the data dictionary. We measured first, and four of the
findings changed the design.

| Finding | Evidence | Consequence |
|---|---|---|
| **Heartbeat cadence is 40s, not 60s** | p90 = p95 = 40.0s for `network-activity`, `buffer-health`, `video-resize` | The dictionary says "every 1 minute". A gap threshold derived from the docs misclassifies. Ours is derived from the distribution. |
| **`pause`/`resume` are not event types** | They live inside `event_type='VideoHeartbeat'` as the `event` sub-field | Filtering on `event_type` alone silently counts *all* paused time as watching |
| **A foreground pause keeps emitting heartbeats** | 15,660 of 19,060 foreground pauses (82%) have liveness beats within 120s, median 6 | **"Heartbeat ⇒ watching" is false.** This single fact determines the whole model. |
| **State signals do not balance** | `pause ≠ resume` in 7,091 of 10,866 sessions (65%); `bg ≠ fg` in 466 | Transitions must be *collapsed*, never *paired* |

Three more that cost us working time:

- `content_dim` ships a sentinel `content_id = -987654322`. `UInt64` rejects it
  and aborts the entire load. `content_id` is `Int64`.
- The server timezone is `Asia/Calcutta` locally and UTC on ClickHouse Cloud.
  A bare `DateTime64(3)` renders differently in the two places — a 5h30m
  divergence between where we build and where we are judged. Every timestamp
  is pinned `DateTime64(3, 'UTC')`.
- Session dimensions are not stable: 120 sessions carry more than one
  `user_id`, 95 more than one `platform`. Attribution uses `argMin` by event
  timestamp so it is reproducible regardless of row arrival order.

---

## 2. The model: `active = intent_playing AND client_alive`

The problem asks to exclude three things — paused, backgrounded, and
heartbeat-missing. They are not three cases of one rule; they are two
independent signals, and the answer is their intersection.

```
intent_playing   toggled ONLY by explicit transitions
                 open  : VideoPlay, AppForegrounded, event='resume'
                 close : event='pause', AppBackgrounded, VideoSessionEnd

client_alive     false during total event silence > 120s

active           intent_playing AND client_alive
```

**Why not a single state machine.** We built one first. It closed an interval
on a heartbeat gap and required an explicit `resume` to reopen. That is wrong
for the common case of a network drop mid-playback: the beats stop, then
return, and no `resume` is emitted because the user never paused. The single
machine undercounts the rest of the session.

**Why heartbeats cannot open an interval.** The mirror-image error. Beats
continue through a foreground pause (82% of them), so treating beat presence
as activity counts paused time as watching — the exact overcount the problem
exists to prevent.

Two step functions, intersected, get both right. `intent` answers *did they
want to watch*; `alive` answers *were they there at all*.

**Threshold.** `GAP_TIMEOUT_MS = 120000`, three times the observed 40.0s
cadence. The gap distribution justifies it: p95 = 40.0s, p99 = 96.4s, and only
0.894% of gaps exceed 120s. It fires on client death, not on jitter.

**Grace = 0.** When a gap closes an interval we credit the viewer only to the
last proof of life. Judges state plainly that overcounting is the failure mode
this problem exists to prevent, so the conservative choice is the defensible
one. It is a parameter, not a constant.

### Result

| | Peak concurrent sessions |
|---|---|
| Naive session-overlap (start → end) | **3,743** |
| Beat-as-activity (the wrong model) | 2,922 |
| **Foreground-only (`intent AND alive`)** | **3,090** |

Naive overlap overstates peak by **17.4%**. On individual sessions the error is
far larger — one representative session runs 1,692s wall-clock with ~173s of
genuine playback, a **10× overcount**, because it sat backgrounded for 25
minutes in the middle.

---

## 3. Storage: why deltas, why checkpoints, why not a minute grid

**Not a minute grid.** Exploding each active interval into one row per minute
makes peak trivial but grows with total *watch time*: 145,820 rows here
against 31,522 deltas, **4.6× more**, and the ratio worsens as sessions
lengthen. The problem statement names per-minute explosion of all history as a
choice that only works at hackathon size. It is right.

**Deltas.** `+1` at the start minute, `−1` after the end minute. Exactly two
rows per interval regardless of duration. `SummingMergeTree` collapses
duplicate (dims, minute) keys on merge.

**Checkpoints.** Concurrency at minute *m* is a running total from the
beginning of time, so a pure delta model must scan all history to answer a
question about one hour. `concurrency_hourly_checkpoint` stores absolute
concurrency at each hour boundary per dimension combination, so a query reads
one checkpoint row plus the deltas since. Cost becomes proportional to the
range queried, not to retention.

Each interval emits only the boundaries it actually spans, so building them is
O(total interval-hours), not O(intervals × retention).

**Peak cannot be pre-aggregated.** It is a max over a running total and it is
not additive across dimensions: `platform=ANDROID_PHONE` peaks at a different
minute than `platform=ANDROID_PHONE AND country=india`. Pre-computing peaks
per combination would require materialising the power set. Deltas therefore
stay at full dimension grain and the cumulative sum runs over whatever slice
the filter selects.

### Ordering was chosen by measurement, and our first choice was wrong

We ordered `(platform, country, video_type, content_id, minute)` on the theory
that filtered queries want one contiguous dimension range. Instrumenting
`read_rows` killed it: a **one-hour query still read all 31,522 rows**, because
a predicate on a trailing key column cannot prune granules.

Every concurrency question carries a time range — that is what makes it a
concurrency question — so `minute` now leads the sort key, with a `PROJECTION`
preserving dimension-first access. `ANDROID_PHONE` went from 24,576 to 18,243
rows read.

A projection over `SummingMergeTree` needs `deduplicate_merge_projection_mode`
set explicitly; we use `'rebuild'` so the projection is regenerated from merged
results rather than silently dropped.

---

## 4. Open sessions and the hot tier

**The provided dataset cannot test this.** All 10,866 sessions have both a
start and an end. Not one is open. Yet the unseen day is documented to contain
them and judges score how the serving layer absorbs them — so the most heavily
weighted behaviour in this problem is the one the given data does not exercise.

`scripts/make_fixture.py` manufactures it by cutting the real event stream at
an artificial "now": every session whose `VideoSessionEnd` falls after the cut
becomes genuinely open, with a real partial history. At a 30-minute cut that is
**3,526 open sessions (47.4%)**.

It found a crash immediately. A truncated session with heartbeats but no state
transitions yields an empty transition array, where
`arrayPushFront(arrayPopBack([]), 0)` is length 1 against length 0 and the
whole `INSERT` dies with `SIZES_OF_ARRAYS_DONT_MATCH`. Unreachable on the
provided data. Certain on any day containing open sessions.

**The tiers.**

```
sealed  closed intervals -> concurrency_minute_delta (+ hourly checkpoints)
        immutable, append-only, never recomputed

hot     open intervals -> sony.open_minute_delta, a VIEW over the
        ReplacingMergeTree, evaluated at read time
        bounded by concurrency, not retention: only sessions open RIGHT NOW

served  sony.concurrency_delta_all = sealed UNION ALL hot
```

A late heartbeat costs one replaced row in `session_active_intervals`. Nothing
is rebuilt and the served number moves on the next query. Checkpoints are
sealed-only, so a query anchoring on one adds back the open intervals spanning
that boundary — otherwise the anchor understates the level and every minute
after it reads low.

**Cost, stated honestly.** The hot tier is not free: reading it takes the
open-interval set through `FINAL`, which raised total rows read from ~19K to
~60K on the fixture. At production scale it would be materialised into a small
table refreshed on a short interval rather than evaluated per query.

---

## 5. Verification

The answer key is private, so agreement between independent implementations is
the only correctness evidence we can generate ourselves. Three paths must agree
on every query:

| Path | What it proves |
|---|---|
| `scripts/oracle.py` — Python, walks raw intervals | ground truth, deliberately simple and auditable |
| ClickHouse cumulative sum from t0 | the delta model is right |
| Checkpoint-anchored serving query | the optimisation did not change the answer |

`scripts/verify_against_oracle.py` compares **every interval**, not aggregates:
35,901 of 35,901 identical on the provided data — boundaries, close reasons and
open flags. On the open-session fixture, 20,298 of 20,298.

Reaching that agreement found five real bugs that would each have produced a
confidently wrong answer:

1. `WITH FILL` started at the first *present* row rather than at `t0`. A slice
   existing only in the last two hours averaged over 119 minutes instead of the
   17,029 requested — wrong by two orders of magnitude.
2. Checkpoints used *instant* containment while deltas used *minute*
   containment, so intervals living inside a single minute disagreed.
3. The checkpoint path dropped minute `t0` whenever `t0` fell on an hour
   boundary — i.e. every "peak hour" dashboard query.
4. The oracle attributed dimensions by file order; 95 sessions change platform
   mid-session.
5. `ReplacingMergeTree` left ghost intervals when re-derivation produced fewer
   intervals than before (see §6).

---

## 6. Incremental updates

`scripts/demo_incremental.py` replays the scenario judges describe: a day cut
30 minutes early, then the next 10 minutes of events arrive.

**Correct.** Incremental re-derivation of only the touched sessions produces
byte-identical output to a full rebuild — 27,150 intervals, 1,448.4 active
hours, peak 3,090.

**The ghost-interval bug.** `ReplacingMergeTree` replaces by
`(video_session_id, interval_seq)`, but re-deriving a session can produce
*fewer* intervals than before: an open session's tail is fragmented by the
alive mask while its last event sits behind the watermark, and late heartbeats
merge those fragments. Orphaned high-seq rows have no replacement and survive —
27,707 intervals instead of 27,150. Fixed by retracting touched sessions with a
lightweight `DELETE` before re-deriving. Still O(touched), never O(history).

**Performance, with its precondition.** On this dataset the incremental path is
*slower* than a full rebuild: 824K rows read vs 707K, 2.4s vs 1.3s. Two
structural reasons:

- 94% of all events fall in a single day (29,651 of 31,522 delta rows), so
  partition pruning has almost no sealed history to skip.
- The late window touched 4,970 of 9,173 sessions (54%) because it lands in a
  live spike.

The design wins when *touched-sessions / total* and *touched-partitions /
total* are both small. That is the production case. On this dataset both are
approximately 1, and we would rather show the measurement and the precondition
than quote a speedup the data does not support.

---

## 7. Behaviour at 100×

| Component | Growth | Bounded by |
|---|---|---|
| `raw_events` | linear in events | partition + TTL |
| `session_active_intervals` | ~3.3 rows/session | partition by `active_start` date |
| `concurrency_minute_delta` | 2 rows/interval, independent of duration | daily partitions |
| `concurrency_hourly_checkpoint` | interval-hours × dimension combos | the checkpoint interval itself is tunable |
| Query read cost | **range queried**, not retention | checkpoint anchor |
| Hot tier | sessions open *now* | concurrency, not history |

The choices that would break at 100× and are deliberately absent: per-minute
explosion of history, recomputing overlap from raw sessions per query, and
rebuilding on every late event.

What we would change next, in order: materialise the hot tier into a small
table on a short refresh instead of a read-time view; make the checkpoint
interval adaptive (hourly is arbitrary — the right value depends on the ratio
of query range to retention); and add distinct-*user* concurrency, which is
non-additive and needs `uniqState` in an `AggregatingMergeTree` rather than
`+1/−1` deltas.

---

## 8. Reproducing

```bash
cp .env.example .env          # point at ClickHouse Cloud or a local server
python scripts/load.py --schema --content <content.csv> --raw <raw.csv>
python scripts/verify_against_oracle.py --raw <raw.csv>
python scripts/benchmark.py --raw <raw.csv> --json out/benchmark.json

# the sealed day, end to end, one command
python scripts/run_sealed.py --raw <sealed.csv> --content <content.csv>
```

`run_sealed.py` is the same code path as everything above — there is no
sealed-day special case, because a special case is a step someone gets wrong at
09:00 while also recording a demo. It writes input checksums, the git commit,
per-stage row counts and timings, an independent oracle parity check, and
ClickHouse's own `query_log` to `out/sealed/<run_id>/`.
