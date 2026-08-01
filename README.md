# Foreground-only concurrency at streaming scale

**Team Nirad · Click-a-thon 2026 · SonyLIV track**

*"How many people are watching right now?"* is the most-asked question in a
streaming business and one of the hardest to answer honestly. An open app is
not a watching viewer. Counting paused, backgrounded and silent sessions
inflates the audience, and every ad-load, capacity and content decision made
on that dashboard inherits the error.

This is a concurrency model on ClickHouse that counts **only genuinely active
playback**, answers filtered minute-grain dashboard queries from a serving
layer rather than from raw session history, and absorbs late heartbeats
incrementally instead of rebuilding.

---

## The result

Measured on the provided dataset (905,558 events / 10,866 sessions / 12 days),
computed on ClickHouse Cloud:

| | Peak concurrent sessions |
|---|---|
| Naive overlap (session start → session end) | **3,743** |
| **Foreground-only (`intent ∧ alive`)** | **3,090** |
| Over-reported audience removed | **653 — 17.4%** |

Segment-level it is worse where it matters most: **live content on Android
phones is over-reported by 26.6%** (448 → 329), and that is exactly the
segment where ad load and capacity are most expensive to get wrong.

One representative session runs **1,692 s wall-clock with ~173 s of real
playback** — a 10× overcount — because it sat backgrounded for 25 minutes in
the middle.

---

## The model

The problem asks to exclude three things: paused, backgrounded, and
heartbeat-missing. They are not three cases of one rule. They are **two
independent signals**, and the answer is their intersection.

```
intent_playing   toggled ONLY by explicit transitions
                 open  : VideoPlay, AppForegrounded, event='resume'
                 close : event='pause', AppBackgrounded, VideoSessionEnd

client_alive     false during total event silence > 120 s

active           intent_playing AND client_alive
```

**Why not one state machine.** We built that first. It closed on a heartbeat
gap and needed an explicit `resume` to reopen — wrong for a network drop
mid-playback, where beats stop and return with no `resume` because the user
never paused. It undercounts the rest of the session.

**Why heartbeats cannot open an interval.** The mirror error, and the one most
teams will make. We measured it: **a foreground pause keeps emitting
heartbeats — 15,660 of 19,060 (82%), median 6 beats within 120 s.** Treating
beat presence as activity counts paused time as watching, which is the exact
overcount this problem exists to prevent.

### Four measurements that drove the design

| Finding | Evidence |
|---|---|
| Heartbeat cadence is **40 s, not the 60 s the data dictionary claims** | p90 = p95 = 40.0 s |
| `pause`/`resume` are **not event types** — they hide inside `event_type='VideoHeartbeat'` as the `event` sub-field | filtering on `event_type` silently counts all paused time as watching |
| State signals **do not balance** — transitions must be collapsed, never paired | `pause ≠ resume` in 65% of sessions; `bg ≠ fg` in 466 |
| Gap threshold of 120 s = 3× cadence fires on client death, not jitter | p99 gap 96.4 s; only 0.894% exceed 120 s |

---

## Architecture

```
raw_events                905,558   SharedMergeTree
  ORDER BY (video_session_id, event_timestamp_ms)   session-contiguous
  PARTITION BY toDate(session_start)                a session never splits
      │
      ▼  array algebra in ClickHouse — no cursor, no data leaves the database
session_active_intervals   35,902   SharedReplacingMergeTree(version)
      │
      ├── SEALED (closed) ──▶ concurrency_minute_delta      31,521 rows
      │                       +1 at start, −1 after end
      │                       ORDER BY (minute, dims…) + PROJECTION by_dimension
      │                       concurrency_hourly_checkpoint  1,964 rows
      │                       absolute level at each hour boundary
      │
      └── HOT (open) ───────▶ open_minute_delta  (VIEW, read-time)
                              bounded by concurrency, not retention

served:  concurrency_delta_all = sealed ∪ hot
query :  checkpoint anchor + deltas since  →  cost ∝ range, not retention
```

**Deltas, not a minute grid.** Two rows per interval regardless of duration:
**31,521 rows against the 145,821** a per-minute explosion needs — 4.6× smaller,
and the ratio worsens as sessions lengthen.

**Ordering was chosen by measurement, and our first choice was wrong.** We
ordered `(platform, country, video_type, content_id, minute)`; instrumenting
`read_rows` showed a one-hour query still read all 31,521 rows, because a
predicate on a trailing key column cannot prune granules. Every concurrency
question carries a time range, so `minute` now leads, with a `PROJECTION`
preserving dimension-first access.

**Peak cannot be pre-aggregated.** It is a max over a running total and is not
additive across dimensions — `platform` peaks at a different minute than
`platform + country`. Deltas stay at full grain and the cumulative sum runs
over whatever slice the filter selects.

---

## Verification

The answer key is private, so agreement between independent implementations is
the only correctness evidence we can generate ourselves. **Three paths must
agree on every query:**

| Path | Proves |
|---|---|
| `scripts/oracle.py` — Python, walks raw intervals | ground truth; deliberately simple and auditable |
| ClickHouse cumulative sum from t0 | the delta model is right |
| Checkpoint-anchored serving query | the optimisation did not change the answer |

`verify_against_oracle.py` compares **every interval**, not aggregates —
35,902 of 35,902 identical on boundaries, close reasons and open flags.

It has earned its keep. Bugs it caught that would each have shipped a
confidently wrong number:

1. **A Cloud-only silent dimension failure.** A dictionary is a *node-local*
   cache and `SYSTEM RELOAD DICTIONARY` without `ON CLUSTER` refreshes one
   node. On Cloud the derive ran against a stale node, every interval got
   `video_type=''`, and `video_type='live'` answered **0 instead of 469** —
   while the dictionary reported `LOADED` with 33,464 elements. A single-node
   local server cannot reproduce it.
2. `WITH FILL` started at the first *present* row, not `t0`, so a slice
   existing only late in the range averaged over 119 minutes instead of 17,029.
3. Checkpoints used *instant* containment while deltas used *minute*
   containment.
4. The checkpoint path dropped minute `t0` on hour boundaries — every "peak
   hour" query.
5. A non-deterministic sort: `pause` and `AppBackgrounded` share a millisecond
   in 8,280 cases and the two engines broke the tie differently.

---

## Open sessions

**The provided dataset cannot test the most heavily judged behaviour.** All
10,866 sessions have both a start and an end; not one is open. Yet the unseen
day is documented to contain them.

`scripts/make_fixture.py` manufactures the case by cutting the real stream at
an artificial "now": **3,526 open sessions (47.4%)**. It found a crash
immediately — a truncated session with heartbeats but no state transitions
gives an empty transition array, and `arrayPushFront(arrayPopBack([]), 0)` is
length 1 against length 0, killing the whole `INSERT`. Unreachable on the
provided data; certain on any day containing open sessions.

A late heartbeat costs **one replaced row**. Nothing is rebuilt.
`demo_incremental.py` proves incremental re-derivation is byte-identical to a
full rebuild.

---

## Running it

```bash
cp .env.example .env          # ClickHouse Cloud host / password

python scripts/load.py --schema --content <content.csv> --raw <raw.csv>
python scripts/verify_against_oracle.py --raw <raw.csv>
python scripts/benchmark.py --raw <raw.csv> --json out/benchmark.json
python scripts/dashboard.py                      # http://localhost:877

# the sealed day, end to end, one command
python scripts/run_sealed.py --raw <sealed.csv> --content <content.csv>
```

`run_sealed.py` is the **same code path** as everything above — no sealed-day
special case, because a special case is a step someone gets wrong at 09:00
while also recording a demo. It writes input SHA-256, the git commit,
per-stage row counts and timings, an oracle parity check, and ClickHouse's own
`query_log` to `out/sealed/<run_id>/`.

---

## Observability — ClickStack

The pipeline observes itself in ClickHouse. `scripts/otel.py` is a stdlib-only
OTLP exporter (no `opentelemetry-sdk`: a pip failure at 03:00 on the one
laptop we have is a worse outcome than 150 lines of JSON assembly).

- `clickhouse.query` — every statement with `read_rows`/`read_bytes` from
  ClickHouse's own summary header. Rows read is what shows whether the sort key
  and projection are earning their keep; wall time on a laptop measures the laptop.
- `pipeline.<stage>` — load / derive / serve / benchmark with row counts.
- `ingest.lag_seconds` — for a streaming concurrency service this decides
  whether the answer is trustworthy at all.

Tracing is env-configured and silently disables when unset. The observability
layer must never be able to fail the pipeline it watches.

---

## Layout

```
sql/01_schema.sql        landing tables, UTC-pinned, Int64 sentinel-safe
sql/02_intervals.sql     the model: intent ∧ alive, as array algebra
sql/03_serving.sql       deltas, checkpoints, projection, hot tier
scripts/oracle.py        independent reference implementation
scripts/verify_against_oracle.py   interval-by-interval parity gate
scripts/benchmark.py     3-way agreement + latency + rows read
scripts/run_sealed.py    one-command sealed-day harness
scripts/demo_incremental.py        late-heartbeat absorption proof
scripts/make_fixture.py  manufactures open sessions
scripts/dashboard.py + web/        live visualisation
docs/DESIGN.md           the full trade-off argument
```

Licensed MIT.
