# Context for a new session

Click-a-thon 2026 · **SonyLIV track** · Team Nirad · repo `C:\d\demo-sonyliv-clickhouse`

**Deadline: 12:00 IST, 2 August 2026.** Server-enforced. No extensions.

Read `docs/DESIGN.md` for the full argument. This file is the operational
handover: what is true, what is done, what will bite you.

---

## Where things run

| | |
|---|---|
| ClickHouse Cloud | `s9vfs5b226.ap-south-1.aws.clickhouse.cloud:8443`, user `default`, db `sony`, v26.2 |
| Credentials | `.env` (gitignored). **Password may have been rotated — check with the user.** |
| Local ClickHouse | standalone binary in WSL Ubuntu-20.04, `~/clickhouse`, port 8123, pw `hack`. Dev only. |
| ClickStack | Docker **inside WSL** (`docker` as root in WSL, NOT Docker Desktop — that never worked). Container `clickstack`, UI `localhost:8080`, OTLP `localhost:4318` |
| Dashboard | `python scripts/dashboard.py` → `localhost:877` |
| Source data | `C:\d\pre-check\click-a-thon-2026\SonyLiv\data\` (905K events + 33K content) |
| GitHub | https://github.com/preethamresearch/nirad-sonyliv-concurrency (public, MIT) |

Docker Desktop is broken on this machine and is **not** worth fixing. Use
`wsl -d Ubuntu-20.04 -u root -- bash -lc '...'`.

---

## The model, in one line

```
active = intent_playing AND client_alive
```

`intent_playing`: toggled ONLY by explicit transitions (VideoPlay /
AppForegrounded / `resume` open; `pause` / AppBackgrounded / VideoSessionEnd
close). `client_alive`: false during total event silence > 120s.

**Do not "simplify" this into one state machine.** Both single-machine
variants are wrong and we tested both:
- Closing on a heartbeat gap needs an explicit `resume` to reopen → undercounts
  a network drop mid-playback.
- Opening on heartbeats → overcounts every foreground pause.

---

## Facts measured from the data (do not re-derive, do not trust the docs)

| Fact | Value |
|---|---|
| Heartbeat cadence | **40.0s** (p90 = p95). The data dictionary says 60s and is **wrong**. |
| Liveness sub-types | `network-activity`, `buffer-health`, `video-resize` only |
| `pause`/`resume` | NOT event types — hidden inside `event_type='VideoHeartbeat'` as the `event` field |
| Foreground pause emits heartbeats | 15,660 / 19,060 (82%), median 6 beats in 120s |
| pause ≠ resume | 65% of sessions. bg ≠ fg in 466. **Collapse transitions, never pair them.** |
| Gap threshold | 120s = 3× cadence; p99 gap 96.4s; 0.894% exceed |
| `content_id` | can be **negative** (sentinel −987654322) → `Int64`, never `UInt64` |
| Timezone | server tz is Asia/Calcutta locally, UTC on Cloud → **all timestamps pinned `DateTime64(3,'UTC')`** |
| Session dims unstable | 120 sessions >1 `user_id`, 95 >1 `platform` → attribute by `argMin(…, event_timestamp_ms)` |
| Open sessions in provided data | **ZERO.** The unseen day will have them. Use `scripts/make_fixture.py`. |

---

## Headline numbers (Cloud, full dataset)

```
peak naive overlap       3,743
peak foreground-only     3,090      17.4% removed
ANDROID_PHONE + live       329 vs 448 → 26.6% removed
intervals               35,902  (oracle parity exact)
delta rows              31,521  vs 145,821 for a minute grid  (4.6x)
raw_events on disk        4.79 MiB from a 222 MB CSV  (~46x, 5.55 B/row)
full sealed run             77 s  (load 40s parallel, derive 3s, serve 4s)
stream throughput        2,028 events/s  one consumer, 6 partitions
```

---

## Bugs already fixed — do not reintroduce

1. **Dictionary is node-local.** `dictGet` for `video_type` returned empty for
   every row on Cloud because `SYSTEM RELOAD DICTIONARY` without `ON CLUSTER`
   refreshes one node. `video_type='live'` answered 0 instead of 469 while the
   dictionary reported `LOADED`. **Enrichment now LEFT JOINs `content_dim`.**
   Never put `dictGet` back in the derive path.
2. **`WITH FILL` must start at `t0`**, not the first present row, or a slice
   that exists only late in the range averages over the wrong denominator.
3. **Checkpoints use MINUTE containment**, matching the delta cumsum. Instant
   containment drops intervals living inside one minute.
4. **Checkpoint path must `FILL FROM` the anchor inclusive**, else minute `t0`
   is dropped on every hour-boundary query.
5. **Sort must be total**: `(ts, state_delta, close_code)`. `pause` and
   `AppBackgrounded` share a millisecond in 8,280 cases; sorting on ts alone is
   non-deterministic across engines.
6. **Empty transition arrays** crash `arrayPushFront(arrayPopBack([]),0)` —
   guarded with `arrayResize`. Only happens on days with open sessions.
7. **`ReplacingMergeTree` leaves ghosts** when re-derivation yields *fewer*
   intervals. Retract with a lightweight `DELETE` before re-inserting.
8. **`minute` must LEAD the sort key.** A trailing key column cannot prune
   granules; a one-hour query read the whole table.

---

## Commands

```bash
python scripts/ch.py                                   # connection check
python scripts/run_sealed.py --raw X.csv --content Y.csv   # THE sealed-day command
python scripts/verify_against_oracle.py --raw X.csv    # parity gate (must PASS)
python scripts/benchmark.py --raw X.csv                # 3-way agreement + latency
python scripts/dashboard.py                            # localhost:877
python scripts/demo_incremental.py                     # late-heartbeat proof
python scripts/make_fixture.py --raw X.csv --out fixtures/open_day.csv --cut-minutes-before-end 30
```

`run_sealed.py` is the **same code path** as everything else — no sealed-day
special case, deliberately.

---

## Status

**Done:** model, serving layer, checkpoints, hot tier, oracle parity (exact),
sealed harness, incremental proof, ClickStack tracing, provenance table,
MCP server, README, 492-word summary, pushed to GitHub.

**Added 1–2 Aug:**
- **8 dashboards** at `/app` (Overview, Stream Health, Content, Engagement,
  Languages, Live Pipeline, Pipeline, Architecture) + landing page at `/`.
  Old single view preserved at `/classic`. Deck at `/deck` (15 slides, print
  to PDF).
- **Streaming pipeline** `scripts/stream_pipeline.py`: Kafka (Redpanda, 6
  partitions, keyed by session) → validate → Redis dedup → ClickHouse, with a
  DLQ topic + `sony.stream_dlq`. 2,028 events/s. Replay proven idempotent
  (32,815 duplicates dropped, zero double-count).
- **Fault injector** `scripts/inject_faults.py`: 20 fault classes, deterministic
  seed, writes a manifest so findings can be checked against ground truth.
- **Resilient loader**: header matched BY NAME via `ALIASES`, String staging,
  SQL casts, `REQUIRED` contract that aborts rather than defaulting.
- `sql/05_streaming.sql`: materialized views (`ingest_rate`, `session_spans`),
  `schema_registry`, content CDC.
- **Query playground** on the Architecture tab; `scripts/codec_bench.py`.

**Bugs found by the dirty-data rehearsal (all fixed) — do not reintroduce:**
1. `run_sealed.py` had its OWN positional CSV insert, bypassing the hardened
   loader entirely. Both paths now go through `load.load_resilient`.
2. `oracle.py` hardcoded the header `event_timestamp` and crashed with
   KeyError on a renamed column. Now resolves via `load.ALIASES`.
3. Loader zeroed unparseable timestamps while the oracle skipped them, so
   parity compared two different populations. Both now REJECT.
4. `system.parts_columns.column_data_compressed_bytes` is **0 on Cloud**
   (SharedMergeTree). Use `system.parts.bytes_on_disk`.
5. Aliasing a column to its own name inside an aggregate
   (`argMinState(platform,…) AS platform`) is rejected as a nested aggregate.
   Qualify the source table.
6. A CDC materialized view cannot look up the "previous" row — it fires AFTER
   the insert, so previous == current. Append versions; diff with a window
   function at read time.
7. **Codec**: DoubleDelta is WRONG for `event_timestamp_ms` here. The sort key
   orders by session, not time, so timestamps jump at session boundaries.
   Delta is 17.1% smaller. Measured, see `scripts/codec_bench.py`.

**Running locally (WSL Docker, 3.7 GiB VM — near its ceiling):**
`clickstack` 8080 · `redpanda` 9092 · `redis` 6379 · `langfuse` 3000 ·
`librechat` 3080 (Gemini 2.5 Flash) · dashboard 877.

**Not done:** demo video (≤5 min). Deck needs Ctrl+P → PDF from `/deck`.
MCP server is stdio-only so it is NOT wired into LibreChat (needs an
HTTP/SSE transport to cross the container boundary).

**Credentials in the transcript — rotate after the event:** ClickHouse
password and the Gemini key both appear in chat history.

---

## Judgement notes

- Judges reward **trade-off reasoning**, not features. Every claim in the docs
  is backed by a measurement; keep it that way.
- **Be honest about what didn't work.** The checkpoint tier shows only ~1.01x
  fewer rows read on this dataset because 94% of events fall in one day. The
  incremental path is *slower* than a full rebuild here for the same reason.
  Both are stated plainly with their preconditions. Do not oversell them.
- The sealed day carries significant weight and **"no pipeline evidence, no
  credit."** Every run writes to `out/sealed/<run_id>/` and `sony.pipeline_runs`.
