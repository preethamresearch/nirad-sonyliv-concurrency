-- =====================================================================
-- Click-a-thon 2026 · SonyLIV · Team Nirad
-- 03 — Serving layer: minute deltas + hourly checkpoints
--
-- The dashboard question is "peak and average concurrency over a range,
-- filtered by platform / country / content / video_type, at minute grain".
--
-- WHY DELTAS AND NOT A MINUTE GRID
-- Exploding every active interval into one row per minute makes the peak
-- query trivial, but storage grows with total *watch time*: ~1.83M
-- minute-rows for 35,901 intervals here, and linearly worse as sessions
-- lengthen. The problem statement names "per-minute explosion of all
-- history" as a choice that only works at hackathon size, and it is right.
-- A delta model stores exactly 2 rows per interval regardless of duration.
--
-- WHY CHECKPOINTS ON TOP OF DELTAS
-- Concurrency at minute m is the running total of deltas from the beginning
-- of time, so a naive delta model must scan all history to answer a query
-- about one hour. Hourly checkpoints store the absolute concurrency at each
-- hour boundary per dimension combination, so a query reads
--     one checkpoint row  +  the deltas since that boundary
-- and its cost becomes proportional to the RANGE QUERIED, not to retention.
-- That is the difference between a design that survives 100x and one that
-- does not.
--
-- WHY PEAK CANNOT BE PRE-AGGREGATED
-- Peak is a max over a running total, and it is not additive across
-- dimensions: platform=ANDROID_PHONE peaks at a different minute than
-- platform=ANDROID_PHONE AND country=india. Pre-computing peaks per
-- dimension combination would require materialising the power set. So
-- deltas stay at full dimension grain and the cumulative sum runs over
-- whatever slice the filter selects.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Minute deltas. +1 at the minute an interval starts, -1 at the minute
-- after it ends (half-open [start, end], so a session alive during any
-- part of a minute counts toward that minute).
--
-- SummingMergeTree collapses duplicate (dims, minute) keys on merge, so
-- concurrent sessions on the same slice cost one row, not N.
--
-- ORDERING: `minute` LEADS the sort key.
--
-- We first ordered this (platform, country, video_type, content_id, minute)
-- on the theory that filtered queries want one contiguous dimension range.
-- Instrumenting rows-read killed that theory: with minute last, a one-hour
-- query still read all 31,522 rows, because a predicate on a trailing key
-- column cannot prune granules. Every concurrency question carries a time
-- range -- that is what makes it a concurrency question -- so time is the
-- selective predicate and it must lead.
--
-- Dimension-first access is preserved by a PROJECTION rather than by a
-- second table: ClickHouse keeps both orderings of the same data in sync and
-- picks whichever answers the query with fewer reads.
--
-- Daily partitions give a second, coarser pruning layer and bound the work a
-- backfill or a TTL drop has to do.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sony.concurrency_minute_delta
(
    platform        LowCardinality(String),
    country         LowCardinality(String),
    video_type      LowCardinality(String),
    content_id      Int64,
    minute          DateTime('UTC'),
    delta           Int32,

    PROJECTION by_dimension
    (
        SELECT platform, country, video_type, content_id, minute, delta
        ORDER BY (platform, country, video_type, content_id, minute)
    )
)
ENGINE = SummingMergeTree(delta)
PARTITION BY toDate(minute)
ORDER BY (minute, platform, country, video_type, content_id)
-- A projection over a SummingMergeTree needs an explicit policy for what
-- happens when a merge collapses rows: ClickHouse refuses by default rather
-- than let the projection drift from the summed base table. 'rebuild'
-- regenerates the projection from the merged result, so the two orderings
-- always agree. 'drop' would be cheaper but silently loses the projection --
-- and a silently-missing index is exactly the kind of thing that looks fine
-- in testing and falls over on the sealed dataset.
SETTINGS deduplicate_merge_projection_mode = 'rebuild';


-- ---------------------------------------------------------------------
-- Hourly checkpoints: absolute concurrency at each hour boundary.
--
-- concurrency_at(H) = number of active intervals whose [start, end] spans H.
-- Written once per hour per dimension combination and never revised for
-- sealed data, so a range query never scans behind its own start.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sony.concurrency_hourly_checkpoint
(
    platform        LowCardinality(String),
    country         LowCardinality(String),
    video_type      LowCardinality(String),
    content_id      Int64,
    hour_boundary   DateTime('UTC'),
    concurrency     UInt32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(hour_boundary)
ORDER BY (platform, country, video_type, content_id, hour_boundary);


-- ---------------------------------------------------------------------
-- Populate deltas from SEALED (closed) intervals only.
--
-- Deliberately an explicit INSERT..SELECT rather than a materialized view
-- on session_active_intervals. The interval table is a ReplacingMergeTree:
-- an open interval that grows as heartbeats arrive is re-emitted with a
-- higher version. A materialized view fires on INSERT and never sees the
-- replacement, so every extension of an open session would double-count in
-- the serving layer. Sealing closed intervals here and computing the small
-- open tail at query time keeps the arithmetic exact.
-- ---------------------------------------------------------------------
INSERT INTO sony.concurrency_minute_delta
SELECT platform, country, video_type, content_id, minute, sum(d) AS delta
FROM
(
    SELECT platform, country, video_type, content_id,
           toDateTime(intDiv(active_start_ms, 60000) * 60, 'UTC') AS minute,
           1 AS d
    FROM sony.session_active_intervals FINAL
    WHERE is_open = 0
    UNION ALL
    SELECT platform, country, video_type, content_id,
           toDateTime((intDiv(active_end_ms, 60000) + 1) * 60, 'UTC') AS minute,
           -1 AS d
    FROM sony.session_active_intervals FINAL
    WHERE is_open = 0
)
GROUP BY platform, country, video_type, content_id, minute;


-- ---------------------------------------------------------------------
-- THE HOT TIER.
--
-- Sealed deltas above cover closed intervals only. Open intervals -- sessions
-- still running at the watermark, whose active range grows every time another
-- heartbeat lands -- are deliberately NOT materialised. Writing them into an
-- append-only SummingMergeTree would mean either re-inserting a correction row
-- per heartbeat or rebuilding the table, and both are the "recompute on every
-- update" behaviour the problem statement singles out as the wrong answer.
--
-- Instead they are computed at read time from the interval table. The set is
-- bounded by concurrency, not by retention: however long the service runs,
-- only sessions open RIGHT NOW appear here. On the open-session fixture that
-- is a few thousand rows against a sealed tier that grows forever.
--
-- A late heartbeat therefore costs exactly one ReplacingMergeTree row in
-- session_active_intervals. Nothing is rebuilt, and the served number moves
-- on the next query.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW sony.open_minute_delta AS
SELECT platform, country, video_type, content_id, minute, toInt32(sum(d)) AS delta
FROM
(
    SELECT platform, country, video_type, content_id,
           toDateTime(intDiv(active_start_ms, 60000) * 60, 'UTC') AS minute, 1 AS d
    FROM sony.session_active_intervals FINAL
    WHERE is_open = 1
    UNION ALL
    SELECT platform, country, video_type, content_id,
           toDateTime((intDiv(active_end_ms, 60000) + 1) * 60, 'UTC') AS minute, -1 AS d
    FROM sony.session_active_intervals FINAL
    WHERE is_open = 1
)
GROUP BY platform, country, video_type, content_id, minute;


-- The single surface every dashboard query reads: sealed history plus the
-- live tail, unioned. Callers never need to know which tier a row came from.
CREATE OR REPLACE VIEW sony.concurrency_delta_all AS
SELECT platform, country, video_type, content_id, minute, delta
FROM sony.concurrency_minute_delta
UNION ALL
SELECT platform, country, video_type, content_id, minute, delta
FROM sony.open_minute_delta;


-- ---------------------------------------------------------------------
-- Populate hourly checkpoints from sealed intervals.
--
-- Checkpoints cover the SEALED tier only, so a query that anchors on one must
-- add back the open intervals spanning that boundary. That correction is in
-- the query layer (scripts/benchmark.py :: sql_anchor), not here, because it
-- has to be recomputed per query against the current open set.
-- ---------------------------------------------------------------------
-- Each interval emits only the hour boundaries it actually spans -- first
-- boundary at or after its start, last at or before its end. Cost is
-- O(total interval-hours), not O(intervals x retention): a 12-minute
-- session contributes at most one row no matter how long the dataset is.
INSERT INTO sony.concurrency_hourly_checkpoint
SELECT
    platform, country, video_type, content_id,
    toDateTime(hb, 'UTC') AS hour_boundary,
    toUInt32(count())     AS concurrency
FROM
(
    SELECT
        platform, country, video_type, content_id,
        -- Checkpoints MUST use the same minute-containment semantics as the
        -- delta cumulative sum, or the two halves of a range query disagree.
        -- An interval is "concurrent at hour boundary H" iff it is concurrent
        -- in the MINUTE containing H -- not iff it spans the instant H. The
        -- instant definition drops any interval that lives entirely inside a
        -- single minute, which the delta path counts.
        intDiv(active_start_ms, 60000) * 60 AS s_min_sec,
        intDiv(active_end_ms,   60000) * 60 AS e_min_sec,
        intDiv(s_min_sec + 3599, 3600) * 3600 AS h_first,
        intDiv(e_min_sec, 3600) * 3600        AS h_last,
        if(h_last >= h_first,
           arrayMap(i -> h_first + (i - 1) * 3600,
                    range(1, toUInt32(intDiv(h_last - h_first, 3600)) + 2)),
           CAST([], 'Array(Int64)')) AS boundaries
    FROM sony.session_active_intervals FINAL
    WHERE is_open = 0
)
ARRAY JOIN boundaries AS hb
GROUP BY platform, country, video_type, content_id, hour_boundary;
