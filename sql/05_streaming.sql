-- =====================================================================
-- Click-a-thon 2026 · SonyLIV · Team Nirad
-- 05 — Streaming layer: materialized views, schema registry, content CDC
--
-- These sit on the ingest path, NOT on the oracle-parity path. That
-- separation is deliberate: parity is checked against an independently
-- computed answer, and a view that silently changed the derivation would
-- invalidate the only check that proves the model correct. Everything here
-- is additive -- it observes and summarises the stream; it does not
-- redefine concurrency.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. INGEST RATE — SummingMergeTree, incremental on insert.
--
-- A batch job that recomputes "events per minute" scans everything it has
-- ever seen. A materialized view adds one row per (minute, platform) per
-- INSERT block and the merge tree collapses them in the background: the
-- cost is proportional to what ARRIVED, not to what is retained. That is
-- the entire argument for incremental transformation.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sony.ingest_rate
(
    minute    DateTime('UTC'),
    platform  LowCardinality(String),
    events    UInt64,
    sessions  AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
ORDER BY (minute, platform);

CREATE MATERIALIZED VIEW IF NOT EXISTS sony.ingest_rate_mv
TO sony.ingest_rate AS
SELECT
    toStartOfMinute(event_time) AS minute,
    platform,
    count()                     AS events,
    uniqState(video_session_id) AS sessions
FROM sony.raw_events
GROUP BY minute, platform;

-- ---------------------------------------------------------------------
-- 2. SESSION SPANS — one row per session, maintained incrementally.
--
-- argMinState / argMaxState keep the FIRST and LAST value by event time
-- across every insert block, which is the same argMin attribution rule the
-- pipeline uses for unstable session dimensions (120 sessions carry more
-- than one user_id; 95 carry more than one platform). Doing it in an
-- AggregatingMergeTree means a late-arriving heartbeat revises the session
-- span without re-reading the session's other events.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sony.session_spans
(
    video_session_id String,
    first_ms         SimpleAggregateFunction(min, Int64),
    last_ms          SimpleAggregateFunction(max, Int64),
    events           SimpleAggregateFunction(sum, UInt64),
    platform         AggregateFunction(argMin, LowCardinality(String), Int64),
    country          AggregateFunction(argMin, LowCardinality(String), Int64),
    content_id       AggregateFunction(argMin, Int64, Int64),
    platforms_seen   AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
ORDER BY video_session_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS sony.session_spans_mv
TO sony.session_spans AS
-- The source table is aliased and every column qualified: an unqualified
-- `argMinState(platform, ...) AS platform` makes the alias shadow the column
-- and ClickHouse rejects it as an aggregate inside an aggregate.
SELECT
    r.video_session_id AS video_session_id,
    min(r.event_timestamp_ms) AS first_ms,
    max(r.event_timestamp_ms) AS last_ms,
    toUInt64(count())         AS events,
    argMinState(r.platform,   r.event_timestamp_ms) AS platform,
    argMinState(r.country,    r.event_timestamp_ms) AS country,
    argMinState(r.content_id, r.event_timestamp_ms) AS content_id,
    uniqState(toString(r.platform))                 AS platforms_seen
FROM sony.raw_events AS r
GROUP BY r.video_session_id;

-- Readable projection of the above. platforms_seen > 1 is the multi-device
-- case: 95 sessions in the provided data, 82 of which genuinely OVERLAP in
-- time rather than being a sequential handoff between devices.
CREATE VIEW IF NOT EXISTS sony.session_spans_v AS
SELECT
    s.video_session_id AS video_session_id,
    min(s.first_ms)  AS first_ms,
    max(s.last_ms)   AS last_ms,
    sum(s.events)    AS events,
    argMinMerge(s.platform)     AS platform,
    argMinMerge(s.country)      AS country,
    argMinMerge(s.content_id)   AS content_id,
    uniqMerge(s.platforms_seen) AS platforms_seen,
    fromUnixTimestamp64Milli(min(s.first_ms)) AS first_seen,
    fromUnixTimestamp64Milli(max(s.last_ms))  AS last_seen
FROM sony.session_spans AS s
GROUP BY s.video_session_id;

-- ---------------------------------------------------------------------
-- 3. SCHEMA REGISTRY
--
-- The producer's field names are not ours and will drift. The loader already
-- carries an alias table and a required-column contract; the registry is
-- where that contract becomes observable rather than buried in Python.
--
-- Every distinct producer key-set seen is registered with a fingerprint. A
-- new fingerprint is a schema change -- which is information, not an error.
-- The `compatible` flag records whether the required columns could still be
-- resolved: a producer may add or reorder freely, but dropping a required
-- field is a breaking change and must be visible the moment it appears.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sony.schema_registry
(
    fingerprint   String,
    producer      LowCardinality(String),
    fields        Array(String),
    mapped        Array(String),
    unmapped      Array(String),
    missing       Array(String),
    compatible    UInt8,
    first_seen    DateTime64(3,'UTC'),
    last_seen     SimpleAggregateFunction(max, DateTime64(3,'UTC')),
    events_seen   SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (producer, fingerprint);

-- ---------------------------------------------------------------------
-- 4. CONTENT CDC
--
-- content_dim is a ReplacingMergeTree keyed by content_id, so a re-load
-- overwrites in place and the previous value is gone at the next merge.
-- That is correct for serving and useless for answering "did this title's
-- category change WHILE it was streaming?" -- which matters, because a
-- change mid-stream silently re-buckets everything already counted.
--
-- The CDC log is append-only and keeps every version with the value that
-- preceded it, so a metadata change becomes an auditable event.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sony.content_cdc
(
    changed_at    DateTime64(3,'UTC') DEFAULT now64(3),
    content_id    Int64,
    op            LowCardinality(String),   -- insert | update
    title         String,
    video_type    LowCardinality(String),
    category      LowCardinality(String),
    prev_title    String,
    prev_video_type LowCardinality(String),
    prev_category   LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (content_id, changed_at);

-- Append EVERY version, compare NOTHING at write time.
--
-- The obvious design -- join the incoming row against the current table to
-- find its predecessor -- does not work, and failing at it is instructive:
-- a materialized view fires AFTER the block is inserted, so a lookup against
-- content_dim already returns the new value. "Previous" resolves to "current"
-- and every change looks like a no-op. This was caught by changing a category
-- and watching the CDC log stay empty.
--
-- So the view does no comparison at all. It records versions; the diff is a
-- read-time window function over them, which is also the only formulation
-- that stays correct when two versions arrive in the same block.
CREATE MATERIALIZED VIEW IF NOT EXISTS sony.content_cdc_mv
TO sony.content_cdc AS
SELECT
    now64(3)   AS changed_at,
    content_id AS content_id,
    'version'  AS op,
    title      AS title,
    video_type AS video_type,
    category   AS category,
    ''         AS prev_title,
    ''         AS prev_video_type,
    ''         AS prev_category
FROM sony.content_dim;

-- The diff. lagInFrame over (content_id ORDER BY changed_at) gives each
-- version its predecessor; the first version of a content_id has none and is
-- an insert. Rows where nothing actually moved are dropped, so an idempotent
-- re-load of the full snapshot produces no spurious changes.
CREATE VIEW IF NOT EXISTS sony.content_changes AS
SELECT changed_at, content_id, op, title, video_type, category,
       prev_title, prev_video_type, prev_category
FROM (
    SELECT
        changed_at, content_id, title, video_type, category,
        lagInFrame(title)      OVER w AS prev_title,
        lagInFrame(video_type) OVER w AS prev_video_type,
        lagInFrame(category)   OVER w AS prev_category,
        row_number()           OVER w AS version,
        if(row_number() OVER w = 1, 'insert', 'update') AS op
    FROM sony.content_cdc
    WINDOW w AS (PARTITION BY content_id ORDER BY changed_at
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
WHERE version = 1
   OR title != prev_title
   OR video_type != prev_video_type
   OR category != prev_category
ORDER BY changed_at DESC;


-- ---------------------------------------------------------------------
-- 5. EVENT-TYPE RATE (decline watch)
--
-- The decline watch compares error / rebuffer / close / start rates across
-- two 15-minute windows. Asking raw_events costs a full scan per refresh
-- (its sort key leads with session, so a time predicate cannot prune) --
-- measured at 6,999,168 rows read for a 30-minute question. This summing
-- MV answers the same question from (minute x event_type) rows: bounded by
-- the window, incremental on insert, and the backfill is one scan ever.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sony.event_type_minute
(
    minute      DateTime('UTC'),
    event_type  LowCardinality(String),
    events      UInt64
)
ENGINE = SummingMergeTree
ORDER BY (minute, event_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS sony.event_type_minute_mv
TO sony.event_type_minute AS
SELECT toDateTime(intDiv(r.event_timestamp_ms, 60000) * 60, 'UTC') AS minute,
       r.event_type AS event_type,
       toUInt64(count()) AS events
FROM sony.raw_events AS r
GROUP BY minute, event_type;
