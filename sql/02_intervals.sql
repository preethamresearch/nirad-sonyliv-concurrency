-- =====================================================================
-- Click-a-thon 2026 · SonyLIV · Team Nirad
-- 02 — Active-interval derivation: the heart of the solution
--
--   active = intent_playing AND client_alive
--
-- intent_playing  toggled ONLY by explicit state transitions
--                 open : VideoPlay, AppForegrounded, event='resume'
--                 close: event='pause', AppBackgrounded, VideoSessionEnd
-- client_alive    false during total event silence > GAP_TIMEOUT_MS
--
-- Why an intersection and not one state machine:
--
--  * A foreground pause KEEPS EMITTING heartbeats -- measured: 15,660 of
--    19,060 foreground pauses (82%) have periodic beats within the next
--    120s, median 6 beats. So "a heartbeat means watching" is false, and
--    intent must come from explicit transitions alone.
--
--  * Conversely a network drop mid-playback stops the beats without any
--    pause event, and the beats returning is not always followed by an
--    explicit `resume`. A single state machine that closes on a gap would
--    need a resume to reopen and would undercount the rest of the session.
--
-- Modelling them as two independent step functions and intersecting gets
-- both right, and maps exactly onto the three exclusions the problem asks
-- for: paused, backgrounded, heartbeat-missing.
--
-- Parameters (substituted from .env / CLI at apply time):
--   GAP_TIMEOUT_MS  120000  = 3x the observed 40.0s beat cadence.
--                             p95 gap 40.0s, p99 96.4s, only 0.894% exceed
--                             120s -- fires on client death, not jitter.
--   GAP_GRACE_MS         0  = credit the viewer only to the last proof of
--                             life. Judges state overcounting is the failure
--                             mode this problem exists to prevent.
-- =====================================================================

CREATE TABLE IF NOT EXISTS sony.session_active_intervals
(
    video_session_id  String              CODEC(ZSTD(3)),
    interval_seq      UInt16,

    active_start      DateTime64(3, 'UTC'),
    active_end        DateTime64(3, 'UTC'),
    active_start_ms   Int64               CODEC(DoubleDelta, ZSTD(1)),
    active_end_ms     Int64               CODEC(DoubleDelta, ZSTD(1)),
    duration_ms       Int64               CODEC(ZSTD(1)),

    is_open           UInt8,
    close_reason      LowCardinality(String),

    -- Dimensions are denormalised onto the interval so the serving layer
    -- never joins. 120 sessions carry more than one user_id and 95 carry
    -- more than one platform, so attribution MUST be deterministic or
    -- filtered concurrency is not reproducible run-to-run. We take the
    -- value at the session's earliest event (argMin), which is stable
    -- under re-ingestion and independent of row arrival order.
    user_id           String              CODEC(ZSTD(3)),
    content_id        Int64,
    platform          LowCardinality(String),
    country           LowCardinality(String),
    app_version       LowCardinality(String),
    audio_language    LowCardinality(String),
    video_type        LowCardinality(String),
    category          LowCardinality(String),

    -- ReplacingMergeTree(version): a late heartbeat that extends an OPEN
    -- interval re-emits the same (session, seq) with a higher version and
    -- replaces it in place. Closed intervals are immutable and are never
    -- recomputed. This is what makes the pipeline update-friendly rather
    -- than rebuild-on-change.
    version           UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(active_start)
ORDER BY (video_session_id, interval_seq);


-- ---------------------------------------------------------------------
-- The derivation itself.
--
-- Runs entirely in ClickHouse as array algebra over per-session event
-- sequences -- no row-by-row cursor, no data leaving the database.
-- ---------------------------------------------------------------------
INSERT INTO sony.session_active_intervals
WITH
    -- Explicit Int64 casts: a bare positive literal types as UInt64, which
    -- will not unify with the Int64 epoch-millisecond timestamps and fails
    -- with NO_COMMON_TYPE deep inside arrayConcat.
    toInt64(${GAP_TIMEOUT_MS}) AS gap_timeout,
    toInt64(${GAP_GRACE_MS})   AS gap_grace,
    toInt64(${WATERMARK_MS})   AS watermark

SELECT
    video_session_id,
    (iv.1) AS interval_seq,
    fromUnixTimestamp64Milli(iv.2, 'UTC')  AS active_start,
    fromUnixTimestamp64Milli(iv.3, 'UTC')  AS active_end,
    iv.2                                   AS active_start_ms,
    iv.3                                   AS active_end_ms,
    iv.3 - iv.2                            AS duration_ms,
    iv.5                                   AS is_open,
    multiIf(iv.4 = 1, 'pause',
            iv.4 = 2, 'backgrounded',
            iv.4 = 3, 'session_end',
            iv.4 = 4, 'evidence_gap',
            iv.4 = 5, 'open_at_watermark',
                      'unknown')           AS close_reason,
    s.user_id        AS user_id,
    s.content_id     AS content_id,
    s.platform       AS platform,
    s.country        AS country,
    s.app_version    AS app_version,
    s.audio_language AS audio_language,
    -- JOIN, not dictGet. A dictionary is a NODE-LOCAL cache and
    -- SYSTEM RELOAD DICTIONARY without ON CLUSTER refreshes only the node it
    -- runs on. On ClickHouse Cloud this derivation executed against a node
    -- still holding the pre-load (empty) snapshot, so every one of 35,901
    -- intervals got video_type='' and the video_type='live' benchmark query
    -- answered 0 instead of 469 -- silently, with the dictionary reporting
    -- LOADED with 33,464 elements the whole time. A single node locally hides
    -- this completely. Enrichment now reads the table, which is the same data
    -- on every node.
    c.video_type     AS video_type,
    c.category       AS category,
    toUInt64(now64(3)) AS version
FROM
(
    SELECT
        video_session_id,
        argMin(user_id,        event_timestamp_ms) AS user_id,
        argMin(content_id,     event_timestamp_ms) AS content_id,
        argMin(platform,       event_timestamp_ms) AS platform,
        argMin(country,        event_timestamp_ms) AS country,
        argMin(app_version,    event_timestamp_ms) AS app_version,
        argMin(audio_language, event_timestamp_ms) AS audio_language,

        -- (ts, state_delta, close_code) ordered by time.
        -- arraySort over the WHOLE tuple, not just the timestamp. `pause` and
        -- `AppBackgrounded` frequently share a millisecond (8,280 cases), and
        -- sorting on the timestamp alone leaves their relative order
        -- unspecified -- ClickHouse and the Python oracle then broke the tie
        -- differently and disagreed on close_reason. Boundaries and peak were
        -- unaffected, but a pipeline judged on reproducibility cannot contain
        -- a non-deterministic sort. Ordering by (ts, state_delta, close_code)
        -- is total: at equal timestamps closes (-1) precede opens (+1), and
        -- pause (1) precedes backgrounded (2).
        arraySort(groupArray(tuple(
            event_timestamp_ms,
            toInt8(state_delta),
            toUInt8(multiIf(event = 'pause',                    1,
                            event_type = 'AppBackgrounded',     2,
                            event_type = 'VideoSessionEnd',     3,
                                                                0))
        ))) AS evs,

        arrayMap(x -> x.1, evs) AS ts_all,

        -- ---------- client_alive ----------
        -- ANY event is proof of life; a dead client emits nothing. Split the
        -- timestamp sequence wherever total silence exceeds the timeout; each
        -- surviving run is one alive window, extended by the grace period.
        arraySplit(
            (cur, prv) -> cur - prv > gap_timeout,
            ts_all,
            arrayPushFront(arrayPopBack(ts_all), ts_all[1])
        ) AS alive_runs,
        arrayMap(r -> tuple(r[1], r[length(r)] + gap_grace), alive_runs) AS alive,

        -- ---------- intent_playing ----------
        -- Keep only state transitions, then collapse repeats: a +1 while
        -- already playing is a no-op, as is a -1 while already stopped. This
        -- is what absorbs the 65% of sessions with unbalanced pause/resume.
        arrayFilter(x -> x.2 != 0, evs) AS tr,
        arrayMap(x -> x.2, tr) AS tr_d,
        -- arrayResize guards the empty case. A session with heartbeats but no
        -- state transitions at all -- which happens the moment a day is cut
        -- mid-stream, and therefore on any day containing open sessions --
        -- gives tr_d = []. Then arrayPushFront(arrayPopBack([]), 0) = [0],
        -- length 1 against length 0, and arrayFilter rejects the whole query
        -- with SIZES_OF_ARRAYS_DONT_MATCH. The provided dataset never hits
        -- this because every one of its sessions ends with a VideoSessionEnd.
        arrayFilter((x, d, pd) -> d != pd, tr, tr_d,
                    arrayResize(arrayPushFront(arrayPopBack(tr_d), toInt8(0)),
                                length(tr_d))) AS kept0,
        -- A leading close (stop before any play) opens nothing; drop it so
        -- the sequence strictly alternates open, close, open, close...
        if(length(kept0) > 0 AND kept0[1].2 = -1, arrayPopFront(kept0), kept0) AS kept,
        length(kept) AS nk,

        -- Complete open/close pairs.
        arrayMap(k -> tuple(kept[2 * k - 1].1, kept[2 * k].1, kept[2 * k].3, toUInt8(0)),
                 range(1, intDiv(nk, 2) + 1)) AS closed_intent,
        -- An unmatched trailing open means the session is STILL OPEN. Truncate
        -- at the watermark, flag it, and never extend it to infinity. The
        -- provided dataset contains zero of these; the unseen day is
        -- documented to contain them, so this path is built and tested via
        -- synthetic truncation rather than left to chance.
        if(nk % 2 = 1,
           [tuple(kept[nk].1, greatest(watermark, kept[nk].1), toUInt8(5), toUInt8(1))],
           CAST([], 'Array(Tuple(Int64, Int64, UInt8, UInt8))')) AS open_intent,
        arrayConcat(closed_intent, open_intent) AS intent,

        -- ---------- active = intent AND alive ----------
        -- Pairwise clip of every intent window against every alive window.
        -- Both lists are small per session (single digits), so the quadratic
        -- clip is far cheaper than any windowing alternative. An interval cut
        -- short by the alive mask is relabelled 4 (evidence_gap): it ended
        -- because the client went silent, not for the reason intent recorded.
        arrayFlatten(arrayMap(i ->
            arrayMap(a -> tuple(greatest(i.1, a.1),
                                least(i.2, a.2),
                                if(least(i.2, a.2) < i.2, toUInt8(4), i.3),
                                -- An interval the alive mask cut short is NOT
                                -- open: the client has been silent longer than
                                -- the timeout, so it is dead and can be sealed.
                                -- Only intervals still running at the watermark
                                -- stay open and subject to later replacement.
                                if(least(i.2, a.2) < i.2, toUInt8(0), i.4)),
                     alive),
            intent)) AS clipped,
        arrayFilter(x -> x.3 > x.2,
            arrayMap(z -> tuple(z.1, z.2.1, z.2.2, z.2.3, z.2.4),
                     arrayZip(arrayEnumerate(clipped), clipped))) AS active_ivs
    FROM sony.raw_events
    GROUP BY video_session_id
) AS s
ARRAY JOIN active_ivs AS iv
-- LEFT JOIN so a content_id missing from the catalogue yields empty strings
-- rather than dropping the interval. Losing viewers because a title is not in
-- the dimension table would be a far worse error than an unlabelled row.
LEFT JOIN
(
    SELECT content_id, video_type, category FROM sony.content_dim FINAL
) AS c ON c.content_id = s.content_id;
