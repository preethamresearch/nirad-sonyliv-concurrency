-- =====================================================================
-- Click-a-thon 2026 · SonyLIV · Team Nirad
-- 04 — Run provenance, in ClickHouse rather than in a JSON file
--
-- Every sealed run already writes manifest.json / stages.jsonl to disk for a
-- judge to read offline. That is evidence, but it is not queryable, and it
-- leaves the metadata layer as the one part of the system that is not
-- ClickHouse.
--
-- This table closes that gap. After it exists, "where did this number come
-- from" is a SQL question:
--
--   SELECT run_id, input_sha256, git_commit, peak_concurrency, oracle_match
--   FROM sony.pipeline_runs ORDER BY started_at DESC
--
-- and so is "did the model parameters change between these two runs", which
-- is exactly what a judge checking reproducibility wants to ask.
--
-- ReplacingMergeTree(finished_at): a run is written once when it starts and
-- again when it completes, and the later row wins. A crashed run therefore
-- leaves a visible half-record rather than disappearing -- we would rather
-- see that a run died than have it silently absent.
-- =====================================================================

CREATE TABLE IF NOT EXISTS sony.pipeline_runs
(
    run_id              String,
    started_at          DateTime64(3, 'UTC'),
    finished_at         DateTime64(3, 'UTC'),
    duration_s          Float32,
    status              LowCardinality(String),   -- running | pass | fail

    -- provenance: exactly which code, against exactly which bytes
    git_commit          String,
    git_dirty           UInt8,
    input_path          String,
    input_bytes         UInt64,
    input_sha256        String,
    content_sha256      String,

    -- the model is a parameter set, not a constant; record it per run so a
    -- result can never be attributed to the wrong threshold
    gap_timeout_ms      Int64,
    gap_grace_ms        Int64,
    liveness_events     Array(String),
    watermark_ms        Int64,

    -- what the pipeline produced
    events              UInt64,
    sessions            UInt64,
    open_sessions       UInt64,
    intervals           UInt64,
    open_intervals      UInt64,
    active_hours        Float64,
    delta_rows          UInt64,
    checkpoint_rows     UInt64,
    grid_rows_avoided   UInt64,

    -- the headline answers
    peak_concurrency    UInt32,
    peak_minute         String,
    naive_peak          UInt32,
    overcount           Int32,
    overcount_pct       Float32,

    -- did it verify
    oracle_match        UInt8,
    oracle_intervals    UInt64,
    benchmark_queries   UInt16,
    benchmark_failures  UInt16,

    -- where it ran and how to find the trace
    ch_host             String,
    ch_version          String,
    clickstack_trace_id String
)
ENGINE = ReplacingMergeTree(finished_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY (run_id);


-- Convenience view: the questions a judge actually asks, in one place.
CREATE OR REPLACE VIEW sony.pipeline_runs_summary AS
SELECT
    run_id,
    started_at,
    status,
    substring(git_commit, 1, 12)  AS commit,
    substring(input_sha256, 1, 16) AS input,
    events,
    intervals,
    peak_concurrency,
    naive_peak,
    overcount_pct,
    if(oracle_match, 'exact', 'DIVERGED') AS oracle,
    benchmark_failures,
    round(duration_s, 1) AS seconds,
    ch_version
FROM sony.pipeline_runs FINAL
ORDER BY started_at DESC;
