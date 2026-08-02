"""One command: sealed dataset in, answers + latencies + trace out.

    python scripts/run_sealed.py --raw <sealed.csv> [--content <content.csv>]

The unseen day is released in the final hours of the hackathon, while we are
also recording a demo and finishing a deck. Anything that needs a human to
remember a step at 09:00 tomorrow is a step that will be got wrong, so the
entire path -- load, derive, serve, benchmark, verify -- is this one script.
It is the SAME code the known dataset ran through: no sealed-day special case.

"No pipeline evidence, no credit." Every stage writes to a trace directory:
input checksums, per-stage row counts and timings, the git commit that
produced them, the ClickHouse query log, and an independent oracle
verification. A judge can open out/sealed/<run_id>/ and follow the whole run.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch          # noqa: E402
import otel        # noqa: E402
import oracle      # noqa: E402
import benchmark   # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GAP_TIMEOUT_MS = 120_000
GAP_GRACE_MS = 0


def sha256(path, limit_mb=None):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            b = fh.read(1 << 20)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def git_sha():
    try:
        return subprocess.check_output(
            ["git", "-C", REPO, "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return "unknown"


def git_dirty():
    try:
        return bool(subprocess.check_output(
            ["git", "-C", REPO, "status", "--porcelain"], text=True, stderr=subprocess.DEVNULL).strip())
    except Exception:
        return None


class Trace:
    """Append-only record of what the pipeline actually did."""

    def __init__(self, outdir):
        self.path = os.path.join(outdir, "stages.jsonl")
        self.stages = []
        self._t0 = time.time()

    def stage(self, name, **fields):
        # Mirror every stage into ClickStack as a span. The JSONL trace is for
        # the judges' offline reading; the spans are what make ingest lag and
        # per-stage cost queryable in ClickHouse alongside everything else.
        otel.event(f"pipeline.{name}", **{k: v for k, v in fields.items()})
        rec = {"stage": name, "at_s": round(time.time() - self._t0, 3), **fields}
        self.stages.append(rec)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")
        detail = "  ".join(f"{k}={v}" for k, v in fields.items())
        print(f"  [{rec['at_s']:7.2f}s] {name:<26} {detail}")
        return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, help="sealed raw events CSV")
    ap.add_argument("--content", help="content CSV (reuse existing if omitted)")
    ap.add_argument("--run-id", help="defaults to sealed-<UTC timestamp>")
    ap.add_argument("--append", action="store_true",
                    help="add to existing data instead of replacing it")
    ap.add_argument("--skip-oracle", action="store_true",
                    help="skip independent verification (NOT for the real run)")
    a = ap.parse_args()

    run_id = a.run_id or "sealed-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outdir = os.path.join(REPO, "out", "sealed", run_id)
    os.makedirs(outdir, exist_ok=True)

    print(f"\n=== SEALED RUN {run_id} ===")
    print(f"output -> {outdir}\n")

    if not ch.ping():
        sys.exit("no ClickHouse connection; check .env")

    wall0 = time.time()
    started_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tr = Trace(outdir)
    cfg = ch.config()
    tr.stage("connect", host=cfg["host"], secure=cfg["secure"],
             server=ch.scalar("SELECT version()"))
    tr.stage("input", raw=os.path.basename(a.raw),
             raw_bytes=os.path.getsize(a.raw), raw_sha256=sha256(a.raw)[:16],
             git=git_sha()[:12], dirty=git_dirty())

    # ---- 1. schema ----------------------------------------------------
    t = time.time()
    ch.execute("CREATE DATABASE IF NOT EXISTS sony")
    ch.script(os.path.join(REPO, "sql", "01_schema.sql"))
    ch.script(os.path.join(REPO, "sql", "04_pipeline_runs.sql"))
    tr.stage("schema", seconds=round(time.time() - t, 2))

    # ---- 2. load ------------------------------------------------------
    if not a.append:
        for tbl in ("raw_events", "session_active_intervals",
                    "concurrency_minute_delta", "concurrency_hourly_checkpoint"):
            ch.execute(f"TRUNCATE TABLE IF EXISTS sony.{tbl}")
        tr.stage("truncate", tables=4)

    if a.content:
        t = time.time()
        ch.execute("TRUNCATE TABLE IF EXISTS sony.content_dim")
        # Same reasoning as raw_events below: match by name, never by position.
        import load  # noqa: E402
        load.load_resilient("sony.content_dim",
                            ["content_id", "title", "video_type", "category"],
                            a.content, load.CONTENT_CASTS,
                            aliases=load.CONTENT_ALIASES)
        ch.execute("SYSTEM RELOAD DICTIONARY sony.content_dict")
        tr.stage("load_content", rows=int(ch.scalar("SELECT count() FROM sony.content_dim")),
                 seconds=round(time.time() - t, 2))

    # Header-matched load, NOT positional binding.
    #
    # This used to be a bare `INSERT ... FORMAT CSV` with a hardcoded column
    # list, which silently assumed the sealed file has exactly our 13 columns
    # in exactly our order. Against a wider or reordered file that does not
    # error -- it shifts every value one column left and loads platform into
    # country. Confidently wrong numbers are the worst outcome available on
    # judging day, so the sealed path now goes through the same resilient
    # loader the rest of the project uses: match by name, stage as String,
    # cast in SQL, abort loudly if a REQUIRED column cannot be resolved.
    t = time.time()
    import load  # noqa: E402
    load_report = load.load_resilient("sony.raw_events", load.RAW_COLS, a.raw, load.CASTS)
    n_raw = int(ch.scalar("SELECT count() FROM sony.raw_events"))
    load_s = round(time.time() - t, 2)
    # Schema drift belongs in the trace, not only on someone's terminal.
    tr.stage("schema_match",
             columns_matched=len(load.RAW_COLS) - len(load_report["missing"]),
             unrecognised=load_report["unknown"],
             missing_defaulted=load_report["missing"],
             staged_rows=load_report["staged"])
    # Ingest lag = wall clock now minus the newest event we hold. For a live
    # concurrency service this is the metric that decides whether the answer
    # is trustworthy: concurrency computed off stale ingest is wrong in a way
    # no amount of query tuning can repair.
    lag_s = float(ch.scalar("SELECT round(dateDiff('second', max(event_time), now64(3, 'UTC')), 1) "
                            "FROM sony.raw_events"))
    tr.stage("load_raw", rows=n_raw, seconds=load_s,
             rows_per_sec=int(n_raw / max(load_s, 0.01)), ingest_lag_seconds=lag_s)

    # session_spans is an insert-fired MV target, and this run replaced
    # raw_events UNDER the MV -- truncation does not fire a materialized
    # view, so without a rebuild the spans still describe the previous
    # dataset. We found 13,735 stale sessions this way; anything reading
    # spans (the naive baseline, multi-device analysis) would silently
    # blend two datasets.
    t = time.time()
    ch.execute("TRUNCATE TABLE sony.session_spans")
    ch.execute("""
INSERT INTO sony.session_spans
SELECT video_session_id,
       min(event_timestamp_ms), max(event_timestamp_ms), toUInt64(count()),
       argMinState(platform, event_timestamp_ms),
       argMinState(country, event_timestamp_ms),
       argMinState(content_id, event_timestamp_ms),
       uniqState(toString(platform))
FROM sony.raw_events GROUP BY video_session_id""")
    ch.execute("TRUNCATE TABLE sony.event_type_minute")
    ch.execute("""
INSERT INTO sony.event_type_minute
SELECT toDateTime(intDiv(event_timestamp_ms,60000)*60,'UTC'), event_type, count()
FROM sony.raw_events GROUP BY 1, 2""")
    tr.stage("rebuild_spans",
             rows=int(ch.scalar("SELECT count() FROM sony.session_spans")),
             event_type_minute=int(ch.scalar("SELECT count() FROM sony.event_type_minute")),
             seconds=round(time.time() - t, 2))

    # Data-quality gates. These are the shapes that broke us on the provided
    # dataset; if the sealed day differs we want it in the trace, loudly,
    # rather than discovered in the answers.
    tr.stage("data_profile",
             sessions=int(ch.scalar("SELECT uniqExact(video_session_id) FROM sony.raw_events")),
             users=int(ch.scalar("SELECT uniqExact(user_id) FROM sony.raw_events")),
             range=ch.scalar("SELECT concat(toString(min(event_time)),' .. ',toString(max(event_time))) FROM sony.raw_events"),
             open_sessions=int(ch.scalar(
                 "SELECT count() FROM (SELECT video_session_id FROM sony.raw_events "
                 "GROUP BY video_session_id HAVING countIf(event_type='VideoSessionEnd') = 0)")),
             dict_misses=int(ch.scalar(
                 "SELECT uniqExactIf(content_id, NOT dictHas('sony.content_dict', tuple(content_id))) "
                 "FROM sony.raw_events")),
             unknown_event_types=ch.scalar(
                 "SELECT arrayStringConcat(groupUniqArray(event_type), ',') FROM sony.raw_events "
                 "WHERE event_type NOT IN ('VideoSessionStart','VideoPlay','VideoHeartbeat',"
                 "'AppBackgrounded','AppForegrounded','VideoSessionEnd','VideoError')") or "none")

    # ---- 3. derive intervals -----------------------------------------
    watermark = ch.scalar("SELECT max(event_timestamp_ms) FROM sony.raw_events")
    t = time.time()
    ch.execute("DROP TABLE IF EXISTS sony.session_active_intervals")
    ch.script(os.path.join(REPO, "sql", "02_intervals.sql"),
              params={"GAP_TIMEOUT_MS": GAP_TIMEOUT_MS, "GAP_GRACE_MS": GAP_GRACE_MS,
                      "WATERMARK_MS": watermark})
    tr.stage("derive_intervals",
             rows=int(ch.scalar("SELECT count() FROM sony.session_active_intervals")),
             open_intervals=int(ch.scalar("SELECT countIf(is_open) FROM sony.session_active_intervals FINAL")),
             active_hours=float(ch.scalar("SELECT round(sum(duration_ms)/3600000,2) FROM sony.session_active_intervals FINAL")),
             watermark_ms=int(watermark), seconds=round(time.time() - t, 2))

    # ---- 4. serving layer --------------------------------------------
    t = time.time()
    for tbl in ("concurrency_minute_delta", "concurrency_hourly_checkpoint"):
        ch.execute(f"DROP TABLE IF EXISTS sony.{tbl}")
    ch.script(os.path.join(REPO, "sql", "03_serving.sql"))
    tr.stage("build_serving",
             delta_rows=int(ch.scalar("SELECT count() FROM sony.concurrency_minute_delta")),
             checkpoint_rows=int(ch.scalar("SELECT count() FROM sony.concurrency_hourly_checkpoint")),
             minute_grid_rows_avoided=int(ch.scalar(
                 "SELECT sum(intDiv(active_end_ms,60000)-intDiv(active_start_ms,60000)+1) "
                 "FROM sony.session_active_intervals FINAL")),
             seconds=round(time.time() - t, 2))

    # The straw man, recomputed from raw_events on this run's data so the
    # comparison can never be against a stale or differently-filtered baseline.
    naive_peak = int(ch.scalar("""
        SELECT max(c) FROM (
          SELECT sum(d) OVER (ORDER BY m) AS c FROM (
            SELECT m, sum(d) AS d FROM (
              SELECT intDiv(min(event_timestamp_ms), 60000) AS m, 1 AS d
              FROM sony.raw_events GROUP BY video_session_id
              UNION ALL
              SELECT intDiv(max(event_timestamp_ms), 60000) + 1 AS m, -1 AS d
              FROM sony.raw_events GROUP BY video_session_id)
            GROUP BY m ORDER BY m))"""))
    tr.stage("naive_baseline", peak=naive_peak)

    # ---- 5. benchmark answers ----------------------------------------
    t = time.time()
    answers = run_benchmark(outdir, a.raw, skip_oracle=a.skip_oracle)
    tr.stage("benchmark", queries=len(answers["results"]),
             disagreements=answers["failures"], seconds=round(time.time() - t, 2))

    # ---- 6. independent verification ---------------------------------
    parity = {"skipped": True}
    if not a.skip_oracle:
        t = time.time()
        params = oracle.Params(gap_timeout_ms=GAP_TIMEOUT_MS, gap_grace_ms=GAP_GRACE_MS,
                               watermark_ms=int(watermark))
        ivs = oracle.build_intervals(a.raw, params)
        o_rows = {(i.session_id, i.start_ms, i.end_ms) for i in ivs}
        text, _ = ch.query("SELECT video_session_id, active_start_ms, active_end_ms "
                           "FROM sony.session_active_intervals FINAL")
        c_rows = {(l.split("\t")[0], int(l.split("\t")[1]), int(l.split("\t")[2]))
                  for l in text.splitlines() if l}
        parity = {"skipped": False, "oracle_intervals": len(o_rows),
                  "clickhouse_intervals": len(c_rows),
                  "only_oracle": len(o_rows - c_rows), "only_clickhouse": len(c_rows - o_rows),
                  "match": o_rows == c_rows}
        tr.stage("oracle_parity", **{k: v for k, v in parity.items() if k != "skipped"},
                 seconds=round(time.time() - t, 2))

    # ---- 7. query log = proof the queries actually ran -----------------
    # ClickHouse's own record of every statement is the strongest form of
    # "it really ran through the pipeline" evidence we can hand a judge --
    # we did not write it, the database did. It is enabled by default on
    # Cloud; a bare local binary may not have it, and that must degrade to a
    # warning rather than lose an otherwise-good run.
    try:
        ch.execute("SYSTEM FLUSH LOGS")
        qlog, _ = ch.query("""
            SELECT event_time, query_duration_ms, read_rows, read_bytes, result_rows,
                   replaceRegexpAll(substring(query, 1, 300), '[\\n\\t]+', ' ')
            FROM system.query_log
            WHERE type = 'QueryFinish' AND event_time > now() - INTERVAL 30 MINUTE
              AND query NOT LIKE '%system.query_log%'
            ORDER BY event_time""")
        with open(os.path.join(outdir, "query_log.tsv"), "w", encoding="utf-8") as fh:
            fh.write("event_time\tduration_ms\tread_rows\tread_bytes\tresult_rows\tquery\n")
            fh.write(qlog)
        tr.stage("query_log", entries=len([l for l in qlog.splitlines() if l]))
    except RuntimeError as e:
        tr.stage("query_log", entries=0, unavailable=str(e)[:120])

    st = otel.status()
    n_spans = otel.flush()
    tr.stage("clickstack_export", enabled=st["enabled"], spans_flushed=n_spans,
             dropped=st["dropped"], trace_id=st["trace_id"])
    manifest_extra_trace = st["trace_id"]

    manifest = {
        "run_id": run_id,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "git_commit": git_sha(), "git_dirty": git_dirty(),
        "clickhouse": {"host": cfg["host"], "secure": cfg["secure"],
                       "version": ch.scalar("SELECT version()")},
        "inputs": {"raw": os.path.abspath(a.raw), "raw_sha256": sha256(a.raw),
                   "content": os.path.abspath(a.content) if a.content else None},
        "model_params": {"gap_timeout_ms": GAP_TIMEOUT_MS, "gap_grace_ms": GAP_GRACE_MS,
                         "liveness_events": sorted(oracle.LIVENESS_EVENTS),
                         "watermark_ms": int(watermark)},
        "stages": tr.stages,
        "oracle_parity": parity,
        "answers_file": "answers.json",
        "clickstack_trace_id": manifest_extra_trace,
    }
    with open(os.path.join(outdir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    ok = answers["failures"] == 0 and (parity.get("skipped") or parity.get("match"))

    # Provenance into ClickHouse, not just onto disk. After this, "where did
    # this number come from" is a SQL question rather than a directory to go
    # rummage in -- which is the form a judge checking reproducibility wants.
    def stage_of(name, field, default=0):
        for r in tr.stages:
            if r["stage"] == name and field in r:
                return r[field]
        return default

    hl = next((r for r in answers["results"] if not r["filters"]), None) or {}
    ch.execute(
        "INSERT INTO sony.pipeline_runs FORMAT JSONEachRow",
        body=json.dumps({
            "run_id": run_id,
            "started_at": started_iso,
            "finished_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "duration_s": round(time.time() - wall0, 2),
            "status": "pass" if ok else "fail",
            "git_commit": git_sha(), "git_dirty": 1 if git_dirty() else 0,
            "input_path": os.path.abspath(a.raw),
            "input_bytes": os.path.getsize(a.raw),
            "input_sha256": sha256(a.raw),
            "content_sha256": sha256(a.content) if a.content else "",
            "gap_timeout_ms": GAP_TIMEOUT_MS, "gap_grace_ms": GAP_GRACE_MS,
            "liveness_events": sorted(oracle.LIVENESS_EVENTS),
            "watermark_ms": int(watermark),
            "events": stage_of("load_raw", "rows"),
            "sessions": stage_of("data_profile", "sessions"),
            "open_sessions": stage_of("data_profile", "open_sessions"),
            "intervals": stage_of("derive_intervals", "rows"),
            "open_intervals": stage_of("derive_intervals", "open_intervals"),
            "active_hours": stage_of("derive_intervals", "active_hours", 0.0),
            "delta_rows": stage_of("build_serving", "delta_rows"),
            "checkpoint_rows": stage_of("build_serving", "checkpoint_rows"),
            "grid_rows_avoided": stage_of("build_serving", "minute_grid_rows_avoided"),
            "peak_concurrency": hl.get("peak", 0),
            "peak_minute": hl.get("peak_minute", ""),
            "naive_peak": naive_peak, "overcount": naive_peak - hl.get("peak", 0),
            "overcount_pct": round((naive_peak - hl.get("peak", 0)) / naive_peak * 100, 2)
                             if naive_peak else 0.0,
            "oracle_match": 1 if parity.get("match") else 0,
            "oracle_intervals": parity.get("oracle_intervals", 0),
            "benchmark_queries": len(answers["results"]),
            "benchmark_failures": answers["failures"],
            "ch_host": cfg["host"], "ch_version": ch.scalar("SELECT version()"),
            "clickstack_trace_id": manifest_extra_trace or "",
        }).encode())
    tr.stage("provenance", table="sony.pipeline_runs", run_id=run_id)
    print(f"\n{'PASS' if ok else 'FAIL'}  -> {outdir}")
    print("  manifest.json   inputs, params, git commit, per-stage counts")
    print("  answers.json    benchmark answers + latency + rows read")
    print("  stages.jsonl    append-only pipeline trace")
    print("  query_log.tsv   ClickHouse's own record of every query")
    sys.exit(0 if ok else 1)


def benchmark_raw_cols():
    from load import RAW_COLS
    return RAW_COLS


def run_benchmark(outdir, raw_path, skip_oracle):
    """Reuse the benchmark module rather than reimplementing the query set."""
    import io
    import contextlib
    argv = sys.argv
    out_json = os.path.join(outdir, "answers.json")
    sys.argv = ["benchmark", "--raw", raw_path, "--json", out_json]
    if skip_oracle:
        sys.argv.append("--skip-oracle")
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            benchmark.main()
    except SystemExit:
        pass
    finally:
        sys.argv = argv
    print("\n".join("    " + l for l in buf.getvalue().splitlines() if l.strip()))
    with open(out_json, encoding="utf-8") as fh:
        return json.load(fh)


if __name__ == "__main__":
    main()
