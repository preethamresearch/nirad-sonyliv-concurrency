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
        with open(a.content, "rb") as fh:
            ch.execute("INSERT INTO sony.content_dim (content_id, title, video_type, category) FORMAT CSV",
                       body=fh, settings={"input_format_csv_skip_first_lines": "1"})
        ch.execute("SYSTEM RELOAD DICTIONARY sony.content_dict")
        tr.stage("load_content", rows=int(ch.scalar("SELECT count() FROM sony.content_dim")),
                 seconds=round(time.time() - t, 2))

    t = time.time()
    with open(a.raw, "rb") as fh:
        ch.execute(f"INSERT INTO sony.raw_events ({', '.join(benchmark_raw_cols())}) FORMAT CSV",
                   body=fh, settings={"input_format_csv_skip_first_lines": "1",
                                      "max_insert_block_size": "1048576"})
    n_raw = int(ch.scalar("SELECT count() FROM sony.raw_events"))
    tr.stage("load_raw", rows=n_raw, seconds=round(time.time() - t, 2))

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
    }
    with open(os.path.join(outdir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    ok = answers["failures"] == 0 and (parity.get("skipped") or parity.get("match"))
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
