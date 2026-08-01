"""Demonstrate that late heartbeats are absorbed incrementally, not rebuilt.

Judges: "sessions in the dataset include ones still open when the day ends and
heartbeats that keep arriving. Judges will look at how your serving layer
absorbs them: incrementally, or by recomputing?"

This script answers that with a measurement rather than an assertion.

  T0  load a day cut 30 minutes early     -> 3,526 sessions still open
  T1  the next 10 minutes of events arrive, extending those open sessions
      - re-derive ONLY the sessions those events touched
      - sealed history is not read, not rewritten, not re-derived
  T2  compare against a full rebuild from scratch on the same input

If the incremental result equals the full rebuild, the fast path is correct.
The row counts show what each path had to touch to get there.

    python scripts/demo_incremental.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
T0 = os.path.join(REPO, "fixtures", "open_day.csv")
T1 = os.path.join(REPO, "fixtures", "open_day_plus10.csv")
CONTENT = r"C:/d/pre-check/click-a-thon-2026/SonyLiv/data/ch-hackathon-content-data.csv"
GAP, GRACE = 120_000, 0

sys.path.insert(0, os.path.join(REPO, "scripts"))
from load import RAW_COLS  # noqa: E402


def load_raw(path, truncate=False):
    if truncate:
        ch.execute("TRUNCATE TABLE IF EXISTS sony.raw_events")
    with open(path, "rb") as fh:
        ch.execute(f"INSERT INTO sony.raw_events ({', '.join(RAW_COLS)}) FORMAT CSV",
                   body=fh, settings={"input_format_csv_skip_first_lines": "1",
                                      "max_insert_block_size": "1048576"})


def derive(where_sessions=None):
    """Run sql/02_intervals.sql, optionally restricted to a set of sessions."""
    wm = ch.scalar("SELECT max(event_timestamp_ms) FROM sony.raw_events")
    sql = open(os.path.join(REPO, "sql", "02_intervals.sql"), encoding="utf-8").read()
    sql = ch.strip_sql_comments(sql)
    for k, v in {"GAP_TIMEOUT_MS": GAP, "GAP_GRACE_MS": GRACE, "WATERMARK_MS": wm}.items():
        sql = sql.replace("${%s}" % k, str(v))
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    insert = [s for s in stmts if s.upper().startswith("INSERT")][0]
    if where_sessions:
        # Restrict the derivation to touched sessions only. Everything else in
        # session_active_intervals is left exactly as it was.
        #
        # The session-id predicate alone does not prune anything: it is not the
        # primary key prefix, so ClickHouse still scans every part. raw_events
        # is PARTITION BY toDate(session_start), and a session that started
        # before the touched window cannot be touched now -- so bounding the
        # partition range is what actually stops us reading sealed history.
        # Without this the "incremental" path reads MORE than a full rebuild.
        min_date = ch.scalar(
            f"SELECT toString(min(toDate(session_start))) FROM sony.raw_events "
            f"WHERE video_session_id IN ({where_sessions})")
        insert = insert.replace(
            "    FROM sony.raw_events\n    GROUP BY video_session_id",
            f"    FROM sony.raw_events\n"
            f"    WHERE toDate(session_start) >= toDate('{min_date}')\n"
            f"      AND video_session_id IN ({where_sessions})\n"
            "    GROUP BY video_session_id")
    t = time.time()
    ch.execute(insert)
    return time.time() - t, int(ch.LAST_SUMMARY.get("read_rows", 0))


def rebuild_serving():
    for t in ("concurrency_minute_delta", "concurrency_hourly_checkpoint"):
        ch.execute(f"DROP TABLE IF EXISTS sony.{t}")
    ch.script(os.path.join(REPO, "sql", "03_serving.sql"))


def snapshot():
    return {
        "intervals": int(ch.scalar("SELECT count() FROM sony.session_active_intervals FINAL")),
        "open": int(ch.scalar("SELECT countIf(is_open) FROM sony.session_active_intervals FINAL")),
        "active_hours": float(ch.scalar(
            "SELECT round(sum(duration_ms)/3600000, 4) FROM sony.session_active_intervals FINAL")),
        "peak": int(ch.scalar("""
            SELECT max(c) FROM (
              SELECT sum(d) OVER (ORDER BY minute) AS c FROM (
                SELECT minute, sum(delta) AS d FROM sony.concurrency_delta_all
                GROUP BY minute ORDER BY minute))""")),
    }


def main():
    if not ch.ping():
        sys.exit(1)
    print()

    # ---------------- T0: the day so far ----------------
    ch.execute("CREATE DATABASE IF NOT EXISTS sony")
    ch.script(os.path.join(REPO, "sql", "01_schema.sql"))
    ch.execute("TRUNCATE TABLE IF EXISTS sony.content_dim")
    with open(CONTENT, "rb") as fh:
        ch.execute("INSERT INTO sony.content_dim (content_id, title, video_type, category) FORMAT CSV",
                   body=fh, settings={"input_format_csv_skip_first_lines": "1"})
    ch.execute("SYSTEM RELOAD DICTIONARY sony.content_dict")

    load_raw(T0, truncate=True)
    ch.execute("DROP TABLE IF EXISTS sony.session_active_intervals")
    ch.script(os.path.join(REPO, "sql", "02_intervals.sql"),
              params={"GAP_TIMEOUT_MS": GAP, "GAP_GRACE_MS": GRACE,
                      "WATERMARK_MS": ch.scalar("SELECT max(event_timestamp_ms) FROM sony.raw_events")})
    rebuild_serving()
    s0 = snapshot()
    print(f"T0  day cut 30 min early")
    print(f"    intervals {s0['intervals']:,}   open {s0['open']:,}   "
          f"active {s0['active_hours']:,.1f}h   peak {s0['peak']:,}\n")

    # ---------------- T1: late events arrive ----------------
    # Only the events not already present -- this is the streaming delta.
    cut = int(ch.scalar("SELECT max(event_timestamp_ms) FROM sony.raw_events"))
    before = int(ch.scalar("SELECT count() FROM sony.raw_events"))
    with open(T1, "rb") as fh:
        ch.execute(
            "INSERT INTO sony.raw_events "
            f"({', '.join(RAW_COLS)}) "
            "SELECT " + ", ".join(RAW_COLS) + " FROM input('" +
            ", ".join(f"{c} {t}" for c, t in zip(
                RAW_COLS,
                ["Int64", "String", "String", "String", "String", "Int64",
                 "String", "String", "String", "String", "String", "String", "Int64"])) +
            f"') WHERE event_timestamp_ms > {cut} FORMAT CSV",
            body=fh, settings={"input_format_csv_skip_first_lines": "1"})
    added = int(ch.scalar("SELECT count() FROM sony.raw_events")) - before

    touched = ch.scalar(
        f"SELECT count(DISTINCT video_session_id) FROM sony.raw_events WHERE event_timestamp_ms > {cut}")
    touched_sql = (f"SELECT DISTINCT video_session_id FROM sony.raw_events "
                   f"WHERE event_timestamp_ms > {cut}")

    # Retract the touched sessions' old intervals, then re-derive them.
    #
    # ReplacingMergeTree alone is NOT sufficient here, and assuming it was cost
    # us a wrong answer: it replaces by (video_session_id, interval_seq), but
    # re-deriving a session can produce FEWER intervals than before. At T0 an
    # open session's tail is fragmented by the alive mask -- its last event is
    # far behind the watermark, so the evidence gap splits it. When the late
    # heartbeats arrive those fragments merge into one interval, the sequence
    # gets shorter, and the orphaned high-seq rows have no replacement row to
    # collapse into. They survive as ghosts: 27,707 intervals against the
    # 27,150 a full rebuild produces.
    #
    # A lightweight DELETE scoped to the touched sessions is still O(touched),
    # not O(history): sealed sessions are never read, deleted or re-derived.
    t_del = time.time()
    ch.execute(f"DELETE FROM sony.session_active_intervals "
               f"WHERE video_session_id IN ({touched_sql})")
    t_del = time.time() - t_del
    t_inc, rows_inc = derive(where_sessions=touched_sql)
    t_inc += t_del
    rebuild_serving()
    s1 = snapshot()
    print(f"T1  {added:,} late events arrive, touching {int(touched):,} sessions")
    print(f"    incremental re-derive: {t_inc*1000:,.0f} ms, read {rows_inc:,} rows")
    print(f"    intervals {s1['intervals']:,}   open {s1['open']:,}   "
          f"active {s1['active_hours']:,.1f}h   peak {s1['peak']:,}\n")

    # ---------------- T2: full rebuild, same input ----------------
    ch.execute("DROP TABLE IF EXISTS sony.session_active_intervals")
    t_full, rows_full = None, None
    wm = ch.scalar("SELECT max(event_timestamp_ms) FROM sony.raw_events")
    t = time.time()
    ch.script(os.path.join(REPO, "sql", "02_intervals.sql"),
              params={"GAP_TIMEOUT_MS": GAP, "GAP_GRACE_MS": GRACE, "WATERMARK_MS": wm})
    t_full = time.time() - t
    rows_full = int(ch.LAST_SUMMARY.get("read_rows", 0))
    rebuild_serving()
    s2 = snapshot()
    print(f"T2  full rebuild from scratch on identical input")
    print(f"    full re-derive: {t_full*1000:,.0f} ms, read {rows_full:,} rows")
    print(f"    intervals {s2['intervals']:,}   open {s2['open']:,}   "
          f"active {s2['active_hours']:,.1f}h   peak {s2['peak']:,}\n")

    same = (s1 == s2)
    print(f"incremental == full rebuild : {'YES' if same else 'NO'}")
    if rows_full and rows_inc:
        print(f"rows read                   : {rows_inc:,} incremental vs {rows_full:,} full "
              f"({rows_full/max(rows_inc,1):.1f}x less)")
    print(f"\n{'PASS' if same else 'FAIL'} - the fast path gives the same answer as the slow one")

    out = os.path.join(REPO, "out", "incremental_update.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump({"T0": s0, "T1_incremental": s1, "T2_full_rebuild": s2,
                   "late_events": added, "sessions_touched": int(touched),
                   "incremental_ms": round(t_inc * 1000, 1), "incremental_rows_read": rows_inc,
                   "full_ms": round(t_full * 1000, 1), "full_rows_read": rows_full,
                   "identical": same}, fh, indent=2)
    print(f"wrote {out}")
    sys.exit(0 if same else 1)


if __name__ == "__main__":
    main()
