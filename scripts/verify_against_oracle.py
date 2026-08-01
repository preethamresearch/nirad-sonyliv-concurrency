"""Prove the ClickHouse pipeline equals the reference oracle, interval by interval.

The answer key is private. An independent implementation agreeing exactly with
the production path is the only correctness evidence we can generate ourselves,
so this is a hard gate: it exits non-zero on any divergence, and it runs as part
of the sealed-day harness.

    python scripts/verify_against_oracle.py --raw <path>
"""
import argparse
import collections
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch      # noqa: E402
import oracle  # noqa: E402


def fetch_ch_intervals():
    text, el = ch.query(
        "SELECT video_session_id, active_start_ms, active_end_ms, close_reason, is_open "
        "FROM sony.session_active_intervals FINAL"
    )
    rows = set()
    meta = {}
    for line in text.splitlines():
        if not line:
            continue
        sid, s, e, reason, is_open = line.split("\t")
        key = (sid, int(s), int(e))
        rows.add(key)
        meta[key] = (reason, is_open == "1")
    return rows, meta, el


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--gap-timeout", type=int, default=120)
    ap.add_argument("--grace", type=int, default=0)
    ap.add_argument("--show", type=int, default=8, help="max example diffs to print")
    a = ap.parse_args()

    if not ch.ping():
        sys.exit(1)

    # The oracle must see exactly the watermark the SQL used, or open-session
    # handling diverges for reasons that have nothing to do with correctness.
    watermark = int(ch.scalar("SELECT max(event_timestamp_ms) FROM sony.raw_events"))
    params = oracle.Params(
        gap_timeout_ms=a.gap_timeout * 1000,
        gap_grace_ms=a.grace * 1000,
        watermark_ms=watermark,
    )
    print(f"watermark_ms = {watermark}   gap_timeout = {a.gap_timeout}s   grace = {a.grace}s\n")

    t0 = time.time()
    ivs = oracle.build_intervals(a.raw, params)
    oracle_rows = {(i.session_id, i.start_ms, i.end_ms) for i in ivs}
    oracle_meta = {(i.session_id, i.start_ms, i.end_ms): (i.close_reason, i.is_open) for i in ivs}
    t_oracle = time.time() - t0

    ch_rows, ch_meta, t_ch = fetch_ch_intervals()

    only_oracle = oracle_rows - ch_rows
    only_ch = ch_rows - oracle_rows
    common = oracle_rows & ch_rows
    reason_diff = [k for k in common if oracle_meta[k][0] != ch_meta[k][0]]
    open_diff = [k for k in common if oracle_meta[k][1] != ch_meta[k][1]]

    print(f"oracle     : {len(oracle_rows):,} intervals   ({t_oracle:.1f}s, python)")
    print(f"clickhouse : {len(ch_rows):,} intervals   ({t_ch:.1f}s, fetch)")
    print(f"identical boundaries : {len(common):,}")
    print(f"only in oracle       : {len(only_oracle):,}")
    print(f"only in clickhouse   : {len(only_ch):,}")
    print(f"close_reason differs : {len(reason_diff):,}")
    print(f"is_open differs      : {len(open_diff):,}")

    for label, items in (("ONLY IN ORACLE", only_oracle), ("ONLY IN CLICKHOUSE", only_ch)):
        if items:
            print(f"\n{label} (first {a.show}):")
            for k in sorted(items)[: a.show]:
                src = oracle_meta if items is only_oracle else ch_meta
                print(f"  {k[0][:16]}  {k[1]} -> {k[2]}  ({k[2]-k[1]:>9,} ms)  {src[k]}")

    if reason_diff:
        print(f"\nREASON MISMATCH (first {a.show}):")
        for k in sorted(reason_diff)[: a.show]:
            print(f"  {k[0][:16]}  {k[1]} -> {k[2]}  oracle={oracle_meta[k][0]:<18} ch={ch_meta[k][0]}")

    # Independent cross-check: the concurrency curve itself, not just intervals.
    print("\nminute-concurrency cross-check (no filters)")
    o_series = oracle.minute_concurrency(ivs)
    o_stats = oracle.peak_and_avg(o_series)
    ch_peak = ch.scalar("""
        SELECT max(c) FROM (
            SELECT sum(d) OVER (ORDER BY m) AS c FROM (
                SELECT m, sum(d) AS d FROM (
                    SELECT intDiv(active_start_ms, 60000) AS m,  1 AS d FROM sony.session_active_intervals FINAL
                    UNION ALL
                    SELECT intDiv(active_end_ms, 60000) + 1 AS m, -1 AS d FROM sony.session_active_intervals FINAL
                ) GROUP BY m ORDER BY m))""")
    print(f"  oracle peak     : {o_stats['peak']:,}")
    print(f"  clickhouse peak : {int(ch_peak):,}")

    ok = (not only_oracle and not only_ch and not reason_diff
          and not open_diff and int(ch_peak) == o_stats["peak"])
    print("\n" + ("PASS - clickhouse matches the oracle exactly"
                  if ok else "FAIL - divergence above"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
