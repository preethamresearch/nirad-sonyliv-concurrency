"""Benchmark the concurrency serving layer: correctness AND latency.

Three independent paths must agree on every query:

  ORACLE      python, walks raw intervals             -- ground truth
  SCAN        ClickHouse, cumulative sum from t0      -- no checkpoint
  CHECKPOINT  ClickHouse, hourly anchor + deltas      -- what a dashboard uses

SCAN proves the delta model is right. CHECKPOINT proves the optimisation that
makes it scale did not change the answer. The organisers' own benchmark query
set was not shipped with the package, so this is a representative set covering
every dimension the problem statement names (platform, country, content,
video_type) at minute, hour and day grain.

    python scripts/benchmark.py --raw <path> [--json out/benchmark.json]
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch      # noqa: E402
import oracle  # noqa: E402


def where_sql(f):
    parts = []
    for k, v in f.items():
        parts.append(f"{k} = {v}" if isinstance(v, int) else f"{k} = '{v}'")
    return (" AND " + " AND ".join(parts)) if parts else ""


def sql_scan(t0, t1, f, fill_from):
    """Cumulative sum over all deltas from the beginning of time. Exact, slow.

    WITH FILL must start at `fill_from` (the global first delta minute), NOT
    at the first row the filter happens to produce. A slice like
    platform='SONY_ANDROID_TV' only exists in the last two hours of the
    dataset; filling from its own first row would average over 119 minutes
    instead of the 17,029 the caller asked about. Minutes with nobody watching
    are still minutes, and a dashboard average that silently skips them is
    wrong by two orders of magnitude.
    """
    return f"""
WITH toDateTime('{t0}', 'UTC') AS t0, toDateTime('{t1}', 'UTC') AS t1
SELECT max(c) AS peak, toString(argMax(minute, c)) AS peak_minute, round(avg(c), 4) AS avg_c
FROM (
    SELECT minute, sum(d) OVER (ORDER BY minute) AS c
    FROM (
        SELECT minute, sum(delta) AS d
        FROM sony.concurrency_delta_all
        WHERE minute <= t1 {where_sql(f)}
        GROUP BY minute
        ORDER BY minute WITH FILL
            FROM toDateTime('{fill_from}', 'UTC')
            TO   toDateTime('{t1}', 'UTC') + INTERVAL 1 MINUTE STEP 60
    )
)
WHERE minute >= t0"""


def sql_anchor(t0, f):
    """Find the newest checkpoint at or before t0 and its concurrency.

    A separate round-trip because WITH FILL FROM must be a literal -- a scalar
    subquery is not constant-folded and ClickHouse rejects it. Both round-trips
    are counted in the reported latency, so the comparison stays honest.
    """
    w = where_sql(f)
    return f"""
WITH toDateTime('{t0}', 'UTC') AS t0,
     (SELECT max(hour_boundary) FROM sony.concurrency_hourly_checkpoint
       WHERE hour_boundary <= t0 {w}) AS anchor
SELECT
    toString(ifNull(anchor, toDateTime(0, 'UTC'))) AS anchor_ts,
    ifNull((SELECT sum(concurrency) FROM sony.concurrency_hourly_checkpoint
             WHERE hour_boundary = anchor {w}), 0)
    -- Checkpoints are built from sealed intervals only. Any session still
    -- open across this boundary is live in the hot tier and must be added,
    -- or the anchor understates the level and every minute after it is low.
  + ifNull((SELECT count() FROM sony.session_active_intervals FINAL
             WHERE is_open = 1
               AND toDateTime(intDiv(active_start_ms, 60000) * 60, 'UTC') <= anchor
               AND toDateTime(intDiv(active_end_ms,   60000) * 60, 'UTC') >= anchor {w}), 0)
    AS base"""


def sql_checkpoint(t0, t1, f, anchor, base):
    """Start from the checkpoint and apply only the deltas since it.

    Read cost is proportional to the range queried, not to how much history
    the table holds -- the whole point of the checkpoint tier.
    """
    return f"""
WITH toDateTime('{t0}', 'UTC') AS t0
SELECT max(c) AS peak, toString(argMax(minute, c)) AS peak_minute, round(avg(c), 4) AS avg_c
FROM (
    SELECT minute, {base} + sum(d) OVER (ORDER BY minute) AS c
    FROM (
        SELECT minute, sum(delta) AS d
        FROM sony.concurrency_delta_all
        WHERE minute > toDateTime('{anchor}', 'UTC')
          AND minute <= toDateTime('{t1}', 'UTC') {where_sql(f)}
        GROUP BY minute
        -- FILL FROM the anchor INCLUSIVE. The anchor minute itself carries no
        -- delta (we filter minute > anchor), so WITH FILL emits it with d=0
        -- and its cumulative sum is exactly `base` -- the checkpoint value.
        -- Starting at anchor+1 instead drops that minute entirely, which is
        -- invisible for most ranges but silently wrong whenever t0 lands on
        -- an hour boundary, i.e. every "peak hour" dashboard query.
        ORDER BY minute WITH FILL
            FROM toDateTime('{anchor}', 'UTC')
            TO   toDateTime('{t1}', 'UTC') + INTERVAL 1 MINUTE STEP 60
    )
)
WHERE minute >= t0"""


def run_checkpoint(t0, t1, f, repeats=3):
    """Anchor lookup + windowed scan, timed together."""
    best, best_rows, res = None, 0, None
    for _ in range(repeats):
        text, el1 = ch.query(sql_anchor(t0, f))
        anchor, base = text.strip().split("\t")
        el_fb = 0.0
        if anchor.startswith("1970"):
            # No checkpoint at or before t0. Anchor exactly one minute before
            # t0 -- NOT at the slice's first delta, which would start the
            # filled series late and skip the zero minutes before it. The
            # level at that anchor is the sum of every earlier delta; that is
            # a history scan, but it only happens for ranges that begin before
            # the first checkpoint, i.e. at the very start of retention.
            text_fb, el_fb = ch.query(
                "SELECT ifNull(sum(delta), 0) FROM sony.concurrency_delta_all "
                f"WHERE minute < toDateTime('{t0}', 'UTC') {where_sql(f)}")
            base = text_fb.strip() or "0"
            anchor = ch.scalar(
                f"SELECT toString(toDateTime('{t0}', 'UTC') - INTERVAL 1 MINUTE)")
        text, el2 = ch.query(sql_checkpoint(t0, t1, f, anchor, base))
        rows = int(ch.LAST_SUMMARY.get("read_rows", 0))
        row = text.strip().split("\t")
        res = (int(row[0]), row[1], float(row[2]))
        ms = (el1 + el_fb + el2) * 1000
        if best is None or ms < best:
            best, best_rows = ms, rows
    return (*res, best, best_rows)


def run_ch(sql, repeats=3):
    """Return (peak, peak_minute, avg, best_ms, rows_read). Best-of-N: we are
    measuring the query plan, not the cold page cache."""
    best, best_rows, res = None, 0, None
    for _ in range(repeats):
        text, el = ch.query(sql)
        rows = int(ch.LAST_SUMMARY.get("read_rows", 0))
        row = text.strip().split("\t")
        res = (int(row[0]), row[1], float(row[2]))
        ms = el * 1000
        if best is None or ms < best:
            best, best_rows = ms, rows
    return (*res, best, best_rows)


def oracle_answer(ivs, t0_ms, t1_ms, f):
    def pred(iv):
        for k, v in f.items():
            got = iv.dims.get(k)
            if k == "content_id":
                if int(got) != int(v):
                    return False
            elif k == "video_type":
                # video_type is not on the raw event; resolve it the same way
                # the pipeline does, via the content dimension.
                if VTYPE.get(str(iv.dims["content_id"]), "") != v:
                    return False
            elif got != v:
                return False
        return True

    series = oracle.minute_concurrency(ivs, where=pred)
    lo, hi = t0_ms // 60000, t1_ms // 60000
    pts = {m: series.get(m, 0) for m in range(lo, hi + 1)}
    # carry the running level into minutes with no delta activity
    last = 0
    for m in range(min(series) if series else lo, lo):
        last = series.get(m, last)
    cur = last
    for m in range(lo, hi + 1):
        cur = series.get(m, cur)
        pts[m] = cur
    peak_m = max(pts, key=lambda m: pts[m])
    return pts[peak_m], peak_m, sum(pts.values()) / len(pts)


VTYPE = {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--json")
    ap.add_argument("--skip-oracle", action="store_true")
    a = ap.parse_args()

    if not ch.ping():
        sys.exit(1)

    tmin = ch.scalar("SELECT toString(toDateTime(intDiv(min(active_start_ms),60000)*60,'UTC')) FROM sony.session_active_intervals FINAL")
    tmax = ch.scalar("SELECT toString(toDateTime(intDiv(max(active_end_ms),60000)*60,'UTC')) FROM sony.session_active_intervals FINAL")
    fill_from = ch.scalar("SELECT toString(min(minute)) FROM sony.concurrency_minute_delta")
    peak_day = ch.scalar("SELECT toString(toDate(argMax(minute, delta))) FROM sony.concurrency_minute_delta")
    print(f"data range {tmin} .. {tmax} UTC\n")

    QUERIES = [
        ("full range, no filter",          tmin, tmax, {}),
        ("full range, ANDROID_PHONE",      tmin, tmax, {"platform": "ANDROID_PHONE"}),
        ("full range, IPHONE",             tmin, tmax, {"platform": "IPHONE"}),
        ("full range, SONY_ANDROID_TV",    tmin, tmax, {"platform": "SONY_ANDROID_TV"}),
        ("full range, video_type=live",    tmin, tmax, {"video_type": "live"}),
        ("full range, country=india",      tmin, tmax, {"country": "india"}),
        ("platform+country combo",         tmin, tmax, {"platform": "ANDROID_PHONE", "country": "india"}),
        ("peak day, minute grain",         f"{peak_day} 00:00:00", f"{peak_day} 23:59:00", {}),
        ("peak day, ANDROID_PHONE",        f"{peak_day} 00:00:00", f"{peak_day} 23:59:00", {"platform": "ANDROID_PHONE"}),
        ("peak hour, minute grain",        f"{peak_day} 10:00:00", f"{peak_day} 11:00:00", {}),
    ]

    ivs = None
    if not a.skip_oracle:
        global VTYPE
        text, _ = ch.query("SELECT toString(content_id), video_type FROM sony.content_dim FINAL")
        VTYPE = dict(l.split("\t") for l in text.splitlines() if l)
        wm = int(ch.scalar("SELECT max(event_timestamp_ms) FROM sony.raw_events"))
        t0 = time.time()
        ivs = oracle.build_intervals(a.raw, oracle.Params(watermark_ms=wm))
        print(f"oracle built {len(ivs):,} intervals in {time.time()-t0:.1f}s\n")

    hdr = f"{'query':<28} {'peak':>7} {'avg':>9} {'scan rows':>10} {'ckpt rows':>10} {'ckpt ms':>8}  {'verdict':<8}"
    print(hdr); print("-" * len(hdr))

    results, failures = [], 0
    for name, t0, t1, f in QUERIES:
        s_peak, s_min, s_avg, s_ms, s_rows = run_ch(sql_scan(t0, t1, f, fill_from))
        c_peak, c_min, c_avg, c_ms, c_rows = run_checkpoint(t0, t1, f)

        agree = (s_peak == c_peak) and abs(s_avg - c_avg) < 1e-6
        verdict = "ok" if agree else "CH-DIFF"

        if ivs is not None:
            import datetime as dt
            to_ms = lambda s: int(dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                                  .replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
            o_peak, _, o_avg = oracle_answer(ivs, to_ms(t0), to_ms(t1), f)
            if o_peak != s_peak or abs(o_avg - s_avg) > 1e-3:
                verdict = "ORACLE!"
                print(f"    {name}: oracle peak={o_peak} avg={o_avg:.4f} "
                      f"vs ch peak={s_peak} avg={s_avg:.4f}")
        if verdict != "ok":
            failures += 1

        print(f"{name:<28} {s_peak:>7,} {s_avg:>9.2f} {s_rows:>10,} {c_rows:>10,} {c_ms:>8.1f}  {verdict:<8}")
        results.append({"query": name, "t0": t0, "t1": t1, "filters": f,
                        "peak": s_peak, "peak_minute": s_min, "avg": s_avg,
                        "scan_ms": round(s_ms, 2), "checkpoint_ms": round(c_ms, 2),
                        "scan_rows_read": s_rows, "checkpoint_rows_read": c_rows,
                        "verdict": verdict})

    tot_s = sum(r["scan_rows_read"] for r in results)
    tot_c = sum(r["checkpoint_rows_read"] for r in results)
    speedup = tot_s / max(tot_c, 1)
    print(f"\nrows read  scan {tot_s:,}   checkpoint {tot_c:,}   -> {speedup:.2f}x fewer")
    print("ALL QUERIES AGREE" if failures == 0 else f"{failures} QUERIES DIVERGED")

    if a.json:
        os.makedirs(os.path.dirname(a.json), exist_ok=True)
        with open(a.json, "w", encoding="utf-8") as fh:
            json.dump({"range": [tmin, tmax], "results": results,
                       "rows_read_scan": tot_s, "rows_read_checkpoint": tot_c,
                       "checkpoint_read_amplification": round(speedup, 3),
                       "failures": failures}, fh, indent=2)
        print(f"wrote {a.json}")
    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    main()
