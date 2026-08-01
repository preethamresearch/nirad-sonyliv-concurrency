"""Synthesise the test case the provided dataset cannot give us.

All 10,866 sessions in ch-hackathon-raw-data.csv have both a start and an end.
Not one is open. But the problem statement says the unseen day "includes ones
still open when the day ends" and that judges "will look at how your serving
layer absorbs them".

So the single most heavily-judged behaviour in this problem is the one the
provided data cannot exercise. We manufacture it by truncating the real event
stream at an artificial "now": every session whose VideoSessionEnd falls after
the cut becomes genuinely open, with a real, partial event history.

    # a day that ends mid-stream, 30 min before the real end
    python scripts/make_fixture.py --raw <in.csv> --out fixtures/open_day.csv --cut-minutes-before-end 30

    # late-arriving heartbeats: the same day, cut later, to replay as an update
    python scripts/make_fixture.py --raw <in.csv> --out fixtures/open_day_plus10.csv --cut-minutes-before-end 20
"""
import argparse
import csv
import os
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--cut-minutes-before-end", type=float, default=30.0)
    ap.add_argument("--last-hours", type=float, default=None,
                    help="also drop events older than this many hours before the cut")
    a = ap.parse_args()

    with open(a.raw, newline="", encoding="utf-8") as fh:
        rdr = csv.reader(fh)
        header = next(rdr)
        rows = list(rdr)

    ts_i = header.index("event_timestamp")
    sid_i = header.index("video_session_id")
    et_i = header.index("event_type")

    tmax = max(int(r[ts_i]) for r in rows)
    cut = tmax - int(a.cut_minutes_before_end * 60_000)
    floor = cut - int(a.last_hours * 3_600_000) if a.last_hours else None

    kept = [r for r in rows
            if int(r[ts_i]) <= cut and (floor is None or int(r[ts_i]) >= floor)]

    ended = {r[sid_i] for r in kept if r[et_i] == "VideoSessionEnd"}
    present = {r[sid_i] for r in kept}
    open_sessions = present - ended

    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(header)
        w.writerows(kept)

    print(f"cut at {cut} ({a.cut_minutes_before_end} min before the real end)")
    print(f"  events   {len(rows):>9,} -> {len(kept):>9,}")
    print(f"  sessions {len(present):>9,}")
    print(f"  OPEN     {len(open_sessions):>9,}  ({len(open_sessions)/max(len(present),1)*100:.1f}% have no VideoSessionEnd)")
    print(f"  wrote {a.out}")
    if not open_sessions:
        sys.exit("fixture produced no open sessions -- move the cut earlier")


if __name__ == "__main__":
    main()
