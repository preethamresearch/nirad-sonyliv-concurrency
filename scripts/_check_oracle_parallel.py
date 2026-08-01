"""Verify build_intervals_parallel == build_intervals, and time both.

A faster oracle that disagrees with the slow one is not an oracle, so this is
a real gate rather than a benchmark. Lives as a file, not a -c snippet,
because Windows multiprocessing needs an importable __main__.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import oracle  # noqa: E402


def key(ivs):
    return sorted((i.session_id, i.seq, i.start_ms, i.end_ms, i.close_reason)
                  for i in ivs)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        r"C:\d\pre-check\click-a-thon-2026\SonyLiv\data", "ch-hackathon-raw-data.csv")
    t = time.time(); par = oracle.build_intervals_parallel(path); tp = time.time() - t
    print(f"parallel : {len(par):,} intervals in {tp:.1f}s")
    t = time.time(); ser = oracle.build_intervals(path); ts = time.time() - t
    print(f"serial   : {len(ser):,} intervals in {ts:.1f}s")
    same = key(par) == key(ser)
    print("IDENTICAL" if same else "!!! DIVERGED")
    print(f"speedup  : {ts / max(tp, .01):.2f}x")
    sys.exit(0 if same else 1)


if __name__ == "__main__":
    main()
