"""Inject realistic ingest faults into a clean event CSV.

The provided dataset is clean. The judged one is described as realtime, wider
and dirty. A pipeline that has only ever seen clean input has not been tested,
so this produces the dirty input on demand -- deterministically, from a seed,
so a demo can be repeated and a failure can be reproduced.

    python scripts/inject_faults.py --raw in.csv --out dirty.csv           # all
    python scripts/inject_faults.py --raw in.csv --out dirty.csv \
        --faults dup_events,late_arrival,casing --rate 0.02
    python scripts/inject_faults.py --list

Each fault is a named, independently switchable transform. The manifest written
alongside the output records exactly what was injected and how many times, so
the validator's findings can be checked against ground truth instead of eyeballed.
"""
import argparse
import csv
import json
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# name -> (category, one-line description)
FAULTS = {
    # --- schema evolution -------------------------------------------------
    "add_columns":    ("schema", "append unexpected columns (ab_bucket, cdn_pop)"),
    "rename_column":  ("schema", "rename event_timestamp -> event_ts_millis"),
    "reorder_columns":("schema", "shuffle column order in the header"),
    "drop_column":    ("schema", "remove subtitle_language entirely"),
    # --- event quality ----------------------------------------------------
    "dup_events":     ("event", "duplicate whole event rows (at-least-once delivery)"),
    "dup_session_end":("event", "emit VideoSessionEnd twice for a session"),
    "out_of_order":   ("event", "shuffle rows so arrival order != event order"),
    "orphan_beats":   ("event", "heartbeats for a session_id that never starts"),
    "replay_after_end":("event","heartbeat timestamped after VideoSessionEnd"),
    # --- timestamps -------------------------------------------------------
    "null_ts":        ("time", "blank event_timestamp"),
    "seconds_epoch":  ("time", "seconds-since-epoch instead of milliseconds"),
    "future_ts":      ("time", "timestamp far in the future (clock skew)"),
    "negative_ts":    ("time", "negative timestamp"),
    # --- dimensions -------------------------------------------------------
    "casing":         ("dim", "platform casing drift: android / ANDROID / Android"),
    "whitespace":     ("dim", "leading/trailing spaces in dimension values"),
    "unicode":        ("dim", "non-ASCII and zero-width characters in values"),
    "unknown_content":("dim", "content_id absent from the content dimension"),
    "empty_country":  ("dim", "empty string where a country is expected"),
    # --- structural -------------------------------------------------------
    "ragged_rows":    ("struct", "rows with too few / too many fields"),
    "blank_lines":    ("struct", "empty lines in the middle of the file"),
}

CATEGORIES = ["schema", "event", "time", "dim", "struct"]


def load(path):
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        r = csv.reader(fh)
        header = next(r)
        return header, [row for row in r]


def col(header, *names):
    """Index of the first present column name, or None."""
    low = [h.strip().lower() for h in header]
    for n in names:
        if n in low:
            return low.index(n)
    return None


def inject(header, rows, faults, rate, seed):
    rng = random.Random(seed)
    counts = {f: 0 for f in faults}
    header = list(header)
    rows = [list(r) for r in rows]

    i_ts = col(header, "event_timestamp", "event_timestamp_ms", "timestamp")
    i_plat = col(header, "platform")
    i_country = col(header, "country")
    i_sess = col(header, "video_session_id", "session_id")
    i_type = col(header, "event_type")
    i_content = col(header, "content_id")
    i_sub = col(header, "subtitle_language")

    def hit():
        return rng.random() < rate

    # ---- per-row value faults ------------------------------------------
    for row in rows:
        if "null_ts" in faults and i_ts is not None and hit():
            row[i_ts] = ""; counts["null_ts"] += 1
        elif "seconds_epoch" in faults and i_ts is not None and hit():
            try:
                row[i_ts] = str(int(row[i_ts]) // 1000); counts["seconds_epoch"] += 1
            except (ValueError, TypeError):
                pass
        elif "future_ts" in faults and i_ts is not None and hit():
            try:
                row[i_ts] = str(int(row[i_ts]) + 86400000 * 365 * 5)
                counts["future_ts"] += 1
            except (ValueError, TypeError):
                pass
        elif "negative_ts" in faults and i_ts is not None and hit():
            row[i_ts] = "-" + str(abs(hash(row[i_ts])) % 10**12); counts["negative_ts"] += 1

        if "casing" in faults and i_plat is not None and hit():
            row[i_plat] = rng.choice([row[i_plat].lower(), row[i_plat].upper(),
                                      row[i_plat].capitalize()])
            counts["casing"] += 1
        if "whitespace" in faults and i_plat is not None and hit():
            row[i_plat] = rng.choice(["  ", " "]) + row[i_plat] + rng.choice([" ", "  "])
            counts["whitespace"] += 1
        if "unicode" in faults and i_country is not None and hit():
            row[i_country] = row[i_country] + rng.choice(["​", " ", "–"])
            counts["unicode"] += 1
        if "empty_country" in faults and i_country is not None and hit():
            row[i_country] = ""; counts["empty_country"] += 1
        if "unknown_content" in faults and i_content is not None and hit():
            row[i_content] = str(rng.randint(9_000_000, 9_999_999))
            counts["unknown_content"] += 1

    # ---- row-set faults --------------------------------------------------
    extra = []
    if "dup_events" in faults:
        for row in rows:
            if hit():
                extra.append(list(row)); counts["dup_events"] += 1

    if "dup_session_end" in faults and i_type is not None:
        for row in rows:
            if row[i_type] == "VideoSessionEnd" and rng.random() < rate * 4:
                extra.append(list(row)); counts["dup_session_end"] += 1

    if "orphan_beats" in faults and i_sess is not None and rows:
        n = max(1, int(len(rows) * rate * 0.2))
        for _ in range(n):
            row = list(rng.choice(rows))
            row[i_sess] = "orphan-" + str(rng.randint(10**9, 10**10))
            if i_type is not None:
                row[i_type] = "VideoHeartbeat"
            extra.append(row); counts["orphan_beats"] += 1

    if "replay_after_end" in faults and i_type is not None and i_ts is not None:
        ends = [r for r in rows if r[i_type] == "VideoSessionEnd"]
        for row in ends:
            if rng.random() < rate * 4:
                r2 = list(row)
                r2[i_type] = "VideoHeartbeat"
                try:
                    r2[i_ts] = str(int(row[i_ts]) + 60000)
                except (ValueError, TypeError):
                    pass
                extra.append(r2); counts["replay_after_end"] += 1

    rows.extend(extra)

    if "out_of_order" in faults:
        rng.shuffle(rows); counts["out_of_order"] = len(rows)

    # ---- schema faults (applied to header + row shape) -------------------
    if "add_columns" in faults:
        header += ["ab_bucket", "cdn_pop"]
        for row in rows:
            row += [rng.choice(["control", "variant_a", "variant_b"]),
                    rng.choice(["BOM1", "DEL2", "MAA1", "HYD3"])]
        counts["add_columns"] = len(rows)

    if "rename_column" in faults and i_ts is not None:
        header[i_ts] = "event_ts_millis"; counts["rename_column"] = 1

    if "drop_column" in faults and i_sub is not None:
        header.pop(i_sub)
        for row in rows:
            if len(row) > i_sub:
                row.pop(i_sub)
        counts["drop_column"] = 1

    if "reorder_columns" in faults:
        order = list(range(len(header)))
        rng.shuffle(order)
        header = [header[i] for i in order]
        rows = [[row[i] if i < len(row) else "" for i in order] for row in rows]
        counts["reorder_columns"] = 1

    # ---- structural faults (emitted at write time) -----------------------
    return header, rows, counts


def write(path, header, rows, faults, rate, seed):
    rng = random.Random(seed + 1)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        n_ragged = n_blank = 0
        for row in rows:
            if "blank_lines" in faults and rng.random() < rate * 0.1:
                fh.write("\n"); n_blank += 1
            if "ragged_rows" in faults and rng.random() < rate * 0.2:
                if rng.random() < 0.5 and len(row) > 3:
                    w.writerow(row[:-rng.randint(1, 2)])
                else:
                    w.writerow(row + ["EXTRA"])
                n_ragged += 1
                continue
            w.writerow(row)
    return n_ragged, n_blank


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw", help="clean input CSV")
    p.add_argument("--out", help="dirty output CSV")
    p.add_argument("--faults", default="all",
                   help="comma-separated fault names, a category name, or 'all'")
    p.add_argument("--rate", type=float, default=0.01,
                   help="per-row probability for value-level faults (default 0.01)")
    p.add_argument("--seed", type=int, default=7, help="deterministic seed")
    p.add_argument("--limit", type=int, help="only read the first N data rows")
    p.add_argument("--list", action="store_true", help="list fault names and exit")
    a = p.parse_args()

    if a.list:
        for cat in CATEGORIES:
            print(f"\n{cat}:")
            for name, (c, desc) in FAULTS.items():
                if c == cat:
                    print(f"  {name:<18} {desc}")
        return
    if not a.raw or not a.out:
        p.error("--raw and --out are required")

    if a.faults == "all":
        faults = set(FAULTS)
    elif a.faults in CATEGORIES:
        faults = {n for n, (c, _) in FAULTS.items() if c == a.faults}
    else:
        faults = {f.strip() for f in a.faults.split(",") if f.strip()}
        unknown = faults - set(FAULTS)
        if unknown:
            sys.exit(f"unknown fault(s): {', '.join(sorted(unknown))}\n"
                     f"run --list to see the catalogue")

    header, rows = load(a.raw)
    if a.limit:
        rows = rows[:a.limit]
    n_in = len(rows)
    header, rows, counts = inject(header, rows, faults, a.rate, a.seed)
    n_ragged, n_blank = write(a.out, header, rows, faults, a.rate, a.seed)
    counts = {k: v for k, v in counts.items() if v}
    if n_ragged:
        counts["ragged_rows"] = n_ragged
    if n_blank:
        counts["blank_lines"] = n_blank

    manifest = {
        "source": os.path.abspath(a.raw), "output": os.path.abspath(a.out),
        "seed": a.seed, "rate": a.rate,
        "rows_in": n_in, "rows_out": len(rows),
        "faults_requested": sorted(faults), "injected": counts,
        "header_out": header,
    }
    mpath = os.path.splitext(a.out)[0] + ".manifest.json"
    with open(mpath, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    print(f"read   {n_in:,} rows from {os.path.basename(a.raw)}")
    print(f"wrote  {len(rows):,} rows to {os.path.basename(a.out)}")
    print(f"header {len(header)} columns")
    print("injected:")
    for k in sorted(counts):
        print(f"  {k:<18} {counts[k]:,}")
    print(f"manifest -> {os.path.basename(mpath)}")


if __name__ == "__main__":
    main()
