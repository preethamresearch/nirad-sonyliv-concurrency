"""Measure codec choices on THIS dataset instead of arguing about them.

Builds candidate tables from the real raw_events data, one codec change at a
time, and reports compressed size per column. Compression arguments are cheap
to make and easy to get wrong -- the point of this script is that every codec
claim in the deck has a number behind it.

    python scripts/codec_bench.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch  # noqa: E402

# name -> the column definition under test. Everything else stays identical to
# the shipped schema, so a difference is attributable to one change.
VARIANTS = {
    "baseline": None,   # the shipped schema, measured as-is
    "user_id_lowcard": (
        "user_id", "LowCardinality(String)"),
    "content_id_t64": (
        "content_id", "Int64 CODEC(T64, ZSTD(1))"),
    "content_id_delta": (
        "content_id", "Int64 CODEC(Delta, ZSTD(1))"),
    "ts_gorilla": (
        "event_timestamp_ms", "Int64 CODEC(Gorilla, ZSTD(1))"),
    "ts_delta": (
        "event_timestamp_ms", "Int64 CODEC(Delta, ZSTD(1))"),
    "ts_t64": (
        "event_timestamp_ms", "Int64 CODEC(T64, ZSTD(1))"),
    "sess_start_delta": (
        "session_start_ms", "Int64 CODEC(Delta, ZSTD(1))"),
    "both_delta": ("__both_ts__", "Delta"),
    "zstd6_everywhere": ("__global__", "ZSTD(6)"),
}

BASE_COLS = [
    ("content_id", "Int64"),
    ("video_session_id", "String CODEC(ZSTD(3))"),
    ("user_id", "String CODEC(ZSTD(3))"),
    ("event_type", "LowCardinality(String)"),
    ("event", "LowCardinality(String)"),
    ("event_timestamp_ms", "Int64 CODEC(DoubleDelta, ZSTD(1))"),
    ("platform", "LowCardinality(String)"),
    ("app_version", "LowCardinality(String)"),
    ("country", "LowCardinality(String)"),
    ("audio_language", "LowCardinality(String)"),
    ("subtitle_language", "LowCardinality(String)"),
    ("player_version", "LowCardinality(String)"),
    ("session_start_ms", "Int64 CODEC(DoubleDelta, ZSTD(1))"),
]
COLNAMES = [c for c, _ in BASE_COLS]


def build(name, change):
    tbl = f"sony.codec_{name}"
    ch.execute(f"DROP TABLE IF EXISTS {tbl}")
    cols = []
    for col, typ in BASE_COLS:
        if change and change[0] == col:
            typ = change[1]
        elif change and change[0] == "__both_ts__" and col in (
                "event_timestamp_ms", "session_start_ms"):
            typ = "Int64 CODEC(Delta, ZSTD(1))"
        elif change and change[0] == "__global__":
            # Swap only the explicit ZSTD levels; LowCardinality columns keep
            # their default so the comparison stays about the codec, not about
            # accidentally disabling dictionary encoding.
            typ = typ.replace("ZSTD(3)", change[1]).replace("ZSTD(1)", change[1])
        cols.append(f"    {col} {typ}")
    ddl = (f"CREATE TABLE {tbl} (\n" + ",\n".join(cols) + "\n) "
           "ENGINE = MergeTree ORDER BY (video_session_id, event_timestamp_ms)")
    ch.execute(ddl)
    t = time.time()
    ch.execute(f"INSERT INTO {tbl} ({', '.join(COLNAMES)}) "
               f"SELECT {', '.join(COLNAMES)} FROM sony.raw_events")
    return tbl, round(time.time() - t, 1)


def sizes(tbl, tries=6):
    """Total compressed bytes, once the parts are actually visible.

    Uses system.parts.bytes_on_disk, NOT system.parts_columns: on ClickHouse
    Cloud (SharedMergeTree) the per-column byte counters read 0, so a
    per-column breakdown is simply not available there. Reporting a real
    total beats reporting a fake breakdown.
    """
    db, name = tbl.split(".")
    ch.execute(f"OPTIMIZE TABLE {tbl} FINAL")
    for attempt in range(tries):
        v = ch.scalar(f"SELECT sum(bytes_on_disk) FROM system.parts "
                      f"WHERE database='{db}' AND table='{name}' AND active")
        total = int(v or 0)
        if total > 0:
            return {}, total
        time.sleep(1 + attempt)
    return {}, 0


def main():
    rows = int(ch.scalar("SELECT count() FROM sony.raw_events"))
    if rows == 0:
        sys.exit("raw_events is empty -- load data first")
    print(f"benchmarking codecs over {rows:,} rows\n")
    base_per, base_total = None, None
    results = []
    for name, change in VARIANTS.items():
        try:
            tbl, secs = build(name, change)
        except RuntimeError as e:
            # A codec the server refuses is a RESULT, not a crash: ClickHouse
            # rejecting Gorilla on an Int64 column is the answer to "should we
            # use Gorilla here", straight from the engine.
            msg = str(e)
            short = msg.split("DB::Exception:", 1)[-1].split("(version")[0].strip()
            print(f"  {name:<20} REJECTED BY SERVER")
            print(f"  {'':<20} {short[:150]}")
            continue
        per, total = sizes(tbl)
        if name == "baseline":
            base_per, base_total = per, total
        col = change[0] if change and change[0] != "__global__" else None
        delta_col = ""
        if col and base_per:
            b, a = base_per.get(col, 0), per.get(col, 0)
            if b:
                delta_col = f"{col}: {b/1024:,.0f} KiB -> {a/1024:,.0f} KiB " \
                            f"({(a-b)/b*100:+.1f}%)"
        results.append((name, total, secs, delta_col))
        pct = "" if base_total is None or name == "baseline" else \
            f"  ({(total-base_total)/base_total*100:+.1f}% total)"
        print(f"  {name:<20} {total/1024/1024:7.2f} MiB   insert {secs:5.1f}s{pct}")
        if delta_col:
            print(f"  {'':<20} {delta_col}")
        ch.execute(f"DROP TABLE IF EXISTS {tbl}")

    print("\nper-column breakdown of the shipped schema:")
    for n, b in sorted(base_per.items(), key=lambda kv: -kv[1])[:8]:
        print(f"  {n:<22} {b/1024:9,.0f} KiB")


if __name__ == "__main__":
    main()
