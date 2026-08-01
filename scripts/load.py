"""Load the SonyLIV playback datasets into ClickHouse.

Deliberately parameterised by file path rather than hardcoded: this same
entry point is what the unseen-day harness calls, so the sealed dataset
loads through exactly the code path the known dataset did. No special case,
no manual step, no "we ran it slightly differently on the day".

    python scripts/load.py --schema                       # create tables
    python scripts/load.py --content <path> --raw <path>  # load data
    python scripts/load.py --verify                       # sanity checks
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Column order as it appears in ch-hackathon-raw-data.csv. The header names
# differ from our column names in two places (event_timestamp -> _ms,
# session_start_epoch -> session_start_ms), so we skip the header and bind
# positionally rather than using CSVWithNames.
RAW_COLS = [
    "content_id", "video_session_id", "user_id", "event_type", "event",
    "event_timestamp_ms", "platform", "app_version", "country",
    "audio_language", "subtitle_language", "player_version", "session_start_ms",
]


def _mb(path):
    return os.path.getsize(path) / (1024 * 1024)


def load_csv(table, cols, path, skip_header=True):
    if not os.path.exists(path):
        sys.exit(f"missing file: {path}")
    collist = ", ".join(cols)
    settings = {
        "input_format_csv_skip_first_lines": "1" if skip_header else "0",
        # The stream is one big INSERT; let the server build wide parts rather
        # than many small ones we would immediately have to merge.
        "max_insert_block_size": "1048576",
        "input_format_allow_errors_num": "0",
    }
    print(f"loading {os.path.basename(path)} ({_mb(path):.1f} MB) -> {table}")
    t0 = time.time()
    with open(path, "rb") as fh:
        ch.execute(f"INSERT INTO {table} ({collist}) FORMAT CSV", body=fh, settings=settings)
    n = ch.scalar(f"SELECT count() FROM {table}")
    print(f"  done in {time.time() - t0:.1f}s -- {int(n):,} rows in {table}")


def verify():
    checks = [
        ("raw events",            "SELECT count() FROM sony.raw_events"),
        ("distinct sessions",     "SELECT uniqExact(video_session_id) FROM sony.raw_events"),
        ("distinct users",        "SELECT uniqExact(user_id) FROM sony.raw_events"),
        ("content rows",          "SELECT count() FROM sony.content_dim"),
        ("time range",            "SELECT concat(toString(min(event_time)), ' .. ', toString(max(event_time))) FROM sony.raw_events"),
        ("liveness events",       "SELECT countIf(is_liveness) FROM sony.raw_events"),
        ("state transitions",     "SELECT countIf(state_delta != 0) FROM sony.raw_events"),
        ("sessions w/o end",      "SELECT count() FROM (SELECT video_session_id FROM sony.raw_events GROUP BY video_session_id HAVING countIf(event_type='VideoSessionEnd') = 0)"),
        # dictHas, not dictGetOrDefault(...)='': 1,089 content rows have a
        # legitimately empty video_type, and the naive check reports those as
        # join failures. They are not.
        ("content join misses",   "SELECT uniqExactIf(content_id, NOT dictHas('sony.content_dict', tuple(content_id))) FROM sony.raw_events"),
        ("sentinel content_ids",  "SELECT count() FROM sony.content_dim WHERE content_id < 0"),
        ("sessions >1 user_id",   "SELECT count() FROM (SELECT video_session_id FROM sony.raw_events GROUP BY video_session_id HAVING uniqExact(user_id) > 1)"),
        ("sessions >1 platform",  "SELECT count() FROM (SELECT video_session_id FROM sony.raw_events GROUP BY video_session_id HAVING uniqExact(platform) > 1)"),
    ]
    print("\nverification")
    for label, sql in checks:
        try:
            print(f"  {label:<22} {ch.scalar(sql)}")
        except RuntimeError as e:
            print(f"  {label:<22} FAILED: {str(e)[:160]}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--schema", action="store_true", help="run sql/01_schema.sql")
    p.add_argument("--content", help="path to ch-hackathon-content-data.csv")
    p.add_argument("--raw", help="path to ch-hackathon-raw-data.csv")
    p.add_argument("--truncate", action="store_true", help="empty tables before loading")
    p.add_argument("--verify", action="store_true")
    a = p.parse_args()

    if not any([a.schema, a.content, a.raw, a.verify]):
        p.print_help()
        return

    if not ch.ping():
        sys.exit("fix the connection in .env first (copy from .env.example)")

    if a.schema:
        print("\napplying sql/01_schema.sql")
        ch.script(os.path.join(REPO, "sql", "01_schema.sql"))

    if a.truncate:
        for t in ("sony.raw_events", "sony.content_dim"):
            ch.execute(f"TRUNCATE TABLE IF EXISTS {t}")
        print("truncated raw_events, content_dim")

    # Content first: the dictionary must be populated before raw enrichment.
    # The content file is a full snapshot, not an append stream, so loading it
    # is idempotent by truncating. ReplacingMergeTree only collapses duplicates
    # at merge time, so without this a re-run silently doubles the dimension.
    if a.content:
        ch.execute("TRUNCATE TABLE IF EXISTS sony.content_dim")
        load_csv("sony.content_dim", ["content_id", "title", "video_type", "category"], a.content)
        ch.execute("SYSTEM RELOAD DICTIONARY sony.content_dict")
        print("  dictionary reloaded")

    if a.raw:
        load_csv("sony.raw_events", RAW_COLS, a.raw)

    if a.verify or a.raw:
        verify()


if __name__ == "__main__":
    main()
