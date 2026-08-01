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


# Header spellings seen or plausible for the same field. The sealed dataset is
# described as wider and dirtier than the sample, so the loader matches columns
# BY NAME through this table instead of by position. Positional binding against
# a file with an extra or reordered column does not fail -- it shifts every
# value one column left and loads platform into country. Wrong answers that
# look right are the worst outcome available on judging day.
ALIASES = {
    "content_id": "content_id", "contentid": "content_id",
    "video_session_id": "video_session_id", "session_id": "video_session_id",
    "videosessionid": "video_session_id",
    "user_id": "user_id", "userid": "user_id",
    "event_type": "event_type", "eventtype": "event_type",
    "event": "event", "event_name": "event", "event_sub_type": "event",
    "event_timestamp_ms": "event_timestamp_ms",
    "event_timestamp": "event_timestamp_ms", "event_time_ms": "event_timestamp_ms",
    "timestamp": "event_timestamp_ms", "event_epoch": "event_timestamp_ms",
    "event_ts_millis": "event_timestamp_ms", "event_ts": "event_timestamp_ms",
    "event_ts_ms": "event_timestamp_ms", "eventtimestamp": "event_timestamp_ms",
    "event_time": "event_timestamp_ms", "ts": "event_timestamp_ms",
    "platform": "platform", "device_platform": "platform",
    "app_version": "app_version", "appversion": "app_version",
    "country": "country", "geo_country": "country",
    "audio_language": "audio_language", "audio_lang": "audio_language",
    "subtitle_language": "subtitle_language", "subtitle_lang": "subtitle_language",
    "player_version": "player_version", "playerversion": "player_version",
    "session_start_ms": "session_start_ms",
    "session_start_epoch": "session_start_ms", "session_start": "session_start_ms",
}

# How each target column is built from the staged String value. Every cast is
# total: a value that will not parse becomes 0 or '' and is COUNTED, never
# allowed to abort the load. `content_id` keeps its sign (sentinels are real).
CASTS = {
    "content_id":         "toInt64OrZero(trim(BOTH ' ' FROM {c}))",
    "video_session_id":   "trim(BOTH ' ' FROM {c})",
    "user_id":            "trim(BOTH ' ' FROM {c})",
    "event_type":         "trim(BOTH ' ' FROM {c})",
    "event":              "trim(BOTH ' ' FROM {c})",
    "event_timestamp_ms": "toInt64OrZero(trim(BOTH ' ' FROM {c}))",
    "platform":           "trim(BOTH ' ' FROM {c})",
    "app_version":        "trim(BOTH ' ' FROM {c})",
    "country":            "lower(trim(BOTH ' ' FROM {c}))",
    "audio_language":     "trim(BOTH ' ' FROM {c})",
    "subtitle_language":  "trim(BOTH ' ' FROM {c})",
    "player_version":     "trim(BOTH ' ' FROM {c})",
    "session_start_ms":   "toInt64OrZero(trim(BOTH ' ' FROM {c}))",
}


# Columns that may NEVER be silently defaulted. An alias table cannot
# anticipate every rename a producer might ship, so the fallback must be a
# loud failure, not a zero. Defaulting event_timestamp_ms to 0 puts every
# event at 1970 and produces a confidently wrong answer -- exactly the class
# of error the column-shift fix exists to prevent.
REQUIRED = ("video_session_id", "event_timestamp_ms", "event_type")


#: content_dim is a different shape, so it needs its own alias and cast tables.
#: title is NOT trimmed of internal whitespace -- it is display text, not a key.
CONTENT_ALIASES = {
    "content_id": "content_id", "contentid": "content_id", "id": "content_id",
    "title": "title", "content_title": "title", "name": "title",
    "video_type": "video_type", "videotype": "video_type", "type": "video_type",
    "category": "category", "genre": "category", "content_category": "category",
}
CONTENT_CASTS = {
    "content_id": "toInt64OrZero(trim(BOTH ' ' FROM {c}))",
    "title":      "trim(BOTH ' ' FROM {c})",
    "video_type": "lower(trim(BOTH ' ' FROM {c}))",
    "category":   "trim(BOTH ' ' FROM {c})",
}


def _mb(path):
    return os.path.getsize(path) / (1024 * 1024)


def _norm(name):
    """Header text -> comparable key. Strips BOM, quotes, spaces, punctuation."""
    n = name.strip().strip('"').strip("'").lstrip("﻿").lower()
    return "".join(ch_ for ch_ in n.replace(" ", "_").replace("-", "_")
                   if ch_.isalnum() or ch_ == "_")


def read_header(path):
    """The file's own header, in file order. Read as bytes so an odd encoding
    cannot raise before we have even looked at the data."""
    with open(path, "rb") as fh:
        line = fh.readline()
    text = line.decode("utf-8", "replace").rstrip("\r\n")
    # naive split is fine for a header: field names do not contain commas
    return [c for c in text.split(",")]


def plan_columns(path, targets, aliases=None):
    """Map the file's header onto our columns. Returns (plan, unknown, missing).

    plan    list of (staging_col, target_col) in FILE order
    unknown header names we do not recognise -- loaded into staging, ignored
    missing target columns the file does not provide -- defaulted, and named
    """
    aliases = aliases or ALIASES
    header = read_header(path)
    plan, unknown, seen = [], [], set()
    for i, raw in enumerate(header):
        key = _norm(raw)
        target = aliases.get(key)
        stage_col = f"c{i}"
        if target and target in targets and target not in seen:
            plan.append((stage_col, target))
            seen.add(target)
        else:
            plan.append((stage_col, None))
            if not target or target not in targets:
                unknown.append(raw.strip() or f"<blank col {i}>")
    missing = [t for t in targets if t not in seen]
    return header, plan, unknown, missing


def split_ranges(path, chunk_mb=64):
    """Byte ranges aligned to row boundaries, header excluded.

    The upload is the whole cost of a run -- 95% of wall clock at 1x, and one
    sequential HTTP stream to ap-south-1 does not get faster with a bigger
    file. Splitting lets N uploads overlap. Ordering does not matter: staging
    is `ORDER BY tuple()` and the real sort key is applied by the downstream
    INSERT..SELECT, so chunks may land in any order.
    """
    size = os.path.getsize(path)
    with open(path, "rb") as fh:
        fh.readline()                 # header belongs to no chunk
        start = fh.tell()
    if size - start <= 0:
        return []
    target = max(1, int(chunk_mb * 1024 * 1024))
    n = max(1, min(64, (size - start + target - 1) // target))
    step = (size - start) // n
    bounds = [start]
    with open(path, "rb") as fh:
        for i in range(1, n):
            fh.seek(start + i * step)
            fh.readline()             # advance to the next row boundary
            pos = fh.tell()
            if pos > bounds[-1]:
                bounds.append(pos)
    bounds.append(size)
    return [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)
            if bounds[i] < bounds[i + 1]]


def _upload_chunk(path, lo, hi, stage, settings):
    with open(path, "rb") as fh:
        fh.seek(lo)
        body = fh.read(hi - lo)
    ch.execute(f"INSERT INTO {stage} FORMAT CSV", body=body, settings=settings)
    return len(body)


def load_resilient(table, targets, path, casts, tolerate_ratio=0.02, aliases=None,
                   workers=6, chunk_mb=64):
    """Header-matched, type-tolerant load via a String staging table.

    The sealed dataset is expected to be wider and dirtier than the sample.
    Three things must not happen: a shifted column must not corrupt results
    silently, one malformed row must not abort the load, and anything the
    loader had to tolerate must be REPORTED rather than swallowed.

    Everything lands in staging as String -- the only type that cannot reject
    a row -- then casts run in SQL where a failure is a countable 0, not an
    exception. Rows the CSV parser itself cannot split are allowed up to
    `tolerate_ratio` and the count is printed.
    """
    if not os.path.exists(path):
        sys.exit(f"missing file: {path}")
    header, plan, unknown, missing = plan_columns(path, targets, aliases)
    stage = table.split(".")[0] + ".stage_" + table.split(".")[-1]

    print(f"loading {os.path.basename(path)} ({_mb(path):.1f} MB) -> {table}")
    print(f"  header: {len(header)} columns; matched {len(header) - len(unknown)}")
    if unknown:
        print(f"  ignoring {len(unknown)} unrecognised column(s): {', '.join(unknown[:8])}"
              + (" ..." if len(unknown) > 8 else ""))
    if missing:
        print(f"  !! MISSING {len(missing)} expected column(s), will default: {', '.join(missing)}")

    fatal = [c for c in missing if c in REQUIRED]
    if fatal:
        sys.exit(
            f"\nABORT: required column(s) not found in {os.path.basename(path)}: "
            f"{', '.join(fatal)}\n"
            f"  file header : {', '.join(h.strip() for h in header)}\n"
            f"  unrecognised: {', '.join(unknown) or '(none)'}\n"
            f"  These cannot be defaulted -- a zero timestamp or empty session id\n"
            f"  produces a confidently wrong answer. Add the correct spelling to\n"
            f"  ALIASES in scripts/load.py and re-run.")

    ch.execute(f"DROP TABLE IF EXISTS {stage}")
    cols_ddl = ", ".join(f"c{i} String" for i in range(len(header)))
    ch.execute(f"CREATE TABLE {stage} ({cols_ddl}) ENGINE = MergeTree ORDER BY tuple()")

    t0 = time.time()
    settings = {
        "input_format_csv_skip_first_lines": "1",
        "max_insert_block_size": "1048576",
        # Tolerate genuinely unparseable LINES, but bound it: a file that is
        # 2% garbage is a different file, and we would rather fail loudly.
        "input_format_allow_errors_ratio": str(tolerate_ratio),
        "input_format_allow_errors_num": "1000",
        # A short row is padded rather than rejected; a long row is truncated.
        "input_format_csv_allow_variable_number_of_columns": "1",
    }
    # Chunks carry no header, so the skip applies only to a single-stream load.
    chunk_settings = dict(settings)
    chunk_settings.pop("input_format_csv_skip_first_lines", None)
    ranges = split_ranges(path, chunk_mb)
    if len(ranges) > 1 and workers > 1:
        import concurrent.futures as _fut
        print(f"  uploading {len(ranges)} chunks on {workers} workers")
        done = 0
        with _fut.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_upload_chunk, path, lo, hi, stage, chunk_settings)
                    for lo, hi in ranges]
            for f in _fut.as_completed(futs):
                f.result()            # re-raise inside the parent
                done += 1
                if done % 5 == 0 or done == len(futs):
                    print(f"    {done}/{len(futs)} chunks", flush=True)
    else:
        with open(path, "rb") as fh:
            ch.execute(f"INSERT INTO {stage} FORMAT CSV", body=fh, settings=settings)
    staged = int(ch.scalar(f"SELECT count() FROM {stage}"))
    el = time.time() - t0
    print(f"  staged {staged:,} rows in {el:.1f}s ({staged/max(el,.01):,.0f} rows/s)")

    src = {t: c for c, t in plan if t}
    select, diag = [], []
    for t in targets:
        if t in src:
            select.append(casts[t].format(c=src[t]) + f" AS {t}")
            if "OrZero" in casts[t]:
                diag.append((t, f"countIf({casts[t].format(c=src[t])} = 0)"))
        else:
            select.append(("toInt64(0)" if "Int64" in casts.get(t, "") or "OrZero" in casts.get(t, "")
                           else "''") + f" AS {t}")

    # A row whose REQUIRED fields cannot be parsed is rejected, not zeroed.
    #
    # Zeroing kept the row and put its event at 1970, which quietly widened the
    # dataset's time range and — worse — made the batch path disagree with both
    # the streaming path (which DLQs such rows) and the oracle (which skips
    # them). A parity gate comparing two different populations proves nothing.
    # Rejection is the behaviour all three now share.
    reject = ""
    if table.endswith("raw_events"):
        sid, ts = src.get("video_session_id"), src.get("event_timestamp_ms")
        conds = []
        if sid:
            conds.append(f"trim(BOTH ' ' FROM {sid}) != ''")
        if ts:
            conds.append(f"toInt64OrZero(trim(BOTH ' ' FROM {ts})) > 0")
        if conds:
            reject = " WHERE " + " AND ".join(conds)

    ch.execute(f"INSERT INTO {table} ({', '.join(targets)}) "
               f"SELECT {', '.join(select)} FROM {stage}{reject}")
    n = int(ch.scalar(f"SELECT count() FROM {table}"))
    rejected = staged - n if reject else 0
    print(f"  loaded {n:,} rows into {table}")
    if rejected > 0:
        print(f"  !! rejected {rejected:,} row(s) with unusable required fields "
              f"({rejected / max(staged,1) * 100:.3f}%) -- not silently zeroed")

    # Report what the casts had to absorb. Silence here would be the same sin
    # as a silent column shift.
    for label, expr in diag:
        bad = int(ch.scalar(f"SELECT {expr} FROM {stage}"))
        if bad:
            print(f"  !! {label}: {bad:,} value(s) unparseable -> 0 "
                  f"({bad / max(staged,1) * 100:.3f}%)")
    ch.execute(f"DROP TABLE IF EXISTS {stage}")
    return {"staged": staged, "loaded": n, "rejected": rejected,
            "unknown": unknown, "missing": missing}


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
    p.add_argument("--strict", action="store_true",
                   help="positional binding, zero error tolerance (the original "
                        "path; fails loudly on any schema drift)")
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
        if a.strict:
            load_csv("sony.raw_events", RAW_COLS, a.raw)
        else:
            load_resilient("sony.raw_events", RAW_COLS, a.raw, CASTS)

    if a.verify or a.raw:
        verify()


if __name__ == "__main__":
    main()
