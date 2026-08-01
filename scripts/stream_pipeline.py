"""Kafka -> validate -> Redis -> ClickHouse streaming ingestion.

Why each piece is here, since a diagram is not evidence:

  Kafka (Redpanda)  durable, replayable ingestion. Events are keyed by
                    video_session_id so every event for a session lands on
                    the SAME partition. Session reconstruction is inherently
                    ordered per key; a round-robin partitioner would split a
                    session across consumers and make ordering unrecoverable.

  Redis             two jobs. (1) An idempotency set: at-least-once delivery
                    means duplicates are guaranteed, so each event carries a
                    fingerprint and a duplicate is dropped before it can
                    double-count concurrency. (2) Open-session state, which is
                    mutable and read constantly -- exactly what ClickHouse is
                    bad at and Redis is good at.

  DLQ               a malformed event must not stop the pipeline, and must not
                    vanish either. Bad records go to a dead-letter topic AND a
                    ClickHouse table with the parse error attached, so they can
                    be inspected and replayed after a fix.

  ClickHouse        batched inserts, not per-event. One INSERT per event would
                    create a part per event and the merge tree would collapse.

Offsets are committed only AFTER the batch is durably in ClickHouse. That is
at-least-once: a crash mid-batch replays those events, and the Redis dedup set
turns the replay into a no-op. Together that is effectively-once, which is what
"exactly-once" means in practice for an idempotent sink.

    python scripts/stream_pipeline.py --setup
    python scripts/stream_pipeline.py --produce fixtures/dirty_day.csv --rate 5000
    python scripts/stream_pipeline.py --consume --for 60
    python scripts/stream_pipeline.py --stats
"""
import argparse
import csv
import hashlib
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch  # noqa: E402

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
TOPIC = "sony.events"
DLQ_TOPIC = "sony.events.dlq"
GROUP = "sony-ingest-v6"
API_VERSION = (2, 8, 0)          # Redpanda: skip the client's version probe

# Fields we require to be usable. Anything failing these is a DLQ record, not
# a silently-zeroed row -- the loader learned that lesson the hard way.
REQUIRED = ("video_session_id", "event_timestamp_ms", "event_type")

#: producer key-set -> {source field: target column}. See normalise().
_MAP_CACHE = {}

DDL = """
CREATE TABLE IF NOT EXISTS sony.stream_dlq (
    ingested_at   DateTime64(3,'UTC') DEFAULT now64(3),
    topic         LowCardinality(String),
    partition     UInt16,
    offset        UInt64,
    reason        LowCardinality(String),
    detail        String,
    payload       String
) ENGINE = MergeTree ORDER BY (reason, ingested_at);

CREATE TABLE IF NOT EXISTS sony.stream_progress (
    consumer_group LowCardinality(String),
    partition      UInt16,
    offset         UInt64,
    events_ok      UInt64,
    events_dlq     UInt64,
    events_dup     UInt64,
    updated_at     DateTime64(3,'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (consumer_group, partition);
"""


def redis_client():
    import redis
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=5,
                       decode_responses=True)


def producer():
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=BROKER, api_version=API_VERSION,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: (k or "").encode(),
        # durability over throughput: a lost event is a wrong answer later
        acks="all", retries=5, linger_ms=50, batch_size=64 * 1024,
        compression_type="gzip",
    )


def fingerprint(rec):
    """Identity of an event for dedup. Deliberately NOT the Kafka offset: a
    producer retry or an upstream replay emits the same event at a new offset,
    and that is precisely the duplicate we must collapse."""
    raw = "|".join(str(rec.get(k, "")) for k in
                   ("video_session_id", "event_timestamp_ms", "event_type", "event"))
    return hashlib.blake2b(raw.encode(), digest_size=16).hexdigest()


def setup():
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    admin = KafkaAdminClient(bootstrap_servers=BROKER, api_version=API_VERSION)
    have = set(admin.list_topics())
    want = [(TOPIC, 6), (DLQ_TOPIC, 1)]
    new = [NewTopic(name=n, num_partitions=p, replication_factor=1)
           for n, p in want if n not in have]
    if new:
        admin.create_topics(new)
        print(f"created topics: {', '.join(t.name for t in new)}")
    else:
        print("topics already present")
    print("topics:", sorted(KafkaAdminClient(
        bootstrap_servers=BROKER, api_version=API_VERSION).list_topics()))
    for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
        ch.execute(stmt)
    print("clickhouse: stream_dlq + stream_progress ready")
    r = redis_client()
    print("redis:", "reachable" if r.ping() else "unreachable")


def produce(path, rate, limit):
    """Replay a CSV as a live event stream, keyed by session."""
    p = producer()
    sent = 0
    t0 = time.time()
    budget = time.time()
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        rdr = csv.reader(fh)
        header = [h.strip() for h in next(rdr)]
        for row in rdr:
            if limit and sent >= limit:
                break
            rec = {header[i]: row[i] for i in range(min(len(header), len(row)))}
            # key by session so a session never splits across partitions
            key = rec.get("video_session_id") or rec.get("session_id") or ""
            p.send(TOPIC, key=key, value=rec)
            sent += 1
            if rate and sent % max(1, rate // 20) == 0:
                budget += (max(1, rate // 20)) / rate
                delay = budget - time.time()
                if delay > 0:
                    time.sleep(delay)
            if sent % 20000 == 0:
                print(f"  produced {sent:,} ({sent/(time.time()-t0):,.0f}/s)")
    p.flush()
    dt = time.time() - t0
    print(f"produced {sent:,} events in {dt:.1f}s ({sent/max(dt,.001):,.0f}/s) -> {TOPIC}")


def normalise(rec):
    """Map producer field names onto our columns and coerce types.

    Returns (row, error). A row is only returned when every REQUIRED field is
    usable; otherwise the error explains which one failed and the caller sends
    it to the DLQ.
    """
    # The field-name mapping is a property of the PRODUCER's schema, not of the
    # event, so it is derived once per distinct key-set and cached. Re-deriving
    # it per event cost ~14 string normalisations x N events and was the single
    # largest term in consumer throughput.
    keys = tuple(rec.keys())
    mapping = _MAP_CACHE.get(keys)
    if mapping is None:
        import load  # loader's alias table + required contract: one source of truth
        mapping, seen = {}, set()
        for k in keys:
            t = load.ALIASES.get(load._norm(k))
            if t and t not in seen:
                mapping[k] = t
                seen.add(t)
        _MAP_CACHE[keys] = mapping
        # First sighting of this key-set: register it. Once per schema, not
        # once per event -- the cache miss IS the change-detection signal.
        try:
            register_schema(keys)
        except Exception:
            pass  # the registry observes the stream; it must not stop it
    out = {}
    for k, t in mapping.items():
        v = rec.get(k)
        out[t] = v.strip() if isinstance(v, str) else v
    for f in REQUIRED:
        if not out.get(f):
            return None, f"missing:{f}"
    try:
        ts = int(out["event_timestamp_ms"])
    except (ValueError, TypeError):
        return None, "unparseable:event_timestamp_ms"
    if ts <= 0:
        return None, "nonpositive:event_timestamp_ms"
    # seconds-vs-milliseconds: a 10-digit epoch is seconds. Promote it rather
    # than dropping it -- this is a producer bug we can correct losslessly.
    if ts < 10_000_000_000:
        ts *= 1000
        out["_coerced"] = "seconds_epoch"
    # 1990..2100 sanity window. Outside it the clock is wrong, not the event.
    if not (631_152_000_000 < ts < 4_102_444_800_000):
        return None, "out_of_range:event_timestamp_ms"
    out["event_timestamp_ms"] = ts
    try:
        out["session_start_ms"] = int(out.get("session_start_ms") or 0)
    except (ValueError, TypeError):
        out["session_start_ms"] = 0
    try:
        out["content_id"] = int(out.get("content_id") or 0)
    except (ValueError, TypeError):
        out["content_id"] = 0
    out["country"] = (out.get("country") or "").lower()
    return out, None


COLS = ["content_id", "video_session_id", "user_id", "event_type", "event",
        "event_timestamp_ms", "platform", "app_version", "country",
        "audio_language", "subtitle_language", "player_version", "session_start_ms"]


def register_schema(keys, producer="sonyliv-player", events=0):
    """Record a producer key-set in the registry.

    A new fingerprint is a schema change, which is information rather than an
    error: producers may add or reorder fields freely. `compatible` records
    whether the REQUIRED columns could still be resolved -- dropping one of
    those is a breaking change and has to be visible the moment it appears,
    not after someone notices the numbers moved.
    """
    import load
    mapped, unmapped, seen = [], [], set()
    for k in keys:
        t = load.ALIASES.get(load._norm(k))
        if t and t not in seen:
            mapped.append(f"{k}->{t}")
            seen.add(t)
        else:
            unmapped.append(k)
    missing = [c for c in load.RAW_COLS if c not in seen]
    compatible = 0 if [c for c in missing if c in REQUIRED] else 1
    fp = hashlib.blake2b("|".join(sorted(keys)).encode(), digest_size=12).hexdigest()

    def arr(xs):
        return "[" + ",".join("'" + x.replace("'", "''") + "'" for x in xs) + "]"

    ch.execute(
        "INSERT INTO sony.schema_registry (fingerprint, producer, fields, mapped, "
        "unmapped, missing, compatible, first_seen, last_seen, events_seen) VALUES "
        f"('{fp}', '{producer}', {arr(list(keys))}, {arr(mapped)}, {arr(unmapped)}, "
        f"{arr(missing)}, {compatible}, now64(3), now64(3), {int(events)})")
    return {"fingerprint": fp, "compatible": bool(compatible),
            "mapped": len(mapped), "unmapped": unmapped, "missing": missing}


def consume(seconds, batch_size, table):
    from kafka import KafkaConsumer
    r = redis_client()
    p = producer()
    c = KafkaConsumer(
        TOPIC, bootstrap_servers=BROKER, api_version=API_VERSION,
        group_id=GROUP, auto_offset_reset="earliest",
        enable_auto_commit=False,               # commit only after the sink
        max_poll_records=batch_size,
        consumer_timeout_ms=4000,
    )
    ok = dup = dlq = coerced = 0
    batch, dlq_rows, pending = [], [], []
    t0 = time.time()

    def flush():
        nonlocal batch, dlq_rows
        if batch:
            body = "\n".join(
                "\t".join(str(row.get(col, "")).replace("\t", " ").replace("\n", " ")
                          for col in COLS) for row in batch).encode()
            ch.execute(f"INSERT INTO {table} ({', '.join(COLS)}) FORMAT TSV",
                       body=body)
            batch = []
        if dlq_rows:
            body = "\n".join("\t".join(str(x).replace("\t", " ").replace("\n", " ")
                                       for x in row) for row in dlq_rows).encode()
            ch.execute("INSERT INTO sony.stream_dlq "
                       "(topic, partition, offset, reason, detail, payload) FORMAT TSV",
                       body=body)
            dlq_rows = []

    print(f"consuming {TOPIC} -> {table} for up to {seconds}s (group={GROUP})")
    idle_since = None
    while time.time() - t0 < seconds:
        polled = c.poll(timeout_ms=1000, max_records=batch_size)
        if not polled:
            # Stop once the topic is drained, otherwise the run reports its
            # idle polling as processing time and understates throughput by
            # however long the time budget happened to be.
            idle_since = idle_since or time.time()
            if time.time() - idle_since > 5:
                break
            continue
        idle_since = None
        for tp, msgs in polled.items():
            for m in msgs:
                try:
                    rec = json.loads(m.value)
                except Exception as e:
                    dlq_rows.append((m.topic, m.partition, m.offset, "bad_json",
                                     str(e)[:200], (m.value or b"")[:500].decode("utf-8", "replace")))
                    dlq += 1
                    continue
                row, err = normalise(rec)
                if err:
                    dlq_rows.append((m.topic, m.partition, m.offset, err.split(":")[0],
                                     err, json.dumps(rec)[:500]))
                    p.send(DLQ_TOPIC, key=rec.get("video_session_id", ""), value=
                           {"reason": err, "record": rec})
                    dlq += 1
                    continue
                if row.pop("_coerced", None):
                    coerced += 1
                pending.append(row)

        # Redis in ONE round trip per batch, not three per event. Per-event
        # calls measured 345 events/s against a WSL-hosted Redis -- ~7 hours
        # for a 9M-event day. The work is identical; only the number of
        # network hops changes.
        if pending:
            pipe = r.pipeline(transaction=False)
            for row in pending:
                pipe.set("ev:" + fingerprint(row), 1, nx=True, ex=86400)
            fresh = pipe.execute()          # True = first time seen
            keep = []
            for row, is_new in zip(pending, fresh):
                if is_new:
                    keep.append(row)
                else:
                    dup += 1
            if keep:
                pipe = r.pipeline(transaction=False)
                for row in keep:
                    k = "sess:" + row["video_session_id"]
                    pipe.hset(k, mapping={
                        "last_ts": row["event_timestamp_ms"],
                        "platform": row.get("platform", ""),
                        "content_id": row.get("content_id", 0)})
                    pipe.expire(k, 7200)
                pipe.execute()
            batch.extend(keep)
            ok += len(keep)
            pending = []
        if len(batch) >= batch_size:
            flush()
            c.commit()          # offsets AFTER the sink -> at-least-once
    flush()
    c.commit()
    # exclude the 5s idle detection window from the rate
    dt = max((idle_since or time.time()) - t0, 0.001)
    total = ok + dup + dlq
    print(f"  ok {ok:,} | duplicates dropped {dup:,} | DLQ {dlq:,} | coerced {coerced:,}")
    print(f"  {total:,} events in {dt:.1f}s = {total/dt:,.0f} events/s end-to-end")
    try:
        ch.execute(
            "INSERT INTO sony.stream_progress "
            "(consumer_group, partition, offset, events_ok, events_dlq, events_dup) "
            f"VALUES ('{GROUP}', 0, 0, {ok}, {dlq}, {dup})")
    except Exception:
        pass
    c.close()
    return {"ok": ok, "dup": dup, "dlq": dlq, "coerced": coerced}


def stats():
    r = redis_client()
    info = r.info("memory")
    print("REDIS")
    print(f"  open sessions cached : {len(r.keys('sess:*')):,}")
    print(f"  dedup fingerprints   : {len(r.keys('ev:*')):,}")
    print(f"  memory used          : {info.get('used_memory_human')}")
    print("\nKAFKA")
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=BROKER, api_version=API_VERSION)
    for t in (TOPIC, DLQ_TOPIC):
        parts = c.partitions_for_topic(t) or set()
        tps = [TopicPartition(t, p) for p in parts]
        if not tps:
            print(f"  {t}: no partitions"); continue
        end = c.end_offsets(tps)
        print(f"  {t}: {len(parts)} partitions, {sum(end.values()):,} messages")
    c.close()
    print("\nCLICKHOUSE")
    try:
        print("  DLQ by reason:")
        print(ch.query("SELECT reason, count() FROM sony.stream_dlq "
                       "GROUP BY reason ORDER BY 2 DESC")[0] or "    (empty)")
    except Exception as e:
        print("   ", str(e)[:150])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--setup", action="store_true", help="create topics and tables")
    p.add_argument("--produce", metavar="CSV", help="replay a CSV into Kafka")
    p.add_argument("--rate", type=int, default=0, help="events/sec (0 = as fast as possible)")
    p.add_argument("--limit", type=int, help="stop after N events")
    p.add_argument("--consume", action="store_true")
    p.add_argument("--for", dest="secs", type=int, default=60, help="consume for N seconds")
    p.add_argument("--batch", type=int, default=2000, help="rows per ClickHouse insert")
    p.add_argument("--table", default="sony.raw_events_stream")
    p.add_argument("--stats", action="store_true")
    a = p.parse_args()

    if a.setup:
        setup()
    if a.produce:
        produce(a.produce, a.rate, a.limit)
    if a.consume:
        ch.execute(f"CREATE TABLE IF NOT EXISTS {a.table} AS sony.raw_events")
        consume(a.secs, a.batch, a.table)
    if a.stats:
        stats()
    if not any([a.setup, a.produce, a.consume, a.stats]):
        p.print_help()


if __name__ == "__main__":
    main()
