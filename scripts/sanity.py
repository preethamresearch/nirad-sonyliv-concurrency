"""Full-stack sanity check: one command, before a demo or a sealed run.

Playwright covers the browser. This covers everything underneath it -- the
Cloud connection, table integrity, the oracle parity gate, the streaming
infrastructure, and every product in the stack -- so "is it working?" has a
single answer rather than six terminals.

    python scripts/sanity.py            # fast: no oracle recompute
    python scripts/sanity.py --deep     # includes independent oracle parity
"""
import argparse
import os
import socket
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch  # noqa: E402

OK, WARN, FAIL = "PASS", "WARN", "FAIL"
results = []


def check(name, fn, warn_only=False):
    t0 = time.time()
    try:
        detail = fn()
        status = OK
    except Exception as e:
        detail = str(e).split("\n")[0][:110]
        status = WARN if warn_only else FAIL
    ms = (time.time() - t0) * 1000
    results.append((status, name, detail, ms))
    mark = {OK: "  ok  ", WARN: " warn ", FAIL: " FAIL "}[status]
    print(f"[{mark}] {name:<38} {detail:<52} {ms:6.0f}ms", flush=True)


def http(url, timeout=20):
    def go():
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return f"HTTP {r.status}"
    return go


def tcp(host, port):
    def go():
        s = socket.create_connection((host, port), timeout=6)
        s.close()
        return f"{host}:{port} open"
    return go


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deep", action="store_true",
                    help="recompute the oracle and compare (slow, ~15s)")
    ap.add_argument("--raw", default=None, help="raw CSV for the deep parity check")
    a = ap.parse_args()

    print("\n=== ClickHouse Cloud ===")
    check("connection", lambda: f"{ch.config()['host']} · v{ch.scalar('SELECT version()')}")
    check("raw_events", lambda: f"{int(ch.scalar('SELECT count() FROM sony.raw_events')):,} rows")
    check("session_active_intervals",
          lambda: f"{int(ch.scalar('SELECT count() FROM sony.session_active_intervals FINAL')):,} rows")
    check("concurrency deltas",
          lambda: f"{int(ch.scalar('SELECT count() FROM sony.concurrency_minute_delta')):,} rows")
    check("on-disk footprint", lambda: ch.scalar(
        "SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts "
        "WHERE database='sony' AND active"))

    print("\n=== the headline figures ===")
    # Dataset-relative, never hardcoded: on judging day the numbers change but
    # the *relationships* must hold. Foreground-only counts a strict subset of
    # what the naive overlap counts, so fg <= naive always -- and if the two
    # are EQUAL the foreground model silently did nothing, which is also a bug.
    def peaks():
        fg = int(ch.scalar("""
SELECT max(running) FROM (
  SELECT sum(sum(delta)) OVER (ORDER BY minute) AS running
  FROM sony.concurrency_minute_delta GROUP BY minute ORDER BY minute)"""))
        nv = int(ch.scalar("""
SELECT max(c) FROM (
  SELECT sum(sum(d)) OVER (ORDER BY m) AS c FROM (
    SELECT toDateTime(intDiv(a,60000)*60,'UTC') AS m, 1 AS d FROM
      (SELECT min(event_timestamp_ms) a, max(event_timestamp_ms) b
       FROM sony.raw_events GROUP BY video_session_id)
    UNION ALL
    SELECT toDateTime((intDiv(b,60000)+1)*60,'UTC') AS m, -1 AS d FROM
      (SELECT min(event_timestamp_ms) a, max(event_timestamp_ms) b
       FROM sony.raw_events GROUP BY video_session_id))
  GROUP BY m ORDER BY m)"""))
        if fg <= 0:
            raise AssertionError("foreground peak is 0 -- no active intervals")
        if fg > nv:
            raise AssertionError(f"foreground {fg:,} EXCEEDS naive {nv:,} -- impossible")
        if fg == nv:
            raise AssertionError(f"foreground == naive ({fg:,}) -- model applied nothing")
        gap = nv - fg
        return f"fg {fg:,} vs naive {nv:,} · gap {gap:,} ({gap/nv*100:.1f}%)"
    check("peak concurrency (fg < naive)", peaks)

    print("\n=== dashboard ===")
    for path in ["/", "/app", "/deck", "/classic",
                 "/api/overview", "/api/series", "/api/live_ops",
                 "/api/ingest_monitor", "/api/config", "/api/catalog",
                 "/shots/sonyliv.png", "/vendor/mermaid.min.js"]:
        check(f"GET {path}", http("http://localhost:877" + path, timeout=120))

    print("\n=== product stack ===")
    check("ClickStack / HyperDX", http("http://localhost:8080/"))
    check("Langfuse", http("http://localhost:3000/api/public/health"))
    check("LibreChat", http("http://localhost:3080/"))
    check("Kafka / Redpanda", tcp("127.0.0.1", 9092))
    check("Redis", tcp("127.0.0.1", 6379))

    def redis_state():
        import redis
        r = redis.Redis(host="127.0.0.1", port=6379, socket_connect_timeout=5)
        info = r.info("memory")
        return f"ping {r.ping()} · {info['used_memory_human']}"
    check("Redis state", redis_state, warn_only=True)

    def kafka_topics():
        from kafka import KafkaConsumer
        c = KafkaConsumer(bootstrap_servers="127.0.0.1:9092", api_version=(2, 8, 0))
        t = sorted(c.topics()); c.close()
        return ", ".join(t) or "no topics"
    check("Kafka topics", kafka_topics, warn_only=True)

    print("\n=== streaming layer ===")
    check("DLQ table", lambda: f"{int(ch.scalar('SELECT count() FROM sony.stream_dlq')):,} records")
    check("schema registry", lambda:
          f"{int(ch.scalar('SELECT uniqExact(fingerprint) FROM sony.schema_registry')):,} schemas · "
          f"{int(ch.scalar('SELECT countIf(compatible=0) FROM sony.schema_registry'))} incompatible")
    check("materialized views", lambda:
          f"ingest_rate {int(ch.scalar('SELECT count() FROM sony.ingest_rate')):,} · "
          f"session_spans {int(ch.scalar('SELECT count() FROM sony.session_spans')):,}")
    check("provenance", lambda:
          f"{int(ch.scalar('SELECT count() FROM sony.pipeline_runs')):,} runs · last "
          + ch.scalar("SELECT run_id FROM sony.pipeline_runs ORDER BY started_at DESC LIMIT 1"))

    if a.deep:
        print("\n=== independent oracle parity (deep) ===")
        raw = a.raw or os.path.join(
            r"C:\d\pre-check\click-a-thon-2026\SonyLiv\data", "ch-hackathon-raw-data.csv")

        def parity():
            import oracle
            if not os.path.exists(raw):
                raise AssertionError("raw CSV not found: " + raw)
            ivs = oracle.build_intervals(raw)
            chn = int(ch.scalar("SELECT count() FROM sony.session_active_intervals FINAL"))
            if len(ivs) != chn:
                raise AssertionError(f"oracle {len(ivs):,} vs clickhouse {chn:,}")
            return f"{len(ivs):,} intervals · exact match"
        check("oracle vs clickhouse", parity)

    n_fail = sum(1 for r in results if r[0] == FAIL)
    n_warn = sum(1 for r in results if r[0] == WARN)
    print("\n" + "=" * 78)
    print(f"  {len(results) - n_fail - n_warn} passed · {n_warn} warnings · {n_fail} failed")
    print("=" * 78 + "\n")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
