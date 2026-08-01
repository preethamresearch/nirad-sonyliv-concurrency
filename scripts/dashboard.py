"""Live concurrency dashboard. Serves locally, computes on ClickHouse Cloud.

    python scripts/dashboard.py            # http://localhost:877

Every number on the page is produced by a query against the Cloud service --
nothing is precomputed, cached or baked into the page. The latency and
rows-read figures shown in the header are the real ones for the query that
drew the current chart.

The point of the visualisation is the GAP between two curves:

    naive        interval overlap from session start to session end
    foreground   active = intent_playing AND client_alive

The area between them is audience that an open-app-equals-viewer model would
have reported to the business. On the provided dataset that peaks at 653
sessions -- 17.4% of the naive figure.

No CDN, no npm, no chart library: the page is vanilla JS drawing SVG. Venue
wi-fi cannot break the demo, and there is nothing to install at 3am.
"""
import json
import os
import sys
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.join(REPO, "web")

# Cache only the things that cannot change while the server is up.
_bounds = None
_filters = None


def q_ident(v):
    return "'" + str(v).replace("'", "''") + "'"


def where_clause(args, prefix=""):
    parts = []
    for key in ("platform", "country", "video_type"):
        v = args.get(key)
        if v and v != "all":
            parts.append(f"{prefix}{key} = {q_ident(v)}")
    return (" AND " + " AND ".join(parts)) if parts else ""


def bounds():
    global _bounds
    if _bounds is None:
        lo = ch.scalar("SELECT toString(min(minute)) FROM sony.concurrency_delta_all")
        hi = ch.scalar("SELECT toString(max(minute)) FROM sony.concurrency_delta_all")
        _bounds = (lo, hi)
    return _bounds


def filters():
    global _filters
    if _filters is None:
        def vals(sql):
            t, _ = ch.query(sql)
            return [l for l in t.splitlines() if l]
        _filters = {
            "platform": vals("SELECT DISTINCT platform FROM sony.session_active_intervals FINAL "
                             "WHERE platform != '' ORDER BY platform"),
            "country": vals("SELECT DISTINCT country FROM sony.session_active_intervals FINAL "
                            "WHERE country != '' ORDER BY country"),
            "video_type": vals("SELECT DISTINCT video_type FROM sony.session_active_intervals FINAL "
                               "WHERE video_type != '' ORDER BY video_type"),
        }
    return _filters


def series(args):
    """Foreground-only and naive curves for the same slice, both from Cloud."""
    lo, hi = bounds()
    t0 = args.get("from") or lo
    t1 = args.get("to") or hi
    w = where_clause(args)
    total_rows = 0
    t_start = time.time()

    # --- foreground-only: the serving layer (sealed deltas UNION hot tier) ---
    fg_sql = f"""
SELECT toString(minute), toInt32(c) FROM (
  SELECT minute, sum(d) OVER (ORDER BY minute) AS c FROM (
    SELECT minute, sum(delta) AS d
    FROM sony.concurrency_delta_all
    WHERE minute <= toDateTime({q_ident(t1)}, 'UTC') {w}
    GROUP BY minute
    ORDER BY minute WITH FILL
      FROM toDateTime({q_ident(lo)}, 'UTC')
      TO   toDateTime({q_ident(t1)}, 'UTC') + INTERVAL 1 MINUTE STEP 60))
WHERE minute >= toDateTime({q_ident(t0)}, 'UTC')
ORDER BY minute"""
    fg_text, fg_el = ch.query(fg_sql)
    total_rows += int(ch.LAST_SUMMARY.get("read_rows", 0) or 0)

    # --- naive: plain session start -> session end overlap, no state model ---
    # Deliberately recomputed from raw_events rather than stored: it is the
    # straw man, and computing it live proves we are not flattering ourselves
    # with a stale or differently-filtered baseline.
    nv_sql = f"""
WITH sess AS (
  SELECT video_session_id,
         min(event_timestamp_ms) AS a,
         max(event_timestamp_ms) AS b,
         argMin(platform, event_timestamp_ms) AS platform,
         argMin(country,  event_timestamp_ms) AS country,
         argMin(content_id, event_timestamp_ms) AS content_id
  FROM sony.raw_events GROUP BY video_session_id),
enriched AS (
  SELECT s.a AS a, s.b AS b, s.platform AS platform, s.country AS country,
         c.video_type AS video_type
  FROM sess AS s
  LEFT JOIN (SELECT content_id, video_type FROM sony.content_dim FINAL) AS c
    ON c.content_id = s.content_id)
SELECT toString(minute), toInt32(c) FROM (
  SELECT minute, sum(d) OVER (ORDER BY minute) AS c FROM (
    SELECT minute, sum(d) AS d FROM (
      SELECT toDateTime(intDiv(a, 60000) * 60, 'UTC') AS minute, 1 AS d
      FROM enriched WHERE 1=1 {w}
      UNION ALL
      SELECT toDateTime((intDiv(b, 60000) + 1) * 60, 'UTC') AS minute, -1 AS d
      FROM enriched WHERE 1=1 {w})
    WHERE minute <= toDateTime({q_ident(t1)}, 'UTC')
    GROUP BY minute
    ORDER BY minute WITH FILL
      FROM toDateTime({q_ident(lo)}, 'UTC')
      TO   toDateTime({q_ident(t1)}, 'UTC') + INTERVAL 1 MINUTE STEP 60))
WHERE minute >= toDateTime({q_ident(t0)}, 'UTC')
ORDER BY minute"""
    nv_text, nv_el = ch.query(nv_sql)
    total_rows += int(ch.LAST_SUMMARY.get("read_rows", 0) or 0)

    def parse(text):
        out = []
        for line in text.splitlines():
            if not line:
                continue
            m, c = line.split("\t")
            out.append([m, int(c)])
        return out

    fg, nv = parse(fg_text), parse(nv_text)

    # Peak is computed at MINUTE grain before any downsampling for display.
    # Bucketing first and taking the max of averages would understate it.
    def stats(pts):
        if not pts:
            return {"peak": 0, "peak_at": None, "avg": 0.0}
        pk = max(pts, key=lambda p: p[1])
        return {"peak": pk[1], "peak_at": pk[0],
                "avg": round(sum(p[1] for p in pts) / len(pts), 2)}

    fg_s, nv_s = stats(fg), stats(nv)

    # Downsample for the SVG, keeping the MAX in each bucket so the peak
    # survives. ~1400 points is plenty for a 1200px chart.
    def bucket(pts, target=1400):
        if len(pts) <= target:
            return pts
        step = len(pts) / target
        out = []
        i = 0.0
        while int(i) < len(pts):
            chunk = pts[int(i):int(i + step) or int(i) + 1]
            if chunk:
                out.append(max(chunk, key=lambda p: p[1]))
            i += step
        return out

    over = nv_s["peak"] - fg_s["peak"]
    return {
        "from": t0, "to": t1,
        "foreground": bucket(fg), "naive": bucket(nv),
        "minutes": len(fg),
        "fg": fg_s, "nv": nv_s,
        "overcount": over,
        "overcount_pct": round(over / nv_s["peak"] * 100, 1) if nv_s["peak"] else 0.0,
        "latency_ms": round((time.time() - t_start) * 1000, 1),
        "rows_read": total_rows,
        "endpoint": ch.config()["host"],
    }


def overview():
    open_iv = int(ch.scalar("SELECT countIf(is_open) FROM sony.session_active_intervals FINAL"))
    return {
        "endpoint": ch.config()["host"],
        "server": ch.scalar("SELECT version()"),
        "events": int(ch.scalar("SELECT count() FROM sony.raw_events")),
        "sessions": int(ch.scalar("SELECT uniqExact(video_session_id) FROM sony.raw_events")),
        "intervals": int(ch.scalar("SELECT count() FROM sony.session_active_intervals FINAL")),
        "open_intervals": open_iv,
        "delta_rows": int(ch.scalar("SELECT count() FROM sony.concurrency_minute_delta")),
        "checkpoints": int(ch.scalar("SELECT count() FROM sony.concurrency_hourly_checkpoint")),
        "grid_rows_avoided": int(ch.scalar(
            "SELECT sum(intDiv(active_end_ms,60000)-intDiv(active_start_ms,60000)+1) "
            "FROM sony.session_active_intervals FINAL")),
        "bounds": bounds(),
        "filters": filters(),
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass  # keep the console clean during a live demo

    def _send(self, code, body, ctype="application/json"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        args = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
        try:
            if u.path in ("/", "/index.html"):
                with open(os.path.join(WEB, "index.html"), encoding="utf-8") as fh:
                    return self._send(200, fh.read(), "text/html; charset=utf-8")
            if u.path == "/api/overview":
                return self._send(200, overview())
            if u.path == "/api/series":
                return self._send(200, series(args))
            self._send(404, {"error": "not found"})
        except Exception as e:
            self._send(500, {"error": str(e)[:800]})


def main():
    port = int(os.environ.get("PORT", "877"))
    if not ch.ping():
        sys.exit("no ClickHouse connection; check .env")
    print(f"\n  concurrency dashboard  ->  http://localhost:{port}")
    print(f"  querying               ->  {ch.config()['host']}\n")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
