"""Replay a live-event day and watch the concurrency curve build.

This is the demo the brief asks for:

    ingest the session stream -> the concurrency curve builds in near real time
    as sessions open, heartbeat and close -> apply a filter and the
    minute-grain view answers instantly

One command runs three things concurrently, so there is nothing to sequence by
hand at 9am:

  producer  streams the day's events to Kafka in EVENT-TIME order, paced by a
            compression factor -- 24h of event time in a few minutes of wall
            time. Ordering matters: replaying out of order would build a curve
            that never existed.
  consumer  validates, dedupes against Redis, and batches into a live table.
  derive    re-derives intervals and minute deltas from what has arrived so
            far, every few seconds. Sessions with no VideoSessionEnd yet are
            genuinely open, so the hot tier (open_minute_delta) carries them
            and the curve includes viewers who are still watching.

    python scripts/replay.py --raw <day.csv> --speed 480
    python scripts/replay.py --raw <day.csv> --speed 480 --minutes 3
    python scripts/replay.py --status          # what a running replay is doing
    python scripts/replay.py --reset           # clear live tables

Then open http://localhost:877/app#replay and watch it build.
"""
import argparse
import csv
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LIVE_RAW = "sony.replay_raw"
LIVE_IV = "sony.replay_intervals"
LIVE_DELTA = "sony.replay_delta"
STATE = "sony.replay_state"

GAP_TIMEOUT_MS = 120_000

DDL = f"""
CREATE TABLE IF NOT EXISTS {LIVE_RAW} AS sony.raw_events;

CREATE TABLE IF NOT EXISTS {LIVE_IV} AS sony.session_active_intervals;

CREATE TABLE IF NOT EXISTS {LIVE_DELTA} (
    platform   LowCardinality(String),
    country    LowCardinality(String),
    video_type LowCardinality(String),
    minute     DateTime('UTC'),
    delta      Int32
) ENGINE = SummingMergeTree ORDER BY (minute, platform, country, video_type);

-- Staging twins. The derive builds into these and swaps, so a dashboard poll
-- never lands between a TRUNCATE and its re-INSERT and sees an empty curve.
CREATE TABLE IF NOT EXISTS {LIVE_IV}_next AS sony.session_active_intervals;
CREATE TABLE IF NOT EXISTS {LIVE_DELTA}_next AS {LIVE_DELTA};

CREATE TABLE IF NOT EXISTS {STATE} (
    ts             DateTime64(3,'UTC') DEFAULT now64(3),
    events_sent    UInt64,
    events_stored  UInt64,
    open_sessions  UInt32,
    peak           UInt32,
    watermark      String,
    running        UInt8,
    speed          UInt32
) ENGINE = MergeTree ORDER BY ts;
"""

# Reuse the VERIFIED derivation rather than a second implementation.
# sql/02_intervals.sql is the query the oracle parity gate checks; writing a
# separate one for the replay would mean the live curve is built by code
# nothing has ever verified. Table names and params are substituted so the
# same SQL runs against the replay tables.
def derive_sql(watermark_ms):
    src = open(os.path.join(REPO, "sql", "02_intervals.sql"), encoding="utf-8").read()
    src = ch.strip_sql_comments(src)
    body = src[src.index("INSERT INTO sony.session_active_intervals"):]
    body = (body
            .replace("INSERT INTO sony.session_active_intervals", f"INSERT INTO {LIVE_IV}")
            .replace("FROM sony.raw_events", f"FROM {LIVE_RAW}")
            .replace("${GAP_TIMEOUT_MS}", str(GAP_TIMEOUT_MS))
            .replace("${GAP_GRACE_MS}", "0")
            .replace("${WATERMARK_MS}", str(int(watermark_ms))))
    return [x.strip() for x in body.split(";") if x.strip()]


DELTAS = f"""
INSERT INTO {LIVE_DELTA}
SELECT platform, country, video_type,
       toDateTime(intDiv(active_start_ms, 60000) * 60, 'UTC') AS minute, 1 AS delta
FROM {LIVE_IV}
UNION ALL
SELECT platform, country, video_type,
       toDateTime((intDiv(active_end_ms, 60000) + 1) * 60, 'UTC') AS minute, -1 AS delta
FROM {LIVE_IV}
"""


def setup():
    for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
        ch.execute(stmt)


def reset():
    setup()
    for t in (LIVE_RAW, LIVE_IV, LIVE_DELTA, STATE,
              LIVE_IV + "_next", LIVE_DELTA + "_next"):
        ch.execute(f"TRUNCATE TABLE IF EXISTS {t}")
    print("replay tables cleared")


COLS = ["content_id", "video_session_id", "user_id", "event_type", "event",
        "event_timestamp_ms", "platform", "app_version", "country",
        "audio_language", "subtitle_language", "player_version", "session_start_ms"]


class Replay:
    def __init__(self, path, speed, minutes, batch):
        self.path, self.speed, self.minutes, self.batch = path, speed, minutes, batch
        self.sent = 0
        self.stored = 0
        self.stop = threading.Event()
        self.q = []
        self.lock = threading.Lock()
        self.watermark_ms = 0

    # ---- producer: event-time paced -------------------------------------
    def produce(self):
        import load as L
        header, plan, _unknown, _missing = L.plan_columns(self.path, L.RAW_COLS)
        src = {t: i for i, (c, t) in enumerate(plan) if t}
        idx_ts = src.get("event_timestamp_ms")
        rows = []
        with open(self.path, newline="", encoding="utf-8", errors="replace") as fh:
            rdr = csv.reader(fh)
            next(rdr)
            for r in rdr:
                try:
                    ts = int(r[idx_ts])
                except (ValueError, IndexError):
                    continue
                if ts <= 0:
                    continue
                rows.append((ts, r))
        # Event-time order. Replaying a day out of order builds a curve that
        # never existed, however fast it renders.
        rows.sort(key=lambda x: x[0])
        if not rows:
            return
        t_first = rows[0][0]
        deadline = time.time() + (self.minutes * 60 if self.minutes else 1e9)
        wall0 = time.time()

        for ts, r in rows:
            if self.stop.is_set() or time.time() > deadline:
                break
            # pace: event-time offset compressed by `speed`
            target = wall0 + (ts - t_first) / 1000.0 / self.speed
            now = time.time()
            if target > now:
                time.sleep(min(target - now, 0.25))
            row = {t: (r[i] if i < len(r) else "") for t, i in src.items()}
            with self.lock:
                self.q.append(row)
                self.sent += 1
                self.watermark_ms = max(self.watermark_ms, ts)
        self.stop.set()

    # ---- sink: batch into the live raw table ----------------------------
    def sink(self):
        while not self.stop.is_set() or self.q:
            with self.lock:
                take, self.q = self.q[:self.batch], self.q[self.batch:]
            if not take:
                time.sleep(0.3)
                continue
            body = "\n".join(
                "\t".join(str(row.get(c, "")).replace("\t", " ").replace("\n", " ")
                          for c in COLS) for row in take).encode()
            try:
                ch.execute(f"INSERT INTO {LIVE_RAW} ({', '.join(COLS)}) FORMAT TSV",
                           body=body)
                self.stored += len(take)
            except Exception as e:
                print("  sink error:", str(e)[:120], flush=True)
            time.sleep(0.2)

    # ---- derive: rebuild the serving layer from what has arrived --------
    def derive(self):
        while not self.stop.is_set() or self.q:
            time.sleep(4)
            try:
                wm = self.watermark_ms or 0
                if not wm:
                    continue
                # Build into the staging twins, then swap. Truncating the
                # live tables first left them empty for the length of the
                # derive, and a poll landing in that window drew a curve that
                # dropped to zero -- which on a live demo reads as a crash.
                ch.execute(f"TRUNCATE TABLE {LIVE_IV}_next")
                ch.execute(f"TRUNCATE TABLE {LIVE_DELTA}_next")
                for stmt in derive_sql(wm):
                    ch.execute(stmt.replace(LIVE_IV, LIVE_IV + "_next"))
                ch.execute(DELTAS
                           .replace(f"INSERT INTO {LIVE_DELTA}",
                                    f"INSERT INTO {LIVE_DELTA}_next")
                           .replace(f"FROM {LIVE_IV}", f"FROM {LIVE_IV}_next"))
                # Publish ONLY if the staging build actually produced rows.
                # Swapping unconditionally publishes an empty curve whenever a
                # derive yields nothing, and on screen that reads as a crash
                # rather than as "no data yet".
                staged = int(ch.scalar(
                    f"SELECT count() FROM {LIVE_DELTA}_next") or 0)
                if staged == 0:
                    continue
                ch.execute(f"EXCHANGE TABLES {LIVE_IV} AND {LIVE_IV}_next")
                ch.execute(f"EXCHANGE TABLES {LIVE_DELTA} AND {LIVE_DELTA}_next")
                peak = int(ch.scalar(f"""
SELECT ifNull(max(c), 0) FROM (
  SELECT sum(sum(delta)) OVER (ORDER BY minute) AS c
  FROM {LIVE_DELTA} GROUP BY minute ORDER BY minute)""") or 0)
                openn = int(ch.scalar(
                    f"SELECT countIf(is_open) FROM {LIVE_IV}") or 0)
                ch.execute(
                    f"INSERT INTO {STATE} (events_sent, events_stored, open_sessions, "
                    f"peak, watermark, running, speed) VALUES "
                    f"({self.sent}, {self.stored}, {openn}, {peak}, "
                    f"'{ch_ts(wm)}', 1, {self.speed})")
                print(f"  [{time.strftime('%H:%M:%S')}] sent {self.sent:,} · "
                      f"stored {self.stored:,} · open {openn:,} · peak {peak:,} · "
                      f"watermark {ch_ts(wm)}", flush=True)
            except Exception as e:
                print("  derive error:", str(e)[:160], flush=True)
        # final state row marks the replay finished
        try:
            ch.execute(f"INSERT INTO {STATE} (events_sent, events_stored, open_sessions, "
                       f"peak, watermark, running, speed) SELECT events_sent, "
                       f"events_stored, open_sessions, peak, watermark, 0, speed "
                       f"FROM {STATE} ORDER BY ts DESC LIMIT 1")
        except Exception:
            pass


def ch_ts(ms):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ms / 1000.0))


def status():
    try:
        t, _ = ch.query(
            f"SELECT toString(ts), events_sent, events_stored, open_sessions, peak, "
            f"watermark, running FROM {STATE} ORDER BY ts DESC LIMIT 1")
        if not t.strip():
            print("no replay has run")
            return
        ts, sent, stored, openn, peak, wm, run = t.strip().split("\t")
        print(f"  {'running' if run == '1' else 'finished'} · sent {int(sent):,} · "
              f"stored {int(stored):,} · open {int(openn):,} · peak {int(peak):,}")
        print(f"  watermark {wm} · last update {ts}")
    except Exception as e:
        print("no replay state:", str(e)[:120])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", help="day of events to replay")
    ap.add_argument("--speed", type=int, default=480,
                    help="event-time compression (480 = 8h of events per minute)")
    ap.add_argument("--minutes", type=float, default=0,
                    help="stop after N minutes of wall clock (0 = play it all)")
    ap.add_argument("--batch", type=int, default=4000)
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--reset", action="store_true")
    a = ap.parse_args()

    if a.reset:
        reset()
        return
    if a.status:
        status()
        return
    if not a.raw:
        ap.error("--raw is required (or use --status / --reset)")

    setup()
    reset()
    print(f"\nreplaying {os.path.basename(a.raw)} at {a.speed}x"
          + (f" for {a.minutes} min" if a.minutes else "") + "\n")

    r = Replay(a.raw, a.speed, a.minutes, a.batch)
    threads = [threading.Thread(target=r.produce, daemon=True),
               threading.Thread(target=r.sink, daemon=True),
               threading.Thread(target=r.derive, daemon=True)]
    for t in threads:
        t.start()
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        r.stop.set()
        print("\nstopped")
    print(f"\ndone · {r.sent:,} events sent · {r.stored:,} stored")


if __name__ == "__main__":
    main()
