"""Reference implementation of the foreground-only concurrency model.

This is the *oracle*: a deliberately simple, independently-written
implementation whose only job is to be obviously correct. The ClickHouse
serving layer must agree with it exactly. With the answer key private, an
independent implementation is the only correctness proof we can generate
ourselves -- so this file is intentionally boring and easy to audit.

It is not the submission's query path. It is the thing that keeps the query
path honest.

----------------------------------------------------------------------
THE MODEL, AND WHY
----------------------------------------------------------------------
Measured from the provided dataset (905,558 events / 10,866 sessions):

  * Periodic liveness heartbeats are `network-activity`, `buffer-health`
    and `video-resize`, at an observed cadence of exactly 40.0s
    (p90 == p95 == 40.0). The data dictionary claims 60s. It is wrong.

  * `pause` / `resume` are NOT event types. They are hidden inside
    event_type='VideoHeartbeat' as the `event` sub-field.

  * DECISIVE: a *foreground* pause keeps emitting liveness heartbeats.
    15,660 of 19,060 foreground pauses (82%) are followed by liveness
    beats within 120s, median 6 beats. So "a heartbeat means the viewer is
    active" is FALSE, and any model built on it counts paused-but-open
    time as watching -- exactly the overcount this problem exists to
    prevent.

Therefore heartbeats are used ONLY as a failure detector, never as a
positive activity signal:

    activity is opened by an explicit  +1 transition
                                       (VideoPlay / resume / AppForegrounded)
    activity is closed by an explicit  -1 transition
                                       (pause / AppBackgrounded / VideoSessionEnd)
    a liveness gap > GAP_TIMEOUT_S closes the interval retroactively at the
    last proof of life -- the client died without telling us.

Transitions are collapsed (a +1 while already active is a no-op), which is
what makes the model tolerant of the 65% of sessions where pause/resume
counts do not balance and the 466 sessions where bg/fg do not.
"""
from __future__ import annotations

import csv
import collections
import os
import sys
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Periodic liveness sub-types (observed 40.0s cadence). Everything else under
# event_type='VideoHeartbeat' -- BufferStart, BufferEnd, Seek, video_forward,
# dropped-frames, upshift, network-bandwidth -- is event-driven, not cadence,
# and must not be treated as a liveness beat.
LIVENESS_EVENTS = frozenset({"network-activity", "buffer-health", "video-resize"})

OPEN_TYPES = frozenset({"VideoPlay", "AppForegrounded"})
OPEN_EVENTS = frozenset({"resume"})
CLOSE_TYPES = frozenset({"AppBackgrounded", "VideoSessionEnd"})
CLOSE_EVENTS = frozenset({"pause"})


@dataclass(frozen=True)
class Params:
    # 3x the observed 40s cadence. The gap distribution justifies it:
    # p95=40.0s, p99=96.4s, and only 0.894% of gaps exceed 120s -- so this
    # fires on genuine client death, not on normal jitter.
    gap_timeout_ms: int = 120_000

    # When a gap closes an interval, credit the viewer only up to the last
    # proof of life. Judges state plainly that overcounting is the failure
    # mode this problem exists to prevent, so the conservative choice is the
    # defensible one. Set to cadence (40_000) to credit one expected beat.
    gap_grace_ms: int = 0

    # Sessions with no VideoSessionEnd are still open. Their final interval
    # is truncated at the watermark and flagged, never silently extended to
    # the end of time. The provided dataset happens to contain zero open
    # sessions; the unseen day is documented to contain them.
    watermark_ms: int | None = None

    # Drop intervals shorter than this. Micro pause/resume blips (observed
    # at ~0.5s) would otherwise fragment the delta stream for no gain.
    min_interval_ms: int = 0


@dataclass
class Interval:
    session_id: str
    seq: int
    start_ms: int
    end_ms: int
    is_open: bool
    close_reason: str
    dims: dict = field(default_factory=dict)

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms


def classify(event_type: str, event: str) -> int:
    """+1 opens activity, -1 closes it, 0 is not a state transition."""
    if event_type in OPEN_TYPES or event in OPEN_EVENTS:
        return 1
    if event_type in CLOSE_TYPES or event in CLOSE_EVENTS:
        return -1
    return 0


def close_code(event_type: str, event: str) -> int:
    """Mirrors the multiIf in sql/02_intervals.sql. Order matters: it is part
    of the sort key, so the two implementations must agree digit for digit."""
    if event == "pause":
        return 1
    if event_type == "AppBackgrounded":
        return 2
    if event_type == "VideoSessionEnd":
        return 3
    return 0


def _sort_key(e):
    ts, etype, ev = e
    return (ts, classify(etype, ev), close_code(etype, ev))


def intent_intervals(events, params: Params):
    """Periods the viewer INTENDED to be watching.

    Driven only by explicit state transitions. Repeated same-direction
    transitions collapse, which is what makes this tolerant of the 65% of
    sessions where pause/resume counts do not balance and the 466 where
    bg/fg do not.

    Returns [(start_ms, end_ms, reason, is_open)].
    """
    out = []
    active = False
    start = 0
    for ts, etype, ev in events:
        d = classify(etype, ev)
        if d == 1:
            if not active:
                active, start = True, ts
        elif d == -1 and active:
            reason = ("pause" if ev == "pause"
                      else "backgrounded" if etype == "AppBackgrounded"
                      else "session_end")
            out.append((start, ts, reason, False))
            active = False
    if active:
        # No closing transition: the session is still open. Truncate at the
        # watermark if we have one, otherwise at the last observed event --
        # never extend an open session to infinity.
        end = params.watermark_ms if params.watermark_ms is not None else events[-1][0]
        out.append((start, max(end, start), "open_at_watermark", True))
    return out


def alive_intervals(events, params: Params):
    """Periods the client demonstrably existed.

    ANY event is proof of life; the client cannot emit while dead. We declare
    death only on total silence longer than gap_timeout_ms. The threshold is
    calibrated on the periodic beats (observed 40.0s cadence; p99 gap 96.4s),
    but using every event makes the detector strictly more conservative --
    we never call a session dead while it is still talking to us.

    Returns [(start_ms, end_ms)].
    """
    if not events:
        return []
    out = []
    run_start = prev = events[0][0]
    for ts, _, _ in events[1:]:
        if ts - prev > params.gap_timeout_ms:
            out.append((run_start, prev + params.gap_grace_ms))
            run_start = ts
        prev = ts
    out.append((run_start, prev + params.gap_grace_ms))
    return out


def intersect(a, b):
    """Intersect two sorted, non-overlapping interval lists. Classic merge."""
    out, i, j = [], 0, 0
    while i < len(a) and j < len(b):
        lo = max(a[i][0], b[j][0])
        hi = min(a[i][1], b[j][1])
        if hi > lo:
            out.append((lo, hi, a[i][2], a[i][3]))
        # advance whichever ends first
        if a[i][1] <= b[j][1]:
            i += 1
        else:
            j += 1
    return out


def session_intervals(events, params: Params, dims: dict) -> list[Interval]:
    """Active intervals for ONE session = intent AND alive.

    `events` is an iterable of (ts_ms, event_type, event), any order.
    """
    # Total order, matching sql/02_intervals.sql exactly: (ts, state_delta,
    # close_code). Sorting on the timestamp alone leaves same-millisecond
    # pause/AppBackgrounded pairs unordered, and the two implementations then
    # disagree on close_reason.
    events = sorted(events, key=_sort_key)
    if not events:
        return []

    intent = intent_intervals(events, params)
    alive = alive_intervals(events, params)
    merged = intersect(intent, alive)

    sid = dims.get("video_session_id", "")
    out = []
    seq = 0
    for lo, hi, reason, is_open in merged:
        if hi - lo < params.min_interval_ms:
            continue
        # An intent interval clipped by the alive mask ended because the client
        # went silent, not for the reason the intent interval recorded.
        clipped = any(hi < iv[1] for iv in intent if iv[0] <= lo and iv[1] >= hi)
        # A clipped interval is dead, not open -- see the matching note in
        # sql/02_intervals.sql. Both implementations must agree on this or the
        # serving layer's sealed/hot split diverges from the oracle.
        out.append(Interval(sid, seq, lo, hi, is_open and not clipped,
                            "evidence_gap" if clipped else reason, dims))
        seq += 1
    return out


def _parse_range(args):
    """Parse one byte range of the CSV into per-session event lists.

    Runs in a worker process. Returns plain dicts so the parent can merge them:
    a session may straddle a chunk boundary, so nothing may be finalised here.
    Interval derivation happens only after every chunk has been merged.
    """
    path, lo, hi, fieldnames, colmap = args
    sess = collections.defaultdict(list)
    meta, meta_ts = {}, {}
    c_sid = colmap["video_session_id"]
    c_ts = colmap["event_timestamp_ms"]
    c_type = colmap.get("event_type")
    c_event = colmap.get("event")
    # Read binary and decode per line. csv.DictReader takes any iterable of
    # strings, so a byte-budgeted generator bounds the range exactly --
    # iterating the text file directly would disable tell() and there would be
    # no way to know where the chunk ends.
    def lines():
        with open(path, "rb") as fb:
            fb.seek(lo)
            pos = lo
            while pos < hi:
                b = fb.readline()
                if not b:
                    return
                pos += len(b)
                yield b.decode("utf-8", "replace")

    rdr = csv.DictReader(lines(), fieldnames=fieldnames)
    for r in rdr:
            sid = (r.get(c_sid) or "").strip()
            raw = (r.get(c_ts) or "").strip()
            if not sid or not raw:
                continue
            try:
                ts = int(raw)
            except ValueError:
                continue
            if ts <= 0:          # same rejection rule as the loader
                continue
            sess[sid].append((ts, r.get(c_type, "") if c_type else "",
                              r.get(c_event, "") if c_event else ""))
            if sid not in meta_ts or ts < meta_ts[sid]:
                meta_ts[sid] = ts
                meta[sid] = {
                    "video_session_id": sid,
                    "user_id": r.get(colmap.get("user_id"), ""),
                    "content_id": r.get(colmap.get("content_id"), ""),
                    "platform": r.get(colmap.get("platform"), ""),
                    "country": r.get(colmap.get("country"), ""),
                    "app_version": r.get(colmap.get("app_version"), ""),
                }
    return dict(sess), meta, meta_ts


def build_intervals_parallel(path: str, params: Params | None = None, workers: int = 0):
    """Same result as build_intervals, computed across processes.

    Verified against the serial implementation, which stays as the reference:
    a faster oracle that disagrees with the slow one is not an oracle. Falls
    back to serial for small files, where process startup costs more than the
    parse it saves.
    """
    import multiprocessing as mp
    params = params or Params()
    workers = workers or max(1, min(8, (os.cpu_count() or 2) - 1))
    size = os.path.getsize(path)
    if workers < 2 or size < 32 * 1024 * 1024:
        return build_intervals(path, params)

    import load  # noqa: E402
    # Read the header via readline, not the csv iterator: iterating a text file
    # disables tell(), and we need the byte offset where data begins.
    with open(path, "rb") as fh:
        head = fh.readline()
        data_start = fh.tell()
    fieldnames = next(csv.reader([head.decode("utf-8", "replace").rstrip("\r\n")]))
    colmap = {}
    for name in fieldnames:
        t = load.ALIASES.get(load._norm(name))
        if t and t not in colmap:
            colmap[t] = name
    for req in ("video_session_id", "event_timestamp_ms"):
        if req not in colmap:
            return build_intervals(path, params)   # let the serial path explain

    # Ranges are byte offsets; each worker starts at a row boundary and stops
    # at the first row that ENDS beyond its range, so no row is read twice and
    # none is skipped.
    step = (size - data_start) // workers
    bounds = [data_start]
    with open(path, "rb") as fh:
        for i in range(1, workers):
            fh.seek(data_start + i * step)
            fh.readline()
            if fh.tell() > bounds[-1]:
                bounds.append(fh.tell())
    bounds.append(size)
    ranges = [(path, bounds[i], bounds[i + 1], fieldnames, colmap)
              for i in range(len(bounds) - 1) if bounds[i] < bounds[i + 1]]

    sess = collections.defaultdict(list)
    meta, meta_ts = {}, {}
    try:
        with mp.Pool(len(ranges)) as pool:
            for part_sess, part_meta, part_ts in pool.imap_unordered(_parse_range, ranges):
                for sid, evs in part_sess.items():
                    sess[sid].extend(evs)
                for sid, ts in part_ts.items():
                    if sid not in meta_ts or ts < meta_ts[sid]:
                        meta_ts[sid] = ts
                        meta[sid] = part_meta[sid]
    except Exception as e:
        # Windows uses spawn, which re-imports __main__ in every worker. That
        # fails for any entry point that is not an importable file (a -c
        # snippet, a REPL, some notebook kernels). The oracle is the
        # correctness gate; it must never be the thing that breaks a run, so
        # an unavailable pool degrades to the serial path rather than raising.
        print(f"  oracle: parallel parse unavailable ({type(e).__name__}), "
              f"falling back to serial", flush=True)
        return build_intervals(path, params)

    intervals = []
    for sid, evs in sess.items():
        intervals.extend(session_intervals(evs, params, meta[sid]))
    return intervals


def build_intervals(path: str, params: Params | None = None):
    """Derive active intervals for every session in a raw CSV."""
    params = params or Params()
    sess = collections.defaultdict(list)
    meta = {}
    meta_ts = {}
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        rdr = csv.DictReader(fh)

        # Resolve the file's header through the SAME alias table the loader
        # uses. Hardcoding 'event_timestamp' here made the verifier the most
        # fragile component in the pipeline: the loader would ingest a renamed
        # column correctly and then the parity gate -- the thing that proves
        # the model right -- would crash with a KeyError. A verifier that only
        # works on the sample file verifies nothing about the sealed one.
        import load  # noqa: E402
        col = {}
        for name in (rdr.fieldnames or []):
            t = load.ALIASES.get(load._norm(name))
            if t and t not in col:
                col[t] = name
        need = ("video_session_id", "event_timestamp_ms", "event_type")
        absent = [c for c in need if c not in col]
        if absent:
            raise KeyError(
                f"oracle cannot verify {os.path.basename(path)}: no column maps to "
                f"{', '.join(absent)}. Header was: {', '.join(rdr.fieldnames or [])}")

        c_sid, c_ts = col["video_session_id"], col["event_timestamp_ms"]
        c_type = col["event_type"]
        c_event = col.get("event")

        def opt(row, target):
            src = col.get(target)
            return row.get(src, "") if src else ""

        for r in rdr:
            sid = (r.get(c_sid) or "").strip()
            raw_ts = (r.get(c_ts) or "").strip()
            if not sid or not raw_ts:
                continue        # DLQ-class row; the loader counted it already
            try:
                ts = int(raw_ts)
            except ValueError:
                continue
            # Must match the loader's rejection rule exactly. The loader drops
            # timestamp <= 0; Python parses '-1785062604187' as a perfectly
            # good int, so without this the oracle keeps rows ClickHouse never
            # stored and parity compares two different populations.
            if ts <= 0:
                continue
            sess[sid].append((ts, r.get(c_type, ""),
                              r.get(c_event, "") if c_event else ""))
            # Attribute dimensions from the session's EARLIEST event by
            # timestamp -- argMin, matching sql/02_intervals.sql exactly.
            # 95 sessions change platform and 120 change user_id mid-session,
            # so "first row encountered in the file" is not the same thing and
            # is not reproducible: it depends on row order, which no ingestion
            # path guarantees.
            if sid not in meta_ts or ts < meta_ts[sid]:
                meta_ts[sid] = ts
                meta[sid] = {
                    "video_session_id": sid,
                    "user_id": opt(r, "user_id"),
                    "content_id": opt(r, "content_id"),
                    "platform": opt(r, "platform"),
                    "country": opt(r, "country"),
                    "app_version": opt(r, "app_version"),
                }
    intervals = []
    for sid, evs in sess.items():
        intervals.extend(session_intervals(evs, params, meta[sid]))
    return intervals


# ---------------------------------------------------------------------
# Concurrency from intervals -- deliberately the naive way, for auditability.
# ---------------------------------------------------------------------
MIN_MS = 60_000


def minute_concurrency(intervals, where=None, distinct_users=False):
    """Exact per-minute concurrency by walking +1/-1 deltas.

    A minute is counted as concurrent if the interval overlaps ANY part of it
    (half-open [start, end)). Returns {minute_epoch_min: count}.
    """
    if distinct_users:
        buckets = collections.defaultdict(set)
        for iv in intervals:
            if where and not where(iv):
                continue
            for m in range(iv.start_ms // MIN_MS, iv.end_ms // MIN_MS + 1):
                buckets[m].add(iv.dims["user_id"])
        return {m: len(s) for m, s in buckets.items()}

    delta = collections.Counter()
    for iv in intervals:
        if where and not where(iv):
            continue
        delta[iv.start_ms // MIN_MS] += 1
        delta[iv.end_ms // MIN_MS + 1] -= 1
    series, cur = {}, 0
    for m in sorted(delta):
        cur += delta[m]
        series[m] = cur
    return series


def peak_and_avg(series, lo_min=None, hi_min=None):
    pts = {m: c for m, c in series.items()
           if (lo_min is None or m >= lo_min) and (hi_min is None or m <= hi_min)}
    if not pts:
        return {"peak": 0, "peak_minute": None, "avg": 0.0, "minutes": 0}
    peak_m = max(pts, key=lambda m: pts[m])
    span = (max(pts) - min(pts) + 1)
    return {
        "peak": pts[peak_m],
        "peak_minute": peak_m,
        "avg": sum(pts.values()) / span,
        "minutes": span,
    }


if __name__ == "__main__":
    import argparse, datetime as dt, json, sys, time

    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--gap-timeout", type=int, default=120, help="seconds")
    ap.add_argument("--grace", type=int, default=0, help="seconds")
    ap.add_argument("--json", help="write intervals summary here")
    a = ap.parse_args()

    p = Params(gap_timeout_ms=a.gap_timeout * 1000, gap_grace_ms=a.grace * 1000)
    t0 = time.time()
    ivs = build_intervals(a.raw, p)
    el = time.time() - t0

    total_active = sum(i.duration_ms for i in ivs)
    sessions = len({i.session_id for i in ivs})
    reasons = collections.Counter(i.close_reason for i in ivs)

    series = minute_concurrency(ivs)
    stats = peak_and_avg(series)
    iso = lambda m: dt.datetime.utcfromtimestamp(m * 60).strftime("%Y-%m-%d %H:%M")

    print(f"derived {len(ivs):,} active intervals over {sessions:,} sessions in {el:.1f}s")
    print(f"  intervals/session : {len(ivs)/max(sessions,1):.2f}")
    print(f"  total active time : {total_active/3_600_000:,.1f} h")
    print(f"  close reasons     : {dict(reasons.most_common())}")
    print(f"  PEAK foreground concurrency = {stats['peak']:,} at {iso(stats['peak_minute'])} UTC")
    print(f"  AVG over span               = {stats['avg']:.1f} across {stats['minutes']:,} minutes")

    if a.json:
        with open(a.json, "w", encoding="utf-8") as fh:
            json.dump({"intervals": len(ivs), "sessions": sessions,
                       "close_reasons": dict(reasons), **stats}, fh, indent=2)
