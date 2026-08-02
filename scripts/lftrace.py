"""Minimal Langfuse ingestion client for the MCP tool calls.

Stdlib only, same reasoning as otel.py: the observability layer must not be
able to break the thing it observes, and a pip dependency tree is a bigger
liability than 80 lines of documented wire format. Langfuse's public
ingestion API takes batched events with basic auth (public:secret key).

Every chat tool call becomes one trace with one span: the tool name, its
arguments, the JSON it returned, and how long ClickHouse took. That is the
submission evidence that a chat answer corresponds to a real query -- the
trace is exportable as JSON straight from the API, no login needed.

Silently disabled unless LANGFUSE_HOST / LANGFUSE_PUBLIC_KEY /
LANGFUSE_SECRET_KEY are set.
"""
import base64
import json
import os
import random
import threading
import time
import urllib.request

_ID = "%032x"


def _cfg():
    host = os.environ.get("LANGFUSE_HOST", "").rstrip("/")
    pk = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
    sk = os.environ.get("LANGFUSE_SECRET_KEY", "")
    return host, pk, sk


def enabled():
    host, pk, sk = _cfg()
    return bool(host and pk and sk)


def _iso(ts=None):
    t = ts if ts is not None else time.time()
    ms = int((t % 1) * 1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{ms:03d}Z"


def _post(events):
    host, pk, sk = _cfg()
    auth = base64.b64encode(f"{pk}:{sk}".encode()).decode()
    body = json.dumps({"batch": events}).encode()
    req = urllib.request.Request(
        f"{host}/api/public/ingestion", data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": "Basic " + auth})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
    except Exception:
        pass  # counted nowhere on purpose: never let tracing fail a tool call


def tool_call(name, arguments, output, latency_ms, error=None):
    """Record one MCP tool invocation. Fire-and-forget on a thread."""
    if not enabled():
        return
    t0 = time.time()
    trace_id = _ID % random.getrandbits(128)
    events = [
        {"id": _ID % random.getrandbits(128), "type": "trace-create",
         "timestamp": _iso(t0),
         "body": {"id": trace_id, "name": f"mcp.{name}",
                  "input": arguments, "output": output,
                  "timestamp": _iso(t0 - latency_ms / 1000),
                  # One session per UTC hour: a demo run clusters into a
                  # single session view instead of 30 disconnected traces.
                  "sessionId": "mcp-" + time.strftime("%Y-%m-%d-%H", time.gmtime(t0)),
                  "userId": "librechat",
                  "tags": ["mcp", "judged", "clickhouse"],
                  "metadata": {"transport": "streamable-http",
                               "server": "sonyliv-concurrency"}}},
        # Scores make the verification claim queryable: 1 when the tool
        # answered from an oracle-verified query, 0 when the query failed.
        {"id": _ID % random.getrandbits(128), "type": "score-create",
         "timestamp": _iso(t0),
         "body": {"traceId": trace_id, "name": "oracle_verified",
                  "value": 0 if error else 1}},
        {"id": _ID % random.getrandbits(128), "type": "score-create",
         "timestamp": _iso(t0),
         "body": {"traceId": trace_id, "name": "clickhouse_latency_ms",
                  "value": latency_ms}},
        {"id": _ID % random.getrandbits(128), "type": "span-create",
         "timestamp": _iso(t0),
         "body": {"traceId": trace_id, "name": "clickhouse.query",
                  "startTime": _iso(t0 - latency_ms / 1000),
                  "endTime": _iso(t0),
                  "input": arguments, "output": output,
                  "level": "ERROR" if error else "DEFAULT",
                  "statusMessage": error or "",
                  "metadata": {"latency_ms": latency_ms}}},
    ]
    threading.Thread(target=_post, args=(events,), daemon=True).start()
