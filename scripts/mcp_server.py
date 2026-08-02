"""MCP server exposing the concurrency model as typed tools.

Used by LibreChat to answer questions like "what was peak concurrency on
Android during the live window, and how much would a naive model have
over-reported?"

WHY NOT THE GENERIC ClickHouse MCP SERVER
Handing an LLM raw SQL over these tables invites it to invent its own
concurrency query -- and the entire point of this submission is that the
obvious query is wrong. A model that writes

    SELECT count() FROM raw_events WHERE ... GROUP BY minute

produces a confident, plausible, incorrect number. Worse, it would silently
reintroduce the naive session-overlap error we spent the project eliminating.

So the agent gets typed tools, not a SQL prompt. Every tool runs a query that
has already been verified against the independent Python oracle. The LLM
chooses *which* question to ask and narrates the answer; it never decides how
concurrency is computed. Deterministic code computes, the model explains.

Run standalone for a smoke test:
    python scripts/mcp_server.py --selftest
"""
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ch          # noqa: E402
import dashboard   # noqa: E402  (reuses the exact queries the dashboard uses)
import lftrace     # noqa: E402  (chat tool calls -> Langfuse; no-op if unset)

from mcp.server import Server                      # noqa: E402
from mcp.server.stdio import stdio_server          # noqa: E402
from mcp.types import TextContent, Tool            # noqa: E402

server = Server("sonyliv-concurrency")


def _fmt(d):
    return TextContent(type="text", text=json.dumps(d, indent=2, default=str))


@server.list_tools()
async def list_tools():
    dims = ("Optional filters. platform e.g. ANDROID_PHONE / IPHONE / "
            "SONY_ANDROID_TV; country e.g. india; video_type 'live' or 'vod'. "
            "Omit or pass 'all' for no filter.")
    return [
        Tool(
            name="concurrency",
            description=(
                "Foreground-only concurrent viewers for a time range: peak, the minute "
                "the peak occurred, and the average. Counts ONLY genuinely active "
                "playback -- excludes paused, backgrounded, and clients that went "
                "silent. This is the correct number. " + dims),
            inputSchema={
                "type": "object",
                "properties": {
                    "from": {"type": "string", "description": "UTC 'YYYY-MM-DD HH:MM:SS'; omit for earliest"},
                    "to": {"type": "string", "description": "UTC 'YYYY-MM-DD HH:MM:SS'; omit for latest"},
                    "platform": {"type": "string"},
                    "country": {"type": "string"},
                    "video_type": {"type": "string"},
                },
            },
        ),
        Tool(
            name="compare_to_naive",
            description=(
                "Compare foreground-only concurrency against the naive model that counts "
                "a session as watching from its first to its last event. Returns both "
                "peaks and how much audience the naive model over-reports. Use this when "
                "asked how much difference the model makes, or about over-counting. " + dims),
            inputSchema={
                "type": "object",
                "properties": {
                    "from": {"type": "string"}, "to": {"type": "string"},
                    "platform": {"type": "string"}, "country": {"type": "string"},
                    "video_type": {"type": "string"},
                },
            },
        ),
        Tool(
            name="dimensions",
            description="List the filter values available (platforms, countries, video types) "
                        "and the time range the data covers. Call this first if unsure.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="model_explain",
            description="Explain how 'active' is defined and the measured evidence behind it. "
                        "Use when asked why a number differs from a naive count, or how "
                        "paused / backgrounded / silent sessions are handled.",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@server.call_tool()
async def call_tool(name, arguments):
    args = {k: v for k, v in (arguments or {}).items() if v not in (None, "", "all")}
    t0 = time.time()
    try:
        out = await _dispatch(name, args)
        lftrace.tool_call(name, args, out[0].text[:4000],
                          round((time.time() - t0) * 1000, 1))
        return out
    except Exception as e:
        lftrace.tool_call(name, args, None,
                          round((time.time() - t0) * 1000, 1), error=str(e)[:300])
        # Surface failures to the model as data, so it reports "I could not get
        # that" instead of inventing a plausible number.
        return [_fmt({"error": str(e)[:500],
                      "note": "query failed; do not estimate a value"})]


async def _dispatch(name, args):
    try:
        if name == "dimensions":
            ov = dashboard.overview()
            return [_fmt({
                "time_range_utc": ov["bounds"],
                "platforms": ov["filters"]["platform"],
                "countries": ov["filters"]["country"],
                "video_types": ov["filters"]["video_type"],
                "events": ov["events"], "sessions": ov["sessions"],
            })]

        if name == "model_explain":
            return [_fmt({
                "definition": "active = intent_playing AND client_alive",
                "intent_playing": "toggled only by explicit transitions: VideoPlay / "
                                  "AppForegrounded / resume open it; pause / "
                                  "AppBackgrounded / VideoSessionEnd close it",
                "client_alive": "false during total event silence longer than 120s "
                                "(3x the measured 40s heartbeat cadence)",
                "why_heartbeats_are_not_activity":
                    "A foreground pause keeps emitting heartbeats -- measured at 15,660 "
                    "of 19,060 foreground pauses (82%), median 6 beats within 120s. So a "
                    "heartbeat proves the client exists, not that anyone is watching.",
                "measured_facts": {
                    "heartbeat_cadence_s": 40.0,
                    "data_dictionary_claims_s": 60,
                    "pause_resume_unbalanced_sessions_pct": 65,
                    "gap_threshold_s": 120,
                    "gaps_exceeding_threshold_pct": 0.894,
                },
                "verification": "Every interval is cross-checked against an independently "
                                "written Python oracle; the two agree exactly.",
            })]

        if name in ("concurrency", "compare_to_naive"):
            d = dashboard.series(args)
            base = {
                "range_utc": [d["from"], d["to"]],
                "filters": args or "none",
                "peak_concurrent_viewers": d["fg"]["peak"],
                "peak_at_utc": d["fg"]["peak_at"],
                "average_concurrent_viewers": d["fg"]["avg"],
                "minutes_covered": d["minutes"],
                "computed_on": d["endpoint"],
                "query_latency_ms": d["latency_ms"],
            }
            if name == "compare_to_naive":
                base.update({
                    "naive_peak": d["nv"]["peak"],
                    "foreground_only_peak": d["fg"]["peak"],
                    "over_reported_viewers": d["overcount"],
                    "over_reported_pct": d["overcount_pct"],
                    "interpretation":
                        f"A naive session-overlap model would report {d['nv']['peak']:,} "
                        f"concurrent viewers at peak. Only {d['fg']['peak']:,} were actually "
                        f"watching -- {d['overcount']:,} ({d['overcount_pct']}%) were paused, "
                        f"backgrounded, or had gone silent.",
                })
            return [_fmt(base)]

        return [_fmt({"error": f"unknown tool {name}"})]
    except Exception as e:
        # Surface failures to the model as data, so it reports "I could not get
        # that" instead of inventing a plausible number.
        return [_fmt({"error": str(e)[:500],
                      "note": "query failed; do not estimate a value"})]


async def _main():
    async with stdio_server() as (r, w):
        await server.run(r, w, server.create_initialization_options())


def _serve_http(host, port):
    """Serve the same tools over streamable HTTP.

    stdio is the right transport when the client can spawn the process.
    LibreChat cannot: it runs in a Linux container and this is a Windows
    process, so there is no process for it to spawn. Streamable HTTP crosses
    that boundary without changing a single tool definition -- the transport
    is the only thing that differs.

    Bound to 0.0.0.0 so the container can reach it over the WSL bridge, and
    stateless so a container restart does not orphan a session.
    """
    import contextlib
    import uvicorn
    from starlette.applications import Starlette
    from starlette.routing import Mount
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

    manager = StreamableHTTPSessionManager(app=server, json_response=False,
                                           stateless=True)

    async def handle(scope, receive, send):
        await manager.handle_request(scope, receive, send)

    async def health(_request):
        return JSONResponse({"ok": True, "server": "sonyliv-concurrency",
                             "clickhouse": ch.config()["host"]})

    @contextlib.asynccontextmanager
    async def lifespan(_app):
        async with manager.run():
            print(f"  MCP streamable-http on http://{host}:{port}/mcp", flush=True)
            yield

    app = Starlette(debug=False, lifespan=lifespan, routes=[
        Route("/health", health),
        Mount("/mcp", app=handle),
    ])
    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        if not ch.ping():
            sys.exit(1)
        print("\ndimensions:")
        print(asyncio.run(call_tool("dimensions", {}))[0].text[:400])
        print("\ncompare_to_naive  platform=ANDROID_PHONE video_type=live:")
        print(asyncio.run(call_tool("compare_to_naive",
              {"platform": "ANDROID_PHONE", "video_type": "live"}))[0].text)
    elif "--http" in sys.argv:
        port = 8765
        if "--port" in sys.argv:
            port = int(sys.argv[sys.argv.index("--port") + 1])
        _serve_http("0.0.0.0", port)
    else:
        asyncio.run(_main())
