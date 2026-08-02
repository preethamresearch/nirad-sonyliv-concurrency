"""Cloud sanity: every hosted surface, checked for LINKAGE, not just liveness.

An HTTP 200 proves a container is up. It does not prove the dashboard is
reading the judged dataset, that traces are arriving, or that the chat
tools return verified numbers -- each check here asserts the link, with the
expected value stated so a drift is a FAIL, not a shrug.

    python scripts/sanity_cloud.py
"""
import base64
import json
import os
import sys
import time
import urllib.request

APP = "https://watchhouse-1045532154243.asia-south1.run.app"
EDGE = "http://8.231.76.83"
OTLP_KEY = os.environ.get("HYPERDX_INGEST_KEY", "1e196294-783e-4f9e-a892-e09bf0871520")

results = []


def check(name, fn):
    t0 = time.time()
    try:
        detail, ok = fn(), True
    except Exception as e:
        detail, ok = str(e).split("\n")[0][:100], False
    ms = (time.time() - t0) * 1000
    results.append(ok)
    print(f"[{' ok ' if ok else 'FAIL'}] {name:<46} {str(detail):<60} {ms:6.0f}ms",
          flush=True)


def fetch(url, timeout=90, headers=None, data=None):
    req = urllib.request.Request(url, headers=headers or {}, data=data,
                                 method="POST" if data else "GET")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def jget(url, timeout=90, headers=None, data=None):
    return json.loads(fetch(url, timeout, headers, data)[1])


def main():
    print("\n=== Cloud Run: the product ===")
    for path in ["/", "/app", "/deck", "/classic"]:
        check(f"GET {path}", lambda p=path: f"HTTP {fetch(APP + p)[0]}")
    check("logo asset", lambda: f"HTTP {fetch(APP + '/shots/sonyliv.png')[0]}")
    check("mermaid vendored", lambda: f"HTTP {fetch(APP + '/vendor/mermaid.min.js')[0]}")

    print("\n=== Cloud Run -> ClickHouse Cloud (the judged numbers) ===")
    def series():
        d = jget(APP + "/api/series")
        fg, nv = d["fg"]["peak"], d["nv"]["peak"]
        assert fg == 18936, f"fg peak {fg} != 18,936"
        assert nv == 24087, f"nv peak {nv} != 24,087"
        return f"fg 18,936 · nv 24,087 · gap {d['overcount_pct']}%"
    check("judged peaks", series)

    def overview():
        d = jget(APP + "/api/overview")
        assert d["events"] == 7_000_000, d["events"]
        assert d["intervals"] == 149_500, d["intervals"]
        return f"{d['events']:,} events · {d['intervals']:,} intervals"
    check("judged dataset loaded", overview)

    def filtered():
        d = jget(APP + "/api/series?platform=ANDROID_PHONE")
        assert d["fg"]["peak"] == 6046, d["fg"]["peak"]
        return "ANDROID_PHONE peak 6,046 -- filter bites"
    check("filter applies to curve", filtered)

    for path in ["/api/breakdown?dim=platform", "/api/heatmap", "/api/config",
                 "/api/catalog", "/api/pipeline/status", "/api/live_ops"]:
        check(f"GET {path.split('?')[0]}",
              lambda p=path: "error-free JSON"
              if "error" not in jget(APP + p) else (_ for _ in ()).throw(
                  AssertionError(jget(APP + p)["error"])))

    print("\n=== HyperDX / ClickStack (hosted traces) ===")
    check("UI", lambda: f"HTTP {fetch(EDGE + ':8080/', 30)[0]}")

    def otlp_auth():
        try:
            fetch(EDGE + ":4318/v1/traces", 20,
                  {"Content-Type": "application/json"}, b'{"resourceSpans":[]}')
            raise AssertionError("unauthenticated ingest ACCEPTED -- key gone?")
        except urllib.error.HTTPError as e:
            assert e.code == 401, f"expected 401, got {e.code}"
            return "unauthenticated ingest refused (401)"
    check("OTLP requires key", otlp_auth)

    check("OTLP accepts our key", lambda: "HTTP %d" % fetch(
        EDGE + ":4318/v1/traces", 20,
        {"Content-Type": "application/json", "authorization": OTLP_KEY},
        b'{"resourceSpans":[]}')[0])

    print("\n=== MCP (chat tools -> ClickHouse Cloud) ===")
    # The architecture claim is that MCP is NOT internet-facing -- LibreChat
    # reaches it on the compose network. So the public check asserts
    # unreachability, and the round-trip runs from inside the VM over SSH.
    def mcp_not_public():
        import socket
        try:
            socket.create_connection((EDGE.split("//")[1], 8765), timeout=6).close()
            raise AssertionError("port 8765 is PUBLICLY reachable -- firewall drifted")
        except (socket.timeout, TimeoutError, ConnectionRefusedError, OSError):
            return "8765 unreachable from the internet, as designed"
    check("MCP not exposed publicly", mcp_not_public)

    def mcp_via_ssh():
        import subprocess
        key = os.path.expanduser("~/.ssh/google_compute_engine")
        if not os.path.exists(key):
            return "skipped -- no SSH key on this machine"
        body = ('{"jsonrpc":"2.0","id":1,"method":"tools/call","params":'
                '{"name":"compare_to_naive","arguments":{"platform":"ANDROID_PHONE"}}}')
        out = subprocess.run(
            ["ssh", "-i", key, "-o", "ConnectTimeout=15", "-o", "StrictHostKeyChecking=accept-new",
             "edge@" + EDGE.split("//")[1],
             "curl -s -m 60 -X POST http://localhost:8765/mcp/ "
             "-H 'Content-Type: application/json' "
             "-H 'Accept: application/json, text/event-stream' "
             f"-d '{body}'"],
            capture_output=True, text=True, timeout=120).stdout
        assert "6046" in out, "expected 6,046 in tool output"
        assert "clickhouse.cloud" in out, "tool did not name the ClickHouse host"
        return "compare_to_naive -> 6,046, computed on ClickHouse Cloud"
    check("tool round-trip (inside VM)", mcp_via_ssh)

    print("\n=== Langfuse (chat trace evidence) ===")
    check("health", lambda: f"HTTP {fetch(EDGE + ':3000/api/public/health', 30)[0]}")

    pk = os.environ.get("LANGFUSE_PK", "")
    sk = os.environ.get("LANGFUSE_SK", "")
    if pk and sk:
        def lf_traces():
            auth = base64.b64encode(f"{pk}:{sk}".encode()).decode()
            d = jget(EDGE + ":3000/api/public/traces?limit=3", 30,
                     {"Authorization": "Basic " + auth})
            n = len(d["data"])
            assert n > 0, "no traces recorded"
            return f"{n}+ traces · latest: {d['data'][0]['name']}"
        check("traces recorded", lf_traces)
    else:
        print("       (set LANGFUSE_PK / LANGFUSE_SK to assert trace contents)")

    print("\n=== LibreChat (the chat surface) ===")
    check("UI", lambda: f"HTTP {fetch(EDGE + ':3080/', 30)[0]}")

    n_fail = results.count(False)
    print("\n" + "=" * 78)
    print(f"  {results.count(True)} passed · {n_fail} failed")
    print("=" * 78 + "\n")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    import urllib.error  # noqa: E402  (used in otlp_auth)
    main()
