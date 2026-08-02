"""Export Langfuse traces as JSON evidence for the submission.

Judges must not need a login to see that chat answers correspond to real
queries (submission guidelines), so the traces are pulled through Langfuse's
public API and committed as a plain JSON file.

    python scripts/export_langfuse.py \
        --host http://8.231.76.83:3000 --pk pk-lf-... --sk sk-lf-... \
        --out results/evidence/langfuse_traces.json
"""
import argparse
import base64
import json
import os
import urllib.request


def get(host, pk, sk, path):
    auth = base64.b64encode(f"{pk}:{sk}".encode()).decode()
    req = urllib.request.Request(host.rstrip("/") + path,
                                 headers={"Authorization": "Basic " + auth})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("LANGFUSE_HOST", ""))
    ap.add_argument("--pk", default=os.environ.get("LANGFUSE_PUBLIC_KEY", ""))
    ap.add_argument("--sk", default=os.environ.get("LANGFUSE_SECRET_KEY", ""))
    ap.add_argument("--out", default="results/evidence/langfuse_traces.json")
    a = ap.parse_args()
    if not (a.host and a.pk and a.sk):
        raise SystemExit("need --host / --pk / --sk (or LANGFUSE_* env)")

    traces = get(a.host, a.pk, a.sk, "/api/public/traces?limit=100")["data"]
    # The full record per trace, observations included -- the evidence is the
    # tool's exact input and output, not just its name.
    full = [get(a.host, a.pk, a.sk, f"/api/public/traces/{t['id']}")
            for t in traces]

    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump({"exported_from": a.host, "trace_count": len(full),
                   "traces": full}, fh, indent=2)
    print(f"wrote {a.out}: {len(full)} traces")


if __name__ == "__main__":
    main()
