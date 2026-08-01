"""Minimal ClickHouse HTTP client.

Stdlib only, on purpose: this has to run on the one laptop we have, against
ClickHouse Cloud (HTTPS:8443) or a local container (HTTP:8123), with no
native client binary and no wheel that might fail to build at 3am.
"""
import gzip
import io
import os
import ssl
import threading
import sys
import time
import urllib.parse
import urllib.request

_ENV_LOADED = False
LAST_SUMMARY = {}


_ENV_LOCK = threading.Lock()


def _load_env(path=None):
    """Read .env from the repo root if present. No dependency on python-dotenv.

    Guarded by a lock, and the loaded flag is set only AFTER os.environ is
    populated. The obvious version -- set the flag first, then read the file --
    is a double-checked-locking race: a second thread sees the flag, returns
    immediately, and reads an environment the first thread has not finished
    filling. It yields an empty password and a 401 that looks exactly like a
    rotated credential. Harmless while every caller was sequential; the moment
    queries began running concurrently it became an intermittent auth failure.
    """
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    with _ENV_LOCK:
        if _ENV_LOADED:          # another thread finished while we waited
            return
        path = path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
        _ENV_LOADED = True       # only now is the environment usable


def config():
    _load_env()
    secure = os.environ.get("CH_SECURE", "0") in ("1", "true", "True")
    return {
        "host": os.environ.get("CH_HOST", "localhost"),
        "port": int(os.environ.get("CH_PORT", "8443" if secure else "8123")),
        "user": os.environ.get("CH_USER", "default"),
        "password": os.environ.get("CH_PASSWORD", ""),
        "secure": secure,
        "db": os.environ.get("CH_DB", "sony"),
    }


def _url(cfg, params):
    scheme = "https" if cfg["secure"] else "http"
    qs = urllib.parse.urlencode(params)
    return f"{scheme}://{cfg['host']}:{cfg['port']}/?{qs}"


def execute(query, body=None, settings=None, timeout=1800, retries=3):
    """Run `query`. If `body` is bytes or a file object, stream it as INSERT data.

    Returns (text, elapsed_seconds). Raises RuntimeError with the server's
    message on failure -- ClickHouse errors are genuinely informative and
    swallowing them wastes debugging time we do not have.
    """
    cfg = config()
    params = {"query": query, "database": cfg["db"]}
    params.update(settings or {})

    data = body
    headers = {
        "X-ClickHouse-User": cfg["user"],
        "X-ClickHouse-Key": cfg["password"],
    }
    if isinstance(data, (bytes, bytearray)):
        headers["Content-Length"] = str(len(data))
    elif data is not None:
        # File object: Content-Length lets urllib stream instead of buffering.
        headers["Content-Length"] = str(os.fstat(data.fileno()).st_size)

    ctx = ssl.create_default_context() if cfg["secure"] else None
    last_err = None

    # Trace every statement into ClickStack. Imported lazily and guarded so
    # that a missing or misconfigured collector can never fail a query -- the
    # observability layer must not be able to break the thing it observes.
    try:
        import otel as _otel
        _span = _otel.span("clickhouse.query",
                           **{"db.system": "clickhouse",
                              "db.statement": " ".join(query.split())[:400],
                              "db.name": cfg["db"], "server.address": cfg["host"]})
    except Exception:
        _span = None

    for attempt in range(retries):
        try:
            t0 = time.time()
            req = urllib.request.Request(_url(cfg, params), data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                body = resp.read().decode("utf-8", "replace")
                # X-ClickHouse-Summary carries read_rows / read_bytes. Wall
                # time on a laptop-sized dataset says almost nothing about a
                # design's behaviour at 100x; rows read says everything.
                global LAST_SUMMARY
                try:
                    import json as _json
                    LAST_SUMMARY = _json.loads(resp.headers.get("X-ClickHouse-Summary") or "{}")
                except Exception:
                    LAST_SUMMARY = {}
                elapsed = time.time() - t0
                if _span is not None:
                    # rows_read is the number that actually tells us whether the
                    # sort key and projection are earning their keep; wall time
                    # on a laptop-sized dataset mostly measures the laptop.
                    with _span:
                        _span.set(duration_ms=round(elapsed * 1000, 2),
                                  read_rows=int(LAST_SUMMARY.get("read_rows", 0) or 0),
                                  read_bytes=int(LAST_SUMMARY.get("read_bytes", 0) or 0),
                                  result_rows=int(LAST_SUMMARY.get("result_rows", 0) or 0))
                return body, elapsed
        except urllib.error.HTTPError as e:
            msg = e.read().decode("utf-8", "replace")
            raise RuntimeError(f"ClickHouse HTTP {e.code}: {msg[:2000]}") from None
        except Exception as e:  # transient network / cold Cloud service
            last_err = e
            if data is not None and not isinstance(data, (bytes, bytearray)):
                raise  # cannot replay a consumed stream
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"ClickHouse unreachable after {retries} attempts: {last_err}")


def query(sql, fmt="TabSeparated", **kw):
    text, elapsed = execute(f"{sql} FORMAT {fmt}", **kw)
    return text, elapsed


def scalar(sql, **kw):
    text, _ = query(sql, **kw)
    return text.strip()


def rows(sql, **kw):
    text, elapsed = query(sql, **kw)
    out = [line.split("\t") for line in text.splitlines() if line]
    return out, elapsed


def strip_sql_comments(sql):
    """Remove `--` comments, respecting single-quoted string literals.

    Must run BEFORE splitting on `;`, because our DDL comments explain design
    decisions in prose and prose contains semicolons.
    """
    out = []
    for line in sql.splitlines():
        in_str = False
        cut = None
        i = 0
        while i < len(line):
            c = line[i]
            if c == "'":
                # '' is an escaped quote inside a literal
                if in_str and i + 1 < len(line) and line[i + 1] == "'":
                    i += 2
                    continue
                in_str = not in_str
            elif c == "-" and not in_str and i + 1 < len(line) and line[i + 1] == "-":
                cut = i
                break
            i += 1
        out.append(line[:cut] if cut is not None else line)
    return "\n".join(out)


def script(path, params=None):
    """Execute a .sql file as a sequence of `;`-terminated statements.

    ${NAME} placeholders are substituted from `params`, then the environment.
    Used to inject credentials into dictionary sources without committing them.
    """
    _load_env()
    with open(path, encoding="utf-8") as fh:
        raw = fh.read()

    def sub(m):
        k = m.group(1)
        if params and k in params:
            return str(params[k])
        if k in os.environ:
            return os.environ[k]
        raise RuntimeError(f"{path}: ${{{k}}} is not set in params or environment")

    import re
    raw = re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", sub, raw)

    stmts = [s.strip() for s in strip_sql_comments(raw).split(";")]
    stmts = [s for s in stmts if s]
    for s in stmts:
        label = " ".join(s.split())[:78]
        try:
            _, el = execute(s)
            print(f"  ok  ({el:5.2f}s)  {label}")
        except RuntimeError as e:
            print(f"  FAIL          {label}\n{e}", file=sys.stderr)
            raise
    return len(stmts)


def ping():
    cfg = config()
    where = f"{'https' if cfg['secure'] else 'http'}://{cfg['host']}:{cfg['port']} db={cfg['db']}"
    try:
        v = scalar("SELECT version()")
        print(f"connected: {where}  server={v}")
        return True
    except Exception as e:
        print(f"NOT connected: {where}\n  {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        text, el = query(" ".join(sys.argv[1:]), fmt="PrettyCompact")
        print(text)
        print(f"-- {el:.3f}s", file=sys.stderr)
    else:
        sys.exit(0 if ping() else 1)
