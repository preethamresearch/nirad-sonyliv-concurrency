"""End-to-end tests for Watchhouse.

Everything here was previously checked by hand, once, and re-broken later --
twice by careless edits during the same session. These are the checks that
would have caught those regressions:

  * every view renders with no console error
  * nothing overflows its card (the class of bug that produced black map tiles
    and a 102px-wide key/value table)
  * filters actually change the number they claim to filter (two of four were
    silently ignored for a while)
  * the log accordion survives the polling refresh (it used to snap shut and
    reset scroll)
  * the deck has no horizontal scrollbar and every screenshot loads

Run:  pytest tests/ -v          (dashboard must be on http://localhost:877)
"""
import re

import pytest
from playwright.sync_api import expect

BASE = "http://localhost:877"

VIEWS = ["overview", "ops", "analyst", "product", "regions",
         "replay", "liveops", "ingest", "pipeline", "config", "arch"]

# Views whose first paint issues several Cloud queries need a longer settle.
SETTLE_MS = 9000


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------
@pytest.fixture(scope="function")
def app(page):
    """A page with console errors collected, so any test can assert on them."""
    page.errors = []
    page.on("console", lambda m: page.errors.append(m.text)
            if m.type == "error" else None)
    page.on("pageerror", lambda e: page.errors.append(str(e)))
    page.set_viewport_size({"width": 1440, "height": 900})
    return page


def goto_view(page, view):
    page.goto(f"{BASE}/app#{view}", wait_until="domcontentloaded")
    page.wait_for_selector("main .card", timeout=SETTLE_MS)
    page.wait_for_timeout(2500)          # let the async cards fill in


# --------------------------------------------------------------------------
# sanity: the server and its data
# --------------------------------------------------------------------------
def test_all_routes_respond(app):
    for path in ["/", "/app", "/deck", "/classic",
                 "/shots/01-landing.jpg", "/shots/sonyliv.png",
                 "/vendor/mermaid.min.js"]:
        r = app.request.get(BASE + path)
        assert r.status == 200, f"{path} -> {r.status}"


@pytest.mark.parametrize("path", [
    "/api/overview", "/api/series", "/api/facets", "/api/live_ops",
    "/api/ingest_monitor", "/api/config", "/api/heatmap", "/api/language",
    "/api/catalog", "/api/pipeline_live",
    "/api/breakdown?dim=platform", "/api/breakdown?dim=close_reason",
    "/api/top_content?limit=5",
])
def test_api_endpoints(app, path):
    r = app.request.get(BASE + path, timeout=120000)
    assert r.status == 200, f"{path} -> {r.status}"
    body = r.json()
    assert "error" not in body, f"{path} returned {body.get('error')}"


def test_headline_numbers_are_internally_consistent(app):
    """Dataset-relative on purpose: judging day swaps the data under us.

    Foreground-only counts a strict subset of the naive overlap, so
    fg < naive must hold on ANY dataset -- and equality means the model
    silently applied nothing, which is also a failure.
    """
    d = app.request.get(BASE + "/api/series", timeout=120000).json()
    nv, fg = d["nv"]["peak"], d["fg"]["peak"]
    assert fg > 0, "foreground peak is zero"
    assert fg < nv, f"foreground {fg} not below naive {nv}"
    assert d["overcount"] == nv - fg
    assert d["overcount_pct"] == round((nv - fg) / nv * 100, 1)


def test_oracle_parity_table_intact(app):
    d = app.request.get(BASE + "/api/overview", timeout=120000).json()
    assert d["intervals"] > 0, "no intervals"
    assert d["events"] > d["intervals"], (d["events"], d["intervals"])


# --------------------------------------------------------------------------
# every view renders
# --------------------------------------------------------------------------
@pytest.mark.parametrize("view", VIEWS)
def test_view_renders_without_errors(app, view):
    goto_view(app, view)
    assert app.locator("main .card").count() > 0, f"{view}: no cards"
    assert app.locator("main .err").count() == 0, \
        f"{view}: error shown -> {app.locator('main .err').first.inner_text()}"
    ignorable = ("favicon", "ERR_INTERNET_DISCONNECTED")
    real = [e for e in app.errors if not any(i in e for i in ignorable)]
    assert not real, f"{view}: console errors {real[:3]}"


@pytest.mark.parametrize("view", VIEWS)
def test_nothing_overflows_its_card(app, view):
    """An element wider than its card is the bug that produced black map tiles
    and a key/value table 102px past its container. Elements inside a
    scrollable ancestor are excluded -- those are scrolling, not overflowing."""
    goto_view(app, view)
    overflow = app.evaluate("""() => {
      const bad = [];
      const scrollable = el => {
        for (let n = el.parentElement; n && !n.classList.contains('card'); n = n.parentElement) {
          const ov = getComputedStyle(n).overflowX;
          if (ov === 'auto' || ov === 'scroll' || ov === 'hidden') return true;
        }
        return false;
      };
      document.querySelectorAll('main .card').forEach(card => {
        const cr = card.getBoundingClientRect();
        card.querySelectorAll('*').forEach(el => {
          if (el.closest('.pop,.menu,.fpanel,.tip')) return;
          if (scrollable(el)) return;
          const r = el.getBoundingClientRect();
          if (!r.width || !r.height) return;
          const over = Math.max(r.right - cr.right, cr.left - r.left);
          if (over > 2) bad.push({
            el: el.tagName.toLowerCase() + '.' + String(el.className || '').split(' ')[0],
            over: Math.round(over), text: (el.textContent || '').trim().slice(0, 40)});
        });
      });
      return bad;
    }""")
    assert not overflow, f"{view}: {overflow[:3]}"


def test_no_horizontal_page_scroll(app):
    for view in VIEWS:
        goto_view(app, view)
        assert app.evaluate(
            "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1"
        ), f"{view} scrolls horizontally"


# --------------------------------------------------------------------------
# filters must actually filter
# --------------------------------------------------------------------------
def test_filter_changes_the_number(app):
    """Two of four filters were silently ignored by the concurrency curve for
    a while. A filter that does not move the figure is worse than none."""
    base = app.request.get(BASE + "/api/series", timeout=120000).json()
    cases = {
        "platform=ANDROID_PHONE": "platform=ANDROID_PHONE",
        "video_type=live": "video_type=live",
        "category": "category=cdbgg",
        "multi-select": "platform=ANDROID_PHONE,IPHONE",
        "combination": "platform=ANDROID_PHONE&video_type=live",
    }
    for label, qs in cases.items():
        d = app.request.get(f"{BASE}/api/series?{qs}", timeout=120000).json()
        assert d["fg"]["peak"] != base["fg"]["peak"], f"{label} did not filter"
        # both curves must move together or the gap itself becomes a lie
        assert d["nv"]["peak"] != base["nv"]["peak"], f"{label}: naive did not move"


def test_filter_panel_builds_a_predicate(app):
    """The panel stays open across a selection on purpose -- you pick several
    values without reopening it -- so this must not click the toggle again."""
    goto_view(app, "overview")
    app.click("#fbtn")
    app.wait_for_selector(".fpanel:not([hidden])")
    app.click('.frow[data-dim="platform"][data-val="ANDROID_PHONE"]')
    app.wait_for_timeout(7000)
    app.wait_for_selector(".fpanel:not([hidden])", timeout=15000)
    pred = app.locator(".fpred pre").inner_text()
    assert "platform" in pred and "ANDROID_PHONE" in pred, pred
    assert app.locator(".chip").count() >= 1
    assert app.locator("#fbtn .badge").inner_text() == "1"

    # a second dimension must AND with the first, not replace it
    app.click('.frow[data-dim="video_type"][data-val="live"]')
    app.wait_for_timeout(7000)
    pred2 = app.locator(".fpred pre").inner_text()
    assert "AND" in pred2 and "video_type" in pred2, pred2


def test_row_filter_is_an_explicit_control(app):
    """Clicking anywhere on a row used to filter, which is undiscoverable and
    fires while selecting text."""
    goto_view(app, "overview")
    buttons = app.locator(".rowfilter")
    assert buttons.count() > 0, "no explicit row-filter control"
    before = app.locator("main .tile .value").nth(1).inner_text()
    buttons.first.click()
    app.wait_for_timeout(7000)
    after = app.locator("main .tile .value").nth(1).inner_text()
    assert before != after, f"row filter did not change peak ({before} -> {after})"
    assert app.locator("tr.rowon").count() >= 1


def test_view_only_offers_filters_it_can_honour(app):
    d = app.request.get(BASE + "/api/overview", timeout=120000).json()
    assert "close_reason" not in d["view_dims"]["overview"], \
        "overview must not offer close_reason: the delta tables are not keyed by it"
    assert "close_reason" in d["view_dims"]["ops"]


# --------------------------------------------------------------------------
# pagination
# --------------------------------------------------------------------------
def test_table_pagination(app):
    goto_view(app, "analyst")          # category table is 84 rows
    pager = app.locator("#tbl .pgnum")
    assert pager.count() == 1, "no pager on an 84-row table"
    first = app.locator("#tbl tbody tr").first.inner_text()
    app.locator('#tbl .pgbtn[data-d="1"]').click()
    app.wait_for_timeout(600)
    assert app.locator("#tbl tbody tr").first.inner_text() != first
    assert "Page 2" in pager.inner_text()


def test_log_accordion_survives_polling(app):
    """The ingestion view polls every 6s; it used to re-render the whole list,
    closing any open row and resetting scroll."""
    goto_view(app, "ingest")
    app.wait_for_selector(".logrow")
    row = app.locator(".logrow").nth(2)
    key = row.get_attribute("data-k")
    row.locator(".lhead").click()
    app.wait_for_timeout(300)
    assert row.locator(".ldetail").is_visible()
    app.wait_for_timeout(8000)           # spans a poll cycle
    still = app.locator(f'.logrow[data-k="{key}"]')
    assert still.count() == 1, "row disappeared across the poll"
    assert still.locator(".ldetail").is_visible(), "accordion closed on poll"


def test_log_detail_is_indented_json_not_a_truncated_line(app):
    goto_view(app, "ingest")
    app.wait_for_selector(".logrow")
    app.locator(".logrow").first.locator(".lhead").click()
    app.wait_for_timeout(300)
    code = app.locator("pre.code").first.inner_text()
    assert code.strip().startswith("{")
    assert "\n  " in code, "JSON is not indented"
    # the header must never carry raw JSON
    heads = app.locator(".lhead").all_inner_texts()
    assert not any('{"' in h for h in heads), "raw JSON leaked into a log header"


# --------------------------------------------------------------------------
# playground and configuration
# --------------------------------------------------------------------------
def test_playground_runs_and_reports_metrics(app):
    goto_view(app, "arch")
    app.wait_for_selector("#pg-out table", timeout=40000)
    tiles = app.locator("#pg-metrics .tile .value").all_inner_texts()
    assert len(tiles) >= 4, tiles
    assert app.locator("#pg-out tbody tr").count() >= 1


def test_playground_refuses_writes(app):
    for bad in ["DROP TABLE sony.raw_events",
                "INSERT INTO sony.raw_events VALUES (1)",
                "SELECT 1; DROP TABLE x"]:
        r = app.request.get(f"{BASE}/api/playground",
                            params={"sql": bad, "runs": "1"}, timeout=60000)
        body = r.json()
        assert "error" in body, f"NOT BLOCKED: {bad}"


def test_config_dry_run_reports_schema_plan(app):
    r = app.request.get(f"{BASE}/api/config",
                        params={"path": r"C:\d\demo-sonyliv-clickhouse\fixtures\dirty_day.csv"},
                        timeout=120000)
    plan = r.json().get("plan", {})
    assert plan.get("ok") is True, plan
    assert len(plan["mapped"]) >= 12
    assert "cdn_pop" in plan["ignored"]
    assert "subtitle_language" in plan["missing"]


# --------------------------------------------------------------------------
# deck
# --------------------------------------------------------------------------
def test_deck_is_15_slides_with_working_images(app):
    app.goto(f"{BASE}/deck", wait_until="load")
    app.wait_for_timeout(1500)
    assert app.locator(".slide").count() == 15
    broken = app.evaluate(
        "[...document.querySelectorAll('img')].filter(i=>!i.complete||!i.naturalWidth)"
        ".map(i=>i.src)")
    assert not broken, broken


def test_deck_has_no_horizontal_scrollbar(app):
    for width in (1920, 1440, 1280, 1100):
        app.set_viewport_size({"width": width, "height": 900})
        app.goto(f"{BASE}/deck", wait_until="load")
        app.wait_for_timeout(700)
        assert app.evaluate(
            "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1"
        ), f"deck scrolls horizontally at {width}px"


def test_deck_content_fits_inside_slides(app):
    app.goto(f"{BASE}/deck", wait_until="load")
    app.wait_for_timeout(1200)
    worst = app.evaluate("""() => {
      let worst = 0;
      document.querySelectorAll('.slide').forEach(sl => {
        const sr = sl.getBoundingClientRect();
        sl.querySelectorAll('*').forEach(el => {
          const r = el.getBoundingClientRect();
          if (!r.height) return;
          worst = Math.max(worst, r.bottom - sr.bottom, r.right - sr.right);
        });
      });
      return Math.round(worst);
    }""")
    assert worst <= 2, f"deck content overflows slide by {worst}px"


# --------------------------------------------------------------------------
# landing page
# --------------------------------------------------------------------------
def test_landing_pulls_live_figures(app):
    app.goto(BASE, wait_until="load")
    app.wait_for_timeout(6000)
    naive = int(app.locator("#f-naive").inner_text().replace(",", ""))
    fg = int(app.locator("#f-fg").inner_text().replace(",", ""))
    assert fg > 0 and naive > fg, (naive, fg)
    note = app.locator("#gap-note").inner_text()
    assert "phantom viewers" in note, note
    assert app.locator("#gapchart path").count() >= 2, "hero chart did not draw"


def test_landing_responsive_and_branded(app):
    for width in (1440, 1024, 768, 420):
        app.set_viewport_size({"width": width, "height": 900})
        app.goto(BASE, wait_until="load")
        app.wait_for_timeout(900)
        assert app.evaluate(
            "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1"
        ), f"landing scrolls horizontally at {width}px"
    assert app.locator(".trackbadge").count() == 1


# --------------------------------------------------------------------------
# responsive app shell
# --------------------------------------------------------------------------
def test_sidebar_collapses_on_narrow_screens(app):
    app.set_viewport_size({"width": 800, "height": 900})
    goto_view(app, "overview")
    assert app.locator("#navtoggle").is_visible(), "no nav toggle below 900px"
    app.click("#navtoggle")
    app.wait_for_timeout(400)
    assert app.evaluate("document.querySelector('.side').classList.contains('open')")


def test_sidebar_footer_not_clipped(app):
    goto_view(app, "overview")
    ok = app.evaluate("""() => {
      const s = document.querySelector('.side').getBoundingClientRect();
      const f = document.querySelector('.side .foot').getBoundingClientRect();
      return f.bottom <= s.bottom + 1 && f.top >= s.top;
    }""")
    assert ok, "sidebar footer is clipped"


# --------------------------------------------------------------------------
# replay -- the demo the brief actually asks for
# --------------------------------------------------------------------------
def test_replay_endpoint_shape(app):
    d = app.request.get(BASE + "/api/replay", timeout=120000).json()
    for k in ("state", "series", "platforms", "stored_now", "open_now", "peak_now"):
        assert k in d, f"missing {k}"


def test_replay_chart_updates_in_place(app):
    """Polling must update, not repaint. The first version rebuilt innerHTML
    every tick, which cleared the SVG and destroyed any hover state."""
    goto_view(app, "replay")
    app.wait_for_selector("#rpsvg", timeout=20000)
    app.evaluate("document.querySelector('#rpsvg').dataset.marker = 'original'")
    app.wait_for_timeout(7000)          # spans at least one poll
    survived = app.evaluate(
        "document.querySelector('#rpsvg')?.dataset.marker === 'original'")
    assert survived, "the chart node was replaced instead of updated"


def test_replay_chart_has_a_tooltip(app):
    goto_view(app, "replay")
    app.wait_for_selector("#rphit", timeout=20000)
    app.evaluate("""() => {
      const svg = document.querySelector('#rpsvg');
      const bb = svg.getBoundingClientRect();
      svg.querySelector('#rphit').dispatchEvent(new PointerEvent('pointermove', {
        clientX: bb.left + bb.width * 0.6, clientY: bb.top + bb.height * 0.5,
        bubbles: true}));
    }""")
    app.wait_for_timeout(300)
    assert app.locator("#tip").is_visible(), "no tooltip on the replay chart"
    assert "concurrent" in app.locator("#tip").inner_text()


def test_replay_never_publishes_an_empty_curve(app):
    """A derive that yields nothing must not be swapped in: an empty curve on
    screen reads as a crash rather than as 'no data yet'."""
    seen_data = False
    for _ in range(6):
        d = app.request.get(BASE + "/api/replay", timeout=120000).json()
        n = len(d["series"])
        if seen_data:
            assert n > 0, "curve dropped to empty after having data"
        if n > 0:
            seen_data = True
        app.wait_for_timeout(2500)
