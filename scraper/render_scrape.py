#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings -- Rendered Scraper (headless browser)
========================================================================
Last-resort text capture for postings a plain HTTP fetch cannot read.

backfill_text.py uses urllib, so it gets nothing from boards that build the
posting in JavaScript -- Workday, Ashby, Oracle Cloud, iCIMS. Those pages return
a shell that is literally 0 characters after tag-stripping. This script drives a
real browser, lets the page render, then walks the DOM the way the review-page
bookmarklet did.

The extraction is a direct port of that bookmarklet: same tag skip-list, same
per-node length filter, same nav-word filter, same line thresholds. What is
*not* ported is its plumbing -- the window.opener/postMessage handshake and the
alert() calls existed to send text back to a review page that is being retired.
Here the text is written straight to job_details.jsonl.

One deliberate change: the bookmarklet accepted anything over 100 characters.
That is well below the point where a page is distinguishable from its own
navigation, so results here go through jobtext.looks_like_job_text() -- the same
gate every other capture path uses. A posting stored from this script is
indistinguishable from one stored by the nightly backfill.

Cost: this is slow (seconds per posting, a browser per run) and needs a ~150 MB
browser download. It is not part of the nightly schedule -- run it when the
cheap paths have been exhausted.

Setup
-----
    pip install -r scraper/requirements-render.txt
    python -m playwright install chromium

Usage
-----
    python scraper/render_scrape.py --dry-run       # what it would attempt
    python scraper/render_scrape.py --limit 20      # try 20
    python scraper/render_scrape.py                 # everything missing text
    python scraper/render_scrape.py --only-js       # only known JS-rendered hosts
    python scraper/render_scrape.py --headed        # watch it work
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jobtext import looks_gone, looks_like_job_text, normalize  # noqa: E402

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    sys.exit("ERROR: pip install -r scraper/requirements-render.txt "
             "&& python -m playwright install chromium")

ROOT          = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR      = os.path.join(ROOT, "data")
DETAILS_CSV   = os.path.join(DATA_DIR, "job_details.csv")
DETAILS_JSONL = os.path.join(DATA_DIR, "job_details.jsonl")
STATUS_CSV    = os.path.join(DATA_DIR, "render_status.csv")

STATUS_HEADERS = ["id", "status", "chars", "lines", "attempts", "last_attempt", "note"]

# Boards that render the posting client-side. --only-js narrows to these, which
# is where a browser earns its cost; everything else the cheap path can usually
# already read.
JS_HOSTS = (
    "myworkdayjobs.com", "workday.com", "wd1.", "wd3.", "wd5.", "wd103.", "wd501.",
    "ashbyhq.com", "oraclecloud.com", "icims.com", "eightfold.ai",
    "dayforcehcm.com", "successfactors", "avature.net", "phenompeople.com",
)

PAGE_TIMEOUT_MS  = 30_000
SETTLE_MS        = 3_000   # the bookmarklet's 3s wait, for late-rendering boards
CHECKPOINT_EVERY = 10

# Direct port of the bookmarklet's DOM walk. Kept verbatim in shape so results
# match what the manual tool produced.
EXTRACT_JS = r"""
() => {
  const replay = document.querySelector('replay-web-page');
  if (replay) {
    // A GhostArchive replay wrapper, not the posting. Hand back the original
    // URL so the caller can navigate to it instead.
    return { replayUrl: replay.getAttribute('url') || '', lines: 0, text: '' };
  }

  const skip = ['SCRIPT', 'STYLE', 'NAV', 'HEADER', 'FOOTER'];
  const allText = [];
  function walk(n, depth) {
    if (depth > 50) return;
    if (n.nodeType === 3) {
      const t = n.textContent.trim();
      if (t.length > 3 && !t.match(/^(Search|Menu|Apply|Jobs|Sign|Log|Home)/i)) {
        allText.push(t);
      }
    } else if (n.nodeType === 1 && !skip.includes(n.tagName)) {
      for (const c of n.childNodes) walk(c, depth + 1);
    }
  }
  walk(document.body, 0);

  const text = allText.join('\n').replace(/\n{3,}/g, '\n\n').trim();
  const lines = text.split('\n').filter(l => l.trim().length > 2);
  return { replayUrl: '', lines: lines.length, text: lines.slice(0, 400).join('\n') };
}
"""

MIN_LINES = 40   # the bookmarklet's threshold: fewer means the page never rendered

# Walls that render fine and contain no posting. Naming them in the status file
# separates "we could not read this" from "there was nothing to read", which is
# the difference between worth retrying and not. Verified on iCIMS: the cookie
# wall survives a reload, so it is a block rather than a timing problem.
WALL_MARKERS = [
    "please enable cookies",
    "enable cookies in your browser",
    "your browser does not support javascript",
    "access denied",
    "verify you are a human",
    "checking your browser",
]


# -- Data I/O ------------------------------------------------------------------
def load_details():
    if not os.path.exists(DETAILS_CSV):
        sys.exit(f"ERROR: {DETAILS_CSV} not found. Run this on the data branch.")
    with open(DETAILS_CSV, encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if r.get("id")]


def load_texts():
    texts = {}
    if not os.path.exists(DETAILS_JSONL):
        return texts
    with open(DETAILS_JSONL, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except Exception:
                continue
            if e.get("id") and e.get("raw_text", "").strip():
                texts[e["id"]] = e["raw_text"]
    return texts


def save_texts(texts, order):
    rank = {jid: i for i, jid in enumerate(order)}
    tmp = DETAILS_JSONL + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for jid in sorted(texts, key=lambda j: rank.get(j, len(rank))):
            f.write(json.dumps({"id": jid, "raw_text": texts[jid]}) + "\n")
    os.replace(tmp, DETAILS_JSONL)


def load_status():
    status = {}
    if not os.path.exists(STATUS_CSV):
        return status
    with open(STATUS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("id"):
                status[row["id"]] = row
    return status


def save_status(status):
    tmp = STATUS_CSV + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=STATUS_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(status.values(), key=lambda r: r.get("id", "")))
    os.replace(tmp, STATUS_CSV)


def is_js_host(url):
    return any(h in (url or "").lower() for h in JS_HOSTS)


# -- Scraping ------------------------------------------------------------------
def scrape_one(page, url):
    """
    Render one URL and extract text.

    Returns (text, lines, note). text is "" when nothing usable was found.
    """
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    except PWTimeout:
        return "", 0, "page_timeout"
    except Exception as e:
        return "", 0, f"nav_{type(e).__name__}"

    # Give client-side rendering a chance. networkidle is the good signal; the
    # fixed settle is the fallback for boards that keep a socket open forever.
    try:
        page.wait_for_load_state("networkidle", timeout=8_000)
    except PWTimeout:
        pass
    page.wait_for_timeout(SETTLE_MS)

    try:
        result = page.evaluate(EXTRACT_JS)
    except Exception as e:
        return "", 0, f"eval_{type(e).__name__}"

    # GhostArchive replay wrapper -- follow through to the original posting,
    # exactly as the bookmarklet did by reopening it in a new tab.
    if result.get("replayUrl"):
        try:
            page.goto(result["replayUrl"], wait_until="domcontentloaded",
                      timeout=PAGE_TIMEOUT_MS)
            page.wait_for_timeout(SETTLE_MS)
            result = page.evaluate(EXTRACT_JS)
        except Exception as e:
            return "", 0, f"replay_{type(e).__name__}"

    lines = result.get("lines", 0)
    text = normalize(result.get("text", ""))

    low = text[:3000].lower()
    if any(marker in low for marker in WALL_MARKERS):
        return "", lines, "blocked (cookie/JS wall)"
    if lines < MIN_LINES:
        return "", lines, f"only_{lines}_lines"
    if looks_gone(text[:1500]):
        return "", lines, "posting taken down"
    if not looks_like_job_text(text):
        return "", lines, f"no posting-like text ({len(text)}c)"
    return text, lines, ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="max postings (0 = all)")
    ap.add_argument("--only-js", action="store_true",
                    help="only hosts known to render client-side")
    ap.add_argument("--headed", action="store_true", help="show the browser")
    ap.add_argument("--retry-failed", action="store_true",
                    help="also retry postings this script already failed on")
    ap.add_argument("--dry-run", action="store_true", help="report the work list only")
    args = ap.parse_args()

    rows = load_details()
    texts = load_texts()
    status = load_status()
    order = [r["id"] for r in rows]

    todo = [r for r in rows if r["id"] not in texts and (r.get("job_url") or "").strip()]
    if args.only_js:
        todo = [r for r in todo if is_js_host(r["job_url"])]
    if not args.retry_failed:
        todo = [r for r in todo if r["id"] not in status]

    print("=" * 70)
    print("  Rendered scrape (headless browser)")
    print("=" * 70)
    print(f"  postings             : {len(rows):,}")
    print(f"  already have text    : {len(texts):,}")
    print(f"  missing text         : {len(rows) - len(texts):,}")
    if args.only_js:
        print("  filter               : JS-rendered hosts only")
    if args.limit:
        todo = todo[: args.limit]
    print(f"  to attempt this run  : {len(todo):,}")
    print()

    if args.dry_run:
        from urllib.parse import urlparse
        hosts = {}
        for r in todo:
            h = urlparse(r["job_url"]).netloc
            hosts[h] = hosts.get(h, 0) + 1
        for h, n in sorted(hosts.items(), key=lambda kv: -kv[1])[:20]:
            print(f"    {n:>4}x  {h}")
        print("\n  (dry run — no browser started)")
        return
    if not todo:
        print("  Nothing to do.")
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ok = failed = 0
    started = time.monotonic()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.headed)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = context.new_page()
        # Images and fonts are pure cost here -- the text is what matters.
        page.route("**/*.{png,jpg,jpeg,gif,svg,webp,woff,woff2,ttf,otf}",
                   lambda route: route.abort())

        try:
            for i, row in enumerate(todo, 1):
                label = f"{row.get('company_name', '')[:22]} — {row.get('title', '')[:30]}"
                text, lines, note = scrape_one(page, row["job_url"])

                prev = status.get(row["id"], {})
                status[row["id"]] = {
                    "id":           row["id"],
                    "status":       "ok" if text else "failed",
                    "chars":        str(len(text)),
                    "lines":        str(lines),
                    "attempts":     str(int(prev.get("attempts", 0) or 0) + 1),
                    "last_attempt": now,
                    "note":         note[:200],
                }
                if text:
                    texts[row["id"]] = text
                    ok += 1
                    print(f"  [{i:>4}/{len(todo)}] OK   {label:<56} {len(text):,}c / {lines} lines",
                          flush=True)
                else:
                    failed += 1
                    print(f"  [{i:>4}/{len(todo)}] --   {label:<56} {note}", flush=True)

                if i % CHECKPOINT_EVERY == 0:
                    save_texts(texts, order)
                    save_status(status)
        except KeyboardInterrupt:
            print("\n  Interrupted — flushing what's done ...")
        finally:
            context.close()
            browser.close()

    save_texts(texts, order)
    save_status(status)

    elapsed = time.monotonic() - started
    print()
    print("-" * 70)
    print(f"  Attempted : {ok + failed} in {elapsed / 60:.1f} min")
    print(f"    ok      : {ok}")
    print(f"    failed  : {failed}")
    print(f"  job_details.jsonl -> {len(texts):,} entries with text "
          f"({len(rows) - len(texts):,} still missing)")
    print(f"  render_status.csv -> {len(status):,} rows")


if __name__ == "__main__":
    main()
