#!/usr/bin/env python3
"""
Job posting text extraction -- shared implementation.
=====================================================
One place that knows how to turn a job URL into usable description text, so
the batch backfill, the incremental updater, and the review app all behave
identically instead of each carrying its own weaker copy.

The public entry point is best_effort_text(). Everything else is the machinery
behind it and is exported mainly so callers can reuse pieces (the review app
scrapes a URL a human supplies, and wants the same quality gate applied).

Standard library only -- this module is vendored into a Vercel serverless app
where adding dependencies is not free.
"""

import gzip
import html as html_mod
import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from html.parser import HTMLParser

# Text shorter than this is a JS shell, a cookie wall, or a stub -- not a
# posting. Measured against the real data: genuine descriptions from these
# boards run 3,000-15,000 chars; JS shells (Workday, Ashby, Oracle) come back
# at literally 0 after tag-stripping.
#
# Length alone is not enough. A site's footer nav repeated three times clears
# any length bar while containing no posting at all, so anything kept also has
# to pass looks_like_job_text().
MIN_CHARS = 600
MAX_CHARS = 60_000   # generous cap; keeps one pathological page from bloating the file

UA = "Mozilla/5.0 (compatible; job-archiver/1.0; +historical-nyc-remote-job-postings)"

# Pages that resolve fine but say the posting is gone. Matching one means the
# job is permanently unrecoverable from that URL -- don't retry it on reruns.
GONE_MARKERS = [
    r"no longer accepting applications",
    r"this (job|position|posting|opening) (is|has been) (no longer|closed|filled|removed)",
    r"posting (is|has) (closed|expired)",
    r"job (not found|has expired)",
    r"position (has been|is) filled",
    r"we're sorry.{0,40}(couldn't find|not found)",
    r"404[^0-9]{0,20}(not found|page)",
]

# Vocabulary that a real posting has and a nav/footer/boilerplate blob does not.
JOB_SIGNALS = [
    r"\bresponsibilit(y|ies)\b",
    r"\bqualifications?\b",
    r"\brequirements?\b",
    r"\b(what you'?ll (do|be doing)|about the (role|job|position))\b",
    r"\byears? of experience\b",
    r"\b(bachelor|master|phd|degree|undergraduate|graduate student)\b",
    r"\b(salary|compensation|pay range|hourly rate|benefits)\b",
    r"\b(intern(ship)?|co-?op|full.?time|part.?time)\b",
    r"\b(skills?|proficien(t|cy)|familiar(ity)? with|experience (with|in))\b",
    r"\b(we are looking for|you will|join (our|the) team|equal opportunity)\b",
]

# Known job-description containers, most specific first. Same list the browser
# extension uses, minus the ones that only make sense against a live DOM.
CONTENT_PATTERNS = [
    r'<div[^>]+id=["\']content["\'][^>]*>(.*?)</div>\s*</div>',          # Greenhouse
    r'<div[^>]+class=["\'][^"\']*job__description[^"\']*["\'][^>]*>(.*?)</div>',
    r'<div[^>]+class=["\'][^"\']*posting-page[^"\']*["\'][^>]*>(.*?)</div>',  # Lever
    r'<div[^>]+class=["\'][^"\']*jobDescription[^"\']*["\'][^>]*>(.*?)</div>',
    r'<div[^>]+data-automation-id=["\']jobPostingDescription["\'][^>]*>(.*?)</div>',
    r'<main[^>]*>(.*?)</main>',
    r'<article[^>]*>(.*?)</article>',
]


# -- Text extraction -----------------------------------------------------------
class _TextExtractor(HTMLParser):
    _SKIP = {"script", "style", "head", "noscript", "iframe", "nav", "footer"}

    def __init__(self):
        super().__init__()
        self._depth = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self._SKIP:
            self._depth += 1

    def handle_endtag(self, tag):
        if tag.lower() in self._SKIP:
            self._depth = max(0, self._depth - 1)

    def handle_data(self, data):
        if not self._depth:
            s = data.strip()
            if s:
                self.parts.append(s)


def strip_tags(markup):
    """Turn a fragment of HTML into readable text."""
    if not markup:
        return ""
    p = _TextExtractor()
    p.feed(markup)
    return "\n".join(p.parts)


def normalize(text):
    text = html_mod.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()[:MAX_CHARS]


def extract_jsonld_description(html):
    """
    Pull the description out of a schema.org JobPosting block.

    Preferred over scraping the rendered page: it's the posting itself, with no
    nav, no "similar jobs" rail, and no cookie banner mixed in. Greenhouse,
    Lever, SmartRecruiters and Jobvite all emit it.
    """
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.S | re.I,
    ):
        try:
            data = json.loads(m.group(1).strip())
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("@type") != "JobPosting":
                continue
            desc = strip_tags(html_mod.unescape(item.get("description", "") or ""))
            if not desc.strip():
                continue
            # Fold in the structured bits the description body usually omits.
            head = []
            if item.get("title"):
                head.append(str(item["title"]))
            org = item.get("hiringOrganization") or {}
            if isinstance(org, dict) and org.get("name"):
                head.append(str(org["name"]))
            if item.get("employmentType"):
                et = item["employmentType"]
                head.append(", ".join(et) if isinstance(et, list) else str(et))
            if item.get("datePosted"):
                head.append(f"Posted: {item['datePosted']}")
            return "\n".join(head + [desc]) if head else desc
    return ""


def extract_container_text(html):
    """Fall back to a known description container before resorting to whole-body."""
    for pat in CONTENT_PATTERNS:
        m = re.search(pat, html, re.S | re.I)
        if m:
            text = strip_tags(m.group(1))
            if len(text.replace(" ", "")) > 200:
                return text
    return ""


def looks_like_job_text(text):
    """
    True if this reads like an actual posting.

    Guards three failure modes a length check waves through:
      - a footer/nav blob repeated until it clears MIN_CHARS
      - company boilerplate ("Founded in 1999, X is a leading ...") scraped
        from a container that didn't hold the description
      - binary/compressed bytes decoded into replacement characters
    """
    if len(text) < MIN_CHARS:
        return False

    # Undecodable bytes -- a compressed or binary response that slipped through.
    if text.count("�") > len(text) * 0.02:
        return False

    # Repetition guard -- a nav bar printed N times has very few unique lines.
    lines = [ln.strip() for ln in text.splitlines() if len(ln.strip()) > 2]
    if len(lines) >= 8:
        if len(set(lines)) / len(lines) < 0.55:
            return False
        # Nav/footer text is overwhelmingly short link labels.
        short = sum(1 for ln in lines if len(ln) < 25)
        if short / len(lines) > 0.8:
            return False

    low = text.lower()
    return sum(1 for p in JOB_SIGNALS if re.search(p, low)) >= 2


def looks_gone(text):
    low = text.lower()
    return any(re.search(p, low) for p in GONE_MARKERS)


def html_to_text(html):
    """
    Best available text for a job page.

    Runs all three extractors and takes the highest-fidelity one that actually
    passes as a posting -- rather than the first one that happens to be long
    enough. That ordering matters: a generic <main> match can be long and still
    be pure boilerplate, while the JSON-LD block right above it is the posting.
    """
    candidates = [
        normalize(extract_jsonld_description(html)),   # cleanest
        normalize(extract_container_text(html)),
        normalize(strip_tags(html)),                   # whole body, last resort
    ]
    for text in candidates:
        if looks_like_job_text(text):
            return text
    # Nothing passed -- hand back the longest attempt so the caller can report
    # *why* (taken down vs. too thin) instead of a bare failure.
    return max(candidates, key=len)


# -- Polite fetching -----------------------------------------------------------
class HostThrottle:
    """Keeps concurrent workers from hitting any single host too fast."""

    def __init__(self, min_interval=0.0):
        self.min_interval = min_interval
        self._last = {}
        self._lock = threading.Lock()

    # archive.org sees one availability lookup for *every* job, so it hits its
    # rate limit long before any individual job board does. A full run without
    # this returned 429 for 82 of 461 postings.
    # archive.ph rate-limits automated access aggressively and does not relent
    # on backoff; it is throttled hard here mostly so we stop being rude to it.
    SLOW_HOSTS = {
        "archive.org":     5.0,
        "web.archive.org": 3.0,
        "archive.ph":      15.0,
        "archive.today":   15.0,
        "archive.is":      15.0,
    }

    def wait(self, host):
        interval = max(self.min_interval, self.SLOW_HOSTS.get(host, 0.0))
        if interval <= 0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                ready = self._last.get(host, 0.0) + interval
                if now >= ready:
                    self._last[host] = now
                    return
                sleep_for = ready - now
            time.sleep(sleep_for)


# Transient HTTP failures worth waiting out rather than giving up on.
RETRY_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 3

# Hosts whose 429 is a policy, not a traffic spike. archive.ph refuses
# automated requests no matter how long you wait, so backing off just burns
# minutes to arrive at the same answer.
NO_RETRY_429 = {"archive.ph", "archive.today", "archive.is"}


def fetch(url, throttle=None, timeout=30, _attempt=0):
    """
    GET a URL. Returns (html, error_label). Exactly one is non-empty.

    Retries rate-limits and 5xx with backoff, honouring Retry-After when the
    server sends one. Without this, a busy archive.org turns a recoverable
    posting into a permanent-looking failure.
    """
    if throttle is not None:
        throttle.wait(urllib.parse.urlparse(url).netloc)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw      = r.read()
            encoding = (r.headers.get("Content-Encoding") or "").lower()
            charset  = r.headers.get_content_charset() or "utf-8"
        # Wayback's raw captures (the id_ form) replay the *original* stored
        # bytes and headers, so they come back gzipped even though we never
        # sent Accept-Encoding. Decoding that as text yields binary mojibake
        # that is long enough to look like a real description.
        if encoding == "gzip":
            try:
                raw = gzip.decompress(raw)
            except Exception:
                return "", "bad_gzip"
        elif encoding == "deflate":
            try:
                raw = zlib.decompress(raw, -zlib.MAX_WBITS)
            except Exception:
                return "", "bad_deflate"
        elif raw[:2] == b"\x1f\x8b":          # gzipped without saying so
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass
        return raw.decode(charset, errors="replace"), ""
    except urllib.error.HTTPError as e:
        host = urllib.parse.urlparse(url).netloc
        retryable = e.code in RETRY_CODES and not (e.code == 429 and host in NO_RETRY_429)
        if retryable and _attempt < MAX_RETRIES:
            try:
                wait = float(e.headers.get("Retry-After", "") or 0)
            except ValueError:
                wait = 0.0
            time.sleep(min(max(wait, 2.0 * (2 ** _attempt)), 60.0))
            return fetch(url, throttle, timeout, _attempt + 1)
        return "", f"http_{e.code}"
    except urllib.error.URLError as e:
        return "", f"urlerror_{str(e.reason)[:40]}"
    except Exception as e:
        return "", f"{type(e).__name__}"


def wayback_snapshot(url, throttle=None):
    """Closest existing Wayback capture, or "". Does not create new captures."""
    api = "https://archive.org/wayback/available?url=" + urllib.parse.quote(url, safe="")
    body, err = fetch(api, throttle, timeout=25)
    if err or not body:
        return ""
    try:
        snap = json.loads(body).get("archived_snapshots", {}).get("closest", {})
    except Exception:
        return ""
    if not snap.get("available"):
        return ""
    snap_url = snap.get("url", "")
    # Force the raw capture (id_) so Wayback's own toolbar chrome isn't scraped
    # in as part of the posting.
    return re.sub(r"/web/(\d+)/", r"/web/\1id_/", snap_url) if snap_url else ""


# -- Public entry point --------------------------------------------------------
def best_effort_text(job_url, archive_url="", archive_source="archive",
                     throttle=None, use_wayback=True):
    """
    Get description text for one posting, trying every source in order.

        1. live job_url      -- server-rendered boards
        2. archive_url       -- whatever was already captured
        3. Wayback snapshot  -- closest existing capture (never creates one)

    Returns {"text", "source", "status", "note"} where status is one of:
        ok    -- text found and it passes as a posting
        gone  -- a page said the posting was taken down
        empty -- pages loaded but held no posting-like text (JS-rendered, stub)
        error -- nothing could be fetched (blocked, 4xx, network)
    """
    attempts = []
    if job_url:
        attempts.append(("live", job_url))
    if archive_url:
        attempts.append((archive_source or "archive", archive_url))

    saw_gone = False
    last_err = ""

    for source, url in attempts:
        html, err = fetch(url, throttle)
        if err:
            last_err = err
            continue
        text = html_to_text(html)
        if looks_like_job_text(text) and not looks_gone(text[:1500]):
            return {"text": text, "source": source, "status": "ok", "note": ""}
        if looks_gone(text[:1500]):
            saw_gone = True
        elif text:
            last_err = f"thin_{len(text)}c"

    # Last resort: an existing Wayback capture of the original URL.
    if use_wayback and job_url:
        snap = wayback_snapshot(job_url, throttle)
        if snap:
            html, err = fetch(snap, throttle)
            if not err:
                text = html_to_text(html)
                if looks_like_job_text(text) and not looks_gone(text[:1500]):
                    return {"text": text, "source": "wayback", "status": "ok",
                            "note": snap}
                if looks_gone(text[:1500]):
                    saw_gone = True
            else:
                last_err = last_err or err

    if saw_gone:
        return {"text": "", "source": "", "status": "gone",
                "note": "posting taken down"}
    if last_err.startswith("thin_"):
        return {"text": "", "source": "", "status": "empty",
                "note": f"no posting-like text above {MIN_CHARS} chars ({last_err})"}
    return {"text": "", "source": "", "status": "error",
            "note": last_err or "no source yielded text"}
