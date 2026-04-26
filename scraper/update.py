#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings — Incremental Updater
=========================================================
Fetches the most recent commits to listings.json via the GitHub API,
appends new NYC/remote jobs, archives job URLs, and scrapes job text.

Usage
-----
    python scraper/update.py              # check last 30 commits (default)
    python scraper/update.py --commits 10
"""

import argparse
import csv
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
OWNER     = "SimplifyJobs"
REPO      = "Summer2026-Internships"
FILE_PATH = ".github/scripts/listings.json"
API_BASE  = f"https://api.github.com/repos/{OWNER}/{REPO}"
RAW_BASE  = f"https://raw.githubusercontent.com/{OWNER}/{REPO}"

ROOT          = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR      = os.path.join(ROOT, "data")
NYC_CSV       = os.path.join(DATA_DIR, "nyc_jobs.csv")
REM_CSV       = os.path.join(DATA_DIR, "remote_jobs.csv")
EXCLUDE_CSV   = os.path.join(DATA_DIR, "excluded_jobs.csv")
DETAILS_CSV   = os.path.join(DATA_DIR, "job_details.csv")

CSV_HEADERS = [
    "company_name", "title", "recruiting_season",
    "date_posted", "first_seen_date", "url", "id",
]

DETAILS_HEADERS = [
    "id", "company_name", "title", "job_url",
    "archive_url", "archive_source",
    "archive_status",   # success | excluded | failed
    "scrape_status",    # success | failed | blocked | skipped
    "scraped_text", "category", "date_archived",
]

EXCL_HEADERS = [
    "id", "company_name", "reason",
    "blocked_title", "blocked_company", "blocked_date",
]

# ── Filters ────────────────────────────────────────────────────────────────────
NYC_KEYWORDS = ["new york, ny", "new york city", "nyc", "new york"]

def is_nyc(locations):
    return any(kw in loc.lower() for loc in locations for kw in NYC_KEYWORDS)

def is_remote(locations):
    return bool(locations) and all("remote" in loc.lower() for loc in locations)


# ── HTTP helpers ───────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "historical-job-scraper/2.0",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

def api_get(url, retries=3):
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                remaining = int(r.headers.get("X-RateLimit-Remaining", 99))
                if remaining < 5:
                    wait = max(0, int(r.headers.get("X-RateLimit-Reset", time.time()+61)) - time.time()) + 2
                    print(f"  [rate limit] waiting {wait:.0f}s", flush=True)
                    time.sleep(wait)
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            if e.code == 403 and "rate limit" in body.lower():
                wait = max(0, int(e.headers.get("X-RateLimit-Reset", time.time()+61)) - time.time()) + 2
                time.sleep(wait)
                continue
            raise RuntimeError(f"GitHub API error {e.code}: {body[:300]}") from e
    raise RuntimeError(f"Failed after {retries} retries: {url}")


def fetch_raw(sha):
    url = f"{RAW_BASE}/{sha}/{FILE_PATH}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "historical-job-scraper/2.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read()
    except Exception:
        return None


# ── Archiving ──────────────────────────────────────────────────────────────────
def archive_wayback(url, retries=2):
    """
    Submit URL to Wayback Machine. Returns (archive_url, status).
    status: 'success' | 'excluded' | 'failed'
    """
    save_url = f"https://web.archive.org/save/{url}"
    req = urllib.request.Request(
        save_url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; job-archiver/1.0)"},
        method="GET",
    )
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                # Archive URL is in Content-Location header
                content_loc = r.headers.get("Content-Location", "")
                if content_loc:
                    archive_url = f"https://web.archive.org{content_loc}"
                    return archive_url, "success"
                # Fallback: construct URL from response URL
                return r.url, "success"
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            if "excluded" in body.lower() or e.code == 403:
                return "", "excluded"
            if attempt < retries - 1:
                time.sleep(5)
                continue
            return "", "failed"
        except Exception:
            if attempt < retries - 1:
                time.sleep(5)
                continue
            return "", "failed"
    return "", "failed"


def archive_ph(url, retries=2):
    """
    Submit URL to archive.ph. Returns (archive_url, status).
    """
    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({"url": url, "anyway": "1"}).encode()
            req = urllib.request.Request(
                "https://archive.ph/submit/",
                data=data,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; job-archiver/1.0)",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            # archive.ph redirects to the archived page
            opener = urllib.request.build_opener(
                urllib.request.HTTPRedirectHandler()
            )
            with opener.open(req, timeout=60) as r:
                archive_url = r.url
                if "archive.ph" in archive_url or "archive.today" in archive_url:
                    return archive_url, "success"
                refresh = r.headers.get("Refresh", "")
                if refresh:
                    match = re.search(r"url=(.+)", refresh)
                    if match:
                        return match.group(1).strip(), "success"
            return "", "failed"
        except urllib.error.HTTPError as e:
            loc = e.headers.get("Location", "")
            if loc and ("archive.ph" in loc or "archive.today" in loc):
                return loc, "success"
            if e.code == 429 and attempt < retries - 1:
                time.sleep(10)
                continue
            return "", "failed"
        except Exception:
            if attempt < retries - 1:
                time.sleep(5)
                continue
            return "", "failed"
    return "", "failed"


def archive_url(job_url):
    """
    Try Wayback Machine first, fall back to archive.ph.
    Returns (archive_url, archive_source, archive_status).
    """
    print(f"    archiving … ", end="", flush=True)

    wb_url, wb_status = archive_wayback(job_url)
    if wb_status == "success":
        print(f"wayback ✓")
        return wb_url, "wayback", "success"
    elif wb_status == "excluded":
        print(f"wayback excluded → archive.ph … ", end="", flush=True)
        ph_url, ph_status = archive_ph(job_url)
        if ph_status == "success":
            print(f"✓")
            return ph_url, "archive.ph", "success"
        else:
            print(f"failed")
            return "", "archive.ph", "failed"
    else:
        print(f"wayback failed → archive.ph … ", end="", flush=True)
        ph_url, ph_status = archive_ph(job_url)
        if ph_status == "success":
            print(f"✓")
            return ph_url, "archive.ph", "success"
        else:
            print(f"failed")
            return "", "none", "failed"


# ── Scraping ───────────────────────────────────────────────────────────────────
def scrape_text(archive_url_str):
    """
    Fetch and extract readable text from an archived URL.
    Returns (text, status) where status is 'success' | 'failed' | 'blocked'.
    """
    if not archive_url_str:
        return "", "skipped"
    try:
        req = urllib.request.Request(
            archive_url_str,
            headers={"User-Agent": "Mozilla/5.0 (compatible; job-scraper/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            raw_html = r.read().decode(errors="replace")
    except Exception as e:
        return "", "failed"

    # Strip scripts, styles, nav boilerplate
    raw_html = re.sub(r"<script[^>]*>.*?</script>", " ", raw_html, flags=re.DOTALL | re.IGNORECASE)
    raw_html = re.sub(r"<style[^>]*>.*?</style>",  " ", raw_html, flags=re.DOTALL | re.IGNORECASE)
    raw_html = re.sub(r"<!--.*?-->", " ", raw_html, flags=re.DOTALL)

    # Remove all remaining tags
    text = re.sub(r"<[^>]+>", " ", raw_html)

    # Decode HTML entities and normalise whitespace
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()

    if len(text) < 200:
        return text, "blocked"

    # Trim to ~8000 chars to keep CSV manageable
    if len(text) > 8000:
        text = text[:8000] + " … [truncated]"

    return text, "success"


# ── CSV helpers ────────────────────────────────────────────────────────────────
def load_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [
            {k: v for k, v in row.items() if k is not None and k in CSV_HEADERS}
            for row in csv.DictReader(f)
        ]

def load_details():
    if not os.path.exists(DETAILS_CSV):
        return {}
    with open(DETAILS_CSV, encoding="utf-8") as f:
        return {row["id"]: row for row in csv.DictReader(f) if row.get("id")}

def save_details(details_map):
    rows = sorted(details_map.values(), key=lambda r: r.get("date_archived", ""))
    with open(DETAILS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DETAILS_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def load_exclusions():
    excluded_ids, all_rows = set(), []
    if not os.path.exists(EXCLUDE_CSV):
        return excluded_ids, all_rows
    with open(EXCLUDE_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            jid = (row.get("id") or "").strip()
            if jid:
                excluded_ids.add(jid)
            all_rows.append({h: row.get(h, "") for h in EXCL_HEADERS})
    return excluded_ids, all_rows

def save_exclusions(rows):
    with open(EXCLUDE_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EXCL_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def save_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    clean_rows = [{k: v for k, v in row.items() if k in CSV_HEADERS} for row in rows]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(clean_rows)

def is_excluded(job_id, excluded_ids):
    return job_id in excluded_ids


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commits", type=int, default=30)
    parser.add_argument("--skip-archive", action="store_true",
                        help="Skip archiving and scraping (for testing)")
    args = parser.parse_args()

    print("=" * 60)
    print("historical-nyc-remote-job-postings — Updater")
    print("=" * 60)
    print(f"  Auth    : unauthenticated (public repo, no token needed)")
    print(f"  Fetch   : last {args.commits} commits touching {FILE_PATH}")
    print(f"  Archive : {'skipped (--skip-archive)' if args.skip_archive else 'enabled'}")
    print()

    # 1. Load exclusion rules
    excluded_ids, excl_rows = load_exclusions()
    existing_excl_ids = {r["id"] for r in excl_rows if r.get("id")}
    print(f"  Exclusions : {len(excluded_ids):,} IDs")

    # 2. Load existing CSVs
    nyc_rows = load_csv(NYC_CSV)
    rem_rows  = load_csv(REM_CSV)

    # Apply exclusions to existing rows
    nyc_before, rem_before = len(nyc_rows), len(rem_rows)
    nyc_rows = [r for r in nyc_rows if not is_excluded(r.get("id",""), excluded_ids)]
    rem_rows  = [r for r in rem_rows  if not is_excluded(r.get("id",""), excluded_ids)]
    removed_existing = (nyc_before - len(nyc_rows)) + (rem_before - len(rem_rows))
    if removed_existing:
        print(f"  Removed {removed_existing:,} rows matching exclusion rules")

    seen_nyc_ids = {r["id"] for r in nyc_rows}
    seen_rem_ids = {r["id"] for r in rem_rows}
    print(f"  CSV        : {len(nyc_rows):,} NYC  |  {len(rem_rows):,} remote")
    print()

    # 3. Load job_details (to know which jobs are already archived)
    details_map = load_details()
    print(f"  job_details: {len(details_map):,} entries")
    print()

    # 4. Get recent commits from GitHub API
    print("  Fetching recent commits from GitHub API …", flush=True)
    url = f"{API_BASE}/commits?path={FILE_PATH}&per_page={min(args.commits, 100)}&sha=dev"
    try:
        commits = api_get(url)
    except RuntimeError as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    commit_list = [(c["sha"], c["commit"]["author"]["date"]) for c in commits]
    print(f"  Got {len(commit_list)} commits")
    print()

    # 5. Process commits oldest-first
    new_nyc, new_rem, new_details, errors = [], [], [], 0

    for i, (sha, date) in enumerate(reversed(commit_list), 1):
        print(f"  [{i:>2}/{len(commit_list)}] {sha[:10]}  {date} … ", end="", flush=True)

        raw = fetch_raw(sha)
        if raw is None:
            print("FETCH FAILED"); errors += 1; continue

        try:
            listings = json.loads(raw)
        except json.JSONDecodeError:
            print("JSON ERROR"); errors += 1; continue

        added_nyc = added_rem = 0
        for job in listings:
            jid   = job.get("id", "")
            locs  = job.get("locations", [])
            title = job.get("title", "").strip()
            if not jid:
                continue

            if is_excluded(jid, excluded_ids):
                continue

            terms = " | ".join(job.get("terms", []))
            row = {
                "company_name":      job.get("company_name", "").strip(),
                "title":             title,
                "recruiting_season": terms,
                "date_posted":       job.get("date_posted", ""),
                "first_seen_date":   date,
                "url":               job.get("url", ""),
                "id":                jid,
            }

            is_new = False
            if jid not in seen_nyc_ids and is_nyc(locs):
                seen_nyc_ids.add(jid); new_nyc.append(row); added_nyc += 1; is_new = True
            if jid not in seen_rem_ids and is_remote(locs):
                seen_rem_ids.add(jid); new_rem.append(row); added_rem += 1; is_new = True

            # Queue for archiving if brand new and not yet in job_details
            if is_new and jid not in details_map:
                new_details.append(row)

        print(f"+{added_nyc} NYC  +{added_rem} remote")
        time.sleep(0.05)

    print()
    print(f"  Errors          : {errors}")
    print(f"  New NYC jobs    : +{len(new_nyc)}")
    print(f"  New remote jobs : +{len(new_rem)}")
    print(f"  To archive      : {len(new_details)}")

    # 6. Archive and scrape new jobs
    if new_details and not args.skip_archive:
        print()
        print(f"  Archiving {len(new_details)} new jobs …")
        for job in new_details:
            jid     = job["id"]
            job_url = job["url"]
            print(f"  → {job['company_name']}: {job['title'][:50]}")

            arc_url, arc_source, arc_status = archive_url(job_url)
            time.sleep(3)  # be polite between archive requests

            scraped_text = scrape_status = ""
            if arc_status == "success" and arc_url:
                scraped_text, scrape_status = scrape_text(arc_url)
                print(f"    scrape: {scrape_status}")
            else:
                scrape_status = "skipped"

            details_map[jid] = {
                "id":             jid,
                "company_name":   job["company_name"],
                "title":          job["title"],
                "job_url":        job_url,
                "archive_url":    arc_url,
                "archive_source": arc_source,
                "archive_status": arc_status,
                "scrape_status":  scrape_status,
                "scraped_text":   scraped_text,
                "category":       "",
                "date_archived":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

    # 7. Save everything
    if new_nyc or new_rem or removed_existing:
        nyc_rows.extend(new_nyc)
        rem_rows.extend(new_rem)
        # New entries are appended at the bottom — no sort needed
        save_csv(NYC_CSV, nyc_rows)
        save_csv(REM_CSV, rem_rows)
        save_exclusions(excl_rows)
        print()
        print(f"  data/nyc_jobs.csv       → {len(nyc_rows):,} rows")
        print(f"  data/remote_jobs.csv    → {len(rem_rows):,} rows")

    if new_details and not args.skip_archive:
        save_details(details_map)
        print(f"  data/job_details.csv    → {len(details_map):,} rows")

    if not new_nyc and not new_rem and not removed_existing:
        print("  No changes to job CSVs.")

    # 8. Print archiving flags
    if details_map:
        failed_archive = [r for r in details_map.values() if r.get("archive_status") == "failed"]
        failed_scrape  = [r for r in details_map.values() if r.get("scrape_status") in ("failed","blocked")]
        if failed_archive:
            print(f"\n  ⚠️  {len(failed_archive)} jobs failed to archive:")
            for r in failed_archive[-5:]:
                print(f"     {r['company_name']}: {r['title'][:50]}")
        if failed_scrape:
            print(f"\n  ⚠️  {len(failed_scrape)} jobs failed/blocked on scrape:")
            for r in failed_scrape[-5:]:
                print(f"     [{r['scrape_status']}] {r['company_name']}: {r['title'][:50]}")

    print("\n  Done ✓")


if __name__ == "__main__":
    main()