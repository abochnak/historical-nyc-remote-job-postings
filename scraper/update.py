#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings -- Incremental Updater
==========================================================
Fetches recent commits to listings.json, appends new NYC/remote jobs,
archives job URLs via Wayback Machine (manual archive via Flask UI if failed),
and maintains a persistent pending_archive.csv queue so no job is
ever lost due to the per-run archive cap.

Usage
-----
    python scraper/update.py              # check last 5 commits (default)
    python scraper/update.py --commits 10
    python scraper/update.py --skip-archive
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# -- Config --------------------------------------------------------------------
OWNER     = "SimplifyJobs"
REPO      = "Summer2026-Internships"
FILE_PATH = ".github/scripts/listings.json"
API_BASE  = f"https://api.github.com/repos/{OWNER}/{REPO}"
RAW_BASE  = f"https://raw.githubusercontent.com/{OWNER}/{REPO}"

ROOT        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR    = os.path.join(ROOT, "data")
NYC_CSV     = os.path.join(DATA_DIR, "nyc_jobs.csv")
REM_CSV     = os.path.join(DATA_DIR, "remote_jobs.csv")
EXCLUDE_CSV = os.path.join(DATA_DIR, "excluded_jobs.csv")
DETAILS_CSV = os.path.join(DATA_DIR, "job_details.csv")
QUEUE_CSV      = os.path.join(DATA_DIR, "pending_archive.csv")

MAX_ARCHIVE_PER_RUN  = 5  # matches 30-min cron; typical window has 1-3 new jobs
MAX_ARCHIVE_ATTEMPTS = 3  # stop retrying after this many failures

CSV_HEADERS = [
    "company_name", "title", "recruiting_season",
    "date_posted", "first_seen_date", "url", "id",
]

DETAILS_HEADERS = [
    "id", "company_name", "title", "job_url",
    "archive_url", "archive_source",
    "archive_status",
    "category", "class_year", "degree_enrollment", "additional_skills", "language_requirements", "date_archived", "status", "first_seen_date",
]

EXCL_HEADERS = [
    "id", "company_name", "reason",
    "blocked_title", "blocked_company", "blocked_date", "job_url", "archive_url",
]

QUEUE_HEADERS = ["id", "company_name", "title", "job_url", "first_seen_date"]

# -- Filters -------------------------------------------------------------------
NYC_KEYWORDS = ["new york, ny", "new york city", "nyc", "new york"]

def is_nyc(locations):
    return any(kw in loc.lower() for loc in locations for kw in NYC_KEYWORDS)

def is_remote(locations):
    return bool(locations) and all("remote" in loc.lower() for loc in locations)


# -- HTTP helpers --------------------------------------------------------------
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


# -- Archiving -----------------------------------------------------------------
def archive_wayback(url, retries=2):
    req = urllib.request.Request(
        f"https://web.archive.org/save/{url}",
        headers={"User-Agent": "Mozilla/5.0 (compatible; job-archiver/1.0)"},
        method="GET",
    )
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                loc = r.headers.get("Content-Location", "")
                if loc:
                    return f"https://web.archive.org{loc}", "success"
                return r.url, "success"
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            if "excluded" in body.lower() or e.code == 403:
                return "", "excluded"
            if attempt < retries - 1:
                time.sleep(10)
                continue
            return "", "failed"
        except Exception:
            if attempt < retries - 1:
                time.sleep(10)
                continue
            return "", "failed"
    return "", "failed"


def do_archive(job_url):
    print(f"    archiving ... ", end="", flush=True)
    wb_url, wb_status = archive_wayback(job_url)
    if wb_status == "success":
        print("wayback OK")
        return wb_url, "wayback", "success"
    print("wayback failed -- flagged for manual archive")
    return "", "none", "failed"


# -- CSV helpers ---------------------------------------------------------------
def load_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [
            {k: v for k, v in row.items() if k is not None and k in CSV_HEADERS}
            for row in csv.DictReader(f)
        ]

def save_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    clean = [{k: v for k, v in r.items() if k in CSV_HEADERS} for r in rows]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(clean)

def load_details():
    if not os.path.exists(DETAILS_CSV):
        return {}
    with open(DETAILS_CSV, encoding="utf-8") as f:
        return {row["id"]: row for row in csv.DictReader(f) if row.get("id")}

def save_details(details_map):
    rows = sorted(details_map.values(), key=lambda r: r.get("first_seen_date", ""))
    with open(DETAILS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DETAILS_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def load_queue():
    if not os.path.exists(QUEUE_CSV):
        return []
    with open(QUEUE_CSV, encoding="utf-8") as f:
        return [row for row in csv.DictReader(f) if row.get("id")]

def save_queue(rows):
    """Persist queue. Always writes the file even when empty to avoid git churn."""
    with open(QUEUE_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=QUEUE_HEADERS, extrasaction="ignore")
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

def is_excluded(job_id, excluded_ids):
    return job_id in excluded_ids


# -- Main ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commits", type=int, default=5)
    parser.add_argument("--skip-archive", action="store_true",
                        help="Skip archiving; new jobs still added to queue")
    args = parser.parse_args()

    print("=" * 60)
    print("historical-nyc-remote-job-postings -- Updater")
    print("=" * 60)
    print(f"  Auth    : unauthenticated (public repo)")
    print(f"  Fetch   : last {args.commits} commits touching {FILE_PATH}")
    print(f"  Archive : {'skipped (--skip-archive)' if args.skip_archive else 'enabled'}")
    print()

    # 1. Load exclusions
    excluded_ids, excl_rows = load_exclusions()
    print(f"  Exclusions : {len(excluded_ids):,} IDs")

    # 2. Load existing CSVs and apply exclusions
    nyc_rows = load_csv(NYC_CSV)
    rem_rows  = load_csv(REM_CSV)
    nyc_before, rem_before = len(nyc_rows), len(rem_rows)
    nyc_rows = [r for r in nyc_rows if not is_excluded(r.get("id", ""), excluded_ids)]
    rem_rows  = [r for r in rem_rows  if not is_excluded(r.get("id", ""), excluded_ids)]
    removed_existing = (nyc_before - len(nyc_rows)) + (rem_before - len(rem_rows))
    if removed_existing:
        print(f"  Removed {removed_existing:,} rows matching exclusion rules")

    seen_nyc_ids = {r["id"] for r in nyc_rows}
    seen_rem_ids = {r["id"] for r in rem_rows}
    print(f"  CSV     : {len(nyc_rows):,} NYC  |  {len(rem_rows):,} remote")

    # 3. Load job_details and persistent queue
    details_map   = load_details()
    pending_queue = load_queue()
    # Derive watermark from max first_seen_date already in job_details
    watermark = max(
        (r.get("first_seen_date", "") for r in details_map.values()),
        default=""
    )
    # Clean queue: remove already-archived or newly excluded jobs
    pending_queue = [
        j for j in pending_queue
        if j["id"] not in details_map and not is_excluded(j["id"], excluded_ids)
    ]
    queued_ids = {j["id"] for j in pending_queue}
    print(f"  Details : {len(details_map):,} archived entries")
    print(f"  Queue   : {len(pending_queue):,} jobs pending archive")
    print()

    # 4. Fetch recent commits
    print("  Fetching recent commits from GitHub API ...", flush=True)
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
    new_nyc, new_rem, errors = [], [], 0
    for i, (sha, date) in enumerate(reversed(commit_list), 1):
        print(f"  [{i:>2}/{len(commit_list)}] {sha[:10]}  {date} ... ", end="", flush=True)
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
            if not jid or is_excluded(jid, excluded_ids):
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

            # Add to job_details if:
            # - job is new to the CSVs AND
            # - not already in details AND
            # - its first_seen_date is after the watermark (or no watermark set)
            job_date = row["first_seen_date"]
            after_watermark = (not watermark) or (job_date >= watermark)
            if is_new and jid not in details_map and jid not in queued_ids and after_watermark:
                pending_queue.append({
                    "id":              jid,
                    "company_name":    row["company_name"],
                    "title":           row["title"],
                    "job_url":         row["url"],
                    "first_seen_date": row["first_seen_date"],
                })
                queued_ids.add(jid)
                # Update watermark to track latest date processed
                if not watermark or job_date > watermark:
                    watermark = job_date
                # Pre-add to job_details as unreviewed so it shows in review queue
                # immediately, even before archiving completes
                details_map[jid] = {
                    "id":               jid,
                    "company_name":     row["company_name"],
                    "title":            row["title"],
                    "job_url":          row["url"],
                    "archive_url":      "",
                    "archive_source":   "",
                    "archive_status":   "pending",
                    "category":         "",
                    "date_archived":    "",
                    "status":           "unreviewed",
                    "first_seen_date":  row["first_seen_date"],
                }

        print(f"+{added_nyc} NYC  +{added_rem} remote")
        time.sleep(0.05)

    print()
    print(f"  Errors          : {errors}")
    print(f"  New NYC jobs    : +{len(new_nyc)}")
    print(f"  New remote jobs : +{len(new_rem)}")
    print(f"  Queue total     : {len(pending_queue):,} jobs pending archive")

    # 6. Archive from persistent queue
    if pending_queue and not args.skip_archive:
        to_archive = pending_queue[:MAX_ARCHIVE_PER_RUN]
        carry_over = len(pending_queue) - len(to_archive)
        print()
        print(f"  Archiving {len(to_archive)} jobs | {carry_over} carry over to next run")

        archived_ids = set()
        for job in to_archive:
            jid      = job["id"]
            attempts = int(job.get("archive_attempts", 0))
            print(f"  -> {job['company_name']}: {job['title'][:55]}")
            arc_url, arc_src, arc_status = do_archive(job["job_url"])
            time.sleep(3)
            # Update existing entry preserving status and category
            existing = details_map.get(jid, {})
            details_map[jid] = {
                "id":              jid,
                "company_name":    job["company_name"],
                "title":           job["title"],
                "job_url":         job["job_url"],
                "archive_url":     arc_url,
                "archive_source":  arc_src,
                "archive_status":  arc_status,
                "category":        existing.get("category", ""),
                "date_archived":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "status":          existing.get("status", "unreviewed"),
                "first_seen_date": existing.get("first_seen_date", job.get("first_seen_date", "")),
            }
            if arc_status == "success":
                archived_ids.add(jid)  # success — remove from queue
            else:
                # Failed — increment attempt count, keep in queue for retry
                job["archive_attempts"] = str(attempts + 1)
                remaining = MAX_ARCHIVE_ATTEMPTS - (attempts + 1)
                if remaining > 0:
                    print(f"     will retry ({remaining} attempt(s) remaining)")
                else:
                    print(f"     max attempts reached — flagged for manual archive")
                    archived_ids.add(jid)  # remove from queue after final failure

        # Remove archived jobs from queue; rest carry over
        pending_queue = [j for j in pending_queue if j["id"] not in archived_ids]

    # 7. Save everything
    if new_nyc or new_rem or removed_existing:
        nyc_rows.extend(new_nyc)
        rem_rows.extend(new_rem)
        save_csv(NYC_CSV, nyc_rows)
        save_csv(REM_CSV, rem_rows)
        if removed_existing:
            # Only rewrite excluded_jobs.csv when rows were actually removed
            # to avoid overwriting manual edits made via the Flask app
            save_exclusions(excl_rows)
        print()
        print(f"  data/nyc_jobs.csv    -> {len(nyc_rows):,} rows")
        print(f"  data/remote_jobs.csv -> {len(rem_rows):,} rows")

    # Always save queue -- zero data loss guarantee
    save_queue(pending_queue)
    if pending_queue:
        print(f"  data/pending_archive.csv -> {len(pending_queue):,} jobs still queued")

    # Always save job_details when new jobs were added or archiving ran
    if details_map and (new_nyc or new_rem or not args.skip_archive):
        save_details(details_map)
        failed = [r for r in details_map.values() if r.get("archive_status") == "failed"]
        print(f"  data/job_details.csv -> {len(details_map):,} entries")
        if failed:
            print(f"  Warning: {len(failed)} jobs failed to archive:")
            for r in failed[-5:]:
                print(f"     {r['company_name']}: {r['title'][:50]}")

    # Notify Discord if new jobs were found
    if new_nyc or new_rem:
        notify_discord(new_nyc, new_rem)

    if not new_nyc and not new_rem and not removed_existing:
        print("  No changes to job CSVs.")

    print("\n  Done OK")


if __name__ == "__main__":
    main()
