#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings — Incremental Updater
=========================================================
Fetches the most recent commits to listings.json via the GitHub API
(no token needed — public repo) and appends any new NYC/remote jobs
to the existing CSV files.

No SQLite required. State is derived entirely from the committed CSVs,
so GitHub Actions always has the correct baseline on every run.

Usage
-----
    python scraper/update.py              # check last 30 commits (default)
    python scraper/update.py --commits 10
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
OWNER     = "SimplifyJobs"
REPO      = "Summer2026-Internships"
FILE_PATH = ".github/scripts/listings.json"
API_BASE  = f"https://api.github.com/repos/{OWNER}/{REPO}"
RAW_BASE  = f"https://raw.githubusercontent.com/{OWNER}/{REPO}"

ROOT         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR     = os.path.join(ROOT, "data")
NYC_CSV      = os.path.join(DATA_DIR, "nyc_jobs.csv")
REM_CSV      = os.path.join(DATA_DIR, "remote_jobs.csv")
EXCLUDE_CSV  = os.path.join(DATA_DIR, "excluded_jobs.csv")

CSV_HEADERS = [
    "company_name", "title", "recruiting_season",
    "date_posted", "first_seen_date", "url", "id",
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


# ── CSV helpers ────────────────────────────────────────────────────────────────
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
    clean_rows = [{k: v for k, v in row.items() if k in CSV_HEADERS} for row in rows]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(clean_rows)

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


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commits", type=int, default=30,
                        help="Number of recent commits to check (default: 30)")
    args = parser.parse_args()

    print("=" * 60)
    print("historical-nyc-remote-job-postings — Updater")
    print("=" * 60)
    print(f"  Auth  : unauthenticated (public repo, no token needed)")
    print(f"  Fetch : last {args.commits} commits touching {FILE_PATH}")
    print()

    # 1. Load exclusion rules
    excluded_ids, excl_rows = load_exclusions()
    print(f"  Exclusions : {len(excluded_ids):,} IDs")

    # 2. Load existing CSVs
    nyc_rows = load_csv(NYC_CSV)
    rem_rows  = load_csv(REM_CSV)

    # Apply exclusions to existing rows
    nyc_before, rem_before = len(nyc_rows), len(rem_rows)
    nyc_rows = [r for r in nyc_rows if not is_excluded(r.get("id", ""), excluded_ids)]
    rem_rows  = [r for r in rem_rows  if not is_excluded(r.get("id", ""), excluded_ids)]
    removed_existing = (nyc_before - len(nyc_rows)) + (rem_before - len(rem_rows))
    if removed_existing:
        print(f"  Removed {removed_existing:,} rows matching exclusion rules")

    seen_nyc_ids = {r["id"] for r in nyc_rows}
    seen_rem_ids = {r["id"] for r in rem_rows}
    print(f"  CSV : {len(nyc_rows):,} NYC  |  {len(rem_rows):,} remote")
    print()

    # 3. Get recent commits from GitHub API
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

    # 4. Process commits oldest-first
    new_nyc, new_rem, errors = [], [], 0

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

            if jid not in seen_nyc_ids and is_nyc(locs):
                seen_nyc_ids.add(jid)
                new_nyc.append(row)
                added_nyc += 1

            if jid not in seen_rem_ids and is_remote(locs):
                seen_rem_ids.add(jid)
                new_rem.append(row)
                added_rem += 1

        print(f"+{added_nyc} NYC  +{added_rem} remote")
        time.sleep(0.05)

    print()
    print(f"  Errors          : {errors}")
    print(f"  New NYC jobs    : +{len(new_nyc)}")
    print(f"  New remote jobs : +{len(new_rem)}")

    if not new_nyc and not new_rem and not removed_existing:
        print("  No changes — CSVs unchanged.")
        return

    # 5. Append new rows and save
    nyc_rows.extend(new_nyc)
    rem_rows.extend(new_rem)
    # New entries appended at the bottom — no sort

    save_csv(NYC_CSV, nyc_rows)
    save_csv(REM_CSV, rem_rows)
    save_exclusions(excl_rows)

    print()
    print(f"  data/nyc_jobs.csv    → {len(nyc_rows):,} rows")
    print(f"  data/remote_jobs.csv → {len(rem_rows):,} rows")
    print("  Done ✓")


if __name__ == "__main__":
    main()