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
import io
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

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
NYC_CSV  = os.path.join(DATA_DIR, "nyc_jobs.csv")
REM_CSV  = os.path.join(DATA_DIR, "remote_jobs.csv")

CSV_HEADERS = [
    "company_name", "title", "recruiting_season",
    "date_posted", "first_seen_date", "url", "id",
]

# ── Filters ────────────────────────────────────────────────────────────────────
NYC_KEYWORDS = ["new york, ny", "new york city", "nyc", "new york"]

def is_nyc(locations):
    return any(kw in loc.lower() for loc in locations for kw in NYC_KEYWORDS)

def is_remote(locations):
    return bool(locations) and all("remote" in loc.lower() for loc in locations)


# ── HTTP ───────────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "historical-job-scraper/2.0",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    # Intentionally NO Authorization header — the Actions GITHUB_TOKEN is
    # scoped to your repo only and causes 404s on external repos.
}

def api_get(url, retries=3):
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                remaining = int(r.headers.get("X-RateLimit-Remaining", 99))
                if remaining < 5:
                    wait = max(0, int(r.headers.get("X-RateLimit-Reset", time.time() + 61)) - time.time()) + 2
                    print(f"  [rate limit] {remaining} left — waiting {wait:.0f}s", flush=True)
                    time.sleep(wait)
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            if e.code == 403 and "rate limit" in body.lower():
                wait = max(0, int(e.headers.get("X-RateLimit-Reset", time.time() + 61)) - time.time()) + 2
                print(f"  [rate limit] 403 — waiting {wait:.0f}s", flush=True)
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
    """Load a CSV into a list of dicts. Returns empty list if file missing."""
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        w.writeheader()
        w.writerows(rows)


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

    # 1. Load existing CSVs to get known job IDs and commit SHAs
    nyc_rows = load_csv(NYC_CSV)
    rem_rows = load_csv(REM_CSV)

    seen_nyc_ids  = {r["id"] for r in nyc_rows}
    seen_rem_ids  = {r["id"] for r in rem_rows}

    # Derive already-processed commit SHAs from the first_seen_date column.
    # The CSVs store the commit date as first_seen_date; we also store the
    # commit SHA directly in the source data so we can read it back.
    # Since we don't store SHA in CSV, we use job IDs as the dedup key —
    # if a job ID already exists we skip it regardless of commit.
    print(f"  CSV: {len(nyc_rows):,} NYC jobs  |  {len(rem_rows):,} remote jobs")
    print()

    # 2. Get recent commits from GitHub API (no token)
    print("  Fetching recent commits from GitHub API …", flush=True)
    url = f"{API_BASE}/commits?path={FILE_PATH}&per_page={min(args.commits, 100)}&sha=main"
    try:
        commits = api_get(url)
    except RuntimeError as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    commit_list = [(c["sha"], c["commit"]["author"]["date"]) for c in commits]
    print(f"  Got {len(commit_list)} commits from API")
    print()

    # 3. Process each commit oldest-first, skipping if it adds nothing new
    new_nyc, new_rem, errors = [], [], 0

    for i, (sha, date) in enumerate(reversed(commit_list), 1):
        print(f"  [{i:>2}/{len(commit_list)}] {sha[:10]}  {date} … ", end="", flush=True)

        raw = fetch_raw(sha)
        if raw is None:
            print("FETCH FAILED")
            errors += 1
            continue

        try:
            listings = json.loads(raw)
        except json.JSONDecodeError:
            print("JSON ERROR")
            errors += 1
            continue

        added_nyc = added_rem = 0
        for job in listings:
            jid  = job.get("id", "")
            locs = job.get("locations", [])
            if not jid:
                continue

            terms = " | ".join(job.get("terms", []))
            row = {
                "company_name":      job.get("company_name", "").strip(),
                "title":             job.get("title", "").strip(),
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
    print(f"  Errors : {errors}")
    print(f"  New NYC jobs    : +{len(new_nyc)}")
    print(f"  New remote jobs : +{len(new_rem)}")

    if not new_nyc and not new_rem:
        print("  No new jobs — CSVs unchanged.")
        return

    # 4. Append new rows and save
    nyc_rows.extend(new_nyc)
    rem_rows.extend(new_rem)

    # Sort by first_seen_date
    nyc_rows.sort(key=lambda r: r.get("first_seen_date", ""))
    rem_rows.sort(key=lambda r: r.get("first_seen_date", ""))

    save_csv(NYC_CSV, nyc_rows)
    save_csv(REM_CSV, rem_rows)

    print()
    print(f"  data/nyc_jobs.csv    → {len(nyc_rows):,} rows total")
    print(f"  data/remote_jobs.csv → {len(rem_rows):,} rows total")
    print("  Done ✓")


if __name__ == "__main__":
    main()