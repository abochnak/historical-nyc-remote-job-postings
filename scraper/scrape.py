#!/usr/bin/env python3
"""
Historical NYC & Remote Job Postings Scraper
============================================
Scrapes job history entirely via GitHub APIs — no git clone required.

Two datasets are built:

  nyc_jobs    — postings whose location is strictly NYC
                ("New York, NY" / "New York City" / "NYC" / "New York")
  remote_jobs — postings where every listed location is Remote

Output
------
  data/nyc_jobs.csv
  data/remote_jobs.csv
  data/jobs.sql  (SQL dump — both tables, importable anywhere)

Usage
-----
  # Recommended: set a GitHub Personal Access Token to avoid rate limits
  #   Without token : 60  API requests/hour  (~4 hours for commit list alone)
  #   With token    : 5000 API requests/hour (~2 minutes for commit list)
  #
  # Create a token at: https://github.com/settings/tokens
  # (no scopes needed for public repos)

  export GITHUB_TOKEN=ghp_...
  python scraper/scrape.py

  # Or pass it directly:
  python scraper/scrape.py --token ghp_...

  # Re-running is safe — processed commits are checkpointed in jobs.sql.
  # Only commits seen since the last run will be fetched.
"""

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
DB_PATH  = os.path.join(DATA_DIR, "jobs.db")   # internal SQLite (not committed)
SQL_PATH = os.path.join(DATA_DIR, "jobs.sql")
NYC_CSV  = os.path.join(DATA_DIR, "nyc_jobs.csv")
REM_CSV  = os.path.join(DATA_DIR, "remote_jobs.csv")

# ── Source ─────────────────────────────────────────────────────────────────────
OWNER     = "SimplifyJobs"
REPO      = "Summer2026-Internships"
FILE_PATH = ".github/scripts/listings.json"
API_BASE  = f"https://api.github.com/repos/{OWNER}/{REPO}"
RAW_BASE  = f"https://raw.githubusercontent.com/{OWNER}/{REPO}"

# ── Tuning ─────────────────────────────────────────────────────────────────────
STEP         = 10    # sample every Nth commit  (1 = every commit, max precision)
WORKERS      = 20    # parallel raw-content fetches
PAGE_SIZE    = 100   # GitHub API max per page
RETRY_MAX    = 3     # retries on transient errors
RETRY_DELAY  = 2.0   # seconds between retries

# ── Filters ────────────────────────────────────────────────────────────────────
NYC_KEYWORDS = ["new york, ny", "new york city", "nyc", "new york"]

def is_nyc(locations):
    return any(kw in loc.lower() for loc in locations for kw in NYC_KEYWORDS)

def is_remote(locations):
    return bool(locations) and all("remote" in loc.lower() for loc in locations)


# ── GitHub API ─────────────────────────────────────────────────────────────────
def make_headers(token=None):
    h = {"User-Agent": "historical-job-scraper/2.0",
         "Accept": "application/vnd.github+json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

def api_get(url, headers, retry=RETRY_MAX):
    """GET a GitHub API URL, respecting rate limits and retrying on errors."""
    for attempt in range(retry):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as r:
                remaining  = int(r.headers.get("X-RateLimit-Remaining", 1))
                reset_ts   = int(r.headers.get("X-RateLimit-Reset", 0))
                link       = r.headers.get("Link", "")
                body       = json.loads(r.read())

                # Slow down when close to limit
                if remaining < 5:
                    wait = max(0, reset_ts - time.time()) + 1
                    print(f"\n  Rate limit nearly exhausted — sleeping {wait:.0f}s …", flush=True)
                    time.sleep(wait)

                return body, link
        except urllib.error.HTTPError as e:
            if e.code == 403:
                reset_ts = int(e.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait = max(1, reset_ts - time.time()) + 1
                print(f"\n  Rate limited (403) — sleeping {wait:.0f}s …", flush=True)
                time.sleep(wait)
            elif e.code in (429, 500, 502, 503) and attempt < retry - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                raise
        except Exception:
            if attempt < retry - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise
    return None, ""


def get_all_commits(headers):
    """
    Return list of (sha, iso_date) tuples, oldest-first, for every commit
    that touched FILE_PATH.
    """
    commits = []
    page    = 1
    total_pages = None

    while True:
        url  = f"{API_BASE}/commits?path={FILE_PATH}&per_page={PAGE_SIZE}&page={page}"
        data, link = api_get(url, headers)
        if not data:
            break

        for item in data:
            sha  = item["sha"]
            date = item["commit"]["author"]["date"]
            commits.append((sha, date))

        # Determine total pages from Link header on first call
        if total_pages is None:
            m = re.search(r'page=(\d+)>; rel="last"', link)
            total_pages = int(m.group(1)) if m else page

        print(
            f"\r  Fetching commit list … page {page}/{total_pages}"
            f"  ({len(commits):,} commits so far)   ",
            end="", flush=True,
        )

        if page >= total_pages or len(data) < PAGE_SIZE:
            break
        page += 1

    print()
    # API returns newest-first; reverse to oldest-first
    commits.reverse()
    return commits


# ── Raw content fetch ──────────────────────────────────────────────────────────
def fetch_raw(args):
    sha, date = args
    url = f"{RAW_BASE}/{sha}/{FILE_PATH}"
    for attempt in range(RETRY_MAX):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "historical-job-scraper/2.0"}
            )
            with urllib.request.urlopen(req, timeout=20) as r:
                return sha, date, r.read()
        except Exception:
            if attempt < RETRY_MAX - 1:
                time.sleep(RETRY_DELAY)
    return sha, date, None


# ── Database ───────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS nyc_jobs (
    id                TEXT PRIMARY KEY,
    company_name      TEXT,
    title             TEXT,
    terms             TEXT,
    date_posted       TEXT,
    first_seen_date   TEXT,
    url               TEXT,
    source            TEXT,
    first_seen_commit TEXT
);
CREATE TABLE IF NOT EXISTS remote_jobs (
    id                TEXT PRIMARY KEY,
    company_name      TEXT,
    title             TEXT,
    terms             TEXT,
    date_posted       TEXT,
    first_seen_date   TEXT,
    url               TEXT,
    source            TEXT,
    first_seen_commit TEXT
);
CREATE TABLE IF NOT EXISTS done_commits (
    sha TEXT PRIMARY KEY
);
"""

INSERT = (
    "INSERT OR IGNORE INTO {table} "
    "(id,company_name,title,terms,date_posted,first_seen_date,url,source,first_seen_commit) "
    "VALUES (?,?,?,?,?,?,?,?,?)"
)

def init_db(conn):
    conn.executescript(SCHEMA)
    conn.commit()


# ── Processing ─────────────────────────────────────────────────────────────────
def process_listing(sha, date, raw, seen_nyc, seen_rem):
    nyc_rows, rem_rows = [], []
    try:
        listings = json.loads(raw)
    except Exception:
        return nyc_rows, rem_rows

    for job in listings:
        jid  = job.get("id", "")
        if not jid:
            continue
        locs = job.get("locations", [])
        row  = (
            jid,
            job.get("company_name", ""),
            job.get("title", ""),
            json.dumps(job.get("terms", [])),
            job.get("date_posted", ""),
            date,
            job.get("url", ""),
            job.get("source", ""),
            sha,
        )
        if jid not in seen_nyc and is_nyc(locs):
            seen_nyc.add(jid)
            nyc_rows.append(row)
        if jid not in seen_rem and is_remote(locs):
            seen_rem.add(jid)
            rem_rows.append(row)

    return nyc_rows, rem_rows


def run_chunk(conn, seen_nyc, seen_rem, chunk):
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        results = list(ex.map(fetch_raw, chunk))

    nyc_all, rem_all, done_all = [], [], []
    errors = 0
    for sha, date, raw in results:
        done_all.append((sha,))
        if raw is None:
            errors += 1
            continue
        nb, rb = process_listing(sha, date, raw, seen_nyc, seen_rem)
        nyc_all.extend(nb)
        rem_all.extend(rb)

    if nyc_all:
        conn.executemany(INSERT.format(table="nyc_jobs"),    nyc_all)
    if rem_all:
        conn.executemany(INSERT.format(table="remote_jobs"), rem_all)
    conn.executemany("INSERT OR IGNORE INTO done_commits VALUES (?)", done_all)
    conn.commit()
    return len(nyc_all), len(rem_all), errors


# ── SQL export ─────────────────────────────────────────────────────────────────
def export_sql(conn):
    with open(SQL_PATH, "w", encoding="utf-8") as f:
        f.write("-- historical-nyc-remote-job-postings\n")
        f.write(f"-- Source: {OWNER}/{REPO}\n")
        f.write(f"-- Generated: {time.strftime('%Y-%m-%d')}\n\n")
        f.write("BEGIN TRANSACTION;\n\n")
        for table in ["nyc_jobs", "remote_jobs"]:
            schema = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()[0]
            f.write(f"{schema};\n\n")
            rows = conn.execute(f"SELECT * FROM {table}").fetchall()
            for row in rows:
                vals = ", ".join(
                    "NULL" if v is None
                    else "'" + str(v).replace("'", "''") + "'"
                    for v in row
                )
                f.write(f"INSERT INTO {table} VALUES ({vals});\n")
            f.write(f"\n-- {len(rows):,} rows in {table}\n\n")
        f.write("COMMIT;\n")


# ── CSV export ─────────────────────────────────────────────────────────────────
CSV_HEADERS = ["company_name", "title", "recruiting_season",
               "date_posted", "first_seen_date", "url", "id"]

def export_csv(conn, table, path):
    rows = conn.execute(
        f"SELECT company_name,title,terms,date_posted,first_seen_date,url,id "
        f"FROM {table} ORDER BY first_seen_date,company_name,title"
    ).fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADERS)
        for co, title, terms, dp, fsd, url, jid in rows:
            try:
                terms = " | ".join(json.loads(terms))
            except Exception:
                pass
            w.writerow([co, title, terms, dp, fsd, url, jid])
    return len(rows)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Scrape NYC & remote job history from GitHub")
    parser.add_argument("--token", help="GitHub Personal Access Token (or set GITHUB_TOKEN env var)")
    parser.add_argument("--step",  type=int, default=STEP,
                        help=f"Sample every Nth commit (default: {STEP}; 1 = every commit)")
    parser.add_argument("--workers", type=int, default=WORKERS,
                        help=f"Parallel fetch workers (default: {WORKERS})")
    args = parser.parse_args()

    token = args.token or os.environ.get("GITHUB_TOKEN")
    step  = args.step
    headers = make_headers(token)

    if not token:
        print("⚠️  No GitHub token provided.")
        print("   Unauthenticated rate limit: 60 req/hr (~4 hours for commit list).")
        print("   Set GITHUB_TOKEN or pass --token for 5000 req/hr.\n")

    print("=" * 60)
    print("Historical NYC & Remote Job Postings Scraper")
    print(f"  Source : {OWNER}/{REPO}")
    print(f"  File   : {FILE_PATH}")
    print(f"  Step   : every {step}th commit")
    print("=" * 60)

    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    seen_nyc   = set(r[0] for r in conn.execute("SELECT id FROM nyc_jobs"))
    seen_rem   = set(r[0] for r in conn.execute("SELECT id FROM remote_jobs"))
    done_shas  = set(r[0] for r in conn.execute("SELECT sha FROM done_commits"))

    print(f"  Existing NYC jobs    : {len(seen_nyc):,}")
    print(f"  Existing remote jobs : {len(seen_rem):,}")
    print(f"  Processed commits    : {len(done_shas):,}\n")

    # ── 1. Get full commit list ────────────────────────────────────────────────
    all_commits = get_all_commits(headers)
    print(f"  Total commits touching file : {len(all_commits):,}")

    # ── 2. Sample and filter pending ──────────────────────────────────────────
    indices  = set([0, len(all_commits) - 1]) | set(range(0, len(all_commits), step))
    sampled  = [all_commits[i] for i in sorted(indices)]
    pending  = [(sha, date) for sha, date in sampled if sha not in done_shas]
    print(f"  Sample points               : {len(sampled):,}  (step={step})")
    print(f"  Pending (not yet fetched)   : {len(pending):,}\n")

    if not pending:
        print("  Already up to date — nothing to fetch.")
    else:
        CHUNK   = args.workers * 2
        t0      = time.time()
        tot_nyc = tot_rem = tot_err = 0

        for start in range(0, len(pending), CHUNK):
            chunk = pending[start : start + CHUNK]
            new_nyc, new_rem, errs = run_chunk(conn, seen_nyc, seen_rem, chunk)
            tot_nyc += new_nyc
            tot_rem += new_rem
            tot_err += errs

            done    = start + len(chunk)
            elapsed = time.time() - t0 or 1
            eta     = (len(pending) - done) / (done / elapsed)
            nyc_total = conn.execute("SELECT COUNT(*) FROM nyc_jobs").fetchone()[0]
            rem_total = conn.execute("SELECT COUNT(*) FROM remote_jobs").fetchone()[0]
            print(
                f"\r  [{done/len(pending)*100:5.1f}%] {done:>5,}/{len(pending):,}"
                f"  NYC: {nyc_total:,}  Remote: {rem_total:,}"
                f"  ETA: {eta:.0f}s   ",
                end="", flush=True,
            )

        print(f"\n\n  Finished in {time.time()-t0:.0f}s")
        print(f"  New NYC jobs    : +{tot_nyc:,}")
        print(f"  New remote jobs : +{tot_rem:,}")
        if tot_err:
            print(f"  Fetch errors    : {tot_err:,} (skipped)")

    # ── 3. Export ──────────────────────────────────────────────────────────────
    print("\nExporting …")
    n = export_csv(conn, "nyc_jobs",    NYC_CSV)
    r = export_csv(conn, "remote_jobs", REM_CSV)
    export_sql(conn)

    nyc_total = conn.execute("SELECT COUNT(*) FROM nyc_jobs").fetchone()[0]
    rem_total = conn.execute("SELECT COUNT(*) FROM remote_jobs").fetchone()[0]
    print(f"  data/nyc_jobs.csv    — {n:,} rows")
    print(f"  data/remote_jobs.csv — {r:,} rows")
    print(f"  data/jobs.sql        — {nyc_total:,} NYC  |  {rem_total:,} remote")
    conn.close()
    print("\nDone ✓")


if __name__ == "__main__":
    main()