#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings — Incremental Updater
=========================================================
Fetches only the most recent commits to listings.json via the GitHub API,
then updates data/nyc_jobs.csv, data/remote_jobs.csv, and data/jobs.sql.

No git clone required. No token required for public repos
(GitHub allows 60 unauthenticated API requests/hour; a token raises this
to 5,000/hr — see GITHUB_TOKEN below if you need it).

Usage
-----
    python scraper/update.py              # fetch last 30 commits (default)
    python scraper/update.py --commits 5  # fetch last N commits
    GITHUB_TOKEN=ghp_... python scraper/update.py  # authenticated (optional)
"""

import json
import os
import csv
import sys
import time
import sqlite3
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
OWNER      = "SimplifyJobs"
REPO       = "Summer2026-Internships"
FILE_PATH  = ".github/scripts/listings.json"
API_BASE   = f"https://api.github.com/repos/{OWNER}/{REPO}"
RAW_BASE   = f"https://raw.githubusercontent.com/{OWNER}/{REPO}"

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
DB_PATH  = os.path.join(DATA_DIR, "jobs.db")       # SQLite (local working copy)
SQL_PATH = os.path.join(DATA_DIR, "jobs.sql")       # plain-text export
NYC_CSV  = os.path.join(DATA_DIR, "nyc_jobs.csv")
REM_CSV  = os.path.join(DATA_DIR, "remote_jobs.csv")

# ── Filters ───────────────────────────────────────────────────────────────────
NYC_KEYWORDS = ["new york, ny", "new york city", "nyc", "new york"]

def is_nyc(locations):
    return any(kw in loc.lower() for loc in locations for kw in NYC_KEYWORDS)

def is_remote(locations):
    return bool(locations) and all("remote" in loc.lower() for loc in locations)


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def _headers():
    h = {
        "User-Agent": "historical-job-scraper/2.0",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def api_get(url, retries=3):
    """GET a GitHub API URL, return parsed JSON. Raises on rate-limit or error."""
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=_headers())
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                remaining = int(r.headers.get("X-RateLimit-Remaining", 60))
                if remaining < 5:
                    reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
                    wait  = max(0, reset - time.time()) + 2
                    print(f"  [rate limit] {remaining} calls left — waiting {wait:.0f}s", flush=True)
                    time.sleep(wait)
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            if e.code == 403 and "rate limit" in body.lower():
                reset = int(e.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait  = max(0, reset - time.time()) + 2
                print(f"  [rate limit] 403 — waiting {wait:.0f}s", flush=True)
                time.sleep(wait)
                continue
            raise RuntimeError(f"GitHub API error {e.code}: {body[:200]}") from e
    raise RuntimeError(f"Failed after {retries} retries: {url}")


def fetch_raw(commit_sha):
    """Fetch the raw listings.json at a given commit SHA. Returns bytes or None."""
    url = f"{RAW_BASE}/{commit_sha}/{FILE_PATH}"
    req = urllib.request.Request(url, headers={"User-Agent": "historical-job-scraper/2.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read()
    except Exception:
        return None


# ── GitHub API calls ──────────────────────────────────────────────────────────
def get_recent_commits(n=30):
    """
    Return a list of (sha, iso_date) for the N most recent commits touching
    FILE_PATH, newest first.
    """
    url = (
        f"{API_BASE}/commits"
        f"?path={FILE_PATH}&per_page={min(n, 100)}&sha=main"
    )
    data = api_get(url)
    return [
        (c["sha"], c["commit"]["author"]["date"])
        for c in data
    ]


# ── Database ──────────────────────────────────────────────────────────────────
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
CREATE TABLE IF NOT EXISTS processed_commits (
    sha  TEXT PRIMARY KEY,
    date TEXT
);
"""

INSERT = (
    "INSERT OR IGNORE INTO {table} "
    "(id,company_name,title,terms,date_posted,first_seen_date,url,source,first_seen_commit) "
    "VALUES (?,?,?,?,?,?,?,?,?)"
)


def open_db():
    """Open (or create) the working SQLite DB, seeded from jobs.sql if needed."""
    fresh = not os.path.exists(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.commit()

    if fresh and os.path.exists(SQL_PATH):
        print("  Seeding jobs.db from jobs.sql …", flush=True)
        with open(SQL_PATH, encoding="utf-8") as f:
            sql = f.read()
        conn.executescript(sql)
        conn.commit()
        # Populate processed_commits from the data already in both tables
        conn.execute("""
            INSERT OR IGNORE INTO processed_commits (sha, date)
            SELECT first_seen_commit, first_seen_date FROM nyc_jobs
            UNION
            SELECT first_seen_commit, first_seen_date FROM remote_jobs
        """)
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM processed_commits").fetchone()[0]
        print(f"  Seeded {n:,} processed commit records from existing data.", flush=True)

    return conn


# ── Processing ────────────────────────────────────────────────────────────────
def process_listing(job, commit_sha, commit_date, seen_nyc, seen_rem):
    nyc_row = rem_row = None
    jid  = job.get("id", "")
    locs = job.get("locations", [])
    if not jid:
        return nyc_row, rem_row
    row = (
        jid,
        job.get("company_name", ""),
        job.get("title", ""),
        json.dumps(job.get("terms", [])),
        job.get("date_posted", ""),
        commit_date,
        job.get("url", ""),
        job.get("source", ""),
        commit_sha,
    )
    if jid not in seen_nyc and is_nyc(locs):
        seen_nyc.add(jid); nyc_row = row
    if jid not in seen_rem and is_remote(locs):
        seen_rem.add(jid); rem_row = row
    return nyc_row, rem_row


# ── CSV / SQL export ──────────────────────────────────────────────────────────
HEADERS = ["company_name", "title", "recruiting_season",
           "date_posted", "first_seen_date", "url", "id"]


def export_csv(conn, table, path):
    rows = conn.execute(
        f"SELECT company_name,title,terms,date_posted,first_seen_date,url,id "
        f"FROM {table} ORDER BY first_seen_date,company_name,title"
    ).fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADERS)
        for co, title, terms, dp, fsd, url, jid in rows:
            try:
                terms = " | ".join(json.loads(terms))
            except Exception:
                pass
            w.writerow([co, title, terms, dp, fsd, url, jid])
    return len(rows)


def export_sql(conn, path):
    """Write a clean plain-text SQL dump of nyc_jobs and remote_jobs."""
    nyc_n = conn.execute("SELECT COUNT(*) FROM nyc_jobs").fetchone()[0]
    rem_n = conn.execute("SELECT COUNT(*) FROM remote_jobs").fetchone()[0]
    now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"-- historical-nyc-remote-job-postings\n")
        f.write(f"-- Source: {OWNER}/{REPO}\n")
        f.write(f"-- Updated: {now}\n")
        f.write(f"-- Tables: nyc_jobs ({nyc_n:,} rows), remote_jobs ({rem_n:,} rows)\n\n")
        f.write("BEGIN TRANSACTION;\n\n")
        for table in ("nyc_jobs", "remote_jobs"):
            schema = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()[0]
            f.write(f"{schema};\n\n")
            for row in conn.execute(f"SELECT * FROM {table}"):
                vals = ", ".join(
                    "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"
                    for v in row
                )
                f.write(f"INSERT INTO {table} VALUES ({vals});\n")
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            f.write(f"\n-- {n:,} rows in {table}\n\n")
        f.write("COMMIT;\n")
    return nyc_n, rem_n


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Incrementally update NYC/remote job data from GitHub.")
    parser.add_argument("--commits", type=int, default=30, metavar="N",
                        help="Number of recent commits to check (default: 30)")
    args = parser.parse_args()

    print("=" * 60)
    print("historical-nyc-remote-job-postings — Updater")
    print("=" * 60)

    token = os.environ.get("GITHUB_TOKEN", "")
    print(f"  Auth  : {'token set ✓' if token else 'unauthenticated (60 req/hr limit)'}")
    print(f"  Fetch : last {args.commits} commits touching {FILE_PATH}")
    print()

    # 1. Open/seed DB
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = open_db()

    seen_nyc  = set(r[0] for r in conn.execute("SELECT id FROM nyc_jobs"))
    seen_rem  = set(r[0] for r in conn.execute("SELECT id FROM remote_jobs"))
    done_shas = set(r[0] for r in conn.execute("SELECT sha FROM processed_commits"))

    print(f"  DB: {len(seen_nyc):,} NYC jobs  |  {len(seen_rem):,} remote jobs  |  {len(done_shas):,} commits processed")
    print()

    # 2. Fetch recent commits from GitHub API
    print(f"  Fetching commit list from GitHub API …", flush=True)
    try:
        commits = get_recent_commits(args.commits)
    except RuntimeError as e:
        print(f"  ERROR: {e}")
        print("  If you're hitting the 60 req/hr limit, set GITHUB_TOKEN env var.")
        sys.exit(1)

    pending = [(sha, date) for sha, date in commits if sha not in done_shas]
    print(f"  Found {len(commits)} recent commits  →  {len(pending)} not yet processed")
    print()

    if not pending:
        print("  Already up to date — nothing new to fetch.")
        conn.close()
        return

    # 3. Fetch and process each new commit (oldest first so first_seen_date is accurate)
    new_nyc = new_rem = errors = 0
    t0 = time.time()

    for i, (sha, date) in enumerate(reversed(pending), 1):
        print(f"  [{i}/{len(pending)}] {sha[:10]}  {date} … ", end="", flush=True)

        raw = fetch_raw(sha)
        if raw is None:
            print("FETCH FAILED")
            errors += 1
            conn.execute("INSERT OR IGNORE INTO processed_commits VALUES (?,?)", (sha, date))
            conn.commit()
            continue

        try:
            listings = json.loads(raw)
        except json.JSONDecodeError:
            print("JSON ERROR")
            errors += 1
            conn.execute("INSERT OR IGNORE INTO processed_commits VALUES (?,?)", (sha, date))
            conn.commit()
            continue

        nyc_batch, rem_batch = [], []
        for job in listings:
            nr, rr = process_listing(job, sha, date, seen_nyc, seen_rem)
            if nr: nyc_batch.append(nr)
            if rr: rem_batch.append(rr)

        if nyc_batch:
            conn.executemany(INSERT.format(table="nyc_jobs"), nyc_batch)
        if rem_batch:
            conn.executemany(INSERT.format(table="remote_jobs"), rem_batch)
        conn.execute("INSERT OR IGNORE INTO processed_commits VALUES (?,?)", (sha, date))
        conn.commit()

        new_nyc += len(nyc_batch)
        new_rem += len(rem_batch)
        print(f"+{len(nyc_batch)} NYC  +{len(rem_batch)} remote")
        time.sleep(0.1)  # be polite to raw CDN

    elapsed = time.time() - t0
    print()
    print(f"  Processed {len(pending)} commits in {elapsed:.1f}s  |  errors: {errors}")
    print(f"  New NYC jobs   : +{new_nyc}")
    print(f"  New remote jobs: +{new_rem}")
    print()

    if new_nyc == 0 and new_rem == 0:
        print("  No new jobs found in these commits.")
        conn.close()
        return

    # 4. Export
    print("  Exporting …", flush=True)
    n_nyc = export_csv(conn, "nyc_jobs",    NYC_CSV)
    n_rem = export_csv(conn, "remote_jobs", REM_CSV)
    s_nyc, s_rem = export_sql(conn, SQL_PATH)
    print(f"  data/nyc_jobs.csv    → {n_nyc:,} rows")
    print(f"  data/remote_jobs.csv → {n_rem:,} rows")
    print(f"  data/jobs.sql        → {s_nyc:,} NYC  +  {s_rem:,} remote")
    print()
    print("  Done ✓  Commit data/ to persist the update.")
    conn.close()


if __name__ == "__main__":
    main()