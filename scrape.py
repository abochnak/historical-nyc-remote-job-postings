#!/usr/bin/env python3
"""
Historical NYC & Remote Job Postings Scraper
============================================
Scans the full git history of SimplifyJobs/Summer2026-Internships,
building two datasets:

  nyc_jobs   — postings whose location includes New York (strict: "New York, NY",
                "New York City", "NYC", or "New York")
  remote_jobs — postings where every listed location is Remote

Output
------
  data/nyc_jobs.csv
  data/remote_jobs.csv
  data/jobs.db  (SQLite — both tables)

Usage
-----
  # First time (or to update):
  git clone https://github.com/SimplifyJobs/Summer2026-Internships.git repo
  python scraper/scrape.py

  # Re-running is safe — already-processed blobs are skipped via the
  # done_blobs checkpoint table inside jobs.db.

  # For full precision (sample every commit instead of every 10th):
  #   Set STEP = 1 below. Requires ~4.7 GB of blob data and takes longer.
"""

import subprocess, json, sqlite3, csv, os, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO     = os.path.join(ROOT, "repo")
DATA_DIR = os.path.join(ROOT, "data")
DB_PATH  = os.path.join(DATA_DIR, "jobs.db")
NYC_CSV  = os.path.join(DATA_DIR, "nyc_jobs.csv")
REM_CSV  = os.path.join(DATA_DIR, "remote_jobs.csv")

# ── Source repo ────────────────────────────────────────────────────────────────
REPO_URL  = "https://github.com/SimplifyJobs/Summer2026-Internships.git"
FILE_PATH = ".github/scripts/listings.json"
RAW_BASE  = "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships"

# ── Tuning ─────────────────────────────────────────────────────────────────────
STEP     = 10   # sample every Nth unique blob (1 = full precision, ~4.7 GB)
WORKERS  = 15   # concurrent HTTP workers
CHUNK    = 50   # blobs per batch

# ── Filters ────────────────────────────────────────────────────────────────────
NYC_KEYWORDS = ["new york, ny", "new york city", "nyc", "new york"]

def is_nyc(locations):
    """True if any location string matches a strict NYC keyword."""
    return any(kw in loc.lower() for loc in locations for kw in NYC_KEYWORDS)

def is_remote(locations):
    """True if the job lists at least one location and all of them are Remote."""
    return bool(locations) and all("remote" in loc.lower() for loc in locations)


# ── Git helpers ────────────────────────────────────────────────────────────────
def get_unique_blobs():
    """
    Return [(commit_hash, iso_date, blob_hash), ...] oldest-first.
    Uses `git log --raw` to get blob hashes without per-commit subprocess calls.
    Deduplicates consecutive identical blobs.
    """
    r = subprocess.run(
        ["git", "log", "--raw", "--format=COMMIT %H %aI", "--reverse", "--", FILE_PATH],
        cwd=REPO, capture_output=True, text=True,
    )
    entries, ch, cd = [], None, None
    for line in r.stdout.splitlines():
        if line.startswith("COMMIT "):
            p = line.split(" ", 2); ch, cd = p[1], p[2]
        elif FILE_PATH in line:
            cols = line.split()
            if len(cols) >= 4 and ch:
                entries.append((ch, cd, cols[3]))

    seen, unique = set(), []
    for h, d, b in entries:
        if b not in seen:
            seen.add(b); unique.append((h, d, b))
    return unique


# ── HTTP fetch ─────────────────────────────────────────────────────────────────
def fetch(args):
    h, d, b = args
    try:
        req = urllib.request.Request(
            f"{RAW_BASE}/{h}/{FILE_PATH}",
            headers={"User-Agent": "historical-job-scraper/1.0"},
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            return h, d, b, r.read()
    except Exception:
        return h, d, b, None


# ── Database ───────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS nyc_jobs (
    id                TEXT PRIMARY KEY,
    company_name      TEXT,
    title             TEXT,
    terms             TEXT,   -- JSON list, e.g. '["Summer 2026"]'
    date_posted       TEXT,
    first_seen_date   TEXT,   -- ISO-8601 git author date of first appearance
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
CREATE TABLE IF NOT EXISTS done_blobs (
    blob TEXT PRIMARY KEY       -- tracks processed blob hashes for resume
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
def process_blob(h, d, raw, seen_nyc, seen_rem):
    nyc_rows, rem_rows = [], []
    try:
        listings = json.loads(raw)
    except (json.JSONDecodeError, Exception):
        return nyc_rows, rem_rows

    for job in listings:
        jid = job.get("id", "")
        if not jid:
            continue
        locs = job.get("locations", [])
        row = (
            jid,
            job.get("company_name", ""),
            job.get("title", ""),
            json.dumps(job.get("terms", [])),
            job.get("date_posted", ""),
            d,
            job.get("url", ""),
            job.get("source", ""),
            h,
        )
        if jid not in seen_nyc and is_nyc(locs):
            seen_nyc.add(jid); nyc_rows.append(row)
        if jid not in seen_rem and is_remote(locs):
            seen_rem.add(jid); rem_rows.append(row)

    return nyc_rows, rem_rows


def run_chunk(conn, seen_nyc, seen_rem, chunk):
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        results = list(ex.map(fetch, chunk))

    nyc_all, rem_all, done_all = [], [], []
    for h, d, b, raw in results:
        done_all.append((b,))
        if raw:
            nb, rb = process_blob(h, d, raw, seen_nyc, seen_rem)
            nyc_all.extend(nb)
            rem_all.extend(rb)

    if nyc_all:
        conn.executemany(INSERT.format(table="nyc_jobs"), nyc_all)
    if rem_all:
        conn.executemany(INSERT.format(table="remote_jobs"), rem_all)
    conn.executemany("INSERT OR IGNORE INTO done_blobs VALUES (?)", done_all)
    conn.commit()
    return len(nyc_all), len(rem_all)


# ── CSV export ─────────────────────────────────────────────────────────────────
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


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    if not os.path.isdir(REPO):
        print(f"Repo not found at '{REPO}'.")
        print(f"Run:  git clone {REPO_URL} {REPO}")
        raise SystemExit(1)

    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    seen_nyc  = set(r[0] for r in conn.execute("SELECT id FROM nyc_jobs"))
    seen_rem  = set(r[0] for r in conn.execute("SELECT id FROM remote_jobs"))
    done_blobs = set(r[0] for r in conn.execute("SELECT blob FROM done_blobs"))

    print("Fetching commit/blob list from repo…", flush=True)
    unique  = get_unique_blobs()
    indices = set([0, len(unique) - 1]) | set(range(0, len(unique), STEP))
    sampled = [unique[i] for i in sorted(indices)]
    pending = [(h, d, b) for h, d, b in sampled if b not in done_blobs]

    print(f"  Unique blobs : {len(unique):,}")
    print(f"  Sample size  : {len(sampled):,}  (STEP={STEP})")
    print(f"  Pending      : {len(pending):,}")
    print(f"  NYC jobs so far  : {len(seen_nyc):,}")
    print(f"  Remote jobs so far: {len(seen_rem):,}")

    if not pending:
        print("Already up to date.")
    else:
        t0 = time.time()
        total_nyc = total_rem = 0
        for start in range(0, len(pending), CHUNK):
            chunk = pending[start : start + CHUNK]
            new_nyc, new_rem = run_chunk(conn, seen_nyc, seen_rem, chunk)
            total_nyc += new_nyc
            total_rem  += new_rem
            done = start + len(chunk)
            elapsed = time.time() - t0 or 1
            eta = (len(pending) - done) / (done / elapsed)
            print(
                f"\r  [{done/len(pending)*100:5.1f}%] {done:>5,}/{len(pending):,} blobs  "
                f"NYC: +{total_nyc}  Remote: +{total_rem}  ETA: {eta:.0f}s   ",
                end="", flush=True,
            )
        print(f"\n  Done in {time.time()-t0:.0f}s")

    nyc_total = conn.execute("SELECT COUNT(*) FROM nyc_jobs").fetchone()[0]
    rem_total = conn.execute("SELECT COUNT(*) FROM remote_jobs").fetchone()[0]
    print(f"\nExporting CSVs…")
    n = export_csv(conn, "nyc_jobs",    NYC_CSV)
    r = export_csv(conn, "remote_jobs", REM_CSV)
    print(f"  data/nyc_jobs.csv    — {n:,} rows")
    print(f"  data/remote_jobs.csv — {r:,} rows")
    print(f"  data/jobs.db         — {nyc_total:,} NYC  |  {rem_total:,} remote")
    conn.close()


if __name__ == "__main__":
    main()
