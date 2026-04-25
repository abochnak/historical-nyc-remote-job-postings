#!/usr/bin/env python3
"""
NYC Internship Scraper v2
- Strict NYC filter: 'New York, NY' / 'New York City' / 'NYC' / 'New York' only
- Remote jobs table: jobs where ALL locations are remote
- CSV exports without a locations column
"""
import subprocess, json, sqlite3, csv, os, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

REPO     = '/home/claude/repo'
FP       = '.github/scripts/listings.json'
BASE     = 'https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships'
DB_PATH  = '/home/claude/nyc_jobs_v2.db'
NYC_CSV  = '/home/claude/nyc_jobs_v2.csv'
REM_CSV  = '/home/claude/remote_jobs_v2.csv'

# Strict NYC keywords only
NYC = ['new york, ny', 'new york city', 'nyc', 'new york']
def is_nyc(locs): return any(kw in l.lower() for l in locs for kw in NYC)

# Strictly remote: every location string contains 'remote'
def is_remote(locs):
    return bool(locs) and all('remote' in l.lower() for l in locs)

def fetch(args):
    h, d, b = args
    try:
        req = urllib.request.Request(f'{BASE}/{h}/{FP}', headers={'User-Agent':'scraper/1.0'})
        with urllib.request.urlopen(req, timeout=20) as r:
            return h, d, b, r.read()
    except:
        return h, d, b, None

def get_unique_blobs():
    r = subprocess.run(
        ['git','log','--raw','--format=COMMIT %H %aI','--reverse','--',FP],
        cwd=REPO, capture_output=True, text=True
    )
    entries, ch, cd = [], None, None
    for line in r.stdout.splitlines():
        if line.startswith('COMMIT '): p=line.split(' ',2); ch,cd=p[1],p[2]
        elif FP in line:
            cols=line.split()
            if len(cols)>=4 and ch: entries.append((ch,cd,cols[3]))
    seen, unique = set(), []
    for h,d,b in entries:
        if b not in seen: seen.add(b); unique.append((h,d,b))
    return unique

def init_db(conn):
    conn.executescript("""
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
        CREATE TABLE IF NOT EXISTS done_blobs (blob TEXT PRIMARY KEY);
    """)
    conn.commit()

INSERT_SQL = """INSERT OR IGNORE INTO {table}
    (id,company_name,title,terms,date_posted,first_seen_date,url,source,first_seen_commit)
    VALUES (?,?,?,?,?,?,?,?,?)"""

def process_blob(h, d, b, raw, seen_nyc, seen_rem):
    nyc_batch, rem_batch = [], []
    try: listings = json.loads(raw)
    except: return nyc_batch, rem_batch
    for job in listings:
        jid = job.get('id','')
        if not jid: continue
        locs  = job.get('locations', [])
        terms = json.dumps(job.get('terms', []))
        dp    = job.get('date_posted','')
        row   = (jid, job.get('company_name',''), job.get('title',''),
                 terms, dp, d, job.get('url',''), job.get('source',''), h)
        if jid not in seen_nyc and is_nyc(locs):
            seen_nyc.add(jid); nyc_batch.append(row)
        if jid not in seen_rem and is_remote(locs):
            seen_rem.add(jid); rem_batch.append(row)
    return nyc_batch, rem_batch

def export_csvs(conn):
    # NYC CSV — no locations column
    rows = conn.execute(
        'SELECT company_name,title,terms,date_posted,first_seen_date,url,id '
        'FROM nyc_jobs ORDER BY first_seen_date,company_name,title'
    ).fetchall()
    with open(NYC_CSV,'w',newline='',encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['company_name','title','recruiting_season','date_posted',
                    'first_seen_date','url','id'])
        for co,title,terms,dp,fsd,url,jid in rows:
            try: terms = ' | '.join(json.loads(terms))
            except: pass
            w.writerow([co,title,terms,dp,fsd,url,jid])
    # Remote CSV — no locations column
    rows2 = conn.execute(
        'SELECT company_name,title,terms,date_posted,first_seen_date,url,id '
        'FROM remote_jobs ORDER BY first_seen_date,company_name,title'
    ).fetchall()
    with open(REM_CSV,'w',newline='',encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['company_name','title','recruiting_season','date_posted',
                    'first_seen_date','url','id'])
        for co,title,terms,dp,fsd,url,jid in rows2:
            try: terms = ' | '.join(json.loads(terms))
            except: pass
            w.writerow([co,title,terms,dp,fsd,url,jid])
    return len(rows), len(rows2)

def run_chunk(conn, seen_nyc, seen_rem, chunk):
    with ThreadPoolExecutor(max_workers=15) as ex:
        results = list(ex.map(fetch, chunk))
    nyc_all, rem_all, done_all = [], [], []
    for h,d,b,raw in results:
        done_all.append((b,))
        if raw:
            nb, rb = process_blob(h,d,b,raw,seen_nyc,seen_rem)
            nyc_all.extend(nb); rem_all.extend(rb)
    if nyc_all: conn.executemany(INSERT_SQL.format(table='nyc_jobs'), nyc_all)
    if rem_all: conn.executemany(INSERT_SQL.format(table='remote_jobs'), rem_all)
    conn.executemany('INSERT OR IGNORE INTO done_blobs VALUES (?)', done_all)
    conn.commit()
    return len(nyc_all), len(rem_all)

if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    seen_nyc = set(r[0] for r in conn.execute('SELECT id FROM nyc_jobs'))
    seen_rem = set(r[0] for r in conn.execute('SELECT id FROM remote_jobs'))
    done_blobs = set(r[0] for r in conn.execute('SELECT blob FROM done_blobs'))

    unique = get_unique_blobs()
    indices = set([0, len(unique)-1]) | set(range(0, len(unique), 10))
    sampled = [unique[i] for i in sorted(indices)]
    pending = [(h,d,b) for h,d,b in sampled if b not in done_blobs]

    nyc_total = conn.execute('SELECT COUNT(*) FROM nyc_jobs').fetchone()[0]
    rem_total = conn.execute('SELECT COUNT(*) FROM remote_jobs').fetchone()[0]
    print(f'Pending: {len(pending)} | NYC so far: {nyc_total} | Remote so far: {rem_total}', flush=True)

    chunk = pending[:50]
    t0 = time.time()
    new_nyc, new_rem = run_chunk(conn, seen_nyc, seen_rem, chunk)
    nyc_total = conn.execute('SELECT COUNT(*) FROM nyc_jobs').fetchone()[0]
    rem_total = conn.execute('SELECT COUNT(*) FROM remote_jobs').fetchone()[0]
    done_total = conn.execute('SELECT COUNT(*) FROM done_blobs').fetchone()[0]
    print(f'+{new_nyc} NYC | +{new_rem} remote | totals: {nyc_total} NYC, {rem_total} remote | blobs: {done_total}/{len(sampled)} | {time.time()-t0:.0f}s')

    if len(pending) <= 50:
        n, r = export_csvs(conn)
        print(f'DONE. CSV: {n} NYC rows, {r} remote rows')
    conn.close()