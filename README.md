# historical-nyc-remote-job-postings

A historical database of internship and job postings scraped from the
[SimplifyJobs/Summer2026-Internships](https://github.com/SimplifyJobs/Summer2026-Internships)
repository, covering **August 2023 → April 2026** across 22,000+ commits.

Two datasets are maintained:

| Dataset | Filter | Rows |
|---|---|---|
| `nyc_jobs` | Location includes "New York, NY", "New York City", "NYC", or "New York" | 2,345 |
| `remote_jobs` | Every listed location is Remote | 1,800 |

---

## Keeping the data up to date

No git clone or token required. The updater hits the public GitHub API
(60 unauthenticated requests/hour is more than enough for a daily run):

```bash
python scraper/update.py              # check last 30 commits (default)
python scraper/update.py --commits 10 # check only the 10 most recent
```

Then commit the updated data files:

```bash
git add data/
git commit -m "chore: update job data $(date +%Y-%m-%d)"
```

### Optional: GitHub token for higher rate limits

A token raises the limit from 60 → 5,000 API requests/hour:

```bash
GITHUB_TOKEN=ghp_... python scraper/update.py
```

Generate one at https://github.com/settings/tokens — no scopes needed for public repos.

---

## Repository structure

```
historical-nyc-remote-job-postings/
├── data/
│   ├── nyc_jobs.csv       # NYC postings — flat CSV, no locations column
│   ├── remote_jobs.csv    # Remote-only postings — flat CSV
│   └── jobs.sql           # Plain-text SQL dump (both tables, importable anywhere)
├── scraper/
│   ├── update.py          # ← Run this to update (GitHub API, no clone needed)
│   └── scrape.py          # Full historical scraper (initial build, requires clone)
└── README.md
```

---

## Data schema

Both `nyc_jobs` and `remote_jobs` share the same columns:

| Column | Description |
|---|---|
| `id` | Stable UUID from SimplifyJobs |
| `company_name` | e.g. `Goldman Sachs` |
| `title` | e.g. `Software Engineering Intern` |
| `recruiting_season` | e.g. `Summer 2026` (pipe-separated if multi-season) |
| `date_posted` | Date from the original job record |
| `first_seen_date` | ISO-8601 git commit date when the posting first appeared |
| `url` | Application link |

---

## Querying the data

**SQL (load jobs.sql into SQLite):**

```bash
sqlite3 jobs.db < data/jobs.sql
```

```sql
-- NYC jobs for Summer 2026
SELECT company_name, title, date_posted, url
FROM nyc_jobs
WHERE terms LIKE '%Summer 2026%'
ORDER BY date_posted;

-- Remote postings from a specific company
SELECT title, first_seen_date, url
FROM remote_jobs
WHERE company_name = 'Hugging Face'
ORDER BY first_seen_date;

-- Jobs that appear in both tables (NYC + remote)
SELECT n.company_name, n.title, n.first_seen_date
FROM nyc_jobs n
JOIN remote_jobs r ON n.id = r.id;
```

**Python (read CSVs directly):**

```python
import csv

with open("data/nyc_jobs.csv") as f:
    jobs = list(csv.DictReader(f))

summer_2026 = [j for j in jobs if "Summer 2026" in j["recruiting_season"]]
```

---

## Highlights

### NYC jobs — top companies
| Company | Postings |
|---|---|
| Uber | 61 |
| Meta | 39 |
| Point72 | 37 |
| The Walt Disney Company | 36 |
| S&P Global | 27 |
| Dow Jones | 24 |
| American Express | 24 |
| The New York Times | 22 |
| Schonfeld | 22 |
| Palantir | 22 |

### Remote jobs — top companies
| Company | Postings |
|---|---|
| Leidos | 79 |
| DigitalOcean | 49 |
| Hugging Face | 44 |
| CrowdStrike | 42 |
| Intel | 41 |
| 1Password | 32 |
| Autodesk | 31 |
| Dropbox | 22 |

### Seasons covered
`Summer 2023` · `Fall 2023` · `Winter 2024` · `Spring 2024` · `Summer 2024` ·
`Fall 2024` · `Winter 2025` · `Spring 2025` · `Summer 2025` · `Fall 2025` ·
`Winter 2026` · `Spring 2026` · `Summer 2026`

---

## Rebuilding from scratch

If you ever need to rebuild the full historical dataset from all 22,000+ commits:

```bash
git clone https://github.com/SimplifyJobs/Summer2026-Internships.git repo
python scraper/scrape.py
```

This is a one-time operation (~2 min with a full local clone). After that,
use `update.py` for all future updates.

---

## Data source

All job data sourced from
[SimplifyJobs/Summer2026-Internships](https://github.com/SimplifyJobs/Summer2026-Internships).
This repository stores filtered snapshots only and does not claim ownership of the underlying data.