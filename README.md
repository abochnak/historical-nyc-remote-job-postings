# historical-nyc-remote-job-postings

Archive of NYC and remote internship postings scraped from
[SimplifyJobs/Summer2026-Internships](https://github.com/SimplifyJobs/Summer2026-Internships),
with the full description text of each posting captured before it disappears.

Code lives on `main`. Data lives on the `data` branch, kept as a single
squashed commit.

## How a posting gets its text

Nothing here needs a human. The three scripts share one extractor
(`scraper/jobtext.py`) so a posting captured by any of them comes out the same.

```
listings.json commit
        │
        ▼
  update.py  (every 30 min)  ── new NYC/remote jobs → CSVs
        │                       tries live fetch, then creates an archive
        ▼
  job_details.csv + job_details.jsonl
        ▲
        │
  backfill_text.py  (nightly)  ── retries everything still missing text:
                                  live URL → stored archive → Wayback capture
```

`update.py` gets one attempt per posting as it's discovered. Whatever it misses
— a slow page, a brief block, a posting only archived later — is picked up by
the nightly backfill. The review app's "Scrape Text" button runs the same code
on one job, for when you want a retry immediately rather than tomorrow.

## Scripts

| File | What it does |
|---|---|
| `scraper/jobtext.py` | Shared extraction: fetch, JSON-LD/container/body extraction, quality gate. The one place this logic lives. |
| `scraper/update.py` | Incremental updater. New commits → new jobs → CSVs, archives, first text attempt. |
| `scraper/backfill_text.py` | Bulk backfill of missing `raw_text`. Resumable, checkpointed, safe to re-run. |
| `scraper/scrape.py` | Full historical build from scratch (rarely needed). |

## Backfilling by hand

```bash
python scraper/backfill_text.py --dry-run      # what's missing, by host
python scraper/backfill_text.py --limit 50     # work through 50
python scraper/backfill_text.py                # everything missing
python scraper/backfill_text.py --retry-failed # revisit gone/thin postings too
```

Outcomes are recorded per job in `data/backfill_status.csv`:

| status | meaning | retried on rerun? |
|---|---|---|
| `ok` | text stored | no — it has text |
| `error` | couldn't reach it (blocked, 4xx, network) | yes |
| `empty` | pages loaded, no posting text (JS-rendered board) | only with `--retry-failed` |
| `gone` | a page said the posting was taken down | only with `--retry-failed` |

## What can't be recovered

Some postings have no retrievable text, and re-running won't change that:

- **JS-rendered boards** — Workday, Ashby, Oracle Cloud and similar render the
  description client-side. Fetching the URL returns an empty shell. Unless the
  Wayback Machine happens to hold a rendered capture, there is nothing to
  extract without running a browser.
- **Taken down with no archive** — the posting closed before anything captured
  it.

These are recorded as `empty` / `gone` rather than retried forever.

## Data files (on the `data` branch)

| File | Contents |
|---|---|
| `data/nyc_jobs.csv` | Postings located in NYC |
| `data/remote_jobs.csv` | Postings where every location is remote |
| `data/job_details.csv` | Per-posting metadata, archive URL, review status |
| `data/job_details.jsonl` | `{id, raw_text}` — the description text |
| `data/backfill_status.csv` | Per-posting scrape outcome |
| `data/excluded_jobs.csv` | Postings filtered out, with the reason |
| `data/pending_archive.csv` | Queue of jobs still awaiting an archive attempt |

## A note on the data branch

Every workflow that writes `data` force-pushes it, because the branch is
deliberately kept to one commit. They therefore share a
`concurrency: data-branch-write` group so two runs can never overlap — without
it, one run's results silently overwrite the other's.
