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
  update.py  (every 30 min)
        │
        ├─ new NYC/remote jobs → CSVs
        ├─ capture description text for EVERY new posting, immediately
        └─ create an archive for up to 5 of them per run
        ▼
  job_details.csv + job_details.jsonl
        ▲
        │
  backfill_text.py  (nightly)  ── retries everything still missing text:
                                  live URL → stored archive → Wayback capture
```

**Text is captured the moment a posting is discovered.** A posting is at its
most fetchable the minute it goes up and may be gone within days, so text
capture is deliberately *not* gated on the archive queue: archiving is
rate-limited to 5 per run and can lag by hours, which is long enough to lose a
posting. One GET per job is cheap; the Wayback save is what isn't.

`MAX_TEXT_PER_RUN` (80) exists only so a catch-up run (`--commits 200`) can't
fetch thousands of pages at once. Anything over it falls to the nightly job.

The nightly backfill is the safety net for whatever the first attempt missed —
a slow page, a brief block, a posting only archived later. The review app's
"Scrape Text" button runs the same code on one job, for when you want a retry
immediately rather than tomorrow.

## Scripts

| File | What it does |
|---|---|
| `scraper/jobtext.py` | Shared extraction: fetch, JSON-LD/container/body extraction, quality gate. The one place this logic lives. |
| `scraper/update.py` | Incremental updater. New commits → new jobs → CSVs, immediate text capture, archives. |
| `scraper/backfill_text.py` | Bulk backfill of missing `raw_text`. Resumable, checkpointed, safe to re-run. |
| `scraper/notify.py` | Posts run results to a Discord webhook. Inert unless `DISCORD_WEBHOOK_URL` is set. |
| `scraper/simplify_closes.py` | Records when postings stop accepting applications, from Simplify's active/inactive lists. |
| `scraper/classify.py` | Fills the five review fields from the description text. Scores itself against human labels with `--eval`. |
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

## Degree level classification

`classify.py` reads the posting text and answers one question: does this
posting need a graduate student, or can an undergraduate apply? Rules only —
no API key, no network, standard library.

```bash
python scraper/classify.py --eval      # score against hand labels; writes nothing
python scraper/classify.py --dry-run   # show what would change
python scraper/classify.py             # fill in degree_enrollment
python scraper/classify.py --explain ID
```

Measured against 106 postings with both a hand label and text:

| | |
|---|---|
| binary accuracy (needs a grad student) | **91%** |
| grad recall | 84% |
| grad precision | 70% |
| undergrad precision | 96% |

**Read the grad precision honestly:** about three in ten postings flagged
grad-only are not. Good for narrowing a list; not for settling an individual
posting.

The narrow question is what makes this work. An earlier version assigned all
four `degree_enrollment` values and managed 67% — largely because
"Bachelor's or Master's degree" is genuinely ambiguous as a *label* (the hand
labels call it "Open to All Degrees" on some postings and
"BS/BA Required | MS Required" on others) while being completely unambiguous as
a binary: either way, an undergraduate can apply.

Only `degree_enrollment` is written. The other review fields are left alone.

## Discord notifications (optional)

The pipeline runs unattended, so it can report to a Discord channel instead of
you checking the data branch:

| Event | Message |
|---|---|
| New postings found | company, title, and estimated degree level |
| Posting closed or reopened | which ones |
| Backfill run | how many descriptions were recovered |

### Setup

1. In Discord: **Server Settings → Integrations → Webhooks → New Webhook**,
   pick the channel, and copy the URL.
2. In GitHub: **Settings → Secrets and variables → Actions → New repository
   secret**, named exactly `DISCORD_WEBHOOK_URL`.
3. Check it, locally:

```bash
export DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/...'
python scraper/notify.py --test
```

That's all — the three workflows already pass the secret through.

**Treat the webhook URL as a credential.** Anyone holding it can post to your
channel. It belongs in the repository secret and nowhere else: not in a file,
not in a commit, not in a workflow's `run:` block. `notify.py` never prints it,
including in error messages, because a failed webhook call can echo the URL
back and CI logs are not private.

With no secret set, notifications are a silent no-op — the scrapers run exactly
as they do now, and a Discord outage can never fail a scrape.

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
| `data/simplify_transitions.csv` | Append-only log of observed open/close transitions |
| `data/simplify_job_state.json` | Last seen Simplify state, for the next diff |
| `data/excluded_jobs.csv` | Postings filtered out, with the reason |
| `data/pending_archive.csv` | Queue of jobs still awaiting an archive attempt |

## A note on the data branch

Every workflow that writes `data` force-pushes it, because the branch is
deliberately kept to one commit. They therefore share a
`concurrency: data-branch-write` group so two runs can never overlap — without
it, one run's results silently overwrite the other's.
