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
| `scraper/render_scrape.py` | Headless-browser capture for JS-built postings. Port of the review-page bookmarklet. |
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

Each new posting is posted as:

```
💼  Web Development Engineer Intern (Summer 2026)
🔗 https://job-boards.greenhouse.io/eulerity/jobs/4689194006
```

One message per posting, up to ten; beyond that they are grouped so a catch-up
run can't fire dozens of requests at a webhook that allows about thirty a
minute. The season is dropped when the posting has none.

The term is stored on every posting in `job_details.csv` as `recruiting_season`,
joined from `listings.json` (via the two job CSVs) by `classify.py`. Of 543
archived postings, 486 have one: 481 from `listings.json`, 5 read from the
description.

When `listings.json` says `N/A` — 11% of the archive —
`classify.extract_season()` reads it out of the description instead, but only
when the description names one term unambiguously (93% precision, commits on
26% of postings). In practice this recovers few: of 61 archived postings with
no term, 39 have text and 5 yielded one. It is a small gain that degrades to
the existing behaviour of simply omitting the parenthetical.

| Event | When | Message |
|---|---|---|
| New postings found | every 30 min (`update.yml`) | company, role, season, link |
| Posting closed or reopened | daily (`track-simplify-closes.yml`) | which ones |
| Backfill run | nightly (`backfill-text.yml`) | how many descriptions were recovered |

**Grad-only postings are not announced.** An undergraduate cannot apply to them,
so the alert is noise. The **title** is checked as well as the description,
which matters because JS-rendered boards often yield no description at all — a
posting titled "Technical Intern - Masters or PhD" was announced purely because
nothing could be read from its page.

Only a positive grad call suppresses. A posting with neither a telling title nor
readable text is still announced, because "we don't know" must not become
"don't tell them".

This is a real trade, and worth understanding before relying on it: the grad
call runs at **70% precision**, so roughly three of every ten suppressed
postings are ones an undergraduate could have applied to. Every suppression is
printed in the run log with the posting's name, so the cost is visible rather
than silent.

Each posting is announced **exactly once**. The announcement is driven by what
a run actually discovered, and `data/notified_ids.txt` records what has been
sent, so a run that dies between notifying and committing doesn't repost.

The **first** run with a webhook configured sets a baseline instead of
announcing, so switching notifications on doesn't dump the whole backlog into
the channel. New postings are announced from the run after that.

### Setup

Make two webhooks in Discord (**Server Settings → Integrations → Webhooks**) —
one on the channel you actually watch, one on a scratch channel — and add both
as repository secrets under **Settings → Secrets and variables → Actions**:

| Secret | Channel |
|---|---|
| `DISCORD_WEBHOOK_URL` | production |
| `DISCORD_WEBHOOK_URL_2` | production, optional |
| `DISCORD_WEBHOOK_URL_3` | production, optional |
| `DISCORD_WEBHOOK_URL_TEST` | the scratch one |

Production posts to every one of the three that is set — one message per
channel, same content. Only `DISCORD_WEBHOOK_URL` is required; the numbering may
be sparse, so setting `_3` without `_2` works, and the same URL in two secrets
posts once rather than twice.

Check them locally:

```bash
export DISCORD_WEBHOOK_URL_TEST='https://discord.com/api/webhooks/...'
python scraper/notify.py --test              # -> test channel

export DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/...'
python scraper/notify.py --test --prod       # -> the real channel, deliberately
```

That's all — all four workflows already pass both secrets through.

### Which channel a run uses

`NOTIFY_TARGET` decides: `test` picks the scratch channel, anything else picks
the real one.

- **Scheduled runs** have no inputs, so they resolve to `prod`.
- **Hand-triggered runs** default to `test`, because a manual run is usually an
  experiment. Every workflow's *Run workflow* dialog has a channel picker if you
  want the real one.

Two properties worth knowing, both tested:

- **A test target never falls back to the real webhooks.** If
  `DISCORD_WEBHOOK_URL_TEST` is unset, notifications are simply off. Falling
  back would mean the one command you ran to avoid the real channels is the
  command that posts to them.
- **A posting counts as announced if any channel accepted it.** Requiring all of
  them sounds safer and behaves worse: a deleted webhook returns 404 forever,
  the posting is never recorded, and every healthy channel gets it again on the
  next run — 48 duplicates a day rather than one miss. Partial delivery is
  printed (`delivered to 2 of 3 channels`) so a dead channel is findable.
- **Each target has its own ledger** — `notified_ids.txt` and
  `notified_ids.test.txt`. Sharing one would let a test run mark postings as
  announced, and the real channel would then never hear about them.

Runs print which channel they are talking to, by name — never the URL.

**Treat both webhook URLs as credentials.** Anyone holding one can post to that
channel. They belong in repository secrets and nowhere else: not in a file,
not in a commit, not in a workflow's `run:` block. `notify.py` never prints it,
including in error messages, because a failed webhook call can echo the URL
back and CI logs are not private.

With no secret set, notifications are a silent no-op — the scrapers run exactly
as they do now, and a Discord outage can never fail a scrape.

## Rendered scrape (for JavaScript-built postings)

`backfill_text.py` uses urllib, so it gets **zero** characters from boards that
build the posting client-side — Workday, Ashby, Oracle Cloud, iCIMS.
`render_scrape.py` drives a headless browser, lets the page render, then walks
the DOM. It is a port of the review-page bookmarklet: same tag skip-list, same
nav-word filter, same line thresholds, with the `window.opener`/`postMessage`
plumbing dropped and the shared quality gate applied instead of the
bookmarklet's 100-character floor.

```bash
pip install -r scraper/requirements-render.txt
python -m playwright install chromium

python scraper/render_scrape.py --only-js --dry-run   # what it would attempt
python scraper/render_scrape.py --only-js --limit 20  # try 20
python scraper/render_scrape.py --headed              # watch it work
```

**It runs in the 30-minute cycle**, as the last step of `update.yml`: up to 12
postings per run, newest first, so a posting discovered minutes ago is rendered
while it is still live. That ordering matters — rendering the historical backlog
hits 42%, and nearly all the failures are expired pages that render "job not
found" in a handful of lines. A live posting reads cleanly.

The browser is cached between runs, so the ~150 MB download happens once rather
than 48 times a day, and the step is `continue-on-error` — the CSV updates are
already committed by then, and a slow board must not cost them.

`render-scrape.yml` is the wider sweep: a bigger batch, optionally non-JS hosts,
optionally retrying known failures. Weekly, or on demand from the Actions tab.

Outcomes are recorded per posting in `data/render_status.csv`, including which
pages are hard blocks (`blocked (cookie/JS wall)`) rather than transient
failures — iCIMS serves a cookie wall that survives a reload, so those are not
worth retrying.

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
| `data/job_details.csv` | Per-posting metadata, recruiting term, archive URL, review status |
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
