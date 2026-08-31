# Simplify close tracking

Close time — when a posting stops accepting applications — is recorded by
[`scraper/simplify_closes.py`](scraper/simplify_closes.py), run daily by
[`.github/workflows/track-simplify-closes.yml`](.github/workflows/track-simplify-closes.yml).

It watches Simplify move postings between `README.md` (active) and
`README-Inactive.md` (closed), and writes:

| File | Contents |
|---|---|
| `data/simplify_transitions.csv` | Append-only log of every observed change |
| `data/simplify_job_state.json` | Last seen state, for the next run's diff |
| `data/job_details.csv` | `application_closes`, on exact-matched rows |

Timestamps record when a change was **observed**, not when it happened. Running
daily puts a close time within a day of the truth.

```bash
python scraper/simplify_closes.py            # track, and write results
python scraper/simplify_closes.py --dry-run  # report, write nothing
python scraper/simplify_closes.py --no-stamp # log transitions only
```

## What used to be here

Two root-level scripts, `simplify_close_tracker.py` and
`backfill_closes_from_simplify.py`, plus instructions for running them. Both are
removed. Neither worked, and the second was actively harmful.

**`simplify_close_tracker.py` could never detect a transition.** It saved state
with `(company, role)` tuple keys stringified by `json.dumps` — producing
`"('Acme', 'SWE Intern')"` — then read those strings back and indexed `k[0]` and
`k[1]` as though they were still tuples, which yields the characters `(` and `'`.
Every run compared the one-element set `{"( | '"}` against real job names, found
no intersection, and reported zero transitions. It ran daily for a long time and
never produced a single one. The logic is ported and fixed in
`scraper/simplify_closes.py`, which also migrates the old tuple-repr state keys
on read.

**`backfill_closes_from_simplify.py` fabricated close times, and is deliberately
not ported.** Its matcher scored any single shared word at 60 out of 100 against
a 50-point threshold. Every posting in this archive is an internship, so the word
"Intern" alone cleared the role side, and matching collapsed into loose
company-name overlap. It then set `application_closes` to the time the script
ran rather than to any close time. Run against this dataset it would have
written invented timestamps across a large share of rows.

Close times are therefore recorded **going forward only**, from observed
transitions, on normalized-exact company and title matches, skipping any
transition that matches more than one row. There is no backfill, and a posting
that closed before tracking started simply has no close time — which is
accurate, where a fabricated date would not be.
