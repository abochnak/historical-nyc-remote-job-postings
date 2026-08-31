#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings -- Bulk Description Backfill
===============================================================
Fills raw_text for every entry in job_details.csv that doesn't have one yet.

These are *historical* postings, so a large share are already taken down. Each
job is tried against three sources, in order, and the first one that yields
usable text wins:

    1. live job_url      -- works for server-rendered boards (Greenhouse,
                            Lever, SmartRecruiters, Jobvite, Rippling)
    2. archive_url       -- whatever update.py already captured
    3. Wayback snapshot  -- closest existing capture, via the availability API

Extraction itself lives in scraper/jobtext.py, shared with update.py and the
review app so all three behave identically. Nothing here *creates* archives; it
only reads ones that already exist. Use update.py for the save-to-Wayback path.

Work list comes from job_details.csv, which is the only file that has job_url
and archive_url. (The previous version of this script read its work list from
job_details.jsonl and filtered on `archive_url` -- a field the JSONL has never
contained, so it always found zero rows to do.)

Results merge into job_details.jsonl, preserving every entry already there.
Per-job outcomes are recorded in backfill_status.csv so reruns skip postings
that are permanently gone instead of hammering dead URLs again.

Usage
-----
    python scraper/backfill_text.py                    # everything missing
    python scraper/backfill_text.py --limit 50         # first 50 only
    python scraper/backfill_text.py --workers 4        # gentler (default 8)
    python scraper/backfill_text.py --host-delay 4     # secs between hits/host
    python scraper/backfill_text.py --retry-failed     # re-try past failures
    python scraper/backfill_text.py --dry-run          # report, fetch nothing
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jobtext import (HostThrottle, MIN_CHARS, best_effort_text,  # noqa: E402
                     looks_like_job_text)

ROOT          = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR      = os.path.join(ROOT, "data")
DETAILS_CSV   = os.path.join(DATA_DIR, "job_details.csv")
DETAILS_JSONL = os.path.join(DATA_DIR, "job_details.jsonl")
STATUS_CSV    = os.path.join(DATA_DIR, "backfill_status.csv")

STATUS_HEADERS = ["id", "status", "source", "chars", "attempts", "last_attempt", "note"]

CHECKPOINT_EVERY = 25  # flush to disk this often, so a Ctrl-C costs at most 25 fetches
MAX_ATTEMPTS     = 3   # stop auto-retrying a job that keeps erroring


# -- Per-job pipeline ----------------------------------------------------------
def backfill_one(row, throttle, use_wayback=True):
    """Run one job through the shared extractor and tag the result with its id."""
    result = best_effort_text(
        (row.get("job_url") or "").strip(),
        (row.get("archive_url") or "").strip(),
        row.get("archive_source") or "archive",
        throttle=throttle,
        use_wayback=use_wayback,
    )
    result["id"] = row["id"]
    return result


# -- Data I/O ------------------------------------------------------------------
def load_details():
    if not os.path.exists(DETAILS_CSV):
        sys.exit(f"ERROR: {DETAILS_CSV} not found. Run this on the data branch.")
    with open(DETAILS_CSV, encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if r.get("id")]


def load_texts():
    """Existing raw_text keyed by id. Every one of these is preserved on write."""
    texts = {}
    if not os.path.exists(DETAILS_JSONL):
        return texts
    with open(DETAILS_JSONL, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except Exception:
                continue
            if entry.get("id") and entry.get("raw_text", "").strip():
                texts[entry["id"]] = entry["raw_text"]
    return texts


def save_texts(texts, order):
    """
    Rewrite the JSONL with id + raw_text only -- the exact shape update.py's
    save_jsonl() writes, so the two scripts can't fight over the format.
    """
    rank = {jid: i for i, jid in enumerate(order)}
    tmp = DETAILS_JSONL + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for jid in sorted(texts, key=lambda j: rank.get(j, len(rank))):
            f.write(json.dumps({"id": jid, "raw_text": texts[jid]}) + "\n")
    os.replace(tmp, DETAILS_JSONL)


def load_status():
    status = {}
    if not os.path.exists(STATUS_CSV):
        return status
    with open(STATUS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("id"):
                status[row["id"]] = row
    return status


def save_status(status):
    tmp = STATUS_CSV + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=STATUS_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(status.values(), key=lambda r: r.get("id", "")))
    os.replace(tmp, STATUS_CSV)


# -- Main ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="max jobs this run (0 = all)")
    ap.add_argument("--workers", type=int, default=8, help="concurrent fetches (default 8)")
    ap.add_argument("--host-delay", type=float, default=2.0,
                    help="min seconds between requests to the same host (default 2)")
    ap.add_argument("--retry-failed", action="store_true",
                    help="also retry jobs previously recorded gone/empty/error")
    ap.add_argument("--no-wayback", action="store_true",
                    help="skip the Wayback availability fallback")
    ap.add_argument("--drop-bad", action="store_true",
                    help="discard stored text that fails the quality gate, then refetch it")
    ap.add_argument("--dry-run", action="store_true", help="report the work list, fetch nothing")
    args = ap.parse_args()

    rows   = load_details()
    texts  = load_texts()

    # Text stored before the quality gate existed includes things that are not
    # job descriptions at all -- most commonly the Wayback Machine's own toolbar
    # ("1 capture / About this capture / COLLECTED BY ...") captured instead of
    # the page it was framing. Dropping those puts the jobs back in the queue,
    # where they'll be refetched from the raw capture and get real text or an
    # honest failure.
    status = load_status()
    order  = [r["id"] for r in rows]

    dropped = 0
    if args.drop_bad:
        bad = [jid for jid, text in texts.items() if not looks_like_job_text(text)]
        for jid in bad:
            del texts[jid]
            status.pop(jid, None)   # clear the ledger so it isn't skipped as exhausted
        dropped = len(bad)

    todo = [r for r in rows if r["id"] not in texts]
    skipped_permanent = 0
    if not args.retry_failed:
        before = len(todo)

        def worth_retrying(job_id):
            st = status.get(job_id)
            if not st:
                return True                       # never attempted
            if st.get("status") in ("gone", "empty"):
                return False                      # nothing there to get
            # Errors are usually transient, but not always: archive.ph answers
            # every automated request with 429 no matter how long we wait. Give
            # up after MAX_ATTEMPTS so the nightly run stops hammering hosts
            # that have made their position clear.
            return int(st.get("attempts", 0) or 0) < MAX_ATTEMPTS

        todo = [r for r in todo if worth_retrying(r["id"])]
        skipped_permanent = before - len(todo)

    print("=" * 62)
    print("  Bulk description backfill")
    print("=" * 62)
    print(f"  job_details.csv     : {len(rows):,} entries")
    print(f"  already have text   : {len(texts):,}")
    print(f"  missing text        : {len(rows) - len(texts):,}")
    if dropped:
        print(f"  dropped (not a job) : {dropped:,}  (--drop-bad; queued for refetch)")
    if skipped_permanent:
        print(f"  skipped (exhausted) : {skipped_permanent:,}  "
              f"(gone/thin, or {MAX_ATTEMPTS}+ failed attempts — --retry-failed to include)")
    if args.limit:
        todo = todo[: args.limit]
    print(f"  to fetch this run   : {len(todo):,}")
    print(f"  workers / host-delay: {args.workers} / {args.host_delay}s")
    print()

    if args.dry_run:
        hosts = {}
        for r in todo:
            h = urllib.parse.urlparse(r["job_url"]).netloc
            hosts[h] = hosts.get(h, 0) + 1
        for h, n in sorted(hosts.items(), key=lambda kv: -kv[1])[:20]:
            print(f"    {n:>4}x  {h}")
        print("\n  (dry run -- nothing fetched)")
        return

    if not todo:
        print("  Nothing to do.")
        return

    throttle = HostThrottle(args.host_delay)
    now      = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    counts   = {"ok": 0, "gone": 0, "empty": 0, "error": 0}
    done     = 0
    started  = time.monotonic()

    def record(res, row):
        prev = status.get(res["id"], {})
        status[res["id"]] = {
            "id":           res["id"],
            "status":       res["status"],
            "source":       res["source"],
            "chars":        str(len(res["text"])),
            "attempts":     str(int(prev.get("attempts", 0) or 0) + 1),
            "last_attempt": now,
            "note":         res["note"][:200],
        }
        if res["status"] == "ok":
            texts[res["id"]] = res["text"]

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(backfill_one, r, throttle, not args.no_wayback): r
                for r in todo
            }
            for fut in as_completed(futures):
                row = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    res = {"id": row["id"], "text": "", "source": "",
                           "status": "error", "note": f"{type(e).__name__}: {e}"[:200]}
                record(res, row)
                counts[res["status"]] += 1
                done += 1

                mark = {"ok": "OK  ", "gone": "GONE", "empty": "THIN", "error": "ERR "}[res["status"]]
                label = f"{row.get('company_name','')[:24]} - {row.get('title','')[:34]}"
                extra = f"{len(res['text']):,}c via {res['source']}" if res["status"] == "ok" else res["note"][:40]
                print(f"  [{done:>4}/{len(todo)}] {mark} {label:<62} {extra}", flush=True)

                if done % CHECKPOINT_EVERY == 0:
                    save_texts(texts, order)
                    save_status(status)
    except KeyboardInterrupt:
        print("\n  Interrupted -- flushing what's done so far ...")

    save_texts(texts, order)
    save_status(status)

    elapsed = time.monotonic() - started
    print()
    print("-" * 62)
    print(f"  Attempted : {done:,} in {elapsed/60:.1f} min")
    print(f"    ok      : {counts['ok']:,}")
    print(f"    gone    : {counts['gone']:,}  (posting taken down)")
    print(f"    thin    : {counts['empty']:,}  (JS-rendered or stub page)")
    print(f"    error   : {counts['error']:,}  (blocked / network / 4xx)")
    print()
    print(f"  job_details.jsonl   -> {len(texts):,} entries with text "
          f"({len(rows) - len(texts):,} still missing)")
    print(f"  backfill_status.csv -> {len(status):,} rows")
    if counts["error"]:
        print("\n  Re-run to retry errors; add --retry-failed to also revisit gone/thin.")


if __name__ == "__main__":
    main()
