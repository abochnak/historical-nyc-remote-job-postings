#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings -- Simplify Close Tracking
=============================================================
Records when a posting stops accepting applications, by watching Simplify move
it between README.md (active) and README-Inactive.md (closed).

For a historical archive this is the most interesting signal available: how
long a posting actually stayed open. Nothing else in the pipeline captures it —
by the time a posting 404s, the close date is already lost.

What gets written
-----------------
    data/simplify_transitions.csv   append-only log of every observed change
    data/simplify_job_state.json    last seen state, for the next run's diff
    data/job_details.csv            application_closes, for exact-matched rows

On timestamps: this records when a change was *observed*, not when it happened.
Running daily means a close time is accurate to within a day, which is why the
column is populated only by this forward-looking tracker and never backfilled
(see "Why there is no backfill" below).

Usage
-----
    python scraper/simplify_closes.py              # track; writes results
    python scraper/simplify_closes.py --dry-run    # report, write nothing
    python scraper/simplify_closes.py --no-stamp   # log transitions only

Why there is no backfill
------------------------
A backfill_closes_from_simplify.py existed in the review app and is deliberately
not ported. It matched historical jobs to Simplify's inactive list with a
similarity score that returned 60/100 for any single shared word — and since
every posting here is an internship, the word "Intern" alone cleared the role
side of a 50-point threshold, so matches collapsed to loose company-name
overlap. It then stamped application_closes with the time the script ran rather
than any close time. Run against this dataset it would have written fabricated
timestamps onto a large share of rows. Close times are recorded going forward,
from observed transitions, or not at all.
"""

import argparse
import ast
import csv
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import notify

ROOT        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR    = os.path.join(ROOT, "data")
STATE_JSON  = os.path.join(DATA_DIR, "simplify_job_state.json")
# The review app wrote its state to the repo root, and that file is live on the
# data branch. Read it when the data/ copy doesn't exist yet, so the first run
# here continues from the existing baseline instead of starting blind — a fresh
# start would silently miss every posting that closed in between.
LEGACY_STATE_JSON = os.path.join(ROOT, "simplify_job_state.json")
TRANS_CSV   = os.path.join(DATA_DIR, "simplify_transitions.csv")
DETAILS_CSV = os.path.join(DATA_DIR, "job_details.csv")

RAW_BASE = "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev"

TRANS_HEADERS = ["timestamp", "company", "role", "status_change", "reason"]

UA = "historical-job-scraper/2.0"


# -- Fetch & parse -------------------------------------------------------------
def fetch_readme(filename):
    url = f"{RAW_BASE}/{filename}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  ERROR fetching {filename}: {e}")
        return None


def parse_jobs(content):
    """Extract {(company, role)} from the README's HTML tables."""
    jobs = {}
    for row in re.findall(r"<tr>(.*?)</tr>", content, re.DOTALL):
        cells = re.findall(r"<td[^>]*>([^<]*(?:<[^>]*>[^<]*)*?)</td>", row)
        if len(cells) < 5:
            continue

        company_raw = cells[0]
        m = re.search(r">([^<]+)<", company_raw)
        company = (m.group(1) if m else company_raw).strip()
        company = re.sub(r"<[^>]+>|\[|\]|\(.*?\)|↳", "", company).strip()

        role = re.sub(r"<[^>]+>|🛂|🇺🇸|🔒|🔥|🎓|↳", "", cells[1]).strip()

        if len(company) < 3 or len(role) < 4:
            continue
        if company == "Company" or role == "Role":
            continue
        jobs[job_key(company, role)] = {"company": company, "role": role}
    return jobs


def job_key(company, role):
    """
    Stable identity for a posting, used as-is in the state file.

    This is the bug the original had: it stored tuple keys through
    json.dumps as their Python repr -- "('Acme', 'SWE Intern')" -- and then read
    them back and indexed k[0]/k[1] as though they were still tuples, which
    yields the characters "(" and "'". Every previous run therefore compared
    the set {"( | '"} against real job names, found no intersection, and
    detected exactly zero transitions -- forever.
    """
    return f"{company.strip()} | {role.strip()}"


# -- State ---------------------------------------------------------------------
def load_state():
    path = STATE_JSON
    if not os.path.exists(path):
        if os.path.exists(LEGACY_STATE_JSON):
            print(f"  Using legacy state from {os.path.basename(LEGACY_STATE_JSON)} "
                  "(repo root); future runs write to data/")
            path = LEGACY_STATE_JSON
        else:
            return {"active": {}, "inactive": {}}
    try:
        with open(path, encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return {"active": {}, "inactive": {}}
    return {
        "active":   migrate_keys(state.get("active") or {}),
        "inactive": migrate_keys(state.get("inactive") or {}),
    }


def migrate_keys(section):
    """
    Accept the old tuple-repr keys so an existing state file still works.

    Old: "('Acme', 'SWE Intern')"   New: "Acme | SWE Intern"
    """
    out = {}
    for k, v in section.items():
        if k.startswith("(") and k.endswith(")"):
            try:
                parsed = ast.literal_eval(k)
                if isinstance(parsed, tuple) and len(parsed) == 2:
                    k = job_key(str(parsed[0]), str(parsed[1]))
            except Exception:
                pass
        out[k] = v
    return out


def save_state(active, inactive, now):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = STATE_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"timestamp": now, "active": active, "inactive": inactive},
                  f, indent=2, sort_keys=True)
    os.replace(tmp, STATE_JSON)


# -- Transitions ---------------------------------------------------------------
def detect_transitions(prev, active, inactive, now):
    prev_active   = set(prev["active"])
    prev_inactive = set(prev["inactive"])
    cur_active    = set(active)
    cur_inactive  = set(inactive)

    transitions = []
    for key in sorted((prev_active - cur_active) & cur_inactive):
        company, _, role = key.partition(" | ")
        transitions.append({"timestamp": now, "company": company, "role": role,
                            "status_change": "active -> inactive",
                            "reason": "Applications closed"})
    for key in sorted((prev_inactive - cur_inactive) & cur_active):
        company, _, role = key.partition(" | ")
        transitions.append({"timestamp": now, "company": company, "role": role,
                            "status_change": "inactive -> active",
                            "reason": "Reopened"})
    return transitions


def save_transitions(transitions):
    """
    Append to the log, always leaving a real file behind.

    The header is written even on a run with no transitions. The original only
    created this file when it had rows to add, so the workflow's
    `git add simplify_transitions.csv` failed on a missing path and the whole
    step errored out -- which is the second reason nothing was ever committed.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    exists = os.path.exists(TRANS_CSV)
    with open(TRANS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TRANS_HEADERS, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerows(transitions)


# -- Stamping job_details ------------------------------------------------------
def normalize(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def stamp_closes(transitions, now):
    """
    Record application_closes on rows that match a closed posting exactly.

    Matching is normalized-exact on both company and title, and a transition
    matching more than one row is skipped rather than guessed at. Loose fuzzy
    matching is what made the old backfill unusable; a close time on the wrong
    posting is worse than no close time.
    """
    if not os.path.exists(DETAILS_CSV):
        return 0, 0

    with open(DETAILS_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if "application_closes" not in fieldnames:
        fieldnames.append("application_closes")

    index = {}
    for row in rows:
        index.setdefault(
            (normalize(row.get("company_name")), normalize(row.get("title"))), []
        ).append(row)

    stamped = ambiguous = 0
    for t in transitions:
        if t["status_change"] != "active -> inactive":
            continue
        matches = index.get((normalize(t["company"]), normalize(t["role"])), [])
        if len(matches) > 1:
            ambiguous += 1
            continue
        for row in matches:
            if not row.get("application_closes", "").strip():
                row["application_closes"] = now
                stamped += 1

    if stamped:
        tmp = DETAILS_CSV + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for row in rows:
                row.setdefault("application_closes", "")
                w.writerow(row)
        os.replace(tmp, DETAILS_CSV)

    return stamped, ambiguous


# -- Main ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    ap.add_argument("--no-stamp", action="store_true",
                    help="log transitions but don't touch job_details.csv")
    args = ap.parse_args()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=" * 62)
    print("  Simplify close tracking")
    print("=" * 62)

    active_md   = fetch_readme("README.md")
    inactive_md = fetch_readme("README-Inactive.md")
    if not active_md or not inactive_md:
        sys.exit("  Could not fetch Simplify READMEs — aborting without writing.")

    active   = parse_jobs(active_md)
    inactive = parse_jobs(inactive_md)
    print(f"  Simplify now : {len(active):,} active | {len(inactive):,} inactive")

    prev = load_state()
    first_run = not (prev["active"] or prev["inactive"])
    print(f"  Previous state: "
          + ("none — first run, establishing a baseline"
             if first_run else
             f"{len(prev['active']):,} active | {len(prev['inactive']):,} inactive"))

    transitions = [] if first_run else detect_transitions(prev, active, inactive, now)
    closed   = [t for t in transitions if t["status_change"] == "active -> inactive"]
    reopened = [t for t in transitions if t["status_change"] == "inactive -> active"]
    print(f"  Transitions   : {len(closed):,} closed | {len(reopened):,} reopened")

    for t in closed[:15]:
        print(f"    closed   {t['company'][:28]:<28} {t['role'][:40]}")
    if len(closed) > 15:
        print(f"    ... and {len(closed) - 15:,} more")

    if args.dry_run:
        print("\n  (dry run — nothing written)")
        return

    save_transitions(transitions)
    save_state(active, inactive, now)

    if transitions:
        notify.notify_closes(transitions)

    if transitions and not args.no_stamp:
        stamped, ambiguous = stamp_closes(transitions, now)
        print(f"  Stamped application_closes on {stamped:,} row(s)"
              + (f" | {ambiguous:,} skipped as ambiguous" if ambiguous else ""))

    print()
    print(f"  data/simplify_transitions.csv  -> +{len(transitions):,} row(s)")
    print(f"  data/simplify_job_state.json   -> baseline for the next run")
    if first_run:
        print("\n  First run only records a baseline; transitions appear from the next run on.")


if __name__ == "__main__":
    main()
