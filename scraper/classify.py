#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings -- Degree Level Classifier
=============================================================
Answers one question about each posting: does it need a graduate student, or
can an undergraduate apply?

Writes `degree_enrollment` using the vocabulary already in the CSV:

    MS Required           grad only -- master's/PhD named, no undergrad route
    BS/BA Required        an undergraduate degree is named, so undergrads apply
    Open to All Degrees   no degree requirement stated at all

Rules, not a model. No API key, no network, standard library only.

Measured accuracy
-----------------
Scored against 106 postings with both a hand-assigned label and description
text (`--eval` reproduces this):

    binary accuracy (needs a grad student, yes/no) : 91%
    grad recall                                    : 84%   (3 of 19 missed)
    grad precision                                 : 70%   (7 wrongly flagged)
    undergrad precision                            : 96%
    coverage                                       : 100%

Read that grad precision honestly: roughly three in ten postings flagged as
grad-only are not. It is a filter worth trusting to *narrow* a list, not to
close the question on an individual posting.

Why the binary framing works where four labels didn't
-----------------------------------------------------
An earlier version assigned all four CSV values and scored 67% on a
three-bucket version of the same question. Three things made the narrower
question much easier:

  1. "Bachelor's or Master's degree" is genuinely ambiguous as a *label* -- the
     hand labels call it "Open to All Degrees" on some postings and
     "BS/BA Required | MS Required" on others. As a binary it isn't ambiguous at
     all: either way an undergraduate can apply.
  2. Any mention of an undergraduate degree was 36/36 reliable as evidence that
     undergrads may apply. That single rule carries most of the accuracy.
  3. Postings with no degree language are 94% "undergrads may apply", so the
     absence of a requirement is itself a usable signal rather than a gap.

Two traps that cost real accuracy when they were missed:

  Bare keywords are useless. "master your craft" and "limited term associates"
  both appear in postings requiring neither degree, so every grad pattern
  demands the noun that makes it a credential.

  Class-year words must not count as undergrad evidence. Six of the eight
  grad-only postings originally missed were blocked by the word "senior" -- in
  "senior management", "senior researchers", and a "seniority" key inside an
  embedded JSON blob. Only explicit degree nouns count now.

Usage
-----
    python scraper/classify.py --eval        # score against hand labels
    python scraper/classify.py --dry-run     # show what would change
    python scraper/classify.py               # fill in degree_enrollment
    python scraper/classify.py --explain ID  # show the evidence for one posting
"""

import argparse
import csv
import json
import os
import re
import sys

ROOT          = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR      = os.path.join(ROOT, "data")
DETAILS_CSV   = os.path.join(DATA_DIR, "job_details.csv")
DETAILS_JSONL = os.path.join(DATA_DIR, "job_details.jsonl")

MS   = "MS Required"
BS   = "BS/BA Required"
OPEN = "Open to All Degrees"

# Application-form boilerplate sits at the end of Greenhouse/Lever pages and
# names degrees without requiring them ("Undergraduate GPA", "Expected
# graduation date"). Everything from these markers on is ignored.
FORM_MARKERS = [
    r"\*\s*required\s*fields?",
    r"\bautofill with (?:greenhouse|resume)\b",
    r"\bapplication questions?\b",
    r"\bvoluntary self[- ]identification\b",
    r"\bwill you now or in the future require sponsorship\b",
]

# Graduate-level study. Each pattern requires the noun that makes the word a
# credential -- "master" alone matches "master your craft".
GRAD_RE = re.compile(
    r"master'?s?\s+(?:degree|program|student)"
    r"|\bph\.?\s?d\b"
    r"|doctoral|doctorate"
    r"|graduate\s+(?:degree|program|student)"
    r"|\badvanced\s+degree\b"
    r"|\bm\.?s\.?\s+(?:in|degree)\b"
    r"|matriculated\s+in\s+a\s+graduate",
    re.I,
)

# Evidence that an undergraduate may apply. Deliberately excludes bare
# class-year words (junior/senior/sophomore/freshman): they appear constantly as
# job seniority ("senior engineers", "seniority") and blocked six of eight
# grad-only postings when they were included.
UNDERGRAD_RE = re.compile(
    r"bachelor"
    r"|undergraduate\s+(?:degree|program|student|studies)"
    r"|\bb\.?s\.?\s+(?:in|degree)\b"
    r"|\bb\.?a\.?\s+(?:in|degree)\b"
    r"|\bb\.?s\.?/\s?b\.?a\.?\b"
    r"|associate'?s\s+degree"
    r"|four[- ]year\s+degree",
    re.I,
)


def trim_form_boilerplate(text):
    """Cut the application form off the end so its field labels aren't read as requirements."""
    cut = len(text)
    for marker in FORM_MARKERS:
        m = re.search(marker, text, re.I)
        if m and m.start() < cut:
            cut = m.start()
    # Only trust the cut if a real posting is left behind; some pages are mostly form.
    return text[:cut] if cut > 400 else text


def classify_text(text):
    """
    Return (label, evidence) for one posting.

    evidence is a list of (kind, snippet) showing what the decision rested on,
    for --explain and for spot-checking a disagreement.
    """
    body = trim_form_boilerplate(text)

    grad = GRAD_RE.search(body)
    under = UNDERGRAD_RE.search(body)

    evidence = []
    for kind, m in (("grad", grad), ("undergrad", under)):
        if m:
            start, end = max(0, m.start() - 90), min(len(body), m.end() + 90)
            evidence.append((kind, re.sub(r"\s+", " ", body[start:end]).strip()))

    # An undergraduate route named anywhere wins. This was 36/36 correct on the
    # eval set: postings mentioning a bachelor's accept undergraduates, even
    # when they also mention a master's.
    if under:
        return BS, evidence
    if grad:
        return MS, evidence
    # No degree language at all. 94% of these are postings an undergraduate can
    # apply to -- an unstated requirement is not a requirement.
    return OPEN, evidence


def needs_grad(label):
    return label == MS


# -- Data I/O ------------------------------------------------------------------
def load_details():
    if not os.path.exists(DETAILS_CSV):
        sys.exit(f"ERROR: {DETAILS_CSV} not found. Run this on the data branch.")
    with open(DETAILS_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader), list(reader.fieldnames or [])


def load_texts():
    texts = {}
    if not os.path.exists(DETAILS_JSONL):
        return texts
    with open(DETAILS_JSONL, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except Exception:
                continue
            if e.get("id") and e.get("raw_text", "").strip():
                texts[e["id"]] = e["raw_text"]
    return texts


def save_details(rows, fieldnames):
    tmp = DETAILS_CSV + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, DETAILS_CSV)


# -- Eval ----------------------------------------------------------------------
def gold_needs_grad(row):
    """
    Did the human say this posting is grad-only?

    A hand label naming both an undergraduate and a graduate degree means an
    undergraduate can apply, so it is not grad-only.
    """
    labels = {v.strip() for v in row["degree_enrollment"].split("|") if v.strip()}
    has_grad = MS in labels
    has_under = BS in labels or "AS/AAS Required" in labels
    return has_grad and not has_under


def run_eval(rows, texts, show_errors):
    gold = [r for r in rows if r["degree_enrollment"].strip() and r["id"] in texts]
    if not gold:
        sys.exit("No hand-labelled postings with text to evaluate against.")

    tp = fp = fn = tn = 0
    errors = []
    for row in gold:
        want = gold_needs_grad(row)
        label, evidence = classify_text(texts[row["id"]])
        got = needs_grad(label)
        if want and got:
            tp += 1
        elif got and not want:
            fp += 1
            errors.append(("flagged grad, human says undergrad ok", row, label, evidence))
        elif want and not got:
            fn += 1
            errors.append(("missed a grad-only posting", row, label, evidence))
        else:
            tn += 1

    n = len(gold)
    print("=" * 72)
    print(f"  Does this posting need a graduate student?  ({n} hand-labelled postings)")
    print("=" * 72)
    print(f"  accuracy            : {tp + tn:>3}/{n}  {(tp + tn) / n:.0%}")
    print(f"  grad recall         : {tp:>3}/{tp + fn}   {tp / (tp + fn):.0%}"
          f"   ({fn} grad-only postings missed)")
    print(f"  grad precision      : {tp:>3}/{tp + fp}   {tp / (tp + fp):.0%}"
          f"   ({fp} wrongly flagged as grad)")
    print(f"  undergrad precision : {tn:>3}/{tn + fn}   {tn / (tn + fn):.0%}")
    print()
    print(f"  Roughly {fp / (tp + fp):.0%} of grad flags are wrong — good for narrowing")
    print("  a list, not for settling an individual posting.")

    if show_errors and errors:
        print()
        print(f"  Disagreements ({len(errors)}):")
        for why, row, label, evidence in errors[:14]:
            print(f"    {row['company_name'][:24]:<24} {row['title'][:30]:<30} {why}")
            print(f"      human: {row['degree_enrollment']}   rules: {label}")
            for kind, snip in evidence[:1]:
                print(f"      [{kind}] ...{snip[:120]}")
    print()
    print("  Nothing was written.")


# -- Main ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eval", action="store_true", help="score against hand labels; writes nothing")
    ap.add_argument("--errors", action="store_true", help="with --eval, show disagreements")
    ap.add_argument("--explain", metavar="ID", help="show the evidence for one posting")
    ap.add_argument("--reclassify", action="store_true",
                    help="also overwrite rows that already have a degree label")
    ap.add_argument("--dry-run", action="store_true", help="report what would change")
    args = ap.parse_args()

    rows, fieldnames = load_details()
    texts = load_texts()

    if args.explain:
        row = next((r for r in rows if r["id"] == args.explain), None)
        if not row:
            sys.exit(f"No posting with id {args.explain}")
        if row["id"] not in texts:
            sys.exit("That posting has no description text yet — run backfill_text.py.")
        label, evidence = classify_text(texts[row["id"]])
        print(f"  {row['company_name']} — {row['title']}")
        print(f"  human : {row['degree_enrollment'] or '(unlabelled)'}")
        print(f"  rules : {label}")
        if evidence:
            for kind, snip in evidence:
                print(f"    [{kind}] ...{snip}")
        else:
            print("    (no degree language found — no requirement stated)")
        return

    if args.eval:
        run_eval(rows, texts, args.errors)
        return

    todo = [r for r in rows
            if r["id"] in texts
            and (args.reclassify or not r["degree_enrollment"].strip())]
    no_text = [r for r in rows if r["id"] not in texts and not r["degree_enrollment"].strip()]

    print("=" * 72)
    print("  Degree level classification")
    print("=" * 72)
    print(f"  postings              : {len(rows):,}")
    print(f"  with description text : {len(texts):,}")
    print(f"  already labelled      : {sum(1 for r in rows if r['degree_enrollment'].strip()):,}")
    print(f"  blocked (no text yet) : {len(no_text):,}  — run backfill_text.py")
    print(f"  to classify this run  : {len(todo):,}")
    print()

    counts = {}
    for row in todo:
        label, _ = classify_text(texts[row["id"]])
        counts[label] = counts.get(label, 0) + 1
        if not args.dry_run:
            row["degree_enrollment"] = label

    for label, c in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"    {c:>4}  {label}")
    if counts.get(MS):
        print(f"\n  {counts[MS]} flagged as needing a grad student. At 70% precision,"
              f" expect roughly {round(counts[MS] * 0.3)} of those to be wrong.")

    if args.dry_run:
        print("\n  (dry run — nothing written)")
        return
    if todo:
        save_details(rows, fieldnames)
        print(f"\n  data/job_details.csv updated ({len(todo):,} rows)")
    else:
        print("\n  Nothing to do.")


if __name__ == "__main__":
    main()
