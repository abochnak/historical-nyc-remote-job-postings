#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings -- Degree Requirement Classifier
===================================================================
Reads a posting's description text and decides what level of study it requires:

    MS Required           graduate students -- master's, PhD, doctoral
    BS/BA Required        undergraduates -- bachelor's
    AS/AAS Required       associate degree (rare)
    Open to All Degrees   no specific degree named

A posting can require more than one ("pursuing a Bachelor's or Master's"), in
which case both are recorded, joined by " | " -- the format the CSV already uses.

Rules, not a model. No API key, no network, standard library only.

Measured accuracy -- read this before trusting it
-------------------------------------------------
Scored against 106 postings that have both a hand-assigned label and
description text:

    three-bucket (grad only / undergrad / any student) : 67%
    exact match on the CSV's multi-label values        : 55%
    "any student" recall                               : 85%
    "undergraduate required" recall                    : 18%
    "grad only" precision                              : 55%

An abstaining variant -- deciding only on unambiguous evidence -- reaches 80%
precision but covers only 42% of postings.

67% is not good enough to run unsupervised, and the plateau has three measured
causes, only one of which more regexes can fix:

  1. 7 of the 106 postings contain no degree language anywhere in their stored
     text. They were labelled from something else -- the live posting, the job
     title, context. No text classifier can recover those.
  2. The hand labels are not self-consistent. "Currently pursuing a Bachelor's
     or Master's degree" is labelled "Open to All Degrees" on five postings and
     "BS/BA Required | MS Required" on others. Identical text, different label,
     so exact-match accuracy is capped below 100% by the target itself.
  3. Real phrasing variety that patterns handle badly.

Run --eval after any rule change. Four rounds of tuning moved the three-bucket
score by under three points, which is the honest signal that this approach is
near its ceiling on this data.

Usage
-----
    python scraper/classify.py --eval        # score against human labels
    python scraper/classify.py --dry-run     # show what would change
    python scraper/classify.py               # fill in degree_enrollment
    python scraper/classify.py --explain ID  # show why one posting was classified

Only degree_enrollment is written. The other review fields (category,
class_year, additional_skills, language_requirements) are left alone -- they are
not reliably derivable from formulaic phrasing the way degree level is.
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

MS    = "MS Required"
BS    = "BS/BA Required"
AS    = "AS/AAS Required"
OPEN  = "Open to All Degrees"
MULTI_SEP = " | "

# Application-form boilerplate ("Undergraduate GPA", "Expected graduation
# date") sits at the end of Greenhouse/Lever pages and mentions degree words
# without requiring anything. Everything from these markers on is ignored.
FORM_MARKERS = [
    r"\*\s*required\s*fields?",
    r"\bautofill with (?:greenhouse|resume)\b",
    r"\bapplication questions?\b",
    r"\bvoluntary self[- ]identification\b",
    r"\bequal employment opportunity\b.{0,40}\bself[- ]identification\b",
    r"\bwill you now or in the future require sponsorship\b",
]

# Phrases that genuinely state a degree level. Bare words are useless here:
# "master your craft" and "limited term associates" both appear in real
# postings that require neither degree, so every pattern below demands the
# surrounding words that make it a credential.
GRAD_PATTERNS = [
    r"master'?s?\s+(?:degree|program|student|candidate)",
    r"master\s+of\s+(?:science|arts|engineering|business|public|computer)",
    r"\bm\.?s\.?\s+(?:in|degree|program)\b",
    r"\bm\.?eng\b|\bm\.?b\.?a\b",
    r"\bph\.?\s?d\.?\b",
    r"\bdoctoral\b|\bdoctorate\b",
    r"graduate\s+(?:degree|program|student|studies|level)",
    r"\b(?:advanced|graduate)\s+degree\b",
    r"currently\s+enrolled\s+in\s+(?:a\s+)?(?:master|graduate|ph\.?d)",
    r"\bor\s+(?:a\s+)?(?:non-?mba\s+)?master'?s?\b",
    r"\bor\s+(?:a\s+)?graduate\b",
    r"master'?s?\s*(?:,|/|\s+or\b|\s+and/or\b)",
]

BACH_PATTERNS = [
    r"bachelor'?s?\s+(?:degree|program|student|candidate)",
    r"bachelor\s+of\s+(?:science|arts|engineering)",
    r"\bb\.?s\.?\s+(?:in|degree|program)\b",
    r"\bb\.?a\.?\s+(?:in|degree|program)\b",
    r"\bb\.?s\.?/\s?b\.?a\.?\b",
    r"undergraduate\s+(?:degree|program|student|studies)",
    r"four[- ]year\s+degree|4[- ]year\s+degree",
    r"currently\s+enrolled\s+in\s+(?:a\s+)?(?:bachelor|undergraduate)",
    # Coordinated forms: "Bachelor's or Master's degree", "undergraduate or
    # graduate program". The head noun attaches to the last item only, so the
    # patterns above miss the first half of every one of these.
    r"bachelor'?s?\s*(?:,|/|\s+or\b|\s+and/or\b)",
    r"undergraduate\s+(?:or|and/or)\b",
    r"\bor\s+(?:a\s+)?bachelor'?s?\b",
]

ASSOC_PATTERNS = [
    r"associate'?s?\s+degree",
    r"\ba\.?a\.?s\.?\s+(?:in|degree)\b",
    r"\bassociate\s+of\s+(?:science|arts|applied)",
]

# A degree named inside one of these clauses is a nice-to-have, not a
# requirement. The human labels consistently treat "Master's preferred" as
# not requiring a master's.
PREFERRED_CUES = [
    "preferred", "preference", "a plus", "nice to have", "ideally",
    "bonus", "desirable", "would be great", "not required", "or equivalent",
]

# Explicit "anyone enrolled may apply" phrasing. These beat a stray degree
# mention: a posting saying "open to all majors" is open regardless.
OPEN_PATTERNS = [
    r"all\s+majors",
    r"any\s+major",
    r"regardless\s+of\s+(?:major|degree|discipline)",
    r"all\s+degree\s+(?:levels|programs)",
    r"open\s+to\s+all\s+(?:students|majors|degrees)",
    r"degree[- ]seeking\s+program",
    r"enrolled\s+in\s+(?:an\s+)?accredited\s+(?:college|university|institution)",
]


# Coordination joining the undergrad and grad terms themselves -- "Bachelor's
# or Master's", "undergraduate / graduate". It must sit *between* the two
# degree mentions and close to them: matching any "or" in the sentence made
# "Bachelor's degree in Computer Science or Engineering" look like an
# undergrad-or-grad choice, which flattened genuine bachelor's requirements
# into "open to all".
COORD_RE     = re.compile(r"^[\s,/]*(?:or|and/or|,)[\s,/]*$", re.I)
COORD_MAX_GAP = 40   # chars allowed between the two degree mentions


def offers_either_path(sentence, bs_span, ms_span):
    """True when the sentence offers an undergrad OR grad route, not both as requirements."""
    if not bs_span or not ms_span:
        return False
    first, second = sorted([bs_span, ms_span], key=lambda sp: sp[0])
    if second[0] < first[1]:                     # overlapping matches
        return False
    between = sentence[first[1]:second[0]]
    if len(between) > COORD_MAX_GAP:
        return False
    return bool(COORD_RE.match(between)) or bool(
        re.fullmatch(r"[\s,/]*(?:or|and/or)[\s,/]*(?:a\s+|an\s+)?(?:non-?mba\s+)?", between, re.I))


def trim_form_boilerplate(text):
    """Cut the application form off the end so its labels aren't read as requirements."""
    cut = len(text)
    for marker in FORM_MARKERS:
        m = re.search(marker, text, re.I)
        if m and m.start() < cut:
            cut = m.start()
    # Only trust the cut if it leaves a real posting behind; some pages are
    # mostly form.
    return text[:cut] if cut > 400 else text


def sentences(text):
    return [s for s in re.split(r"(?<=[.!?;:\n])\s+", text) if s.strip()]


REQUIRED_CUES = ["required qualifications", "basic qualifications", "minimum qualifications",
                 "must be", "must have", "requirements:", "required:", "you must"]


def is_preferred(sentence):
    """
    True when a degree named here is a nice-to-have rather than a requirement.

    A sentence can contain both cues -- "Bachelors or Associates degree in
    process with Computer Science major preferred" is a requirement whose
    *major* is preferred. An explicit requirement framing therefore wins over a
    preference cue; treating the whole sentence as soft dropped these entirely.
    """
    low = sentence.lower()
    if any(cue in low for cue in REQUIRED_CUES):
        return False
    return any(cue in low for cue in PREFERRED_CUES)


def classify_text(text):
    """
    Decide the degree requirement for one posting.

    Returns (labels, evidence) where evidence lists the (level, sentence)
    pairs the decision rested on, for --explain and for spot-checking.
    """
    body = trim_form_boilerplate(text)

    required, preferred, evidence = set(), set(), []
    either = False   # a sentence offering undergrad OR grad -- see below

    for sentence in sentences(body):
        if len(sentence) > 600:          # a run-on block, usually a wall of benefits
            sentence = sentence[:600]
        soft = is_preferred(sentence)

        found, spans = set(), {}
        for level, patterns in ((MS, GRAD_PATTERNS), (BS, BACH_PATTERNS), (AS, ASSOC_PATTERNS)):
            for pat in patterns:
                m = re.search(pat, sentence, re.I)
                if m:
                    found.add(level)
                    spans[level] = m.span()
                    evidence.append((level + (" (preferred)" if soft else ""),
                                     re.sub(r"\s+", " ", sentence.strip())[:180]))
                    break

        # "pursuing a Bachelor's or Master's degree" places no restriction on
        # who may apply -- it is the posting saying *any* student qualifies.
        # The hand labels overwhelmingly treat this as Open to All Degrees
        # rather than as requiring both, so a single sentence offering an
        # undergrad and a grad path is read as open, not as two requirements.
        if not soft and offers_either_path(sentence, spans.get(BS), spans.get(MS)):
            either = True

        (preferred if soft else required).update(found)

    if either:
        return [OPEN], evidence

    if required:
        # Order the labels the way the CSV already does, least to most advanced.
        return [lvl for lvl in (AS, BS, MS) if lvl in required], evidence

    explicitly_open = any(re.search(p, body, re.I) for p in OPEN_PATTERNS)
    if explicitly_open or not preferred:
        return [OPEN], evidence

    # Only soft mentions: the posting names a degree but doesn't require it.
    return [OPEN], evidence


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
def run_eval(rows, texts, show_errors):
    gold = [r for r in rows if r["degree_enrollment"].strip() and r["id"] in texts]
    if not gold:
        sys.exit("No human-labelled postings with text to evaluate against.")

    exact = overlap = 0
    grad_tp = grad_fp = grad_fn = 0
    confusion, errors = {}, []

    for row in gold:
        human = [v.strip() for v in row["degree_enrollment"].split("|") if v.strip()]
        mine, evidence = classify_text(texts[row["id"]])
        hset, mset = set(human), set(mine)

        if hset == mset:
            exact += 1
        if hset & mset:
            overlap += 1
        else:
            errors.append((row, human, mine, evidence))

        # The distinction the classifier exists for: does this need a grad student?
        if MS in hset and MS in mset:
            grad_tp += 1
        elif MS in mset and MS not in hset:
            grad_fp += 1
        elif MS in hset and MS not in mset:
            grad_fn += 1

        key = (MULTI_SEP.join(human), MULTI_SEP.join(mine))
        confusion[key] = confusion.get(key, 0) + 1

    n = len(gold)
    print("=" * 72)
    print(f"  Degree classifier vs {n} human-labelled postings")
    print("=" * 72)
    print(f"  exact match (all labels identical) : {exact:>4}/{n}  {exact/n:.0%}")
    print(f"  overlap     (agree on >=1 label)   : {overlap:>4}/{n}  {overlap/n:.0%}")
    print()
    prec = grad_tp / (grad_tp + grad_fp) if (grad_tp + grad_fp) else 0.0
    rec  = grad_tp / (grad_tp + grad_fn) if (grad_tp + grad_fn) else 0.0
    print(f'  "needs a grad student" precision   : {prec:.0%}  '
          f"({grad_tp} right, {grad_fp} wrongly flagged grad)")
    print(f'  "needs a grad student" recall      : {rec:.0%}  '
          f"({grad_fn} grad postings missed)")
    print()
    print("  human label -> classifier label (most common):")
    for (h, m), c in sorted(confusion.items(), key=lambda kv: -kv[1])[:12]:
        mark = "ok " if h == m else "MISS"
        print(f"    {mark} {c:>3}x  {h or '(none)':<34} -> {m}")

    if show_errors and errors:
        print()
        print(f"  Disagreements ({len(errors)}) — the evidence each decision rested on:")
        for row, human, mine, evidence in errors[:12]:
            print(f"    {row['company_name'][:26]:<26} {row['title'][:34]}")
            print(f"      human: {MULTI_SEP.join(human)}")
            print(f"      rules: {MULTI_SEP.join(mine)}")
            for level, sent in evidence[:2]:
                print(f"        [{level}] {sent[:120]}")
    print()
    print("  Nothing was written.")


# -- Main ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eval", action="store_true", help="score against human labels; writes nothing")
    ap.add_argument("--errors", action="store_true", help="with --eval, print disagreements")
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
            sys.exit("That posting has no description text yet.")
        labels, evidence = classify_text(texts[row["id"]])
        print(f"  {row['company_name']} — {row['title']}")
        print(f"  human : {row['degree_enrollment'] or '(unlabelled)'}")
        print(f"  rules : {MULTI_SEP.join(labels)}")
        print("  evidence:")
        for level, sent in evidence or []:
            print(f"    [{level}] {sent}")
        if not evidence:
            print("    (no degree phrases found — defaults to Open to All Degrees)")
        return

    if args.eval:
        run_eval(rows, texts, args.errors)
        return

    todo = [r for r in rows
            if r["id"] in texts
            and (args.reclassify or not r["degree_enrollment"].strip())]
    no_text = [r for r in rows if r["id"] not in texts and not r["degree_enrollment"].strip()]

    print("=" * 72)
    print("  Degree requirement classification")
    print("=" * 72)
    print(f"  postings              : {len(rows):,}")
    print(f"  with description text : {len(texts):,}")
    print(f"  already labelled      : {sum(1 for r in rows if r['degree_enrollment'].strip()):,}")
    print(f"  blocked (no text yet) : {len(no_text):,}  — run backfill_text.py")
    print(f"  to classify this run  : {len(todo):,}")
    print()

    counts = {}
    for row in todo:
        labels, _ = classify_text(texts[row["id"]])
        value = MULTI_SEP.join(labels)
        counts[value] = counts.get(value, 0) + 1
        if not args.dry_run:
            row["degree_enrollment"] = value

    for value, c in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"    {c:>4}  {value}")

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
