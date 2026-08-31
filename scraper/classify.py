#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings -- Automatic Posting Classifier
==================================================================
Fills the five review fields from the posting's own description text, so
classifying postings by hand stops being a job anyone has to do.

    category                Software Engineering, Data Analysis, ...
    class_year              Freshman / Sophomore / Junior / Senior / Grad Student
    degree_enrollment       AS/AAS, BS/BA, MS required, or open
    additional_skills       free-form list pulled from the posting
    language_requirements   English, plus anything explicitly required

Only postings that already have raw_text can be classified -- there is nothing
to read otherwise. Run backfill_text.py first.

Trust, then automate
--------------------
194 postings were classified by hand before this existed. `--eval` scores the
model against those, per field, so you can see what the automation actually
agrees on before letting it write anything:

    python scraper/classify.py --eval           # score against human labels
    python scraper/classify.py --limit 20       # classify 20, write results
    python scraper/classify.py                  # classify everything unclassified
    python scraper/classify.py --dry-run        # show what would be done

Requires ANTHROPIC_API_KEY (or an `ant auth login` profile) and:

    pip install anthropic
"""

import argparse
import csv
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

try:
    import anthropic
except ImportError:
    sys.exit("ERROR: pip install anthropic")

from pydantic import BaseModel, Field

ROOT          = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR      = os.path.join(ROOT, "data")
DETAILS_CSV   = os.path.join(DATA_DIR, "job_details.csv")
DETAILS_JSONL = os.path.join(DATA_DIR, "job_details.jsonl")

MODEL       = "claude-opus-5"
MAX_TOKENS  = 1024
EFFORT      = "low"   # classification against a closed taxonomy; see --effort
WORKERS     = 6

# The description is the only input that matters, and the tail of a long
# posting is boilerplate (EEO statements, benefits, application instructions).
# Trimming keeps cost predictable without losing the part that classifies.
MAX_TEXT_CHARS = 12_000

# -- Taxonomy ------------------------------------------------------------------
# Derived from the 194 postings classified by hand. Keep these lists in sync
# with what the review app offers; the model is constrained to exactly these
# values, so anything missing here can never be assigned.
CATEGORIES = [
    "Software Engineering", "Data Analysis", "Machine Learning / AI",
    "Data Science", "Data Engineering", "Product Management", "Cybersecurity",
    "IT Support", "Engineering (Non-Software)", "Quant / Finance",
    "Product Design/UX", "Other",
]

CLASS_YEARS = ["Freshman", "Sophomore", "Junior", "Senior", "Grad Student", "Open to All"]

DEGREES = ["AS/AAS Required", "BS/BA Required", "MS Required", "Open to All Degrees"]

MULTI_SEP = " | "   # how the CSV stores multi-value fields


class Classification(BaseModel):
    category: list[str] = Field(
        description="Every listed category that genuinely fits this role. Most "
                    "roles have exactly one. Use two or more only when the "
                    "posting really spans them."
    )
    class_year: list[str] = Field(
        description='Class years eligible to apply. Use ["Open to All"] alone '
                    "when the posting sets no year restriction."
    )
    degree_enrollment: list[str] = Field(
        description='Degree levels the posting requires. Use ["Open to All '
                    'Degrees"] alone when no specific degree is required.'
    )
    additional_skills: list[str] = Field(
        description="Concrete named technologies, tools, and languages the "
                    "posting asks for. Empty list if none are named."
    )
    language_requirements: list[str] = Field(
        description="Human languages explicitly required. Almost always "
                    '["English"]; add others only when the posting requires them.'
    )
    reasoning: str = Field(
        description="One sentence on why this category was chosen. Kept for "
                    "spot-checking; not written to the CSV."
    )


SYSTEM = f"""You classify internship and early-career job postings for an archive of NYC and remote roles.

You will be given the text of one posting. Assign these five fields.

category — the kind of work. Choose from exactly these values:
{chr(10).join('  - ' + c for c in CATEGORIES)}
Pick the single best fit. Assign more than one only when the role genuinely
spans them (e.g. a role that is equally data engineering and ML). "Other" is for
roles that fit none of the above, not for roles you are unsure about — pick the
closest real category instead.

class_year — which students may apply. Choose from exactly these values:
{chr(10).join('  - ' + c for c in CLASS_YEARS)}
Use ["Open to All"] on its own when the posting names no year restriction; this
is the most common case. When specific years are named, list exactly those and
do not include "Open to All". "Grad Student" covers master's and PhD students.

degree_enrollment — the degree level required. Choose from exactly these values:
{chr(10).join('  - ' + d for d in DEGREES)}
Use ["Open to All Degrees"] on its own when no specific degree is required.
When a posting accepts several levels, list each acceptable one. A posting
saying "pursuing a Bachelor's or Master's" is ["BS/BA Required", "MS Required"].

additional_skills — specific named technologies, languages, tools, and
frameworks the posting asks for (e.g. Python, React, SQL, PyTorch, Excel, AWS).
Name them as the posting does. Do not include soft skills, degree subjects, or
generic phrases like "communication" or "problem solving". Empty list if the
posting names none.

language_requirements — human languages explicitly required. Nearly every
posting is ["English"]. Add another language only when the posting states it is
required, not merely preferred or "a plus".

Rules that matter:
- Use only the exact values listed above for the first three fields. Never
  invent a value or alter its spelling.
- Classify from what the posting says, not from what the company usually hires
  for. If the text does not state a restriction, it is not restricted.
- If the text is truncated or mostly boilerplate, still give your best reading
  of the fields you can see rather than refusing."""


def build_prompt(row, text):
    return (
        f"Company: {row.get('company_name', '')}\n"
        f"Title: {row.get('title', '')}\n\n"
        f"Posting text:\n{text[:MAX_TEXT_CHARS]}"
    )


def classify_one(client, row, text, effort):
    """Classify one posting. Returns (row_id, Classification) or (row_id, None)."""
    try:
        response = client.messages.parse(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            # The taxonomy is identical on every request, so cache it -- with a
            # few hundred postings this is the difference between paying for the
            # instructions once and paying for them every time.
            system=[{
                "type": "text",
                "text": SYSTEM,
                "cache_control": {"type": "ephemeral"},
            }],
            output_config={"effort": effort},
            messages=[{"role": "user", "content": build_prompt(row, text)}],
            output_format=Classification,
        )
        if response.stop_reason == "refusal":
            return row["id"], None
        return row["id"], response.parsed_output
    except anthropic.APIError as e:
        print(f"    API error for {row['id'][:8]}: {type(e).__name__}: {e}", flush=True)
        return row["id"], None


def clean(values, allowed):
    """Drop anything outside the taxonomy -- structured output constrains the
    shape, not the vocabulary, so a stray value is still possible."""
    seen, out = set(), []
    for v in values:
        v = v.strip()
        if v and v in allowed and v not in seen:
            seen.add(v)
            out.append(v)
    return out


# -- Data I/O ------------------------------------------------------------------
def load_details():
    if not os.path.exists(DETAILS_CSV):
        sys.exit(f"ERROR: {DETAILS_CSV} not found. Run this on the data branch.")
    with open(DETAILS_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader), reader.fieldnames


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
def run_eval(client, rows, texts, limit, effort):
    """
    Score the model against postings a human already classified.

    Reported per field:
      exact  — the model's set of values matches the human's exactly
      overlap— they agree on at least one value (the useful bar for multi-label)
    """
    gold = [r for r in rows
            if r.get("category", "").strip() and r["id"] in texts]
    if not gold:
        sys.exit("No human-labelled postings with description text to evaluate against.")
    if limit:
        gold = gold[:limit]

    print(f"  Scoring {len(gold)} human-classified postings (model={MODEL}, effort={effort})")
    print()

    fields = ["category", "class_year", "degree_enrollment", "language_requirements"]
    stats = {f: {"exact": 0, "overlap": 0, "n": 0} for f in fields}
    skills_hit = skills_n = 0
    disagreements = []

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(classify_one, client, r, texts[r["id"]], effort): r
                   for r in gold}
        for i, fut in enumerate(as_completed(futures), 1):
            row = futures[fut]
            _, result = fut.result()
            if result is None:
                continue

            got = {
                "category":              clean(result.category, CATEGORIES),
                "class_year":            clean(result.class_year, CLASS_YEARS),
                "degree_enrollment":     clean(result.degree_enrollment, DEGREES),
                "language_requirements": [v.strip() for v in result.language_requirements],
            }
            for f in fields:
                human = {v.strip() for v in row.get(f, "").split("|") if v.strip()}
                if not human:
                    continue
                mine = set(got[f])
                stats[f]["n"] += 1
                if mine == human:
                    stats[f]["exact"] += 1
                if mine & human:
                    stats[f]["overlap"] += 1
                elif f == "category":
                    disagreements.append(
                        (row.get("company_name", "")[:22], row.get("title", "")[:34],
                         MULTI_SEP.join(sorted(human)), MULTI_SEP.join(sorted(mine)))
                    )

            human_skills = {v.strip().lower() for v in row.get("additional_skills", "").split("|") if v.strip()}
            if human_skills:
                skills_n += 1
                if human_skills & {s.strip().lower() for s in result.additional_skills}:
                    skills_hit += 1

            if i % 10 == 0:
                print(f"    {i}/{len(gold)} scored", flush=True)

    print()
    print("-" * 68)
    print(f"  {'field':<24} {'exact':>14} {'overlap':>14}")
    print("-" * 68)
    for f in fields:
        s = stats[f]
        if not s["n"]:
            continue
        print(f"  {f:<24} {s['exact']:>5}/{s['n']:<3} {s['exact']/s['n']:>5.0%} "
              f"{s['overlap']:>5}/{s['n']:<3} {s['overlap']/s['n']:>5.0%}")
    if skills_n:
        print(f"  {'additional_skills':<24} {'':>14} {skills_hit:>5}/{skills_n:<3} "
              f"{skills_hit/skills_n:>5.0%}")
    print("-" * 68)

    if disagreements:
        print()
        print(f"  Category disagreements ({len(disagreements)}) — worth reading before trusting this:")
        for company, title, human, mine in disagreements[:15]:
            print(f"    {company:<22} {title:<34}")
            print(f"      human: {human}")
            print(f"      model: {mine}")
    print()
    print("  Nothing was written. Use --limit / no flag to classify for real.")


# -- Main ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eval", action="store_true",
                    help="score against human-classified postings; writes nothing")
    ap.add_argument("--limit", type=int, default=0, help="max postings (0 = all)")
    ap.add_argument("--effort", default=EFFORT,
                    choices=["low", "medium", "high", "xhigh", "max"],
                    help=f"reasoning effort (default {EFFORT})")
    ap.add_argument("--reclassify", action="store_true",
                    help="also redo postings that already have a category")
    ap.add_argument("--dry-run", action="store_true", help="report the work list only")
    args = ap.parse_args()

    rows, fieldnames = load_details()
    texts = load_texts()

    client = anthropic.Anthropic()

    if args.eval:
        run_eval(client, rows, texts, args.limit, args.effort)
        return

    todo = [r for r in rows
            if r["id"] in texts
            and (args.reclassify or not r.get("category", "").strip())]

    no_text = [r for r in rows
               if r["id"] not in texts and not r.get("category", "").strip()]

    print("=" * 68)
    print("  Automatic classification")
    print("=" * 68)
    print(f"  postings              : {len(rows):,}")
    print(f"  with description text : {len(texts):,}")
    print(f"  already classified    : {sum(1 for r in rows if r.get('category','').strip()):,}")
    print(f"  blocked (no text yet) : {len(no_text):,}  — run backfill_text.py")
    if args.limit:
        todo = todo[: args.limit]
    print(f"  to classify this run  : {len(todo):,}")
    print(f"  model / effort        : {MODEL} / {args.effort}")
    print()

    if args.dry_run:
        print("  (dry run — nothing sent, nothing written)")
        return
    if not todo:
        print("  Nothing to do.")
        return

    by_id = {r["id"]: r for r in rows}
    done = failed = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(classify_one, client, r, texts[r["id"]], args.effort): r
                   for r in todo}
        for fut in as_completed(futures):
            row = futures[fut]
            jid, result = fut.result()
            if result is None:
                failed += 1
                print(f"  FAIL {row.get('company_name','')[:24]}")
                continue

            target = by_id[jid]
            target["category"]              = MULTI_SEP.join(clean(result.category, CATEGORIES))
            target["class_year"]            = MULTI_SEP.join(clean(result.class_year, CLASS_YEARS))
            target["degree_enrollment"]     = MULTI_SEP.join(clean(result.degree_enrollment, DEGREES))
            target["additional_skills"]     = MULTI_SEP.join(s.strip() for s in result.additional_skills if s.strip())
            target["language_requirements"] = MULTI_SEP.join(s.strip() for s in result.language_requirements if s.strip())
            # Leave `status` alone. Automatic classification is not the same as
            # a human having reviewed the posting, and collapsing the two would
            # throw away the distinction permanently.
            done += 1
            print(f"  OK   {row.get('company_name','')[:24]:<24} {target['category']}")

    save_details(rows, fieldnames)
    print()
    print(f"  Classified : {done:,}   Failed : {failed:,}")
    print(f"  data/job_details.csv updated")


if __name__ == "__main__":
    main()
