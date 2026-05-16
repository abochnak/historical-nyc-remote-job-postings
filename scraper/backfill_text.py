#!/usr/bin/env python3
"""
Backfill raw_text for entries in job_details.jsonl that have an archive_url
but empty raw_text. Processes one chunk at a time to avoid rate-limiting.

Usage
-----
    python scraper/backfill_text.py                  # default: 10 per run
    python scraper/backfill_text.py --batch-size 5
    python scraper/backfill_text.py --batch-size 20 --delay 5
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from html.parser import HTMLParser

ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR  = os.path.join(ROOT, "data")
JSONL     = os.path.join(DATA_DIR, "job_details.jsonl")


class _TextExtractor(HTMLParser):
    _SKIP = {"script", "style", "head", "noscript", "iframe", "nav", "footer"}

    def __init__(self):
        super().__init__()
        self._depth = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self._SKIP:
            self._depth += 1

    def handle_endtag(self, tag):
        if tag.lower() in self._SKIP:
            self._depth = max(0, self._depth - 1)

    def handle_data(self, data):
        if not self._depth:
            s = data.strip()
            if s:
                self.parts.append(s)


def fetch_page_text(url, timeout=30):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 (compatible; job-archiver/1.0)"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            html = r.read().decode(errors="replace")
        p = _TextExtractor()
        p.feed(html)
        return "\n".join(p.parts)
    except Exception as e:
        return ""


def load_jsonl():
    if not os.path.exists(JSONL):
        return []
    entries = []
    with open(JSONL, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except Exception:
                    pass
    return entries


def save_jsonl(entries):
    with open(JSONL, "w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps({
                "id":       entry.get("id", ""),
                "raw_text": entry.get("raw_text", ""),
            }) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=10,
                        help="Number of entries to backfill per run (default: 10)")
    parser.add_argument("--delay", type=float, default=3.0,
                        help="Seconds to wait between fetches (default: 3)")
    args = parser.parse_args()

    entries = load_jsonl()
    needs_text = [e for e in entries if e.get("archive_url") and not e.get("raw_text", "").strip()]
    total_empty = len(needs_text)

    print(f"  Total entries      : {len(entries)}")
    print(f"  Missing raw_text   : {total_empty}")

    if not needs_text:
        print("  Nothing to backfill — all entries already have raw_text.")
        return

    batch = needs_text[: args.batch_size]
    print(f"  Processing batch   : {len(batch)} (--batch-size {args.batch_size})")
    print(f"  Remaining after    : {total_empty - len(batch)}")
    print()

    entry_map = {e["id"]: e for e in entries}
    filled = 0

    for i, entry in enumerate(batch, 1):
        jid     = entry["id"]
        company = entry.get("company_name", "")
        title   = entry.get("title", "")[:50]
        url     = entry["archive_url"]

        print(f"  [{i:>2}/{len(batch)}] {company} — {title}")
        print(f"         {url[:70]}")
        text = fetch_page_text(url)
        if text.strip():
            entry_map[jid]["raw_text"] = text
            filled += 1
            print(f"         OK ({len(text):,} chars)")
        else:
            print(f"         FAILED — leaving empty")

        if i < len(batch):
            time.sleep(args.delay)

    save_jsonl(list(entry_map.values()))

    remaining = total_empty - filled
    print()
    print(f"  Filled  : {filled}/{len(batch)}")
    print(f"  Still empty : {remaining} — run again to continue")
    print()
    print("  Saved job_details.jsonl")
    if remaining:
        print(f"  Run again to process the next batch of up to {args.batch_size}.")


if __name__ == "__main__":
    main()
