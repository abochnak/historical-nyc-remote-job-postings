#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings -- Incremental Updater
==========================================================
Fetches recent commits to listings.json, appends new NYC/remote jobs,
archives job URLs via Wayback Machine (manual archive via Flask UI if failed),
and maintains a persistent pending_archive.csv queue so no job is
ever lost due to the per-run archive cap.

Usage
-----
    python scraper/update.py              # check last 5 commits (default)
    python scraper/update.py --commits 10
    python scraper/update.py --skip-archive
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

# -- URL deduplication --------------------------------------------------------
def extract_job_id(url):
    """Extract the most unique identifier from a job URL for fuzzy deduplication."""
    if not url:
        return None
    patterns = [
        r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',  # UUID
        r'/jobs?/(\d{7,})',           # Greenhouse
        r'/(\d{15,})',                # TikTok-style long IDs
        r'/jobs/(\d{4,})/job',        # iCIMS
        r'/jobs/(\d{7,})/',           # Amazon
        r'/(\d{8,})$',               # Long numeric at end
        r'/(JR\d+)',                  # Workday JR codes
        r'/(R-\d+)',                  # Workday R- codes
        r'/(REQ[-_][\w]+)',           # REQ codes
        r'search/(\d{10,})',          # TikTok search
    ]
    for pattern in patterns:
        m = re.search(pattern, url, re.IGNORECASE)
        if m:
            return m.group(1) if m.lastindex else m.group(0)
    # Fallback: strip query params, use last path segment
    path = urllib.parse.urlparse(url).path.rstrip("/")
    return path.split("/")[-1] if path else url

def url_matches(url1, url2):
    """Return True if two URLs refer to the same job via ID matching."""
    if not url1 or not url2:
        return False
    if url1.rstrip("/") == url2.rstrip("/"):
        return True
    id1 = extract_job_id(url1)
    id2 = extract_job_id(url2)
    if id1 and id2 and len(id1) >= 4:
        return id1 == id2
    return False


# -- Config --------------------------------------------------------------------
OWNER     = "SimplifyJobs"
REPO      = "Summer2026-Internships"
FILE_PATH = ".github/scripts/listings.json"
API_BASE  = f"https://api.github.com/repos/{OWNER}/{REPO}"
RAW_BASE  = f"https://raw.githubusercontent.com/{OWNER}/{REPO}"

ROOT        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR    = os.path.join(ROOT, "data")
NYC_CSV     = os.path.join(DATA_DIR, "nyc_jobs.csv")
REM_CSV     = os.path.join(DATA_DIR, "remote_jobs.csv")
EXCLUDE_CSV = os.path.join(DATA_DIR, "excluded_jobs.csv")
DETAILS_CSV  = os.path.join(DATA_DIR, "job_details.csv")
DETAILS_JSONL = os.path.join(DATA_DIR, "job_details.jsonl")
QUEUE_CSV      = os.path.join(DATA_DIR, "pending_archive.csv")

MAX_ARCHIVE_PER_RUN  = 5  # matches 30-min cron; typical window has 1-3 new jobs
MAX_ARCHIVE_ATTEMPTS = 3  # stop retrying after this many failures

CSV_HEADERS = [
    "company_name", "title", "recruiting_season",
    "date_posted", "first_seen_date", "url", "id", "source",
]

DETAILS_HEADERS = [
    "id", "company_name", "title", "job_url",
    "archive_url", "archive_source",
    "archive_status",
    "category", "class_year", "degree_enrollment", "additional_skills", "language_requirements", "date_archived", "status", "source", "first_seen_date",
]

EXCL_HEADERS = [
    "id", "company_name", "reason",
