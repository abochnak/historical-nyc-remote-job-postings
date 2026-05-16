"""
NYC Job Review Flask App
========================
Security features:
  - Rate limiting (5 failed attempts -> 15 min lockout per IP)
  - CSRF protection on all forms (Flask-WTF)
  - Secure session cookies (HttpOnly, SameSite=Strict, 8hr expiry)
  - Constant-time password comparison (prevents timing attacks)
"""

import base64
import json
import re
import uuid
import csv
import hmac
import io
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from functools import wraps
from html.parser import HTMLParser

import html as _html

import requests
from dotenv import load_dotenv
from flask import (Flask, flash, redirect, render_template,
                   request, session, url_for)
from flask_wtf import CSRFProtect
from flask_wtf.csrf import CSRFError
from markupsafe import Markup

load_dotenv()

def update_jsonl_for_job(job_id, company, title, date_archived, archive_url):
    """Fetch raw text from archive URL and upsert the entry in job_details.jsonl."""
    raw_text = fetch_page_text(archive_url)
    if not raw_text.strip():
        return
    entries, sha = load_jsonl_gh("data/job_details.jsonl")
    entry = {
        "id":            job_id,
        "company_name":  company,
        "title":         title,
        "date_archived": date_archived,
        "archive_url":   archive_url,
        "raw_text":      raw_text,
    }
    updated = [e for e in entries if e.get("id") != job_id]
    updated.append(entry)
    updated.append({"id": job_id, "raw_text": raw_text})
    save_jsonl_gh("data/job_details.jsonl", updated, sha,
                  f"review: archive text for {company} — {title[:50]}")


# -- URL deduplication --------------------------------------------------------
def extract_job_id(url):
    if not url:
        return None
    patterns = [
        r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
        r'/jobs?/(\d{7,})',
        r'/(\d{15,})',
        r'/jobs/(\d{4,})/job',
        r'/jobs/(\d{7,})/',
        r'/(\d{8,})$',
        r'/(JR\d+)',
        r'/(R-\d+)',
        r'/(REQ[-_][\w]+)',
        r'search/(\d{10,})',
    ]
    for pattern in patterns:
        m = re.search(pattern, url, re.IGNORECASE)
        if m:
            return m.group(1) if m.lastindex else m.group(0)
    import urllib.parse
    path = urllib.parse.urlparse(url).path.rstrip("/")
    return path.split("/")[-1] if path else url

def url_matches(url1, url2):
    if not url1 or not url2:
        return False
    if url1.rstrip("/") == url2.rstrip("/"):
        return True
    id1, id2 = extract_job_id(url1), extract_job_id(url2)
    return bool(id1 and id2 and len(id1) >= 4 and id1 == id2)


# -- App setup -----------------------------------------------------------------
_here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
app = Flask(__name__, template_folder=os.path.join(_here, 'templates'))
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-me-in-production")

app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Strict',
    PERMANENT_SESSION_LIFETIME=28800,  # 8 hours
