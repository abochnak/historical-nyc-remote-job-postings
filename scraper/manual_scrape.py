#!/usr/bin/env python3
"""
Manual scraper Flask UI
=======================
Simple interface to manually scrape job postings and extract text.

Usage:
    python scraper/manual_scrape.py
    # Open http://localhost:5000
"""

import os
import json
import csv
import uuid
from datetime import datetime, timezone
from flask import Flask, render_template, request, jsonify, flash, redirect, url_for

# Import scraper functions
import sys
sys.path.insert(0, os.path.dirname(__file__))
from update import fetch_or_archive, load_details, save_details, DETAILS_CSV, DETAILS_JSONL, DETAILS_HEADERS

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")

@app.route("/")
def index():
    """Show manual scraper form."""
    details = load_details()
    recent = sorted(
        [j for j in details.values() if j.get("archive_source") == "live" or (j.get("archive_source") and "manual" in j.get("archive_source", ""))],
        key=lambda x: x.get("date_archived", ""),
        reverse=True
    )[:10]
    return render_template("manual_scrape.html", recent=recent)

@app.route("/scrape", methods=["POST"])
def scrape():
    """Manually scrape a URL."""
    url = request.form.get("url", "").strip()
    company = request.form.get("company", "").strip()
    title = request.form.get("title", "").strip()

    if not url:
        flash("URL is required.", "error")
        return redirect(url_for("index"))

    # Scrape the URL
    arc_url, arc_src, arc_status, raw_text = fetch_or_archive(url)

    # Load existing details
    details = load_details()

    # Create or update entry
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    details[job_id] = {
        "id":                   job_id,
        "company_name":         company or "Manual",
        "title":                title or url.split("/")[-1][:50],
        "job_url":              url,
        "archive_url":          arc_url,
        "archive_source":       arc_src,
        "archive_status":       arc_status,
        "category":             "",
        "class_year":           "",
        "degree_enrollment":    "",
        "additional_skills":    "",
        "language_requirements":"",
        "date_archived":        now,
        "status":               "unreviewed",
        "source":               "manual",
        "first_seen_date":      now,
        "raw_text":             raw_text,
    }

    # Save
    save_details(details)

    if arc_status == "success":
        flash(f"✓ Scraped: {company or 'Job'} ({arc_src}, {len(raw_text):,} chars)")
    else:
        flash(f"✗ Failed to scrape from {url}", "error")

    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True, port=5001)
