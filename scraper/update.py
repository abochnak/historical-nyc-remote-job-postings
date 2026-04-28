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
import csv
import hmac
import io
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from functools import wraps

import requests
from dotenv import load_dotenv
from flask import (Flask, flash, redirect, render_template,
                   request, session, url_for)
from flask_wtf import CSRFProtect
from flask_wtf.csrf import CSRFError

load_dotenv()

# -- App setup -----------------------------------------------------------------
_here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
app = Flask(__name__, template_folder=os.path.join(_here, 'templates'))
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-me-in-production")

app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Strict',
    PERMANENT_SESSION_LIFETIME=28800,  # 8 hours
    WTF_CSRF_TIME_LIMIT=3600,          # 1 hour
)

csrf = CSRFProtect(app)

# -- Config --------------------------------------------------------------------
REVIEW_PASSWORD = os.environ.get("REVIEW_PASSWORD", "changeme")
GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")
GITHUB_USER     = os.environ.get("GITHUB_USER", "")
GITHUB_REPO     = os.environ.get("GITHUB_REPO", "historical-nyc-remote-job-postings")
GITHUB_BRANCH   = os.environ.get("GITHUB_BRANCH", "data")

DETAILS_HEADERS = [
    "id", "company_name", "title", "job_url",
    "archive_url", "archive_source",
    "archive_status",
    "category", "class_year", "additional_skills", "language_requirements", "date_archived", "status", "first_seen_date",
]

EXCL_HEADERS = [
    "id", "company_name", "reason",
    "blocked_title", "blocked_company", "blocked_date",
]

ROLE_CATEGORIES = [
    "Software Engineering",
    "Data Engineering",
    "Data Analysis",
    "Machine Learning / AI",
    "Cybersecurity",
    "Product Management",
    "Quant / Finance",
    "IT Support",
    "Other",
]

CLASS_YEARS = [
    "Open to All",
    "Freshman",
    "Sophomore",
    "Junior",
    "Grad Student",
]

LANGUAGES = [
    "English",
    "Chinese (Mandarin)",
    "Chinese (Cantonese)",
    "Spanish",
    "French",
    "German",
    "Japanese",
    "Korean",
    "Portuguese",
    "Arabic",
    "Hindi",
    "Russian",
    "Other",
]

EXCLUSION_REASONS = [
    "Not relevant to tech majors",
    "Non-US position",
    "PhD students only",
    "Non-English speaking role",
]

# -- Rate limiting -------------------------------------------------------------
MAX_ATTEMPTS = 5
LOCKOUT_SECS = 900  # 15 minutes
_failed: dict = defaultdict(list)

def get_ip():
    return (request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or request.remote_addr or "unknown")

def is_locked(ip):
    now = time.time()
    _failed[ip] = [t for t in _failed[ip] if now - t < LOCKOUT_SECS]
    return len(_failed[ip]) >= MAX_ATTEMPTS

def lockout_remaining(ip):
    if not _failed[ip]:
        return 0
    return max(0, int(LOCKOUT_SECS - (time.time() - min(_failed[ip]))))

# -- GitHub API ----------------------------------------------------------------
def gh_headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

def get_file(path):
    url = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/{path}"
    r = requests.get(url, headers=gh_headers(), params={"ref": GITHUB_BRANCH})
    if r.status_code == 404:
        return "", None
    r.raise_for_status()
    data = r.json()
    return base64.b64decode(data["content"]).decode("utf-8"), data["sha"]

def put_file(path, content, sha, message):
    url = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "branch":  GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    requests.put(url, headers=gh_headers(), json=payload).raise_for_status()

def load_csv_gh(path, fieldnames):
    content, sha = get_file(path)
    if not content.strip():
        return [], sha
    return list(csv.DictReader(io.StringIO(content))), sha

def save_csv_gh(path, rows, fieldnames, sha, msg):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    put_file(path, buf.getvalue(), sha, msg)

# -- Auth ----------------------------------------------------------------------
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

@app.errorhandler(CSRFError)
def csrf_error(e):
    flash("Session expired or invalid request — please try again.")
    return redirect(url_for("review")), 400

@app.route("/login", methods=["GET", "POST"])
def login():
    ip = get_ip()
    if request.method == "POST":
        if is_locked(ip):
            mins = lockout_remaining(ip) // 60
            secs = lockout_remaining(ip) % 60
            flash(f"Too many failed attempts. Try again in {mins}m {secs}s.")
            return render_template("login.html"), 429

        submitted = request.form.get("password", "")
        if hmac.compare_digest(submitted, REVIEW_PASSWORD):
            _failed.pop(ip, None)
            session.permanent = True
            session["logged_in"] = True
            return redirect(url_for("review"))
        else:
            _failed[ip].append(time.time())
            left = MAX_ATTEMPTS - len(_failed[ip])
            if left > 0:
                flash(f"Incorrect password. {left} attempt(s) remaining.")
            else:
                flash("Too many failed attempts. Locked out for 15 minutes.")
            return render_template("login.html"), 401

    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# -- Review queue --------------------------------------------------------------
@app.route("/")
@login_required
def review():
    details, _ = load_csv_gh("data/job_details.csv", DETAILS_HEADERS)
    excl, _    = load_csv_gh("data/excluded_jobs.csv", EXCL_HEADERS)
    excl_ids   = {r["id"] for r in excl}
    unreviewed = [j for j in details
                  if j.get("status") == "unreviewed" and j.get("id") not in excl_ids]
    return render_template(
        "review.html",
        job=unreviewed[0] if unreviewed else None,
        unreviewed_count=len(unreviewed),
        reviewed_count=sum(1 for j in details if j.get("status") == "reviewed"),
        excluded_count=len(excl),
        exclusion_reasons=EXCLUSION_REASONS,
        languages=LANGUAGES,
        role_categories=ROLE_CATEGORIES,
        class_years=CLASS_YEARS,
    )

@app.route("/action", methods=["POST"])
@login_required
def action():
    job_id      = request.form.get("job_id", "").strip()
    act         = request.form.get("action", "")
    archive_url = request.form.get("archive_url", "").strip()

    if not job_id:
        flash("No job ID provided.")
        return redirect(url_for("review"))

    details, d_sha = load_csv_gh("data/job_details.csv", DETAILS_HEADERS)
    excl, e_sha    = load_csv_gh("data/excluded_jobs.csv", EXCL_HEADERS)
    job = next((j for j in details if j.get("id") == job_id), None)

    if not job:
        flash("Job not found.")
        return redirect(url_for("review"))

    if act == "save_archive":
        # Save archive URL without changing review status
        if archive_url:
            for j in details:
                if j.get("id") == job_id:
                    j["archive_url"] = archive_url
                    j["archive_source"] = (
                        "wayback"      if "web.archive.org" in archive_url else
                        "ghostarchive" if "ghostarchive.org" in archive_url else
                        "archive.ph"   if "archive.ph" in archive_url or "archive.today" in archive_url else
                        "manual"
                    )
                    j["archive_status"] = "success"
                    if not j.get("date_archived"):
                        j["date_archived"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    break
            save_csv_gh("data/job_details.csv", details, DETAILS_HEADERS, d_sha,
                        f"review: save archive URL for {job.get('company_name')} — {job.get('title')[:50]}")
            flash(f"Archive URL saved for {job.get('company_name')} — {job.get('title')}")
        else:
            flash("No archive URL provided.")
        return redirect(url_for("review"))

    if act == "exclude":
        reason = request.form.get("exclusion_reason", EXCLUSION_REASONS[0])
        if reason not in EXCLUSION_REASONS:
            reason = EXCLUSION_REASONS[0]
        excl.append({
            "id":              job_id,
            "company_name":    job.get("company_name", ""),
            "reason":          reason,
            "blocked_title":   job.get("title", ""),
            "blocked_company": job.get("company_name", ""),
            "blocked_date":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        details = [j for j in details if j.get("id") != job_id]
        save_csv_gh("data/excluded_jobs.csv", excl, EXCL_HEADERS, e_sha,
                    f"review: exclude {job.get('company_name')} — {job.get('title')[:50]}")
        save_csv_gh("data/job_details.csv", details, DETAILS_HEADERS, d_sha,
                    f"review: remove excluded job {job_id[:8]} from details")
        flash(f"Excluded: {job.get('company_name')} — {job.get('title')}")

    elif act == "reviewed":
        # Combine role and year categories as pipe-separated string
        role_cats  = [c for c in request.form.getlist("role_categories") if c in ROLE_CATEGORIES]
        class_year = request.form.get("class_year", "Open to All")
        if class_year not in CLASS_YEARS:
            class_year = "Open to All"
        category          = " | ".join(role_cats) if role_cats else ""
        additional_skills = request.form.get("additional_skills", "").strip()
        language_skills   = " | ".join(request.form.getlist("language_skills"))
        for j in details:
            if j.get("id") == job_id:
                j["status"]            = "reviewed"
                j["category"]          = category
                j["class_year"]        = class_year
                j["additional_skills"] = additional_skills
                j["language_skills"]   = language_skills
                if archive_url:
                    j["archive_url"] = archive_url
                    j["archive_source"] = (
                        "wayback"      if "web.archive.org" in archive_url else
                        "ghostarchive" if "ghostarchive.org" in archive_url else
                        "archive.ph"   if "archive.ph" in archive_url or "archive.today" in archive_url else
                        "manual"
                    )
                    j["archive_status"] = "success"
                    if not j.get("date_archived"):
                        j["date_archived"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                break
        save_csv_gh("data/job_details.csv", details, DETAILS_HEADERS, d_sha,
                    f"review: approve {job.get('company_name')} — {job.get('title')[:50]}")
        flash(f"Approved: {job.get('company_name')} — {job.get('title')}")

    return redirect(url_for("review"))
