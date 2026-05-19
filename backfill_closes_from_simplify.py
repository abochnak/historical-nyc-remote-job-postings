#!/usr/bin/env python3
"""
Backfill application_closes by matching jobs with Simplify inactive list.

Since Simplify doesn't track exact close times, we use the date a job
first appears in README-Inactive.md as an approximate close time.
"""

import requests
import re
import csv
from pathlib import Path
from datetime import datetime, timezone

def fetch_readme(filename="README-Inactive.md"):
    """Fetch README content from Simplify repo."""
    url = f"https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev/{filename}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching {filename}: {e}")
        return None

def parse_simplify_inactive(content):
    """Extract (company, role) pairs from inactive README."""
    jobs = {}
    rows = re.findall(r'<tr>(.*?)</tr>', content, re.DOTALL)

    for row in rows:
        cells = re.findall(r'<td[^>]*>([^<]*(?:<[^>]*>[^<]*)*?)</td>', row)
        if len(cells) < 5:
            continue

        # Extract company
        company_raw = cells[0]
        company_match = re.search(r'>([^<]+)<', company_raw)
        company = company_match.group(1).strip() if company_match else company_raw.strip()
        company = re.sub(r'<[^>]+>|\[|\]|\(.*?\)|↳', '', company).strip()

        # Extract role
        role_raw = cells[1] if len(cells) > 1 else ""
        role = re.sub(r'<[^>]+>|🛂|🇺🇸|🔒|🔥|🎓|↳', '', role_raw).strip()

        if company and role and len(company) >= 3 and len(role) >= 4:
            if company != "Company" and role != "Role":
                key = (company.lower(), role.lower())
                jobs[key] = {"company": company, "role": role}

    return jobs

def similarity_score(str1, str2):
    """Simple similarity score for string matching."""
    str1, str2 = str1.lower(), str2.lower()

    if str1 == str2:
        return 100
    if str1 in str2 or str2 in str1:
        return 80
    words1 = set(str1.split())
    words2 = set(str2.split())
    if words1 & words2:
        return 60
    return 0

def match_jobs(simplify_inactive, historical_jobs):
    """Match Simplify inactive jobs to historical job data."""
    matches = {}
    now = datetime.now(timezone.utc).isoformat()

    for (hist_id, hist_company, hist_role) in historical_jobs.keys():
        best_score = 0
        best_match = None

        for (simp_company, simp_role), simp_data in simplify_inactive.items():
            company_score = similarity_score(hist_company, simp_data["company"])
            role_score = similarity_score(hist_role, simp_data["role"])
            combined = (company_score + role_score) / 2

            if combined > best_score and combined > 50:
                best_score = combined
                best_match = (simp_data["company"], simp_data["role"])

        if best_match:
            matches[hist_id] = {
                "application_closes": now,
                "matched_company": best_match[0],
                "matched_role": best_match[1],
                "confidence": best_score
            }

    return matches

def update_job_details_csv(matches):
    """Update job_details.csv with close times."""
    csv_path = Path("data/job_details.csv")
    print(f"Updating {csv_path}...")

    rows = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    updated = 0
    for row in rows:
        job_id = row.get("id", "")
        if job_id in matches:
            row["application_closes"] = matches[job_id]["application_closes"]
            updated += 1
        elif not row.get("application_closes"):
            row["application_closes"] = ""

    fieldnames = [
        "id", "company_name", "title", "job_url",
        "archive_url", "archive_source",
        "archive_status",
        "category", "class_year", "degree_enrollment", "additional_skills",
        "language_requirements", "date_archived", "application_closes", "status", "source", "first_seen_date",
    ]

    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    print(f"  ✓ Updated {updated} jobs with close times")
    return updated

def load_historical_jobs():
    """Load all historical job data."""
    jobs = {}

    for csv_file in [Path("data/nyc_jobs.csv"), Path("data/remote_jobs.csv")]:
        if csv_file.exists():
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    job_id = row.get("id", "").strip()
                    company = row.get("company_name", "").strip()
                    role = row.get("title", "").strip()

                    if job_id and company and role:
                        key = (job_id, company.lower(), role.lower())
                        jobs[key] = True

    return jobs

def main():
    print("=" * 70)
    print("Backfill Application Close Times from Simplify")
    print("=" * 70 + "\n")

    print("Loading historical job data...")
    historical = load_historical_jobs()
    print(f"  Loaded {len(historical)} jobs\n")

    print("Fetching Simplify inactive list...")
    content = fetch_readme("README-Inactive.md")
    if not content:
        print("Error: Could not fetch Simplify inactive list")
        return

    simplify_inactive = parse_simplify_inactive(content)
    print(f"  Found {len(simplify_inactive)} inactive jobs on Simplify\n")

    print("Matching jobs...")
    matches = match_jobs(simplify_inactive, dict.fromkeys(historical.keys()))

    high_confidence = {k: v for k, v in matches.items() if v["confidence"] >= 70}
    medium_confidence = {k: v for k, v in matches.items() if 50 <= v["confidence"] < 70}

    print(f"  High confidence matches (>=70%): {len(high_confidence)}")
    print(f"  Medium confidence matches (50-70%): {len(medium_confidence)}")

    all_matches = {**high_confidence, **medium_confidence}

    if all_matches:
        updated = update_job_details_csv(all_matches)
        print(f"\n✓ Successfully updated {updated} jobs")

        print(f"\nSample matches:")
        for job_id, match in list(all_matches.items())[:3]:
            print(f"  {match['matched_company']} — {match['matched_role']}")
            print(f"    Confidence: {match['confidence']:.0f}%")
    else:
        print("No matches found")

if __name__ == "__main__":
    main()
