#!/usr/bin/env python3
"""
Backfill application_closes in remote_jobs.csv and nyc_jobs.csv by matching
jobs with Simplify inactive list.

Since Simplify doesn't track exact close times, we use today's date as an
approximate close time for any job currently in the inactive list.
Only updates rows where application_closes is not already set.
"""

import requests
import re
import csv
from pathlib import Path
from datetime import datetime, timezone

def fetch_readme(filename="README-Inactive.md"):
    url = f"https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev/{filename}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching {filename}: {e}")
        return None

def parse_simplify_inactive(content):
    """Extract (company_lower, role_lower) -> display names from inactive README."""
    jobs = {}
    rows = re.findall(r'<tr>(.*?)</tr>', content, re.DOTALL)

    for row in rows:
        cells = re.findall(r'<td[^>]*>([^<]*(?:<[^>]*>[^<]*)*?)</td>', row)
        if len(cells) < 5:
            continue

        company_raw = cells[0]
        company_match = re.search(r'>([^<]+)<', company_raw)
        company = company_match.group(1).strip() if company_match else company_raw.strip()
        company = re.sub(r'<[^>]+>|\[|\]|\(.*?\)|↳', '', company).strip()

        role_raw = cells[1] if len(cells) > 1 else ""
        role = re.sub(r'<[^>]+>|🛂|🇺🇸|🔒|🔥|🎓|↳', '', role_raw).strip()

        if company and role and len(company) >= 3 and len(role) >= 4:
            if company != "Company" and role != "Role":
                jobs[(company.lower(), role.lower())] = {"company": company, "role": role}

    return jobs

def similarity_score(str1, str2):
    str1, str2 = str1.lower(), str2.lower()
    if str1 == str2:
        return 100
    if str1 in str2 or str2 in str1:
        return 80
    if set(str1.split()) & set(str2.split()):
        return 60
    return 0

def update_csv(csv_path, simplify_inactive, now):
    """Update application_closes in a CSV where it's not already set."""
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    updated = 0
    for row in rows:
        # Skip if already has a close date
        if row.get("application_closes"):
            continue

        company = row.get("company_name", "").strip()
        role = row.get("title", "").strip()

        best_score = 0
        for (simp_company, simp_role), simp_data in simplify_inactive.items():
            score = (similarity_score(company, simp_data["company"]) +
                     similarity_score(role, simp_data["role"])) / 2
            if score > best_score:
                best_score = score

        if best_score >= 70:
            row["application_closes"] = now
            updated += 1

    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    return updated

def main():
    print("=" * 70)
    print("Backfill Application Close Times from Simplify")
    print("=" * 70 + "\n")

    print("Fetching Simplify inactive list...")
    content = fetch_readme("README-Inactive.md")
    if not content:
        print("Error: Could not fetch Simplify inactive list")
        return

    simplify_inactive = parse_simplify_inactive(content)
    print(f"  Found {len(simplify_inactive)} inactive jobs on Simplify\n")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    total_updated = 0

    for csv_path in [Path("data/nyc_jobs.csv"), Path("data/remote_jobs.csv")]:
        if not csv_path.exists():
            print(f"  Skipping {csv_path} (not found)")
            continue
        updated = update_csv(csv_path, simplify_inactive, now)
        print(f"  {csv_path.name}: updated {updated} jobs")
        total_updated += updated

    print(f"\n✓ Total jobs updated: {total_updated}")

if __name__ == "__main__":
    main()
