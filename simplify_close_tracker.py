#!/usr/bin/env python3
"""
Simplify Job Status Tracker

This script:
1. Fetches current job listings from Simplify README files
2. Tracks which jobs are inactive (closed)
3. Compares to previous state to detect transitions
4. Records close dates for newly closed jobs
"""

import requests
import re
from pathlib import Path
from datetime import datetime, timezone
import csv
from urllib.parse import urljoin

def fetch_readme(branch="dev", filename="README.md"):
    """Fetch README content from Simplify repo."""
    url = f"https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/{branch}/{filename}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching {filename}: {e}")
        return None

def parse_jobs_from_readme(content, status="active"):
    """Extract jobs from README HTML tables."""
    jobs = {}

    # Split into table rows
    rows = re.findall(r'<tr>(.*?)</tr>', content, re.DOTALL)

    for row in rows:
        # Extract all td contents
        cells = re.findall(r'<td[^>]*>([^<]*(?:<[^>]*>[^<]*)*?)</td>', row)
        if len(cells) < 5:  # Need at least Company, Role, Location, Application, Age
            continue

        # Extract company (usually in first cell with link)
        company_raw = cells[0]
        company_match = re.search(r'>([^<]+)<', company_raw)
        company = company_match.group(1).strip() if company_match else company_raw.strip()

        # Remove markdown/html artifacts
        company = re.sub(r'<[^>]+>|\[|\]|\(.*?\)|↳', '', company).strip()

        # Extract role (second cell)
        role_raw = cells[1] if len(cells) > 1 else ""
        role = re.sub(r'<[^>]+>|🛂|🇺🇸|🔒|🔥|🎓|↳', '', role_raw).strip()

        # Skip invalid entries
        if not company or not role or len(company) < 3 or len(role) < 4:
            continue
        if company == "Company" or role == "Role":
            continue

        key = (company, role)
        if key not in jobs:
            jobs[key] = {"company": company, "role": role, "status": status}

    return jobs

def load_previous_state():
    """Load previously tracked job states."""
    state_file = Path("simplify_job_state.json")
    if state_file.exists():
        try:
            import json
            with open(state_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_current_state(active_jobs, inactive_jobs):
    """Save current job states."""
    import json
    state = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "active": {str(k): v for k, v in active_jobs.items()},
        "inactive": {str(k): v for k, v in inactive_jobs.items()}
    }

    with open("simplify_job_state.json", 'w') as f:
        json.dump(state, f, indent=2)

def detect_transitions(prev_state, active_jobs, inactive_jobs):
    """Detect jobs that transitioned from active to inactive."""
    now = datetime.now(timezone.utc).isoformat()
    transitions = []

    # Get previous job sets
    prev_active = set(k[0] + " | " + k[1] for k in (prev_state.get("active") or {}).keys())
    prev_inactive = set(k[0] + " | " + k[1] for k in (prev_state.get("inactive") or {}).keys())

    # Get current job sets
    current_active = set(f"{k[0]} | {k[1]}" for k in active_jobs.keys())
    current_inactive = set(f"{k[0]} | {k[1]}" for k in inactive_jobs.keys())

    # Find newly closed (were active, now inactive)
    newly_closed = (prev_active - current_active) & current_inactive

    for job_str in newly_closed:
        company, role = job_str.split(" | ", 1)
        transitions.append({
            "company": company,
            "role": role,
            "status_change": "active -> inactive",
            "timestamp": now,
            "reason": "Job closed"
        })

    # Find newly reopened (were inactive, now active)
    newly_reopened = (prev_inactive - current_inactive) & current_active
    for job_str in newly_reopened:
        company, role = job_str.split(" | ", 1)
        transitions.append({
            "company": company,
            "role": role,
            "status_change": "inactive -> active",
            "timestamp": now,
            "reason": "Job reopened"
        })

    return transitions

def save_transitions(transitions):
    """Append transitions to tracking CSV."""
    csv_file = Path("simplify_transitions.csv")
    file_exists = csv_file.exists()

    with open(csv_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "company", "role", "status_change", "reason"
        ])
        if not file_exists:
            writer.writeheader()
        writer.writerows(transitions)

    return len(transitions)

def main():
    print("=" * 70)
    print("Simplify Job Status Tracker")
    print("=" * 70 + "\n")

    # Fetch current state
    print("Fetching current job listings from Simplify...")
    active_content = fetch_readme("dev", "README.md")
    inactive_content = fetch_readme("dev", "README-Inactive.md")

    if not active_content or not inactive_content:
        print("Error: Could not fetch README files")
        return

    # Parse jobs
    print("Parsing active jobs...")
    active_jobs = parse_jobs_from_readme(active_content, "active")
    print(f"  Found {len(active_jobs)} active jobs")

    print("Parsing inactive jobs...")
    inactive_jobs = parse_jobs_from_readme(inactive_content, "inactive")
    print(f"  Found {len(inactive_jobs)} inactive jobs")

    # Load previous state
    prev_state = load_previous_state()
    print(f"\nPrevious state loaded")

    # Detect transitions
    transitions = detect_transitions(prev_state, active_jobs, inactive_jobs)
    print(f"\nStatus transitions detected: {len(transitions)}")

    if transitions:
        print("\nTransitions:")
        for trans in transitions:
            print(f"  {trans['company']} — {trans['role']}")
            print(f"    {trans['status_change']} at {trans['timestamp']}\n")

        saved = save_transitions(transitions)
        print(f"  Saved {saved} transitions to simplify_transitions.csv")

    # Save current state
    save_current_state(active_jobs, inactive_jobs)
    print(f"\nCurrent state saved for next run")

    # Summary
    print(f"\nSummary:")
    print(f"  Active jobs: {len(active_jobs)}")
    print(f"  Inactive jobs: {len(inactive_jobs)}")
    print(f"  Total: {len(active_jobs) + len(inactive_jobs)}")

if __name__ == "__main__":
    main()
