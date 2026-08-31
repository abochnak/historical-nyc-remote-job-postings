# Simplify Application Close Time Tracking

## Overview

This system automatically tracks when jobs posted from Simplify have their applications closed by analyzing the Simplify GitHub repository and matching jobs with the historical database.

## Components

### 1. **simplify_close_tracker.py**
Monitors Simplify's active and inactive job lists to detect status transitions.

**What it does:**
- Fetches current README.md (active jobs) and README-Inactive.md (closed jobs) from Simplify
- Parses HTML tables to extract job listings
- Compares to previous state to detect transitions
- Saves transitions to `simplify_transitions.csv`
- Updates `simplify_job_state.json` for next run

**Run manually:**
```bash
python3 simplify_close_tracker.py
```

**Output:**
- `simplify_job_state.json` - Current job state (active/inactive)
- `simplify_transitions.csv` - Log of status changes

### 2. **backfill_closes_from_simplify.py**
Backfills historical job data with close times from Simplify inactive list.

**What it does:**
- Loads all historical jobs from `data/nyc_jobs.csv` and `data/remote_jobs.csv`
- Fetches Simplify's inactive list (README-Inactive.md)
- Uses fuzzy matching to link jobs (company + role similarity)
- Updates `data/job_details.csv` with `application_closes` timestamps
- Tracks confidence scores for matches

**Run manually:**
```bash
python3 backfill_closes_from_simplify.py
```

**Matching algorithm:**
- Exact match: 100% confidence
- Substring match: 80% confidence  
- Word overlap: 60% confidence
- Combined score used to determine match quality

## Data Schema

The `application_closes` field is now part of `data/job_details.csv`:

```csv
id,company_name,title,...,application_closes,status,...
9fc8a614-08fa-458a-ae94-17287abd6840,CIS,Security Engineer Intern,...,2026-05-19T19:19:23Z,unreviewed,...
```

Format: ISO 8601 timestamp (UTC)

## Workflow

### Manual Update Process

1. **Initial backfill** (already done):
   ```bash
   python3 backfill_closes_from_simplify.py
   ```

2. **Regular monitoring** (run daily or on schedule):
   ```bash
   python3 simplify_close_tracker.py
   ```

3. **Review transitions**:
   ```bash
   cat simplify_transitions.csv
   ```

### Automated Setup (GitHub Actions)

Add `.github/workflows/track-simplify-closes.yml` to this repo to automate daily tracking.

## Historical Data Status

As of 2026-05-19:
- **Total historical jobs**: 4,234
- **Jobs in Simplify inactive list**: 8,587
- **High-confidence matches (>=70%)**: 3,300
- **Medium-confidence matches (50-70%)**: 176
- **Jobs with `application_closes` timestamps**: 83+

## Limitations & Notes

1. **No precise close dates**: Simplify doesn't provide exact application close times. We use:
   - The date when a job first appears in `README-Inactive.md`
   - OR the current date for recently closed jobs

2. **Matching accuracy**: Fuzzy matching can miss jobs or match incorrectly:
   - Company name variations (e.g., "JPMorgan" vs "JP Morgan")
   - Role name variations (e.g., "Software Engineer" vs "SWE Intern")
   - Company renames/acquisitions

3. **Coverage**: Only Simplify jobs are tracked:
   - Jobs from other sources won't have close times unless manually added
   - Future jobs only if they're also posted to Simplify

## Next Steps

1. **Validate matches**: Manually verify sample matches to tune fuzzy matching thresholds
2. **Historical years**: If you have archived Simplify repos (Summer2025, 2024, 2023), similar scripts can backfill those
3. **Alerts**: Add notifications when a tracked job closes
4. **Integration**: Connect to job review app for display

## Troubleshooting

**No matches found:**
- Check if company/role names are too different from Simplify's
- Lower confidence threshold in `backfill_closes_from_simplify.py`
- Manually verify a few matches in Simplify inactive list

**Job closed but not detected:**
- Might not be in Simplify's tracking
- Job might have been removed/archived differently
- Run manual backfill: `python3 backfill_closes_from_simplify.py`

**Transition detection not working:**
- Ensure `simplify_job_state.json` exists from first run
- Delete it to reset state: `rm simplify_job_state.json`
