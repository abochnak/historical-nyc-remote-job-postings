import streamlit as st
import pandas as pd
import requests
import io
import csv
from datetime import datetime, timezone

st.set_page_config(
    page_title="Classify Jobs",
    page_icon="📋",
    layout="wide",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Mono:wght@400;500&display=swap');
    html, body, [class*="css"] { font-family: 'DM Mono', monospace; font-size: 13px; }
    h1, h2, h3 { font-family: 'Syne', sans-serif !important; }
    .block-container { padding-top: 2rem; max-width: 1400px; }
    .flag-box {
        background: #1a1a2e;
        border: 1px solid #f97316;
        border-radius: 4px;
        padding: 12px 16px;
        margin-bottom: 8px;
        font-size: 12px;
        color: #f97316;
    }
    .archive-url {
        background: #111118;
        border: 1px solid #1e1e2e;
        border-radius: 4px;
        padding: 10px 14px;
        font-size: 11px;
        color: #38bdf8;
        word-break: break-all;
        margin-bottom: 12px;
    }

</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
GITHUB_USER  = "YOUR_GITHUB_USERNAME"
GITHUB_REPO  = "historical-nyc-remote-job-postings"
BRANCH       = "main"

BASE_URL     = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{BRANCH}/data"
DETAILS_URL  = f"{BASE_URL}/job_details.csv"
EXCLUDE_URL  = f"{BASE_URL}/excluded_jobs.csv"
NYC_URL      = f"{BASE_URL}/nyc_jobs.csv"
REM_URL      = f"{BASE_URL}/remote_jobs.csv"

CATEGORIES = [
    "",
    "Software Engineering",
    "Data Analysis",
    "Machine Learning / AI",
    "Cybersecurity",
    "Product Management",
    "Quant / Finance",
    "IT Support",
    "Other",
]

EXCL_HEADERS = ["id", "company_name", "reason", "blocked_title", "blocked_company", "blocked_date"]

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_csv_url(url):
    try:
        r = requests.get(url, timeout=15, headers={"Cache-Control": "no-cache"})
        if r.status_code == 200 and r.text.strip():
            return pd.read_csv(io.StringIO(r.text))
    except Exception:
        pass
    return pd.DataFrame()

def reload():
    st.cache_data.clear()
    st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────
st.markdown("# Job Classification")
st.caption("Review archived job postings, assign categories, and mark exclusions")

col_r, col_f = st.columns([1, 1])
with col_r:
    if st.button("🔄 Refresh data", use_container_width=True):
        reload()

details_df = load_csv_url(DETAILS_URL)
excl_df    = load_csv_url(EXCLUDE_URL)
nyc_df     = load_csv_url(NYC_URL)
rem_df     = load_csv_url(REM_URL)

if details_df.empty:
    st.info("No job_details.csv found yet — it will be populated on the next GitHub Actions run.")
    st.stop()

# Merge category/exclusion state into details
excl_ids = set(excl_df["id"].dropna().astype(str)) if not excl_df.empty and "id" in excl_df.columns else set()
details_df["excluded"] = details_df["id"].astype(str).isin(excl_ids)

# ── Flags summary ─────────────────────────────────────────────────────────────
failed_arc   = details_df[details_df["archive_status"] == "failed"]
failed_scrape = details_df[details_df["scrape_status"].isin(["failed", "blocked"])]
unclassified = details_df[(details_df["category"].isna() | (details_df["category"] == "")) & ~details_df["excluded"]]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total archived",    len(details_df))
c2.metric("Unclassified",      len(unclassified), delta=f"-{len(unclassified)}" if len(unclassified) else None, delta_color="inverse")
c3.metric("Archive failures",  len(failed_arc),   delta=str(len(failed_arc))   if len(failed_arc)   else None, delta_color="inverse")
c4.metric("Scrape issues",     len(failed_scrape), delta=str(len(failed_scrape)) if len(failed_scrape) else None, delta_color="inverse")

if not failed_arc.empty:
    with st.expander(f"⚠️ {len(failed_arc)} jobs failed to archive"):
        st.dataframe(
            failed_arc[["company_name", "title", "job_url", "archive_source"]],
            width="stretch", hide_index=True,
        )

if not failed_scrape.empty:
    with st.expander(f"⚠️ {len(failed_scrape)} jobs have scrape issues"):
        st.dataframe(
            failed_scrape[["company_name", "title", "scrape_status", "archive_url"]],
            width="stretch", hide_index=True,
        )

st.divider()

# ── Filters ───────────────────────────────────────────────────────────────────
st.markdown("### Review jobs")
fc1, fc2, fc3 = st.columns([2, 2, 2])
with fc1:
    filter_cat = st.selectbox(
        "Filter by category",
        ["All", "Unclassified only"] + CATEGORIES[1:],
        key="filter_cat",
    )
with fc2:
    filter_status = st.selectbox(
        "Filter by archive status",
        ["All", "success", "failed", "excluded"],
        key="filter_status",
    )
with fc3:
    filter_excl = st.radio(
        "Show excluded",
        ["Hide excluded", "Show all", "Excluded only"],
        horizontal=True,
        key="filter_excl",
    )

search = st.text_input("Search company or title", placeholder="e.g. Goldman, Software", key="cls_search")

# Apply filters
df = details_df.copy()
if filter_excl == "Hide excluded":
    df = df[~df["excluded"]]
elif filter_excl == "Excluded only":
    df = df[df["excluded"]]

if filter_cat == "Unclassified only":
    df = df[df["category"].isna() | (df["category"] == "")]
elif filter_cat != "All":
    df = df[df["category"] == filter_cat]

if filter_status != "All":
    df = df[df["archive_status"] == filter_status]

if search.strip():
    mask = (
        df["company_name"].str.contains(search.strip(), case=False, na=False) |
        df["title"].str.contains(search.strip(), case=False, na=False)
    )
    df = df[mask]

st.caption(f"{len(df):,} jobs shown")

if df.empty:
    st.info("No jobs match the current filters.")
    st.stop()

# ── Session state for pending changes ─────────────────────────────────────────
if "pending_categories" not in st.session_state:
    st.session_state["pending_categories"] = {}
if "pending_exclusions" not in st.session_state:
    st.session_state["pending_exclusions"] = {}

# ── Job cards ─────────────────────────────────────────────────────────────────
for _, job in df.iterrows():
    jid          = str(job.get("id", ""))
    company      = str(job.get("company_name", ""))
    title        = str(job.get("title", ""))
    job_url      = str(job.get("job_url", ""))
    archive_url  = str(job.get("archive_url", ""))
    archive_src  = str(job.get("archive_source", ""))
    archive_stat = str(job.get("archive_status", ""))
    scrape_stat  = str(job.get("scrape_status", ""))
    current_cat  = str(job.get("category", "") or "")
    is_excl      = bool(job.get("excluded", False))

    with st.expander(
        f"{'🚫 ' if is_excl else ''}{company} — {title}  "
        f"[{archive_src or 'no archive'}]"
        f"{'  ⚠️' if archive_stat == 'failed' or scrape_stat in ('failed','blocked') else ''}",
        expanded=False,
    ):
        left, right = st.columns([3, 2])

        with left:
            # Archive URL
            if archive_url and archive_url != "nan":
                st.markdown(
                    f'<div class="archive-url">📦 Archive ({archive_src}): '
                    f'<a href="{archive_url}" target="_blank">{archive_url}</a></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="flag-box">⚠️ No archive URL — status: {archive_stat}</div>',
                    unsafe_allow_html=True,
                )

            st.caption(f"Scrape status: {scrape_stat}")

            # Original link
            if job_url and job_url != "nan":
                st.markdown(f"[🔗 Original posting]({job_url})", unsafe_allow_html=True)

        with right:
            st.markdown("**Classify this job**")

            # Category selector
            cat_idx = CATEGORIES.index(current_cat) if current_cat in CATEGORIES else 0
            # Check pending state
            if jid in st.session_state["pending_categories"]:
                cat_idx = CATEGORIES.index(st.session_state["pending_categories"][jid])

            new_cat = st.selectbox(
                "Category",
                options=CATEGORIES,
                index=cat_idx,
                key=f"cat_{jid}",
                label_visibility="collapsed",
            )
            if new_cat != current_cat:
                st.session_state["pending_categories"][jid] = new_cat

            st.markdown("**Exclude this job?**")
            excl_reason = st.text_input(
                "Reason for exclusion",
                placeholder="e.g. Not a tech role",
                key=f"excl_reason_{jid}",
                label_visibility="collapsed",
            )
            if st.button("🚫 Mark as excluded", key=f"excl_btn_{jid}", use_container_width=True):
                if not excl_reason.strip():
                    st.warning("Please enter a reason before excluding.")
                else:
                    st.session_state["pending_exclusions"][jid] = {
                        "id":              jid,
                        "company_name":    company,
                        "reason":          excl_reason.strip(),
                        "blocked_title":   title,
                        "blocked_company": company,
                        "blocked_date":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                    st.success(f"Queued for exclusion — save changes below.")

        st.divider()

# ── Save pending changes ───────────────────────────────────────────────────────
n_cat  = len(st.session_state["pending_categories"])
n_excl = len(st.session_state["pending_exclusions"])

if n_cat > 0 or n_excl > 0:
    st.markdown("---")
    st.markdown(f"### 💾 Pending changes: {n_cat} category updates · {n_excl} exclusions")
    st.caption(
        "Download the updated CSVs below and commit them to your repo. "
        "The dashboard will reflect changes on the next data refresh."
    )

    # Build updated job_details CSV
    updated_details = details_df.copy()
    for jid, cat in st.session_state["pending_categories"].items():
        updated_details.loc[updated_details["id"].astype(str) == jid, "category"] = cat

    # Build updated excluded_jobs CSV
    existing_excl = excl_df.to_dict("records") if not excl_df.empty else []
    existing_excl_ids = {r.get("id","") for r in existing_excl}
    new_excl = [
        v for jid, v in st.session_state["pending_exclusions"].items()
        if jid not in existing_excl_ids
    ]
    updated_excl = existing_excl + new_excl

    dl1, dl2, dl3 = st.columns(3)
    with dl1:
        details_out = updated_details.drop(columns=["excluded"], errors="ignore")
        st.download_button(
            "⬇ Download updated job_details.csv",
            data=details_out.to_csv(index=False),
            file_name="job_details.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with dl2:
        excl_buf = io.StringIO()
        w = csv.DictWriter(excl_buf, fieldnames=EXCL_HEADERS, extrasaction="ignore")
        w.writeheader()
        w.writerows(updated_excl)
        st.download_button(
            "⬇ Download updated excluded_jobs.csv",
            data=excl_buf.getvalue(),
            file_name="excluded_jobs.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with dl3:
        if st.button("🗑️ Clear pending changes", use_container_width=True):
            st.session_state["pending_categories"] = {}
            st.session_state["pending_exclusions"] = {}
            st.rerun()

    st.info(
        "**Commit instructions:** replace `data/job_details.csv` and `data/excluded_jobs.csv` "
        "in your repo with the downloaded files, then push. The next GitHub Actions run will "
        "apply the exclusions to the job CSVs automatically."
    )