#!/usr/bin/env python3
"""
historical-nyc-remote-job-postings -- Discord notifications
===========================================================
Posts pipeline results to a Discord channel through a webhook.

The webhook URL is read from the DISCORD_WEBHOOK_URL environment variable and
never appears in code, output, or error messages -- the URL is itself a
credential: anyone holding it can post to the channel. In CI it comes from a
repository secret:

    env:
      DISCORD_WEBHOOK_URL: ${{ secrets.DISCORD_WEBHOOK_URL }}

Everything degrades quietly. With no webhook set this module is a no-op, so
update.py and simplify_closes.py run identically on a laptop with nothing
configured, and a Discord outage can never fail a scrape.

Standard library only, like the rest of the scrapers.

Check the wiring
----------------
    export DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/...'
    python scraper/notify.py --test
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

ENV_VAR = "DISCORD_WEBHOOK_URL"

USERNAME = "job archive"

# Discord's documented limits. Exceeding any of them is a 400, so the payload
# is trimmed to fit rather than risking a rejected post.
MAX_EMBED_DESC   = 4096
MAX_EMBED_TITLE  = 256
MAX_FIELD_VALUE  = 1024
MAX_EMBEDS       = 10

COLOR_NEW     = 0x3BA55D   # green -- new postings found
COLOR_CLOSED   = 0xED4245  # red -- postings closed
COLOR_INFO    = 0x5865F2   # blurple -- run summaries
COLOR_WARN    = 0xFAA81A   # amber -- something needs attention

MAX_RETRIES = 3


def webhook_url():
    return (os.environ.get(ENV_VAR) or "").strip()


def enabled():
    return bool(webhook_url())


def _truncate(s, limit):
    s = s or ""
    return s if len(s) <= limit else s[: limit - 1] + "…"


def post_embed(title, lines, color=COLOR_INFO, footer=None):
    """
    Send one embed built from a list of text lines.

    Returns True if Discord accepted it, False otherwise. Never raises: a
    notification failing must not take a scrape down with it.
    """
    url = webhook_url()
    if not url:
        return False

    description = "\n".join(str(l) for l in lines if str(l).strip())
    payload = {
        "username": USERNAME,
        "embeds": [{
            "title": _truncate(title, MAX_EMBED_TITLE),
            "description": _truncate(description, MAX_EMBED_DESC),
            "color": color,
        }],
    }
    if footer:
        payload["embeds"][0]["footer"] = {"text": _truncate(footer, 2048)}

    return _send(url, payload)


def _send(url, payload):
    body = json.dumps(payload).encode("utf-8")
    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json",
                     "User-Agent": "historical-job-scraper/2.0"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return 200 <= r.status < 300
        except urllib.error.HTTPError as e:
            # 429 carries a retry_after; anything else is not worth retrying.
            if e.code == 429 and attempt < MAX_RETRIES - 1:
                wait = 2.0 * (attempt + 1)
                try:
                    wait = float(json.loads(e.read().decode()).get("retry_after", wait))
                except Exception:
                    pass
                time.sleep(min(wait, 30.0))
                continue
            # Deliberately not printing the response body or URL -- a webhook
            # error can echo the URL back, and CI logs are not private.
            print(f"  [discord] not sent (HTTP {e.code})")
            return False
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2.0 * (attempt + 1))
                continue
            print(f"  [discord] not sent ({type(e).__name__})")
            return False
    return False


# -- Message builders ----------------------------------------------------------
def notify_new_postings(jobs, captured=0, attempted=0):
    """
    jobs: list of dicts with company_name, title, and optionally degree_level.

    Degree level is labelled as approximate on purpose. The classifier runs at
    70% precision on the grad call, and a line in a Discord message reads as
    fact unless it says otherwise.
    """
    if not enabled() or not jobs:
        return False

    lines = []
    for j in jobs[:20]:
        company = _truncate(j.get("company_name", "?"), 40)
        title = _truncate(j.get("title", "?"), 70)
        level = j.get("degree_level")
        suffix = f"  · {level}" if level else ""
        lines.append(f"**{company}** — {title}{suffix}")
    if len(jobs) > 20:
        lines.append(f"_…and {len(jobs) - 20} more_")

    # `captured` is how many of *these* postings we could read a description
    # for. It is deliberately not a capture rate: most new postings have their
    # text fetched later by the nightly backfill, so dividing by everything
    # announced would report a failure that never happened.
    footer = None
    if attempted and captured < attempted:
        footer = f"{captured} of {len(jobs)} readable so far; the rest are queued"
    elif attempted:
        footer = "description text captured for all of them"
    if any(j.get("degree_level") for j in jobs):
        footer = ((footer + " · ") if footer else "") + "degree level is an estimate (~70% precision)"

    plural = "posting" if len(jobs) == 1 else "postings"
    return post_embed(f"{len(jobs)} new {plural}", lines, COLOR_NEW, footer)


def notify_closes(transitions):
    """transitions: dicts with company, role, status_change (from simplify_closes)."""
    if not enabled() or not transitions:
        return False

    closed = [t for t in transitions if t.get("status_change") == "active -> inactive"]
    reopened = [t for t in transitions if t.get("status_change") == "inactive -> active"]

    lines = []
    for t in closed[:15]:
        lines.append(f"🔒 **{_truncate(t['company'], 40)}** — {_truncate(t['role'], 70)}")
    if len(closed) > 15:
        lines.append(f"_…and {len(closed) - 15} more closed_")
    if reopened:
        lines.append("")
        for t in reopened[:5]:
            lines.append(f"🔓 reopened: **{_truncate(t['company'], 40)}** — {_truncate(t['role'], 70)}")
        if len(reopened) > 5:
            lines.append(f"_…and {len(reopened) - 5} more reopened_")

    title = f"{len(closed)} posting{'s' if len(closed) != 1 else ''} closed"
    if not closed and reopened:
        title = f"{len(reopened)} posting{'s' if len(reopened) != 1 else ''} reopened"
    return post_embed(title, lines, COLOR_CLOSED)


def notify_backfill(ok, gone, thin, error, with_text, missing):
    """Summary of a backfill run. Only worth sending when something changed."""
    if not enabled() or not ok:
        return False
    lines = [
        f"recovered **{ok}** description{'s' if ok != 1 else ''}",
        f"{with_text:,} postings now have text · {missing:,} still missing",
    ]
    unrecoverable = gone + thin
    if unrecoverable:
        lines.append(f"{unrecoverable} unrecoverable this pass (taken down or JS-rendered)")
    if error:
        lines.append(f"{error} failed and will be retried")
    return post_embed("Backfill run", lines, COLOR_INFO)


def notify_problem(title, lines):
    """Something a human should look at."""
    return post_embed(title, lines, COLOR_WARN)


# -- CLI -----------------------------------------------------------------------
def main():
    if "--test" not in sys.argv:
        print(__doc__)
        return
    if not enabled():
        sys.exit(f"{ENV_VAR} is not set — nothing to test.")
    ok = post_embed(
        "Webhook connected",
        ["This channel will receive new postings, close times, and backfill summaries.",
         "Sent by `scraper/notify.py --test`."],
        COLOR_INFO,
    )
    print("  Sent." if ok else "  Failed — see the message above.")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
