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
    # test channel (the default for --test)
    export DISCORD_WEBHOOK_URL_TEST='https://discord.com/api/webhooks/...'
    python scraper/notify.py --test

    # the real channel, deliberately
    export DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/...'
    python scraper/notify.py --test --prod
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

# NOTIFY_TARGET picks between production and test; anything other than "test"
# means production, so the default with no configuration at all is production.
#
# Production can fan out to several channels: DISCORD_WEBHOOK_URL plus
# DISCORD_WEBHOOK_URL_2 and _3. Only the ones that are set are used, and the
# numbering may be sparse -- setting _3 without _2 works.
ENV_VAR       = "DISCORD_WEBHOOK_URL"
ENV_VAR_EXTRA = ("DISCORD_WEBHOOK_URL_2", "DISCORD_WEBHOOK_URL_3")
ENV_VAR_TEST  = "DISCORD_WEBHOOK_URL_TEST"
TARGET_VAR    = "NOTIFY_TARGET"

USERNAME = "TTP Job Bot"

# Discord's documented limits. Exceeding any of them is a 400, so the payload
# is trimmed to fit rather than risking a rejected post.
MAX_EMBED_DESC   = 4096
MAX_EMBED_TITLE  = 256
MAX_FIELD_VALUE  = 1024
MAX_EMBEDS       = 10
MAX_CONTENT      = 2000   # plain-message limit

COLOR_NEW     = 0x3BA55D   # green -- new postings found
COLOR_CLOSED   = 0xED4245  # red -- postings closed
COLOR_INFO    = 0x5865F2   # blurple -- run summaries
COLOR_WARN    = 0xFAA81A   # amber -- something needs attention

MAX_RETRIES = 3


def target():
    """
    Which channel to post to: "test" or "prod".

    Anything unrecognised is treated as production.
    """
    return "test" if (os.environ.get(TARGET_VAR) or "").strip().lower() == "test" else "prod"


def webhook_urls():
    """
    Every webhook for the selected target, in order, skipping unset ones.

    A test target NEVER falls back to the production webhooks. Falling back
    would mean the one command you ran to avoid touching the real channels is
    the command that posts to them -- so an unset test webhook disables
    notifications instead.
    """
    if target() == "test":
        url = (os.environ.get(ENV_VAR_TEST) or "").strip()
        return [url] if url else []

    urls = []
    for var in (ENV_VAR,) + ENV_VAR_EXTRA:
        url = (os.environ.get(var) or "").strip()
        if url and url not in urls:      # a duplicated secret shouldn't double-post
            urls.append(url)
    return urls


def webhook_url():
    """First configured webhook for the target, or "". Kept for callers that
    only need to know whether anything is configured."""
    urls = webhook_urls()
    return urls[0] if urls else ""


def enabled():
    return bool(webhook_urls())


def describe_target():
    """One line for run output, naming the channels by count but never the URL."""
    t = target()
    n = len(webhook_urls())
    if not n:
        var = ENV_VAR_TEST if t == "test" else ENV_VAR
        return f"notifications off ({var} not set)"
    if n == 1:
        return f"notifying the {t} channel"
    return f"notifying {n} {t} channels"


def _truncate(s, limit):
    s = s or ""
    return s if len(s) <= limit else s[: limit - 1] + "…"


def post_embed(title, lines, color=COLOR_INFO, footer=None):
    """
    Send one embed built from a list of text lines.

    Returns True if Discord accepted it, False otherwise. Never raises: a
    notification failing must not take a scrape down with it.
    """
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

    return _broadcast(payload)


def post_message(content):
    """
    Send a plain-text message. Returns True if Discord accepted it.

    Used for job postings, whose format is deliberately plain: Discord renders
    a preview card for the link, which an embed would suppress.
    """
    return _broadcast({"username": USERNAME,
                       "content": _truncate(content, MAX_CONTENT)})


def _broadcast(payload):
    """
    Deliver one payload to every configured webhook for the current target.

    Returns True if **at least one** channel accepted it, which is what the
    caller records as "announced".

    That choice is deliberate. Requiring every channel to succeed sounds safer
    but behaves worse: a webhook that has been deleted returns 404 forever, the
    posting is never recorded as announced, and every healthy channel receives
    it again on the next run -- forty-eight duplicate posts a day rather than
    one missed one. Accepting a partial delivery bounds the damage to the dead
    channel, and the failure is printed with its position so it can be found and
    fixed.

    Channels are identified by position, never by URL: the URL is a credential
    and CI logs are not private.
    """
    urls = webhook_urls()
    if not urls:
        return False

    delivered = 0
    for i, url in enumerate(urls, 1):
        if _send(url, payload):
            delivered += 1
        else:
            label = f"{target()} webhook {i}" if len(urls) > 1 else f"{target()} webhook"
            print(f"  [discord] {label} did not accept the message")

    if delivered and delivered < len(urls):
        print(f"  [discord] delivered to {delivered} of {len(urls)} channels")
    return delivered > 0


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
def format_posting(job):
    """
    One posting, as two lines:

        💼   American Express - 2027 Software Engineer, Technology (Summer 2027)
        🔗 https://careers.americanexpress.com/en/sites/CX_1/job/26010970

    Company and role joined with " - ", the recruiting term in parentheses.
    Either of the first two dropping out takes its separator with it rather than
    leaving a dangling dash, and a posting with no usable term simply has no
    parenthetical -- 61 of the archived postings have none, and "(N/A)" would be
    worse than nothing.
    """
    company = (job.get("company_name") or "").strip()
    title   = (job.get("title") or "").strip() or "(untitled posting)"

    head = " - ".join(p for p in (company, title) if p)

    season = (job.get("recruiting_season") or "").strip()
    if season and season.upper() not in ("N/A", "NA", "NONE"):
        # Multi-term postings ("Summer 2026 | Fall 2026") show the first.
        head += f" ({season.split('|')[0].strip()})"

    lines = [f"💼   {head}"]
    url = (job.get("url") or "").strip()
    if url:
        lines.append(f"🔗 {url}")
    return "\n".join(lines)


# One message per posting up to this many; beyond it, postings are grouped so a
# large catch-up run can't fire dozens of requests at a webhook that allows
# about 30 a minute.
PER_MESSAGE_LIMIT = 10
GROUP_SIZE        = 8
SEND_SPACING      = 1.0   # seconds between messages


def notify_new_postings(jobs, captured=0, attempted=0):
    """
    Post each new posting. Returns True if every message was accepted.

    jobs: dicts with title, url, and recruiting_season.
    """
    if not enabled() or not jobs:
        return False

    if len(jobs) <= PER_MESSAGE_LIMIT:
        groups = [[j] for j in jobs]
    else:
        groups = [jobs[i:i + GROUP_SIZE] for i in range(0, len(jobs), GROUP_SIZE)]

    all_ok = True
    for i, group in enumerate(groups):
        content = "\n\n".join(format_posting(j) for j in group)
        if not post_message(content):
            all_ok = False
        if i < len(groups) - 1:
            time.sleep(SEND_SPACING)
    return all_ok


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
    # `notify.py --test --prod` is the deliberate way to prove the real channel
    # works; plain `--test` goes to the test channel.
    if "--prod" in sys.argv:
        os.environ[TARGET_VAR] = "prod"
    else:
        os.environ.setdefault(TARGET_VAR, "test")
    print(f"  {describe_target()}")
    if not enabled():
        var = ENV_VAR_TEST if target() == "test" else ENV_VAR
        sys.exit(f"{var} is not set — nothing to send.")
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
