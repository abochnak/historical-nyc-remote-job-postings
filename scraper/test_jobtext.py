#!/usr/bin/env python3
"""
Tests for the shared extractor.

    python scraper/test_jobtext.py

No network: every case is a fixture. The interesting ones are regressions --
each `test_rejects_*` is something that was actually stored as a job
description before the quality gate existed.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import jobtext


REAL_POSTING = """
Software Engineer Intern
About the role
We are looking for a software engineer intern to join our team in New York.
Responsibilities
- Build and ship features across the stack
- Work with experienced engineers on production systems
Qualifications
- Currently enrolled in a Bachelor's or Master's degree program
- 1+ years of experience with Python or a similar language
- Familiarity with web frameworks and relational databases
- Familiarity with version control, testing, and code review
What you'll do
You will work alongside senior engineers on production systems throughout the
internship, shipping code that reaches real users. You will own a project end
to end, present your work to the team, and take part in design discussions and
code review. Past interns have worked on the payments pipeline, the internal
scheduling tools, and the public API.
Benefits
The hourly rate for this internship is $45-$55 depending on experience and
year of study. Interns receive housing assistance, a commuter benefit, and
access to the same health coverage as full-time staff during the program.
We are an equal opportunity employer and welcome applicants of every
background. All employment decisions are made without regard to race, color,
religion, national origin, sex, sexual orientation, gender identity, age,
disability, or veteran status.
"""


class TestQualityGate(unittest.TestCase):

    def test_accepts_a_real_posting(self):
        self.assertTrue(jobtext.looks_like_job_text(REAL_POSTING))

    def test_rejects_repeated_footer_nav(self):
        # Regression: a site's footer repeated three times cleared the old
        # length-only check and was stored as a description.
        nav = "\n".join([
            "Core", "CRM", "IMS", "AI Agent", "Add-Ons", "Communications",
            "Scoring", "Payment Portal", "Workflow Automation", "Company",
            "About Us", "Careers", "Terms of Service", "Privacy Policy",
        ] * 6)
        self.assertGreater(len(nav), jobtext.MIN_CHARS)   # long enough to fool a length check
        self.assertFalse(jobtext.looks_like_job_text(nav))

    def test_rejects_company_boilerplate_without_a_role(self):
        boilerplate = (
            "Founded in 1999, Audax Group is a leading alternative investment "
            "manager with offices in Boston, New York, San Francisco, London and "
            "Hong Kong. With approximately $42 billion of assets under management "
            "and more than 475 employees, Audax is a leading capital partner for "
            "middle market companies. For more information, visit our website or "
            "follow us on LinkedIn. " * 3
        )
        self.assertFalse(jobtext.looks_like_job_text(boilerplate))

    def test_rejects_mojibake(self):
        # Regression: a gzipped Wayback capture decoded as text produced 1,654
        # characters of binary garbage that was stored as a description.
        garbage = "�" * 400 + "x" * 400
        self.assertFalse(jobtext.looks_like_job_text(garbage))

    def test_rejects_text_below_min_chars(self):
        self.assertFalse(jobtext.looks_like_job_text("Responsibilities. Qualifications."))

    def test_detects_taken_down_postings(self):
        self.assertTrue(jobtext.looks_gone("This job is no longer accepting applications."))
        self.assertTrue(jobtext.looks_gone("The position has been filled."))
        self.assertFalse(jobtext.looks_gone(REAL_POSTING))


class TestExtraction(unittest.TestCase):

    def test_prefers_jsonld_over_page_body(self):
        # The body here is nav noise; the JSON-LD block is the actual posting.
        html = """
        <html><body>
          <div>Home About Careers Contact Sign in Sign up Help Terms Privacy</div>
          <script type="application/ld+json">
          {"@type": "JobPosting",
           "title": "Software Engineer Intern",
           "hiringOrganization": {"name": "Acme"},
           "employmentType": "INTERN",
           "datePosted": "2026-01-15",
           "description": "<p>Responsibilities: build and ship features across the stack. Qualifications: currently enrolled in a Bachelor's or Master's degree program, with 1+ years of experience with Python or a similar language, and familiarity with web frameworks and relational databases. You will work alongside senior engineers on production systems throughout the internship, own a project end to end, and present your work to the team. Compensation is $45-$55 per hour and we are an equal opportunity employer.</p>"}
          </script>
        </body></html>
        """
        text = jobtext.html_to_text(html)
        self.assertIn("Software Engineer Intern", text)
        self.assertIn("Acme", text)
        self.assertIn("Responsibilities", text)
        self.assertNotIn("Sign up", text)      # nav did not win

    def test_falls_through_when_container_holds_no_posting(self):
        # Regression: <main> matched a company blurb while the real description
        # sat in the body. Taking the first long-enough candidate stored 488
        # characters of boilerplate; the gate makes it fall through instead.
        html = f"""
        <html><body>
          <main>Founded in 1999, Acme is a leading manager of things.</main>
          <div>{REAL_POSTING}</div>
        </body></html>
        """
        text = jobtext.html_to_text(html)
        self.assertIn("Qualifications", text)

    def test_strips_script_and_style(self):
        html = "<html><body><script>var x=1;</script><style>p{color:red}</style>" \
               f"<div>{REAL_POSTING}</div></body></html>"
        text = jobtext.html_to_text(html)
        self.assertNotIn("var x", text)
        self.assertNotIn("color:red", text)

    def test_js_shell_yields_nothing_usable(self):
        # Workday/Ashby/Oracle return a shell like this. It must not pass.
        shell = '<html><body><div id="root"></div><script src="/app.js"></script></body></html>'
        self.assertFalse(jobtext.looks_like_job_text(jobtext.html_to_text(shell)))

    def test_normalize_collapses_whitespace_and_caps_length(self):
        self.assertEqual(jobtext.normalize("a   b\n\n\n\nc"), "a b\n\nc")
        self.assertLessEqual(len(jobtext.normalize("x" * (jobtext.MAX_CHARS + 5000))),
                             jobtext.MAX_CHARS)

    def test_unescapes_entities(self):
        self.assertIn("R&D", jobtext.normalize("R&amp;D"))


class TestThrottle(unittest.TestCase):

    def test_archive_ph_is_throttled_harder_than_default(self):
        t = jobtext.HostThrottle(2.0)
        self.assertGreater(t.SLOW_HOSTS["archive.ph"], 2.0)

    def test_no_retry_on_archive_ph_rate_limit(self):
        # Its 429 is a policy, not a traffic spike -- backing off changes nothing.
        self.assertIn("archive.ph", jobtext.NO_RETRY_429)
        self.assertNotIn("archive.org", jobtext.NO_RETRY_429)


if __name__ == "__main__":
    unittest.main(verbosity=2)
