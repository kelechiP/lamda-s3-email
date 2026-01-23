Start Date Logic
Effective start_date resolution

If TEST_MODE=true and TEST_START_DATE is set → use TEST_START_DATE

Else, if event includes start_date → use that date

Else:

If run on Monday (UTC) → use “Monday two weeks ago”

If run any other day → use “exactly 14 days ago”

This ensures:

Scheduled Monday runs always align to cadence weeks

Manual runs can validate/backfill based on “14 days ago” unless overridden

Processing Logic Overview
Step 1 — Discover agencies

Agencies are discovered by listing prefixes under:

BASE_PREFIX_RANKED

This ranked tree is the source of truth for agency enumeration.

Step 2 — For each agency, scan both trees for start_date

Ranked (CSV attachments)

Find files ending in .csv under:
.../cadence=week/start_date=<start_date>/

Each agency’s found CSVs are stored as email attachments

Summary (TXT body content)

Find files ending in .txt under:
.../cadence=week/start_date=<start_date>/

TXT contents are read and concatenated into a per-agency “SUMMARY REPORTS” body section

Step 3 — Track missing data

Two independent missing lists are computed:

agencies_no_ranked: agencies with no ranked CSV attachments

agencies_no_summary: agencies with no summary TXT content

Email Routing Rules
TO / BCC Rules

TO: always MAIL_FROM

BCC: depends on mode and mapping

In TEST_MODE

agency recipients come from TEST_EMAIL_MAP (env JSON)

In PROD

agency recipients come from AGENCY_EMAIL_MAP loaded from S3 (AGENCY_EMAIL_LIST_BUCKET/KEY)

Fallback

For report emails, if an agency has no mapped DL, the code can fall back to DEFAULT_EMAIL_TO list (if configured)

Explicit DL detection

For agency-specific NO DATA emails, the code only sends if the agency has an explicitly mapped DL (no fallback), to avoid spamming defaults.

Email Types & Sending Conditions
A) NO DATA (ALL AGENCIES) — System Outage Mode

Condition:

No ranked CSVs exist for any agency AND

No summary TXT exists for any agency

Action:

Send a single “NO DATA (ALL AGENCIES)” email to DEFAULT_EMAIL_TO list

Include both ranked and summary S3 paths checked

Suppress all other emails (no agency “NO DATA”, no report emails)

Treated as upstream pipeline outage

B) REPORT Email (Per Agency)

Condition:

Agency has at least one ranked CSV attachment

Action:

Subject: DNS Service Bypass Weekly Report <agency>

Attachments: ranked-traffic CSV files for that agency

Body:

SUMMARY REPORTS section (summary TXT content if present)

Footer (questions + disclaimer)

C) AGENCY “NO DATA” Email (Per Agency)

Condition:

Agency has an explicit mapped DL AND

Agency has no ranked CSV attachments for the week

AND system outage mode is NOT active

Body behavior:

No ranked CSV + no summary TXT:
“There is no DNS bypass traffic report data for this week and no summary report.”

No ranked CSV + summary TXT exists:
Same message, and include SUMMARY REPORTS section content.

Action:

Subject: DNS Service Bypass Weekly Report - NO DATA <agency>

No attachments

Sent only to agency mapped DL (no default fallback)

D) Default “NO DATA LIST” Email (Optional Ops Visibility)

Condition:

At least one agency missing ranked CSVs AND not in system outage

Action:

Send to DEFAULT_EMAIL_TO list

Includes:

agencies missing ranked CSV attachments

agencies missing summary TXT

This is intended for ops/monitoring visibility.

S3 Management Requirements (Critical)

To keep reporting reliable, the S3 bucket and folder structure must be managed carefully:

1) Stable Naming Conventions

Agency folder naming must remain consistent (agency=<name>/)

cadence=week/ and start_date=YYYY-MM-DD/ must exist exactly with the expected spelling/casing

2) Correct Weekly Folder Creation

The start_date= folders must correspond to the Lambda’s date logic:

If using Monday cadence, folders should be Monday dates, or the override must be used

If upstream creates folders on different days (Thu vs Mon), use start_date override when needed or align upstream cadence.

3) File Type Correctness

Ranked tree must contain .csv for attachment logic to work

Summary tree must contain .txt for body rendering to work

Misplaced file types will cause false “NO DATA” classification

4) Consistent Tree Parity

Summary and ranked trees should contain matching agency subtrees.

If summary subtree is missing entirely for an agency, the agency will appear in agencies_no_summary even if ranked exists.

5) Permissions & Access

Lambda IAM role must allow:

s3:ListBucket on BUCKET

s3:GetObject for both base prefixes

s3:GetObject for the email map JSON (prod) if used

Observability & Reporting

The Lambda logs:

Data source paths (bucket + prefixes)

Email list source (TEST env vs PROD S3)

Per email: agency name, BCC recipients, attachment count, start_date

No-data modes and suppression behavior

These logs should be monitored in CloudWatch to identify:

late/missing start_date folders

upstream pipeline outages

recipient mapping gaps

SMTP failover events (host1 → host2)
