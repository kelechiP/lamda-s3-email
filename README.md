Weekly Report – Lambda

Overview

This AWS Lambda function generates weekly DNS/DoH/DoT bypass reports by:

Iterating through a deeply nested S3 folder structure

Collecting CSV files for a calculated start_date

Grouping CSV files per agency

Sending one email per agency via SMTP

Attaching all CSV files for that agency

Sending emails BCC to agency distribution lists

Supporting TEST mode vs PROD mode

Running automatically every Monday at 7:00am Eastern Time (via EventBridge)

What the Lambda Does (High Level)

Determines the start_date

Scheduled Monday run → Monday two weeks ago

Manual run → Exactly 14 days ago

Optional overrides via event input

Walks this S3 structure:

s3://<bucket>/
└── dns-bypass-analytic/
    └── stat=reports/
        └── substat=ranked-traffic/
            └── agency=<agency-name>/
                └── bypass-*/
                    └── ipv=*/
                        └── ip_field=*/
                            └── cadence=week/
                                └── start_date=YYYY-MM-DD/
                                    └── *.csv


For each agency:

Collects all CSV files under the resolved start_date

Builds one email

Attaches only CSV files (no inline data)

Sends email using SMTP

Email Behavior
Headers

FROM: MAIL_FROM

TO: Same as sender (MAIL_FROM)

BCC: Agency distribution list

SUBJECT:

DNS Service Bypass Weekly Report <agency-name>


(agency= prefix is automatically removed)

Body (no CSV rows shown)
BODY of EMAIL:
For questions about this report, please reply to this message or e-mail <MAIL_FROM>.
DISCLAIMER: <custom disclaimer>

Attachments

All .csv files found for that agency and start_date

TEST MODE vs PROD MODE

The Lambda supports safe testing in production.

Environment Variables Used

AGENCY_EMAIL_MAP → real production distribution lists

TEST_EMAIL_MAP → test distribution lists

TEST_MODE → controls which map is used

How It Works
TEST_MODE	Email Recipients
true	Uses TEST_EMAIL_MAP
false	Uses AGENCY_EMAIL_MAP

If an agency is not found in the selected map:

Email is sent to DEFAULT_EMAIL_TO

Typical Workflow

Deploy Lambda to prod

Set:

TEST_MODE=true


Run Lambda manually and verify output

Flip:

TEST_MODE=false


Scheduled runs now go to real distribution lists

No code changes required.

Required Environment Variables
SMTP (Required)
MAIL_FROM=dns-bypass-reports@example.mil
SMTP_HOST_1=smtp1.example.mil
SMTP_PORT=587
SMTP_MODE=starttls


If SMTP authentication is required:

SMTP_USER=<username>
SMTP_PASS=<password>


Optional SMTP failover:

SMTP_HOST_2=smtp2.example.mil

Email Routing
Production distribution lists
AGENCY_EMAIL_MAP={
  "agency=wholesales": ["dl-wholesales@example.mil"],
  "agency=retail": ["dl-retail@example.mil"]
}

Test distribution lists
TEST_EMAIL_MAP={
  "agency=wholesales": ["test-user@example.mil"],
  "agency=retail": ["test-user@example.mil"]
}

Toggle
TEST_MODE=true   # or false

Default fallback recipient
DEFAULT_EMAIL_TO=ops@example.mil

Optional Content Customization
DISCLAIMER_TEXT=This report is confidential and intended for authorized recipients only.

IAM Permissions Required

Lambda execution role must allow:

S3
s3:ListBucket
s3:GetObject

(No SNS required – SMTP is used)
EventBridge (CloudWatch Events) Setup

Runs every Monday at 7:00am Eastern Time

⚠️ CloudWatch Events cron schedules are UTC only and do not auto-handle DST.

Cron Expressions

7:00am EST (winter):

cron(0 12 ? * MON *)


7:00am EDT (summer):

cron(0 11 ? * MON *)

Recommended Practice

Use one rule

Update the hour twice per year for DST
OR

Accept a 1-hour shift during DST
OR

Migrate to EventBridge Scheduler (supports time zones automatically)

Console Steps (CloudWatch Events)

Open Amazon EventBridge

Go to Rules

Click Create rule

Rule type: Schedule

Schedule pattern: Cron

Enter cron expression (see above)

Target:

AWS service → Lambda function

Select this Lambda

(Optional) Constant JSON input:

{ "mode": "weekly" }


Create rule

Manual Testing
Example test event (manual run)
{
  "start_date": "2025-12-25",
  "test_mode": true
}


Or:

{
  "days_ago": 14
}

Failure Handling

SMTP primary host failure → automatic retry on SMTP_HOST_2

Agency with no email mapping → falls back to DEFAULT_EMAIL_TO

No CSVs found → no agency email sent (optional fallback email can be added)

Operational Notes

CSV attachment size depends on SMTP server limits (usually 10–25MB)

If CSV volume grows, recommended enhancement:

Zip attachments per agency

Or upload consolidated zip to S3 and email link

Summary

This Lambda provides:

Automated weekly reporting

Safe testing in production

Agency-specific email routing

SMTP failover

Clean separation of logic and configuration
