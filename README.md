# Weekly Report – AWS Lambda

## Overview

This AWS Lambda function generates **weekly DNS / DoH / DoT bypass reports** by:

- Traversing a deeply nested **Amazon S3 folder structure**
- Collecting **CSV files** for a computed reporting date
- Grouping data **by agency**
- Sending **one email per agency**
- Attaching **only CSV files** (no inline CSV rows)
- Sending emails via **SMTP with failover**
- Supporting **TEST mode vs PROD mode**
- Running automatically **every Monday at 7:00am Eastern Time**

---

## What the Lambda Does

1. **Determines the report start date**
   - Scheduled Monday run → **Monday two weeks ago**
   - Manual run → **Exactly 14 days ago**
   - Supports overrides via event input

2. **Traverses this S3 structure**

s3://<bucket>/
└── dns-bypass-/
    └── stat=rep/
        └── substat=ra-tr/
            └── agency=<agency-name>/
                └── bp-/
                    └── ipv=*/
                        └── ip_field=*/
                            └── c=k/
                                └── start_date=YYYY-MM-DD/
                                    └── *.csv


3. **Per agency**
   - Collects all CSV files for the resolved `start_date`
   - Builds **one email**
   - Attaches **all CSVs for that agency**
   - Sends email using SMTP

---

## Email Format

### Headers
- **FROM:** `MAIL_FROM`
- **TO:** Same as sender (`MAIL_FROM`)
- **BCC:** Agency distribution list
- **SUBJECT:**

DNS Service Bypass Weekly Report <agency-name>

(`agency=` prefix is removed automatically)

### Body (no CSV rows)

BODY of EMAIL:
For questions about this report, please reply to this message or e-mail <MAIL_FROM>.
DISCLAIMER: <custom disclaimer text>


### Attachments
- All `.csv` files for the agency and reporting date

---

## TEST MODE vs PROD MODE

The Lambda supports **safe testing in production**.

### Environment Variables Used
- `AGENCY_EMAIL_MAP` → real production distribution lists
- `TEST_EMAIL_MAP` → test distribution lists
- `TEST_MODE` → toggle

### Behavior

| TEST_MODE | Recipient Source |
|---------|------------------|
| `true`  | `TEST_EMAIL_MAP` |
| `false` | `AGENCY_EMAIL_MAP` |

If an agency is **not found** in the selected map, the email is sent to `DEFAULT_EMAIL_TO`.

### Recommended Workflow

1. Deploy Lambda to production
2. Set:

TEST_MODE=true

3. Run Lambda manually and validate output
4. Switch to:


TEST_MODE=false

5. Scheduled runs now go to real recipients

No code changes required.

---

## Required Environment Variables

### SMTP (Required)



MAIL_FROM=dns-bypass-reports@example.mil

SMTP_HOST_1=smtp1.example.mil
SMTP_PORT=587
SMTP_MODE=starttls


If authentication is required:



SMTP_USER=<username>
SMTP_PASS=<password>


Optional SMTP failover:



SMTP_HOST_2=smtp2.example.mil


---

### Email Routing

#### Production Distribution Lists



AGENCY_EMAIL_MAP={
"agency=wholesales": ["dl-wholesales@example.mil
"],
"agency=retail": ["dl-retail@example.mil
"]
}


#### Test Distribution Lists



TEST_EMAIL_MAP={
"agency=wholesales": ["test-user@example.mil
"],
"agency=retail": ["test-user@example.mil
"]
}


#### Toggle



TEST_MODE=true # or false


#### Default Fallback Recipient



DEFAULT_EMAIL_TO=ops@example.mil


---

### Optional Customization



DISCLAIMER_TEXT=This report is confidential and intended for authorized recipients only.


---

## IAM Permissions Required

Lambda execution role must include:

### S3


s3:ListBucket
s3:GetObject


(No SNS permissions required — SMTP is used.)

---

## EventBridge (CloudWatch Events) Setup  
**Every Monday at 7:00am Eastern Time**

> ⚠️ CloudWatch Events schedules use **UTC only** and do **not handle DST automatically**.

### Cron Expressions

- **7:00am EST (winter):**


cron(0 12 ? * MON *)


- **7:00am EDT (summer):**


cron(0 11 ? * MON *)


### Console Steps

1. Open **Amazon EventBridge**
2. Go to **Rules**
3. Click **Create rule**
4. Rule type: **Schedule**
5. Schedule pattern: **Cron**
6. Enter cron expression (above)
7. Target:
   - AWS service → **Lambda function**
8. (Optional) Constant JSON input:
   ```json
   { "mode": "weekly" }
9. Create rule

## Manual Testing
### Example Test Event

{
  "start_date": "2025-12-25",
  "test_mode": true
}

### Or:

{
  "days_ago": 14
}

## Failure Handling

- SMTP primary failure → automatic retry on secondary host

- Missing agency email mapping → fallback to DEFAULT_EMAIL_TO

- No CSV files → no agency email sent (optional fallback email can be added)

## Operational Notes

- SMTP servers usually limit attachments to 10–25 MB

- If CSV volume grows, recommended enhancements:
    - Zip CSVs per agency
    - Upload zip to S3 and email a presigned URL

## Summary

This Lambda provides:

- Automated weekly reporting

- Safe test-first execution in production

- Agency-specific email routing

- SMTP failover

- Clean separation of logic and configuration
