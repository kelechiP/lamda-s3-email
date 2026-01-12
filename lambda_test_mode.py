import boto3
import os
import json
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from collections import defaultdict

s3 = boto3.client("s3")

# =========================
# S3 Config
# =========================
BUCKET = "bucketname"
BASE_PREFIX = "dns-bypass-analytic/stat=reports/substat=ranked-traffic/"
CADENCE_PREFIX = "cadence=week/"

# =========================
# SMTP Config (ENV VARS)
# =========================
SMTP_HOST_1 = os.getenv("SMTP_HOST_1", "").strip()
SMTP_HOST_2 = os.getenv("SMTP_HOST_2", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
MAIL_FROM = os.getenv("MAIL_FROM", "").strip()
SMTP_MODE = os.getenv("SMTP_MODE", "starttls").lower()  # starttls | ssl | plain

# =========================
# Recipient routing (ENV VARS)
# =========================
# Toggle test mode without editing env vars:
#   TEST_MODE="true"  -> use TEST_EMAIL_MAP
#   TEST_MODE="false" -> use AGENCY_EMAIL_MAP
TEST_MODE = os.getenv("TEST_MODE", "false").strip().lower() in ("1", "true", "yes", "y", "on")

# Map agency folder name -> distro list emails (BCC)
# Keys MUST match the folder name, e.g. "agency=wholesales"
AGENCY_EMAIL_MAP: Dict[str, List[str]] = json.loads(os.getenv("AGENCY_EMAIL_MAP", "{}"))
TEST_EMAIL_MAP: Dict[str, List[str]] = json.loads(os.getenv("TEST_EMAIL_MAP", "{}"))

# If an agency has no mapped email, send to this default BCC
DEFAULT_EMAIL_TO = os.getenv("DEFAULT_EMAIL_TO", "").strip()

# Body text only (no CSV rows)
DISCLAIMER_TEXT = os.getenv(
    "DISCLAIMER_TEXT",
    "This report is generated automatically and is for informational purposes only."
).strip()

EMAIL_BODY_TEMPLATE = (
    "BODY of EMAIL:\n"
    "For questions about this report, please reply to this message or e-mail {sender}.\n"
    "DISCLAIMER: {disclaimer}\n"
)


# =========================
# Date resolution
# =========================
def monday_two_weeks_ago(today: datetime) -> str:
    monday_this_week = today - timedelta(days=today.weekday())
    return (monday_this_week - timedelta(days=14)).strftime("%Y-%m-%d")


def exact_days_ago(today: datetime, days: int = 14) -> str:
    return (today - timedelta(days=days)).strftime("%Y-%m-%d")


def resolve_start_date(event: Dict[str, Any]) -> str:
    """
    Default:
      - Monday UTC runs -> Monday two weeks ago
      - Any other day -> exactly 14 days ago
    Overrides:
      {"start_date":"YYYY-MM-DD"} or {"mode":"weekly"} or {"days_ago":14}
    """
    today = datetime.utcnow()
    event = event or {}

    if event.get("start_date"):
        return event["start_date"]

    if event.get("mode") == "weekly":
        return monday_two_weeks_ago(today)

    if event.get("days_ago") is not None:
        return exact_days_ago(today, int(event["days_ago"]))

    if today.weekday() == 0:  # Monday UTC
        return monday_two_weeks_ago(today)

    return exact_days_ago(today, 14)


def normalize_agency_for_subject(agency_folder_name: str) -> str:
    """
    Input:  "agency=wholesales"
    Output: "wholesales"
    If format differs, returns original string.
    """
    if agency_folder_name.startswith("agency="):
        return agency_folder_name.split("agency=", 1)[1]
    return agency_folder_name


def pick_bcc_recipients(agency_folder_name: str) -> List[str]:
    """
    TEST_MODE=True  -> use TEST_EMAIL_MAP
    TEST_MODE=False -> use AGENCY_EMAIL_MAP
    Fallback to DEFAULT_EMAIL_TO if not mapped or mapped list empty.
    """
    mapping = TEST_EMAIL_MAP if TEST_MODE else AGENCY_EMAIL_MAP
    recipients = mapping.get(agency_folder_name, []) or []
    if not recipients and DEFAULT_EMAIL_TO:
        recipients = [DEFAULT_EMAIL_TO]
    return recipients


def lambda_handler(event, context):
    start_date = resolve_start_date(event or {})
    print(f"[INFO] Using start_date={start_date} | TEST_MODE={TEST_MODE}")

    if not MAIL_FROM:
        raise ValueError("MAIL_FROM env var is required.")
    if not (SMTP_HOST_1 or SMTP_HOST_2):
        raise ValueError("Set SMTP_HOST_1 and/or SMTP_HOST_2 env vars.")
    if not DEFAULT_EMAIL_TO and not (TEST_EMAIL_MAP or AGENCY_EMAIL_MAP):
        print("[WARN] No DEFAULT_EMAIL_TO and email maps are empty; some agencies may be skipped.")

    agencies = list_child_prefixes(BASE_PREFIX)
    print(f"[INFO] Found agencies={len(agencies)}")

    # Collect attachments per agency: agency_folder -> list[(filename, bytes)]
    agency_attachments: Dict[str, List[Tuple[str, bytes]]] = defaultdict(list)

    for agency_prefix in agencies:
        agency_folder = agency_prefix.rstrip("/").split("/")[-1]  # e.g. agency=wholesales
        print(f"[INFO] Agency={agency_folder}")

        bypass_prefixes = list_child_prefixes(agency_prefix)
        for bypass_prefix in bypass_prefixes:
            ipv_prefixes = list_child_prefixes(bypass_prefix)
            for ipv_prefix in ipv_prefixes:
                ip_field_prefixes = list_child_prefixes(ipv_prefix)
                for ipf_prefix in ip_field_prefixes:
                    target_prefix = f"{ipf_prefix}{CADENCE_PREFIX}start_date={start_date}/"
                    csv_keys = list_csv_keys(target_prefix)
                    for key in csv_keys:
                        filename = key.split("/")[-1]
                        content = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                        agency_attachments[agency_folder].append((filename, content))

    # Send one email per agency that has attachments
    sent = 0
    skipped = 0

    for agency_folder, attachments in agency_attachments.items():
        bcc_list = pick_bcc_recipients(agency_folder)

        if not bcc_list:
            print(f"[WARN] No mapped recipients for {agency_folder} and DEFAULT_EMAIL_TO not set. Skipping.")
            skipped += 1
            continue

        agency_short = normalize_agency_for_subject(agency_folder)
        subject = f"DNS Service Bypass Weekly Report {agency_short}"

        body = EMAIL_BODY_TEMPLATE.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

        send_email_with_failover(
            subject=subject,
            body=body,
            to_addr=MAIL_FROM,      # TO: same as sender
            bcc_addrs=bcc_list,     # BCC: distro list for agency (or default)
            attachments=attachments
        )
        sent += 1

    # Optional: if you want an email when nothing is found at all
    if not agency_attachments and DEFAULT_EMAIL_TO:
        subject = "DNS Service Bypass Weekly Report - NO DATA"
        body = EMAIL_BODY_TEMPLATE.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)
        body += f"\n(No CSV files found for start_date={start_date} under {BASE_PREFIX})\n"
        send_email_with_failover(
            subject=subject,
            body=body,
            to_addr=MAIL_FROM,
            bcc_addrs=[DEFAULT_EMAIL_TO],
            attachments=[]
        )

    return {
        "status": "ok",
        "start_date": start_date,
        "test_mode": TEST_MODE,
        "emails_sent": sent,
        "skipped_no_recipients": skipped,
        "agencies_with_attachments": len(agency_attachments),
    }


# =========================
# S3 Helpers
# =========================
def list_child_prefixes(parent_prefix: str) -> List[str]:
    prefixes: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=parent_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])
    return prefixes


def list_csv_keys(prefix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                keys.append(key)
    return keys


# =========================
# SMTP sending with failover + attachments
# =========================
def send_email_with_failover(
    subject: str,
    body: str,
    to_addr: str,
    bcc_addrs: List[str],
    attachments: List[Tuple[str, bytes]],
):
    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Reply-To"] = MAIL_FROM
    # BCC header may be removed by some MTAs (normal), but recipients still receive the mail.
    msg["Bcc"] = ", ".join(bcc_addrs)

    msg.set_content(body)

    for filename, content in attachments:
        msg.add_attachment(content, maintype="text", subtype="csv", filename=filename)

    all_rcpts = [to_addr] + bcc_addrs

    errors = []
    for host in [SMTP_HOST_1, SMTP_HOST_2]:
        if not host:
            continue
        try:
            _send_via_host(host, msg, all_rcpts)
            print(f"[INFO] Email sent via {host} | TO={to_addr} | BCC={bcc_addrs} | attachments={len(attachments)}")
            return
        except Exception as e:
            err = f"{host} failed: {repr(e)}"
            print(f"[ERROR] {err}")
            errors.append(err)

    raise RuntimeError("All SMTP hosts failed. " + " | ".join(errors))


def _send_via_host(host: str, msg: EmailMessage, recipients: List[str]):
    if SMTP_MODE == "ssl":
        with smtplib.SMTP_SSL(host, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg, from_addr=MAIL_FROM, to_addrs=recipients)
    else:
        with smtplib.SMTP(host, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            if SMTP_MODE == "starttls":
                server.starttls()
                server.ehlo()
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg, from_addr=MAIL_FROM, to_addrs=recipients)
