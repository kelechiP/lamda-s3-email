import boto3
import os
import json
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

# =========================
# S3 path config (ENV VARS)
# =========================
BUCKET = os.getenv("BUCKET", "bucketname").strip()
BASE_PREFIX = os.getenv("BASE_PREFIX", "dns-bypass-analytic/stat=reports/substat=ranked-traffic/").strip()
CADENCE_PREFIX = os.getenv("CADENCE_PREFIX", "cadence=week/").strip()

# =========================
# SMTP config (ENV VARS)
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
TEST_MODE = os.getenv("TEST_MODE", "false").strip().lower() in ("1", "true", "yes", "y", "on")
DEFAULT_EMAIL_TO = os.getenv("DEFAULT_EMAIL_TO", "").strip()

DISCLAIMER_TEXT = os.getenv(
    "DISCLAIMER_TEXT",
    "This report is generated automatically and is for informational purposes only."
).strip()

EMAIL_BODY_TEMPLATE = (
    "BODY of EMAIL:\n"
    "For questions about this report, please reply to this message or e-mail {sender}.\n"
    "DISCLAIMER: {disclaimer}\n"
)

# Mapping file location in S3 (your teammate’s approach)
AGENCY_EMAIL_LIST_BUCKET = os.getenv("AGENCY_EMAIL_LIST_BUCKET", "").strip()
AGENCY_EMAIL_LIST_KEY = os.getenv("AGENCY_EMAIL_LIST_KEY", "").strip()

# Optional: test map can still come from env json, or also from S3 if you want later
TEST_EMAIL_MAP: Dict[str, List[str]] = json.loads(os.getenv("TEST_EMAIL_MAP", "{}"))

SEP = "=" * 20


# =========================
# Loader: mapping JSON from S3 bucket+key
# =========================
def load_json_from_s3_bucket_key(bucket_name: str, key: str, default=None) -> Dict[str, List[str]]:
    if not bucket_name or not key:
        print("[WARN] AGENCY_EMAIL_LIST_BUCKET/KEY not set; using default mapping")
        return default if default is not None else {}

    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)

        if not isinstance(data, dict):
            raise ValueError("Email map JSON must be an object/dict at top level.")

        # normalize: ensure values are list[str]
        normalized: Dict[str, List[str]] = {}
        for k, v in data.items():
            if isinstance(v, list):
                normalized[k] = [str(x).strip() for x in v if str(x).strip()]
            elif isinstance(v, str):
                # allow single string
                normalized[k] = [v.strip()] if v.strip() else []
            else:
                normalized[k] = []

        return normalized

    except ClientError as e:
        print(f"[ERROR] S3 ClientError loading email map: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON parsing error loading email map: {e}")
        raise


def normalize_agency_for_subject(agency_folder_name: str) -> str:
    return agency_folder_name.split("agency=", 1)[1] if agency_folder_name.startswith("agency=") else agency_folder_name


def pick_bcc_recipients(agency_folder_name: str, agency_email_map: Dict[str, List[str]]) -> List[str]:
    mapping = TEST_EMAIL_MAP if TEST_MODE else agency_email_map
    recipients = mapping.get(agency_folder_name, []) or []
    if not recipients and DEFAULT_EMAIL_TO:
        recipients = [DEFAULT_EMAIL_TO]
    return recipients


# =========================
# Date resolution
# =========================
def monday_two_weeks_ago(today: datetime) -> str:
    monday_this_week = today - timedelta(days=today.weekday())
    return (monday_this_week - timedelta(days=14)).strftime("%Y-%m-%d")


def exact_days_ago(today: datetime, days: int = 14) -> str:
    return (today - timedelta(days=days)).strftime("%Y-%m-%d")


def resolve_start_date(event: Dict[str, Any]) -> str:
    today = datetime.utcnow()
    event = event or {}

    if event.get("start_date"):
        return event["start_date"]
    if event.get("mode") == "weekly":
        return monday_two_weeks_ago(today)
    if event.get("days_ago") is not None:
        return exact_days_ago(today, int(event["days_ago"]))

    return monday_two_weeks_ago(today) if today.weekday() == 0 else exact_days_ago(today, 14)


# =========================
# Lambda handler (ONLY ONE)
# =========================
def lambda_handler(event, context):
    start_date = resolve_start_date(event or {})
    print(f"[INFO] Using start_date={start_date} | TEST_MODE={TEST_MODE}")

    # basic config checks
    if not MAIL_FROM:
        raise ValueError("MAIL_FROM env var is required.")
    if not (SMTP_HOST_1 or SMTP_HOST_2):
        raise ValueError("Set SMTP_HOST_1 and/or SMTP_HOST_2 env vars.")
    if not BASE_PREFIX.endswith("/"):
        raise ValueError("BASE_PREFIX must end with '/'.")
    if not CADENCE_PREFIX.endswith("/"):
        raise ValueError("CADENCE_PREFIX must end with '/'.")

    # Load agency email map from S3 once per invocation
    agency_email_map = load_json_from_s3_bucket_key(
        AGENCY_EMAIL_LIST_BUCKET,
        AGENCY_EMAIL_LIST_KEY,
        default={}
    )
    print(f"[INFO] Loaded email map entries={len(agency_email_map)}")

    agencies = list_child_prefixes(BASE_PREFIX)
    >>>> print(f"[INFO] Found agencies={len(agencies)}")

    # Collect attachments per agency
    agency_attachments: Dict[str, List[Tuple[str, bytes]]] = defaultdict(list)

    for agency_prefix in agencies:
        agency_folder = agency_prefix.rstrip("/").split("/")[-1]  # agency=wholesales

        bypass_prefixes = list_child_prefixes(agency_prefix)
        for bypass_prefix in bypass_prefixes:
            ipv_prefixes = list_child_prefixes(bypass_prefix)
            for ipv_prefix in ipv_prefixes:
                ip_field_prefixes = list_child_prefixes(ipv_prefix)
                for ipf_prefix in ip_field_prefixes:
                    target_prefix = f"{ipf_prefix}{CADENCE_PREFIX}start_date={start_date}/"
                    for key in list_csv_keys(target_prefix):
                        filename = key.split("/")[-1]
                        content = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                        agency_attachments[agency_folder].append((filename, content))

    sent = 0
    skipped = 0

    for agency_folder, attachments in agency_attachments.items():
        bcc_list = pick_bcc_recipients(agency_folder, agency_email_map)
        if not bcc_list:
            print(f"[WARN] No recipients for {agency_folder} and DEFAULT_EMAIL_TO not set. Skipping.")
            skipped += 1
            continue

        agency_short = normalize_agency_for_subject(agency_folder)
        subject = f"DNS Service Bypass Weekly Report {agency_short}"
        body = EMAIL_BODY_TEMPLATE.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

        send_email_with_failover(
            subject=subject,
            body=body,
            to_addr=MAIL_FROM,
            bcc_addrs=bcc_list,
            attachments=attachments
        )

        print(f"[EMAIL SENT] agency={agency_folder} | to={MAIL_FROM} | bcc={','.join(bcc_list)} | attachments={len(attachments)}")
        sent += 1

    return {
        "status": "ok",
        "start_date": start_date,
        "test_mode": TEST_MODE,
        "emails_sent": sent,
        "skipped_no_recipients": skipped,
        "agencies_with_attachments": len(agency_attachments),
    }


# =========================
# S3 helpers
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
            print(f"[INFO] Email sent via {host}")
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
