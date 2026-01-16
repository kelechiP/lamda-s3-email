import boto3
import os
import json
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from botocore.exceptions import ClientError

# =========================
# AWS clients
# =========================
s3 = boto3.client("s3")

# =========================
# S3 data location (ENV)
# =========================
BUCKET = os.getenv("BUCKET", "bucketname").strip()
BASE_PREFIX = os.getenv("BASE_PREFIX", "dns-bypass-analytic/stat=reports/substat=ranked-traffic/").strip()
CADENCE_PREFIX = os.getenv("CADENCE_PREFIX", "cadence=week/").strip()

# =========================
# Email list location (S3) (ENV)
# =========================
AGENCY_EMAIL_LIST_BUCKET = os.getenv("AGENCY_EMAIL_LIST_BUCKET", "").strip()
AGENCY_EMAIL_LIST_KEY = os.getenv("AGENCY_EMAIL_LIST_KEY", "").strip()

# =========================
# SMTP config (ENV)
# =========================
SMTP_HOST_1 = os.getenv("SMTP_HOST_1", "").strip()
SMTP_HOST_2 = os.getenv("SMTP_HOST_2", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
MAIL_FROM = os.getenv("MAIL_FROM", "").strip()
SMTP_MODE = os.getenv("SMTP_MODE", "starttls").lower()  # starttls | ssl | plain

# =========================
# Routing + behavior flags (ENV)
# =========================
TEST_MODE = os.getenv("TEST_MODE", "false").strip().lower() in ("1", "true", "yes", "y", "on")
DEFAULT_EMAIL_TO = os.getenv("DEFAULT_EMAIL_TO", "").strip()

# In TEST_MODE, this env var provides recipients (JSON dict):
# {"agency=wholesales":["testdl@example.com"], ...}
TEST_EMAIL_MAP: Dict[str, List[str]] = json.loads(os.getenv("TEST_EMAIL_MAP", "{}"))

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
# Helpers
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

    return monday_two_weeks_ago(today) if today.weekday() == 0 else exact_days_ago(today, 14)


def normalize_agency_for_subject(agency_folder_name: str) -> str:
    # "agency=wholesales" -> "wholesales"
    return agency_folder_name.split("agency=", 1)[1] if agency_folder_name.startswith("agency=") else agency_folder_name


def load_json_from_s3_bucket_key(bucket_name: str, key: str, default=None) -> Dict[str, List[str]]:
    """
    Load and parse a JSON mapping file from S3.
    Expected shape: {"agency=...": ["dl@example.com", ...], ...}
    """
    if not bucket_name or not key:
        print("[WARN] AGENCY_EMAIL_LIST_BUCKET/KEY not set; using default mapping")
        return default if default is not None else {}

    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)

        if not isinstance(data, dict):
            raise ValueError("Email map JSON must be an object/dict at top level.")

        normalized: Dict[str, List[str]] = {}
        for k, v in data.items():
            if isinstance(v, list):
                normalized[k] = [str(x).strip() for x in v if str(x).strip()]
            elif isinstance(v, str):
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


def pick_bcc_recipients(agency_folder_name: str, agency_email_map: Dict[str, List[str]]) -> List[str]:
    mapping = TEST_EMAIL_MAP if TEST_MODE else agency_email_map
    recipients = mapping.get(agency_folder_name, []) or []
    if not recipients and DEFAULT_EMAIL_TO:
        recipients = [DEFAULT_EMAIL_TO]
    return recipients


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


# =========================
# Lambda handler
# =========================
def lambda_handler(event, context):
    start_date = resolve_start_date(event or {})

    # 🔍 Log configuration before any S3 listing (as requested)
    if TEST_MODE:
        print(
            "[CONFIG] Email list source | "
            "MODE=TEST | "
            "SOURCE=ENV | "
            "ENV_VAR=TEST_EMAIL_MAP"
        )
    else:
        print(
            "[CONFIG] Email list source | "
            "MODE=PROD | "
            "SOURCE=S3 | "
            f"AGENCY_EMAIL_LIST_BUCKET={AGENCY_EMAIL_LIST_BUCKET} | "
            f"AGENCY_EMAIL_LIST_KEY={AGENCY_EMAIL_LIST_KEY}"
        )

    # 🔍 Log data source
    print(
        "[CONFIG] Data source | "
        f"BUCKET={BUCKET} | "
        f"BASE_PREFIX={BASE_PREFIX} | "
        f"CADENCE_PREFIX={CADENCE_PREFIX}"
    )

    print(f"[INFO] Using start_date={start_date} | TEST_MODE={TEST_MODE}")

    # Basic config validation
    if not MAIL_FROM:
        raise ValueError("MAIL_FROM env var is required.")
    if not (SMTP_HOST_1 or SMTP_HOST_2):
        raise ValueError("Set SMTP_HOST_1 and/or SMTP_HOST_2 env vars.")
    if not DEFAULT_EMAIL_TO:
        print("[WARN] DEFAULT_EMAIL_TO not set. 'NO DATA' per-agency emails cannot be sent to default.")
    if not BASE_PREFIX.endswith("/"):
        raise ValueError("BASE_PREFIX must end with '/'.")
    if not CADENCE_PREFIX.endswith("/"):
        raise ValueError("CADENCE_PREFIX must end with '/'.")

    # Load agency email map (prod) once per invocation
    agency_email_map = {}
    if not TEST_MODE:
        agency_email_map = load_json_from_s3_bucket_key(
            AGENCY_EMAIL_LIST_BUCKET,
            AGENCY_EMAIL_LIST_KEY,
            default={}
        )
        print(f"[INFO] Loaded prod email map entries={len(agency_email_map)}")
    else:
        print(f"[INFO] Loaded test email map entries={len(TEST_EMAIL_MAP)}")

    agencies = list_child_prefixes(BASE_PREFIX)
    print(f"[INFO] Found agencies={len(agencies)}")

    # Track attachments per agency + agencies with no data
    agency_attachments: Dict[str, List[Tuple[str, bytes]]] = defaultdict(list)
    agencies_with_no_csv: List[str] = []

    for agency_prefix in agencies:
        agency_folder = agency_prefix.rstrip("/").split("/")[-1]  # e.g., agency=wholesales

        # Collect all CSVs under this agency for start_date
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

        # If this agency has no CSV attachments, track it for NO DATA email
        if not agency_attachments.get(agency_folder):
            agencies_with_no_csv.append(agency_folder)

    # 1) Send normal report emails for agencies that DO have CSVs
    sent = 0
    skipped = 0

    for agency_folder, attachments in agency_attachments.items():
        if not attachments:
            continue

        bcc_list = pick_bcc_recipients(agency_folder, agency_email_map)
        if not bcc_list:
            print(f"[WARN] No recipients for {agency_folder} and DEFAULT_EMAIL_TO not set. Skipping report email.")
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

        print(
            f"[EMAIL SENT] type=REPORT | agency={agency_folder} | to={MAIL_FROM} | "
            f"bcc={','.join(bcc_list)} | attachments={len(attachments)} | start_date={start_date}"
        )
        sent += 1

    # 2) Send a NO DATA email to DEFAULT_EMAIL_TO listing agencies without CSV
    #    (only if there is at least one agency missing CSV AND DEFAULT_EMAIL_TO is set)
    no_data_sent = 0
    if agencies_with_no_csv and DEFAULT_EMAIL_TO:
        agencies_display = ", ".join([normalize_agency_for_subject(a) for a in agencies_with_no_csv])

        subject = f"DNS Service Bypass Weekly Report - NO DATA (start_date={start_date})"
        body = EMAIL_BODY_TEMPLATE.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)
        body += (
            "\nNO DATA DETAILS:\n"
            f"No CSV files were found for start_date={start_date} for the following agencies:\n"
            f"{agencies_display}\n"
        )

        send_email_with_failover(
            subject=subject,
            body=body,
            to_addr=MAIL_FROM,
            bcc_addrs=[DEFAULT_EMAIL_TO],
            attachments=[]
        )

        print(
            f"[EMAIL SENT] type=NO_DATA_PER_AGENCY | to={MAIL_FROM} | bcc={DEFAULT_EMAIL_TO} | "
            f"missing_agencies_count={len(agencies_with_no_csv)} | start_date={start_date}"
        )
        no_data_sent = 1
    elif agencies_with_no_csv and not DEFAULT_EMAIL_TO:
        print(
            f"[WARN] Agencies with no CSV exist ({len(agencies_with_no_csv)}), "
            "but DEFAULT_EMAIL_TO is not set. Skipping per-agency NO DATA email."
        )

    # 3) If nothing found across ALL agencies (existing behavior), also send NO DATA to default (if set)
    #    This happens when sent==0 AND every agency had no csv.
    all_no_data = (sent == 0 and len(agencies) > 0 and len(agencies_with_no_csv) == len(agencies))
    if all_no_data and DEFAULT_EMAIL_TO:
        subject = f"DNS Service Bypass Weekly Report - NO DATA (ALL AGENCIES) start_date={start_date}"
        body = EMAIL_BODY_TEMPLATE.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)
        body += (
            "\nNO DATA DETAILS:\n"
            f"No CSV files were found for ANY agency for start_date={start_date} under:\n"
            f"s3://{BUCKET}/{BASE_PREFIX}\n"
        )

        send_email_with_failover(
            subject=subject,
            body=body,
            to_addr=MAIL_FROM,
            bcc_addrs=[DEFAULT_EMAIL_TO],
            attachments=[]
        )
        print(
            f"[EMAIL SENT] type=NO_DATA_ALL | to={MAIL_FROM} | bcc={DEFAULT_EMAIL_TO} | start_date={start_date}"
        )

    return {
        "status": "ok",
        "start_date": start_date,
        "test_mode": TEST_MODE,
        "emails_sent_report": sent,
        "emails_sent_no_data_per_agency": no_data_sent,
        "report_skipped_no_recipients": skipped,
        "agencies_total": len(agencies),
        "agencies_with_csv": len([a for a, atts in agency_attachments.items() if atts]),
        "agencies_without_csv": len(agencies_with_no_csv),
        "agencies_without_csv_list": agencies_with_no_csv,
    }
