import boto3
import os
import json
import smtplib
import io
import csv
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

# Ranked-traffic base (attachments)
BASE_PREFIX_RANKED = os.getenv(
    "BASE_PREFIX_RANKED",
    "dns-bypass-analytic/stat=reports/substat=ranked-traffic/"
).strip()

# Summary base (body content)
BASE_PREFIX_SUMMARY = os.getenv(
    "BASE_PREFIX_SUMMARY",
    "dns-bypass-analytic/stat=reports/substat=summary/"
).strip()

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

TEST_EMAIL_MAP: Dict[str, List[str]] = json.loads(os.getenv("TEST_EMAIL_MAP", "{}"))

DISCLAIMER_TEXT = os.getenv(
    "DISCLAIMER_TEXT",
    "This report is generated automatically and is for informational purposes only."
).strip()

EMAIL_FOOTER = (
    "\n\n"
    "For questions about this report, please reply to this message or e-mail {sender}.\n"
    "DISCLAIMER: {disclaimer}\n"
)

SEP = "=" * 30


# =========================
# Date helpers
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


def normalize_agency_for_subject(agency_folder_name: str) -> str:
    return agency_folder_name.split("agency=", 1)[1] if agency_folder_name.startswith("agency=") else agency_folder_name


# =========================
# Email map loader
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


# =========================
# S3 helpers (parametrized by base)
# =========================
def list_child_prefixes(bucket: str, parent_prefix: str) -> List[str]:
    prefixes: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=parent_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])
    return prefixes


def list_csv_keys(bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                keys.append(key)
    return keys


def read_csv_as_text(bucket: str, key: str) -> str:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(body))
    lines: List[str] = []
    for row in reader:
        lines.append(", ".join(row))
    return "\n".join(lines).replace("\r\n", "\n").replace("\r", "\n").strip()


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

    # 🔍 Log configuration before any S3 listing
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
        f"BASE_PREFIX_RANKED={BASE_PREFIX_RANKED} | "
        f"BASE_PREFIX_SUMMARY={BASE_PREFIX_SUMMARY} | "
        f"CADENCE_PREFIX={CADENCE_PREFIX}"
    )

    print(f"[INFO] Using start_date={start_date} | TEST_MODE={TEST_MODE}")

    # Basic config validation
    if not MAIL_FROM:
        raise ValueError("MAIL_FROM env var is required.")
    if not (SMTP_HOST_1 or SMTP_HOST_2):
        raise ValueError("Set SMTP_HOST_1 and/or SMTP_HOST_2 env vars.")
    if not DEFAULT_EMAIL_TO:
        print("[WARN] DEFAULT_EMAIL_TO not set. 'NO DATA' emails cannot be sent to default.")
    for p in (BASE_PREFIX_RANKED, BASE_PREFIX_SUMMARY, CADENCE_PREFIX):
        if not p.endswith("/"):
            raise ValueError("All prefixes must end with '/' (BASE_PREFIX_RANKED, BASE_PREFIX_SUMMARY, CADENCE_PREFIX).")

    # Load email map once per invocation (prod)
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

    # Discover agencies from the RANKED base (source of truth)
    agencies = list_child_prefixes(BUCKET, BASE_PREFIX_RANKED)
    print(f"[INFO] Found agencies={len(agencies)}")

    # Per-agency:
    # - ranked CSVs -> attachments
    # - summary CSV text -> body section
    agency_attachments: Dict[str, List[Tuple[str, bytes]]] = defaultdict(list)
    agency_summary_text: Dict[str, str] = defaultdict(str)

    agencies_no_ranked: List[str] = []
    agencies_no_summary: List[str] = []

    for agency_prefix_ranked in agencies:
        agency_folder = agency_prefix_ranked.rstrip("/").split("/")[-1]  # e.g., agency=wholesales

        # ===== RANKED: collect CSV attachments =====
        bypass_prefixes = list_child_prefixes(BUCKET, agency_prefix_ranked)
        for bypass_prefix in bypass_prefixes:
            ipv_prefixes = list_child_prefixes(BUCKET, bypass_prefix)
            for ipv_prefix in ipv_prefixes:
                ip_field_prefixes = list_child_prefixes(BUCKET, ipv_prefix)
                for ipf_prefix in ip_field_prefixes:
                    target_prefix = f"{ipf_prefix}{CADENCE_PREFIX}start_date={start_date}/"
                    for key in list_csv_keys(BUCKET, target_prefix):
                        filename = key.split("/")[-1]
                        content = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                        agency_attachments[agency_folder].append((filename, content))

        if not agency_attachments.get(agency_folder):
            agencies_no_ranked.append(agency_folder)

        # ===== SUMMARY: read CSV content into body =====
        agency_prefix_summary = agency_prefix_ranked.replace(BASE_PREFIX_RANKED, BASE_PREFIX_SUMMARY, 1)

        summary_sections: List[str] = []
        bypass_prefixes_s = list_child_prefixes(BUCKET, agency_prefix_summary)
        for bypass_prefix in bypass_prefixes_s:
            ipv_prefixes_s = list_child_prefixes(BUCKET, bypass_prefix)
            for ipv_prefix in ipv_prefixes_s:
                ip_field_prefixes_s = list_child_prefixes(BUCKET, ipv_prefix)
                for ipf_prefix in ip_field_prefixes_s:
                    target_prefix_s = f"{ipf_prefix}{CADENCE_PREFIX}start_date={start_date}/"
                    csv_keys_s = list_csv_keys(BUCKET, target_prefix_s)

                    for csv_key in csv_keys_s:
                        label = csv_key.replace(agency_prefix_summary, "").rstrip("/")
                        csv_text = read_csv_as_text(BUCKET, csv_key)
                        if csv_text:
                            summary_sections.append(
                                f"\nSUMMARY CSV: {label}\n{SEP}\n{csv_text}\n{SEP}\n"
                            )

        if summary_sections:
            agency_summary_text[agency_folder] = "\n".join(summary_sections).strip()
        else:
            agency_summary_text[agency_folder] = ""
            agencies_no_summary.append(agency_folder)

    # ===== Email sending logic =====

    # 1) Send normal report emails for agencies that DO have ranked CSVs (attachments)
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

        # Body: SUMMARY REPORTS section + footer (exact order requested)
        summary_text = agency_summary_text.get(agency_folder, "").strip()
        body = ""
        if summary_text:
            body += "SUMMARY REPORTS (from substat=summary):\n" + summary_text + "\n"

        body += EMAIL_FOOTER.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

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

    # 3) If nothing found across ALL agencies (no ranked CSVs anywhere), send ONLY the NO_DATA_ALL email
    all_no_data = (sent == 0 and len(agencies) > 0)

    if all_no_data and DEFAULT_EMAIL_TO:
        subject = f"DNS Service Bypass Weekly Report - NO DATA (ALL AGENCIES) start_date={start_date}"
        body = ""
        body += (
            "NO DATA DETAILS:\n"
            f"No ranked-traffic CSV files were found for ANY agency for start_date={start_date} under:\n"
            f"s3://{BUCKET}/{BASE_PREFIX_RANKED}\n\n"
            "Note: No per-agency NO DATA email will be sent when ALL agencies have no ranked-traffic CSV.\n"
        )
        body += EMAIL_FOOTER.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

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

    # 2) If at least one agency missing ranked CSV, send NO_DATA_PER_AGENCY
    #    ALSO include agencies missing summary CSV in a new paragraph
    no_data_sent = 0
    if (not all_no_data) and agencies_no_ranked and DEFAULT_EMAIL_TO:
        ranked_missing = ", ".join([normalize_agency_for_subject(a) for a in agencies_no_ranked])

        # agencies without summary csv (for same week)
        summary_missing = ", ".join([normalize_agency_for_subject(a) for a in agencies_no_summary]) if agencies_no_summary else "None"

        subject = f"DNS Service Bypass Weekly Report - NO DATA (start_date={start_date})"
        body = ""
        body += (
            "NO DATA DETAILS:\n"
            f"No ranked-traffic CSV files were found for start_date={start_date} for the following agencies:\n"
            f"{ranked_missing}\n\n"
            "SUMMARY NO DATA DETAILS:\n"
            f"No summary CSV files were found for start_date={start_date} for the following agencies:\n"
            f"{summary_missing}\n"
        )
        body += EMAIL_FOOTER.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

        send_email_with_failover(
            subject=subject,
            body=body,
            to_addr=MAIL_FROM,
            bcc_addrs=[DEFAULT_EMAIL_TO],
            attachments=[]
        )

        print(
            f"[EMAIL SENT] type=NO_DATA_PER_AGENCY | to={MAIL_FROM} | bcc={DEFAULT_EMAIL_TO} | "
            f"missing_ranked_count={len(agencies_no_ranked)} | missing_summary_count={len(agencies_no_summary)} | "
            f"start_date={start_date}"
        )
        no_data_sent = 1
    elif (not all_no_data) and agencies_no_ranked and not DEFAULT_EMAIL_TO:
        print(
            f"[WARN] Agencies missing ranked CSV exist ({len(agencies_no_ranked)}), "
            "but DEFAULT_EMAIL_TO is not set. Skipping per-agency NO DATA email."
        )

    return {
        "status": "ok",
        "start_date": start_date,
        "test_mode": TEST_MODE,
        "emails_sent_report": sent,
        "emails_sent_no_data_per_agency": no_data_sent,
        "report_skipped_no_recipients": skipped,
        "agencies_total": len(agencies),
        "agencies_no_ranked": len(agencies_no_ranked),
        "agencies_no_summary": len(agencies_no_summary),
    }
