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

BASE_PREFIX_RANKED = os.getenv(
    "BASE_PREFIX_RANKED",
    "dns-bypass-analytic/stat=reports/substat=ranked-traffic/"
).strip()

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

# Allow multiple default emails: comma-separated
DEFAULT_EMAIL_TO_LIST = [
    e.strip() for e in os.getenv("DEFAULT_EMAIL_TO", "").split(",") if e.strip()
]

TEST_EMAIL_MAP: Dict[str, List[str]] = json.loads(os.getenv("TEST_EMAIL_MAP", "{}"))

DISCLAIMER_TEXT = os.getenv(
    "DISCLAIMER_TEXT",
    "This report is generated automatically and is for informational purposes only."
).strip()

# Footer always at end
EMAIL_FOOTER = (
    "\n\n"
    "For questions about this report, please reply to this message or e-mail {sender}.\n"
    "DISCLAIMER: {disclaimer}\n"
)

SEP = "=" * 30

# Optional: test override date without changing logic
TEST_START_DATE = os.getenv("TEST_START_DATE", "").strip()


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

    # default behavior
    return monday_two_weeks_ago(today) if today.weekday() == 0 else exact_days_ago(today, 14)


def resolve_effective_start_date(event: Dict[str, Any]) -> str:
    # Only honor TEST_START_DATE when TEST_MODE is true
    if TEST_MODE and TEST_START_DATE:
        print(f"[CONFIG] TEST_MODE active: forcing start_date={TEST_START_DATE}")
        return TEST_START_DATE
    return resolve_start_date(event or {})


def normalize_agency_for_subject(agency_folder_name: str) -> str:
    return agency_folder_name.split("agency=", 1)[1] if agency_folder_name.startswith("agency=") else agency_folder_name


# =========================
# Email map loader
# =========================
def load_json_from_s3_bucket_key(bucket_name: str, key: str, default=None) -> Dict[str, List[str]]:
    if not bucket_name or not key:
        print("[WARN] AGENCY_EMAIL_LIST_BUCKET/KEY not set; using default mapping")
        return default if default is not None else {}

    response = s3.get_object(Bucket=bucket_name, Key=key)
    content = response["Body"].read().decode("utf-8")
    data = json.loads(content)

    if not isinstance(data, dict):
        raise ValueError("Email map JSON must be a dict/object at top level.")

    normalized: Dict[str, List[str]] = {}
    for k, v in data.items():
        if isinstance(v, list):
            normalized[k] = [str(x).strip() for x in v if str(x).strip()]
        elif isinstance(v, str):
            normalized[k] = [v.strip()] if v.strip() else []
        else:
            normalized[k] = []
    return normalized


def get_active_email_map(prod_map: Dict[str, List[str]]) -> Dict[str, List[str]]:
    return TEST_EMAIL_MAP if TEST_MODE else prod_map


def get_explicit_dl_for_agency(agency_folder: str, active_map: Dict[str, List[str]]) -> List[str]:
    """
    Returns the explicitly mapped DL list for the agency.
    Does NOT fall back to DEFAULT_EMAIL_TO_LIST here.
    """
    dl = active_map.get(agency_folder, []) or []
    # normalize
    return [x.strip() for x in dl if x and x.strip()]


def get_report_recipients_for_agency(agency_folder: str, active_map: Dict[str, List[str]]) -> List[str]:
    """
    Used for report emails: explicit agency DL OR default fallback.
    """
    explicit = get_explicit_dl_for_agency(agency_folder, active_map)
    if explicit:
        return explicit
    return DEFAULT_EMAIL_TO_LIST[:]  # may be empty


# =========================
# S3 helpers
# =========================
def list_child_prefixes(bucket: str, parent_prefix: str) -> List[str]:
    prefixes: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=parent_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])
    return prefixes


def list_keys_with_suffix(bucket: str, prefix: str, suffix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(suffix.lower()):
                keys.append(key)
    return keys


def read_text_file(bucket: str, key: str) -> str:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="replace")
    return body.replace("\r\n", "\n").replace("\r", "\n").strip()


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
    start_date = resolve_effective_start_date(event or {})

    # Config logs
    if TEST_MODE:
        print("[CONFIG] Email list source | MODE=TEST | SOURCE=ENV | ENV_VAR=TEST_EMAIL_MAP")
    else:
        print(
            "[CONFIG] Email list source | MODE=PROD | SOURCE=S3 | "
            f"AGENCY_EMAIL_LIST_BUCKET={AGENCY_EMAIL_LIST_BUCKET} | "
            f"AGENCY_EMAIL_LIST_KEY={AGENCY_EMAIL_LIST_KEY}"
        )

    print(
        "[CONFIG] Data source | "
        f"BUCKET={BUCKET} | "
        f"BASE_PREFIX_RANKED={BASE_PREFIX_RANKED} | "
        f"BASE_PREFIX_SUMMARY={BASE_PREFIX_SUMMARY} | "
        f"CADENCE_PREFIX={CADENCE_PREFIX}"
    )
    print(f"[INFO] Using start_date={start_date}")

    # Basic validation
    if not MAIL_FROM:
        raise ValueError("MAIL_FROM env var is required.")
    if not (SMTP_HOST_1 or SMTP_HOST_2):
        raise ValueError("Set SMTP_HOST_1 and/or SMTP_HOST_2 env vars.")
    for p in (BASE_PREFIX_RANKED, BASE_PREFIX_SUMMARY, CADENCE_PREFIX):
        if not p.endswith("/"):
            raise ValueError("All prefixes must end with '/' (BASE_PREFIX_RANKED, BASE_PREFIX_SUMMARY, CADENCE_PREFIX).")

    # Load prod map (if needed) and choose active map (test vs prod)
    prod_map: Dict[str, List[str]] = {}
    if not TEST_MODE:
        prod_map = load_json_from_s3_bucket_key(AGENCY_EMAIL_LIST_BUCKET, AGENCY_EMAIL_LIST_KEY, default={})
        print(f"[INFO] Loaded prod email map entries={len(prod_map)}")
    else:
        print(f"[INFO] Loaded test email map entries={len(TEST_EMAIL_MAP)}")

    active_map = get_active_email_map(prod_map)

    # Discover agencies from ranked base
    agencies = list_child_prefixes(BUCKET, BASE_PREFIX_RANKED)
    print(f"[INFO] Found agencies={len(agencies)}")

    # Per-agency storage
    agency_attachments: Dict[str, List[Tuple[str, bytes]]] = defaultdict(list)   # ranked csv attachments
    agency_summary_text: Dict[str, str] = defaultdict(str)                      # summary txt body section

    agencies_no_ranked: List[str] = []   # no ranked csv
    agencies_no_summary: List[str] = []  # no summary txt

    # ---------- Scan S3 ----------
    for agency_prefix_ranked in agencies:
        agency_folder = agency_prefix_ranked.rstrip("/").split("/")[-1]  # e.g., agency=wholesales

        # Ranked: attachments (.csv)
        bypass_prefixes = list_child_prefixes(BUCKET, agency_prefix_ranked)
        for bypass_prefix in bypass_prefixes:
            ipv_prefixes = list_child_prefixes(BUCKET, bypass_prefix)
            for ipv_prefix in ipv_prefixes:
                ip_field_prefixes = list_child_prefixes(BUCKET, ipv_prefix)
                for ipf_prefix in ip_field_prefixes:
                    target_prefix = f"{ipf_prefix}{CADENCE_PREFIX}start_date={start_date}/"
                    for key in list_keys_with_suffix(BUCKET, target_prefix, ".csv"):
                        filename = key.split("/")[-1]
                        content = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                        agency_attachments[agency_folder].append((filename, content))

        if not agency_attachments.get(agency_folder):
            agencies_no_ranked.append(agency_folder)

        # Summary: body content (.txt)
        agency_prefix_summary = agency_prefix_ranked.replace(BASE_PREFIX_RANKED, BASE_PREFIX_SUMMARY, 1)

        summary_blocks: List[str] = []
        bypass_prefixes_s = list_child_prefixes(BUCKET, agency_prefix_summary)
        for bypass_prefix in bypass_prefixes_s:
            ipv_prefixes_s = list_child_prefixes(BUCKET, bypass_prefix)
            for ipv_prefix in ipv_prefixes_s:
                ip_field_prefixes_s = list_child_prefixes(BUCKET, ipv_prefix)
                for ipf_prefix in ip_field_prefixes_s:
                    target_prefix_s = f"{ipf_prefix}{CADENCE_PREFIX}start_date={start_date}/"
                    for txt_key in list_keys_with_suffix(BUCKET, target_prefix_s, ".txt"):
                        label = txt_key.replace(agency_prefix_summary, "").lstrip("/")
                        txt_content = read_text_file(BUCKET, txt_key)
                        if txt_content:
                            summary_blocks.append(
                                f"\nSUMMARY TXT: {label}\n{SEP}\n{txt_content}\n{SEP}\n"
                            )

        if summary_blocks:
            agency_summary_text[agency_folder] = "\n".join(summary_blocks).strip()
        else:
            agency_summary_text[agency_folder] = ""
            agencies_no_summary.append(agency_folder)

    # ---------- Send emails ----------
    sent_reports = 0
    sent_agency_no_data = 0

    # 1) REPORT emails: agencies that HAVE ranked CSV attachments
    for agency_folder, attachments in agency_attachments.items():
        if not attachments:
            continue

        bcc_list = get_report_recipients_for_agency(agency_folder, active_map)
        if not bcc_list:
            print(f"[WARN] No recipients for {agency_folder} and DEFAULT_EMAIL_TO not set. Skipping REPORT email.")
            continue

        agency_short = normalize_agency_for_subject(agency_folder)
        subject = f"DNS Service Bypass Weekly Report {agency_short}"

        body = ""
        summary_text = agency_summary_text.get(agency_folder, "").strip()
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
        sent_reports += 1

    # 2) AGENCY "NO DATA" emails (NEW):
    #    If agency has an explicit DL, but NO ranked CSV this week,
    #    send "no DNS bypass traffic report data..." message.
    #    If summary exists, include it in the body.
    for agency_folder in agencies_no_ranked:
        explicit_dl = get_explicit_dl_for_agency(agency_folder, active_map)
        if not explicit_dl:
            # No explicit DL configured -> do not send agency-specific NO DATA email
            continue

        agency_short = normalize_agency_for_subject(agency_folder)
        subject = f"DNS Service Bypass Weekly Report - NO DATA {agency_short}"

        summary_text = agency_summary_text.get(agency_folder, "").strip()
        if summary_text:
            body = (
                "There is no DNS bypass traffic report data for this week. "
                "A summary report is included below.\n\n"
                "SUMMARY REPORTS (from substat=summary):\n"
                f"{summary_text}\n"
            )
        else:
            body = (
                "There is no DNS bypass traffic report data for this week and no summary report.\n"
            )

        body += EMAIL_FOOTER.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

        send_email_with_failover(
            subject=subject,
            body=body,
            to_addr=MAIL_FROM,
            bcc_addrs=explicit_dl,
            attachments=[]
        )

        print(
            f"[EMAIL SENT] type=NO_DATA_AGENCY | agency={agency_folder} | to={MAIL_FROM} | "
            f"bcc={','.join(explicit_dl)} | start_date={start_date} | summary_included={bool(summary_text)}"
        )
        sent_agency_no_data += 1

    # 3) OPTIONAL: DEFAULT "NO DATA" summary email listing missing agencies (to DEFAULT_EMAIL_TO_LIST)
    #    (keeps your previous reporting to the default distribution list)
    if DEFAULT_EMAIL_TO_LIST:
        # Only send this if at least one agency missing ranked CSV
        # and we didn't have the "all agencies have no ranked data" condition.
        all_no_ranked = (len(agencies) > 0 and len(agencies_no_ranked) == len(agencies))
        if agencies_no_ranked and not all_no_ranked:
            ranked_missing = ", ".join([normalize_agency_for_subject(a) for a in agencies_no_ranked])
            summary_missing = ", ".join([normalize_agency_for_subject(a) for a in agencies_no_summary]) if agencies_no_summary else "None"

            subject = f"DNS Service Bypass Weekly Report - NO DATA (start_date={start_date})"
            body = (
                "NO DATA DETAILS:\n"
                f"Agencies missing ranked-traffic CSV attachments for start_date={start_date}:\n"
                f"{ranked_missing}\n\n"
                "SUMMARY NO DATA DETAILS:\n"
                f"Agencies missing summary TXT for start_date={start_date}:\n"
                f"{summary_missing}\n"
            )
            body += EMAIL_FOOTER.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

            send_email_with_failover(
                subject=subject,
                body=body,
                to_addr=MAIL_FROM,
                bcc_addrs=DEFAULT_EMAIL_TO_LIST,
                attachments=[]
            )

            print(
                f"[EMAIL SENT] type=NO_DATA_DEFAULT_LIST | to={MAIL_FROM} | "
                f"bcc={','.join(DEFAULT_EMAIL_TO_LIST)} | start_date={start_date} | "
                f"missing_ranked_count={len(agencies_no_ranked)} | missing_summary_count={len(agencies_no_summary)}"
            )

    return {
        "status": "ok",
        "start_date": start_date,
        "test_mode": TEST_MODE,
        "report_emails_sent": sent_reports,
        "agency_no_data_emails_sent": sent_agency_no_data,
        "agencies_total": len(agencies),
        "agencies_no_ranked": len(agencies_no_ranked),
        "agencies_no_summary": len(agencies_no_summary),
    }
