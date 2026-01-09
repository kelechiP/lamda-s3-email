import boto3
import csv
import io
import os
import json
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from typing import List, Dict, Any
from collections import defaultdict

s3 = boto3.client("s3")

BUCKET = "bucketname"
BASE_PREFIX = "dns-bypass-analytic/stat=reports/substat=ranked-traffic/"
CADENCE_PREFIX = "cadence=week/"
SEP = "=" * 20

# =========================
# SMTP config (ENV VARS)
# =========================
SMTP_HOST_1 = os.getenv("SMTP_HOST_1", "").strip()
SMTP_HOST_2 = os.getenv("SMTP_HOST_2", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
MAIL_FROM = os.getenv("MAIL_FROM", "")
SMTP_MODE = os.getenv("SMTP_MODE", "starttls").lower()  # starttls | ssl | plain

AGENCY_EMAIL_MAP: Dict[str, List[str]] = json.loads(os.getenv("AGENCY_EMAIL_MAP", "{}"))
DEFAULT_EMAIL_TO = os.getenv("DEFAULT_EMAIL_TO", "").strip()


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
    if today.weekday() == 0:  # Monday UTC
        return monday_two_weeks_ago(today)

    return exact_days_ago(today, 14)


def lambda_handler(event, context):
    start_date = resolve_start_date(event or {})
    print(f"[INFO] Using start_date={start_date}")

    agencies = list_child_prefixes(BASE_PREFIX)
    print(f"[INFO] Found agencies={len(agencies)}")

    agency_reports = defaultdict(list)

    for agency_prefix in agencies:
        agency_name = agency_prefix.rstrip("/").split("/")[-1]  # e.g., agency=wholesales
        print(f"[INFO] Agency={agency_name}")

        bypass_prefixes = list_child_prefixes(agency_prefix)
        print(f"[INFO]  bypass_prefixes={len(bypass_prefixes)}")

        for bypass_prefix in bypass_prefixes:
            bypass_name = bypass_prefix[len(agency_prefix):].rstrip("/")
            ipv_prefixes = list_child_prefixes(bypass_prefix)

            for ipv_prefix in ipv_prefixes:
                ipv_name = ipv_prefix[len(bypass_prefix):].rstrip("/")
                ip_field_prefixes = list_child_prefixes(ipv_prefix)

                for ipf_prefix in ip_field_prefixes:
                    ipf_name = ipf_prefix[len(ipv_prefix):].rstrip("/")

                    target_prefix = f"{ipf_prefix}{CADENCE_PREFIX}start_date={start_date}/"
                    print(f"[DEBUG] Checking: {target_prefix}")

                    rows = read_csvs(target_prefix)

                    if rows:
                        csv_text = "\n".join(rows).replace("\r\n", "\n").replace("\r", "\n").strip()
                        header = "\n" + f"Report for {bypass_name} {ipv_name} {ipf_name}" + "\n"
                        report_block = header + f"{SEP}\n" + csv_text + f"\n{SEP}\n"
                        agency_reports[agency_name].append(report_block)

    # Send one email per agency
    if not agency_reports:
        msg = (
            f"No CSV data found for start_date={start_date} under {BASE_PREFIX}\n"
            f"Tip: verify the folder exists exactly: .../{CADENCE_PREFIX}start_date={start_date}/"
        )
        # If you want a fallback alert email:
        if DEFAULT_EMAIL_TO:
            send_email_with_failover(
                subject=f"DNS Bypass Weekly Report (start_date={start_date}) - NO DATA",
                body=msg,
                to_addrs=[DEFAULT_EMAIL_TO],
            )
        else:
            print("[WARN] No data and no DEFAULT_EMAIL_TO configured.")
        return {"status": "no_data", "start_date": start_date}

    sent = 0
    skipped = 0

    for agency_name, blocks in agency_reports.items():
        to_addrs = AGENCY_EMAIL_MAP.get(agency_name, [])
        if not to_addrs and DEFAULT_EMAIL_TO:
            to_addrs = [DEFAULT_EMAIL_TO]

        if not to_addrs:
            print(f"[WARN] No recipients configured for {agency_name} and no DEFAULT_EMAIL_TO. Skipping.")
            skipped += 1
            continue

        body = (
            f"DNS Bypass Weekly Report {start_date} {agency_name}\n\n"
            + "\n".join(blocks)
        )

        send_email_with_failover(
            subject=f"DNS Bypass Weekly Report - {agency_name} (start_date={start_date})",
            body=body,
            to_addrs=to_addrs,
        )
        sent += 1

    return {
        "status": "ok",
        "start_date": start_date,
        "agencies_emailed": sent,
        "agencies_skipped_no_recipients": skipped,
    }


def list_child_prefixes(parent_prefix: str) -> List[str]:
    prefixes: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=parent_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])
    return prefixes


def read_csvs(prefix: str) -> List[str]:
    paginator = s3.get_paginator("list_objects_v2")
    output: List[str] = []
    found_any = False

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            found_any = True
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                print(f"[INFO] Found CSV: {key}")
                body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8", errors="replace")
                reader = csv.reader(io.StringIO(body))
                for row in reader:
                    output.append(", ".join(row))

    if not output:
        if not found_any:
            print(f"[DEBUG] No objects under prefix: {prefix}")
        else:
            print(f"[DEBUG] Objects exist but no .csv matched under prefix: {prefix}")

    return output


# =========================
# SMTP sending with failover
# =========================
def send_email_with_failover(subject: str, body: str, to_addrs: List[str]):
    if not MAIL_FROM:
        raise ValueError("MAIL_FROM env var is required for SMTP email sending.")
    if not SMTP_HOST_1 and not SMTP_HOST_2:
        raise ValueError("Set SMTP_HOST_1 and/or SMTP_HOST_2 env vars.")

    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = ", ".join(to_addrs)
    msg["Subject"] = subject
    msg.set_content(body)

    errors = []
    for host in [SMTP_HOST_1, SMTP_HOST_2]:
        if not host:
            continue
        try:
            _send_via_host(host, msg, to_addrs)
            print(f"[INFO] Email sent via {host} to {to_addrs}")
            return
        except Exception as e:
            err = f"{host} failed: {repr(e)}"
            print(f"[ERROR] {err}")
            errors.append(err)

    raise RuntimeError("All SMTP hosts failed. " + " | ".join(errors))


def _send_via_host(host: str, msg: EmailMessage, to_addrs: List[str]):
    if SMTP_MODE == "ssl":
        with smtplib.SMTP_SSL(host, SMTP_PORT, timeout=20) as server:
            server.ehlo()
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg, from_addr=MAIL_FROM, to_addrs=to_addrs)

    else:
        with smtplib.SMTP(host, SMTP_PORT, timeout=20) as server:
            server.ehlo()
            if SMTP_MODE == "starttls":
                server.starttls()
                server.ehlo()
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg, from_addr=MAIL_FROM, to_addrs=to_addrs)
