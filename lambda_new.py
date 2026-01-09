import boto3
import csv
import io
from datetime import datetime, timedelta
from typing import List, Dict, Any

# =========================
# AWS clients
# =========================
s3 = boto3.client("s3")
sns = boto3.client("sns")

# =========================
# Config
# =========================
BUCKET = "bucketname"
BASE_PREFIX = "dns-bypass-analytic/stat=reports/substat=ranked-traffic/"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:weekly-report"

CADENCE_PREFIX = "cadence=week/"
SNS_MAX = 260000

# ===== MODIFIED: consistent separator line used in SNS body formatting
SEP = "=" * 29

# NOTE:
# We no longer hardcode the full deep patterns (because actual structure has additional levels).
# We discover dynamically: agency -> bypass-* -> ipv=* -> ip_field=* -> cadence=week/start_date=...
# This supports agencies that have only 1-3 bypass folders and variable ipv/ip_field coverage.


# =========================
# Date resolution
# =========================
def monday_two_weeks_ago(today: datetime) -> str:
    monday_this_week = today - timedelta(days=today.weekday())
    return (monday_this_week - timedelta(days=14)).strftime("%Y-%m-%d")


def exact_days_ago(today: datetime, days: int = 14) -> str:
    return (today - timedelta(days=days)).strftime("%Y-%m-%d")


# ===== MODIFIED: supports (a) Monday scheduled cadence, (b) manual exact 14 days,
# and optional overrides {start_date}, {mode:weekly}, {days_ago}
def resolve_start_date(event: Dict[str, Any]) -> str:
    today = datetime.utcnow()
    event = event or {}

    if event.get("start_date"):
        return event["start_date"]

    if event.get("mode") == "weekly":
        return monday_two_weeks_ago(today)

    if event.get("days_ago") is not None:
        return exact_days_ago(today, int(event["days_ago"]))

    # default behavior:
    # - Monday UTC: treat as weekly cadence run (two weeks prior Monday)
    # - Any other day: treat as manual run -> exactly 14 days ago
    if today.weekday() == 0:
        return monday_two_weeks_ago(today)

    return exact_days_ago(today, 14)


# =========================
# Lambda Handler
# =========================
def lambda_handler(event, context):
    start_date = resolve_start_date(event or {})
    print(f"[INFO] Using start_date={start_date}")
    print(f"[INFO] BASE_PREFIX={BASE_PREFIX}")

    agencies = list_child_prefixes(BASE_PREFIX)  # returns full prefixes like .../agency=wholesales/
    print(f"[INFO] Found agencies={len(agencies)}")

    # ===== MODIFIED: group report blocks by agency so we publish one SNS per agency
    agency_reports: Dict[str, List[str]] = {}

    for agency_prefix in agencies:
        agency_name = agency_prefix.rstrip("/").split("/")[-1]  # e.g. agency=wholesales
        print(f"[INFO] Agency={agency_name}")

        # Ensure entry exists
        agency_reports.setdefault(agency_name, [])

        # bypass folders under agency (e.g. bypass-DNS_53/, bypass-DoH/, bypass-DoT/)
        bypass_prefixes = list_child_prefixes(agency_prefix)
        print(f"[INFO]  bypass_prefixes={len(bypass_prefixes)}")

        for bypass_prefix in bypass_prefixes:
            bypass_name = bypass_prefix[len(agency_prefix):].rstrip("/")  # relative label (bypass-DNS_53)
            ipv_prefixes = list_child_prefixes(bypass_prefix)             # ipv=IPv4/, ipv=IPv6/

            for ipv_prefix in ipv_prefixes:
                ipv_name = ipv_prefix[len(bypass_prefix):].rstrip("/")    # ipv=IPv4
                ip_field_prefixes = list_child_prefixes(ipv_prefix)       # ip_field=DIPS/, ip_field=SIPS/

                for ipf_prefix in ip_field_prefixes:
                    ipf_name = ipf_prefix[len(ipv_prefix):].rstrip("/")   # ip_field=DIPS

                    target_prefix = f"{ipf_prefix}{CADENCE_PREFIX}start_date={start_date}/"
                    print(f"[DEBUG] Checking: {target_prefix}")

                    rows = read_csvs(target_prefix)

                    if rows:
                        # ===== MODIFIED: build SNS block in your requested format (no blank line before SEP)
                        csv_text = "\n".join(rows).replace("\r\n", "\n").replace("\r", "\n").strip()

                        report_block = (
                            f"Report for {bypass_name} {ipv_name} {ipf_name}\n"
                            f"{SEP}\n"
                            f"{csv_text}\n"
                            f"{SEP}\n"
                        )

                        agency_reports[agency_name].append(report_block)

    # Remove agencies with no data (optional but keeps output clean)
    agency_reports = {a: blocks for a, blocks in agency_reports.items() if blocks}

    if not agency_reports:
        publish_sns(
            subject=f"DNS Bypass Weekly Report (start_date={start_date})",
            message=f"No CSV data found for start_date={start_date} under {BASE_PREFIX}"
        )
        return {"status": "no_data", "start_date": start_date}

    # ===== MODIFIED: publish one SNS per agency; FIXED variable scoping (no undefined report_blocks)
    published = 0
    for agency_name, blocks in agency_reports.items():
        message = (
            f"DNS Bypass Weekly Report {start_date} {agency_name}\n\n"
            + "\n".join(blocks)
        )

        publish_sns_chunked(
            subject=f"DNS Bypass Weekly Report - {agency_name}",
            message=message
        )
        published += 1

    return {"status": "ok", "start_date": start_date, "agencies_published": published}


# =========================
# S3 Helpers
# =========================
def list_child_prefixes(parent_prefix: str) -> List[str]:
    """
    Lists immediate child prefixes under parent_prefix using Delimiter="/".
    Returns full prefixes, e.g.:
      parent: BASE_PREFIX
      -> [".../agency=wholesales/", ".../agency=foo/"]
    """
    prefixes: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=parent_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])

    return prefixes


def read_csvs(prefix: str) -> List[str]:
    """
    Reads all .csv under the prefix and returns CSV rows as strings.
    """
    paginator = s3.get_paginator("list_objects_v2")
    output: List[str] = []
    found_any_object = False

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            found_any_object = True
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                print(f"[INFO] Found CSV: {key}")
                body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8", errors="replace")
                reader = csv.reader(io.StringIO(body))
                for row in reader:
                    output.append(", ".join(row))

    if not output:
        if not found_any_object:
            print(f"[DEBUG] No objects under prefix: {prefix}")
        else:
            print(f"[DEBUG] Objects exist but no .csv matched under prefix: {prefix}")

    return output


# =========================
# SNS Helpers
# =========================
def publish_sns(subject: str, message: str):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message[:SNS_MAX]
    )


def publish_sns_chunked(subject: str, message: str):
    """
    Split into multiple SNS messages if payload is too large.
    """
    if len(message) <= SNS_MAX:
        publish_sns(subject, message)
        return

    parts: List[str] = []
    start = 0
    while start < len(message):
        end = min(start + SNS_MAX, len(message))
        parts.append(message[start:end])
        start = end

    for i, chunk in enumerate(parts, start=1):
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"{subject} (part {i}/{len(parts)})",
            Message=chunk
        )
