import boto3
import csv
import io
import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from collections import defaultdict

s3 = boto3.client("s3")
sns = boto3.client("sns")

BUCKET = "bucketname"
BASE_PREFIX = "dns-bypass-analytic/stat=reports/substat=ranked-traffic/"
CADENCE_PREFIX = "cadence=week/"
SNS_MAX = 260000
SEP = "=" * 20

# =========================
# MODIFIED: agency -> topic mapping
# Put this in Lambda environment variable AGENCY_TOPIC_MAP as JSON:
# {
#   "agency=wholesales": "arn:aws:sns:us-east-1:123456789012:topic-wholesales",
#   "agency=other":      "arn:aws:sns:us-east-1:123456789012:topic-other"
# }
# Optional DEFAULT_SNS_TOPIC_ARN env var for unmapped agencies.
# =========================
AGENCY_TOPIC_MAP: Dict[str, str] = json.loads(os.getenv("AGENCY_TOPIC_MAP", "{}"))
DEFAULT_SNS_TOPIC_ARN = os.getenv("DEFAULT_SNS_TOPIC_ARN", "")  # optional


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

    if today.weekday() == 0:  # Monday UTC
        return monday_two_weeks_ago(today)

    return exact_days_ago(today, 14)


def lambda_handler(event, context):
    start_date = resolve_start_date(event or {})
    print(f"[INFO] Using start_date={start_date}")

    agencies = list_child_prefixes(BASE_PREFIX)
    print(f"[INFO] Found agencies={len(agencies)}")

    # =========================
    # MODIFIED: initialize agency_reports (you were using it but not defining it)
    # =========================
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

    if not agency_reports:
        # =========================
        # MODIFIED: publish to default topic if configured, otherwise just log
        # =========================
        msg = (
            f"No CSV data found for start_date={start_date} under {BASE_PREFIX}\n"
            f"Tip: verify the folder exists exactly: .../{CADENCE_PREFIX}start_date={start_date}/"
        )
        if DEFAULT_SNS_TOPIC_ARN:
            publish_sns(
                topic_arn=DEFAULT_SNS_TOPIC_ARN,
                subject=f"DNS Bypass Weekly Report (start_date={start_date})",
                message=msg,
            )
        else:
            print("[WARN] No data and no DEFAULT_SNS_TOPIC_ARN set. Message:\n" + msg)

        return {"status": "no_data", "start_date": start_date}

    # =========================
    # MODIFIED: publish one SNS per agency to that agency's topic
    # =========================
    published = 0
    skipped = 0

    for agency_name, blocks in agency_reports.items():
        # Pick topic for this agency
        topic_arn = AGENCY_TOPIC_MAP.get(agency_name) or DEFAULT_SNS_TOPIC_ARN

        if not topic_arn:
            print(f"[WARN] No SNS topic configured for {agency_name} and no DEFAULT_SNS_TOPIC_ARN. Skipping publish.")
            skipped += 1
            continue

        message = (
            f"DNS Bypass Weekly Report {start_date} {agency_name}\n\n"
            + "\n".join(blocks)
        )

        publish_sns_chunked(
            topic_arn=topic_arn,
            subject=f"DNS Bypass Weekly Report - {agency_name}",
            message=message
        )
        published += 1

    return {
        "status": "ok",
        "start_date": start_date,
        "agencies_published": published,
        "agencies_skipped_no_topic": skipped,
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
# MODIFIED: publish functions accept topic_arn
# =========================
def publish_sns(topic_arn: str, subject: str, message: str):
    sns.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message[:SNS_MAX]
    )


def publish_sns_chunked(topic_arn: str, subject: str, message: str):
    if len(message) <= SNS_MAX:
        publish_sns(topic_arn, subject, message)
        return

    start = 0
    parts: List[str] = []
    while start < len(message):
        end = min(start + SNS_MAX, len(message))
        parts.append(message[start:end])
        start = end

    for i, chunk in enumerate(parts, start=1):
        sns.publish(
            TopicArn=topic_arn,
            Subject=f"{subject} (part {i}/{len(parts)})",
            Message=chunk
        )
