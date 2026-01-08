import boto3
import csv
import io
from datetime import datetime, timedelta

s3 = boto3.client("s3")
sns = boto3.client("sns")

BUCKET = "bucketname"
BASE_PREFIX = "folder1/folder2/folder3/"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:weekly-report"

FOLDER_PATTERNS = [
    
]

def lambda_handler(event, context):
    report = []
    today = datetime.utcnow()
    start_date = (today - timedelta(days=14)).strftime("%Y-%m-%d")

    agencies = list_agency_folders()

    for agency in agencies:
        for folder in FOLDER_PATTERNS:
            prefix = (
                f"{BASE_PREFIX}{agency}/"
                f"{folder}cadence-week/start_date={start_date}/"
            )
            rows = read_csvs(prefix)

            if rows:
                section = f"\n===== {agency} | {folder} =====\n"
                section += "\n".join(rows)
                report.append(section)

    if report:
        publish_sns("\n\n".join(report))
    else:
        publish_sns("No CSV data found for 2 weeks ago.")

def list_agency_folders():
    paginator = s3.get_paginator("list_objects_v2")
    agencies = []

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=BASE_PREFIX,
        Delimiter="/"
    ):
        for prefix in page.get("CommonPrefixes", []):
            agencies.append(prefix["Prefix"].split("/")[-2])

    return agencies

def read_csvs(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    output = []

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                response = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                body = response["Body"].read().decode("utf-8")
                reader = csv.reader(io.StringIO(body))
                for row in reader:
                    output.append(", ".join(row))

    return output

def publish_sns(message):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Weekly DN/DH/DT Report (2 Weeks Ago)",
        Message=message[:260000]  # SNS limit safety
    )
