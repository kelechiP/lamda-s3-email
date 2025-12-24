import boto3
import urllib.parse
import os

s3 = boto3.client("s3")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

SNS_MAX_BYTES = 262144
BUFFER = 1024
MAX_MESSAGE_BYTES = SNS_MAX_BYTES - BUFFER


def lambda_handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")

    subject = key.split("/")[-1][:100]

    header = (
        f"S3 Bucket : {bucket}\n"
        f"S3 Object : {key}\n"
        f"S3 URI    : s3://{bucket}/{key}\n"
        "--------------------------------------------\n\n"
    )

    message = header + content
    encoded = message.encode("utf-8")

    if len(encoded) > MAX_MESSAGE_BYTES:
        message = encoded[:MAX_MESSAGE_BYTES].decode("utf-8", errors="ignore")
        message += "\n\n[Content truncated due to SNS size limit]"

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

    return {"status": "Email sent"}
