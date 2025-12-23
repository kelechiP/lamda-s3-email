import json
import boto3
import urllib.parse

s3 = boto3.client("s3")
sns = boto3.client("sns")

SNS_TOPIC_ARN = "arn:aws:sns:us-gov-west-1:123456789012:s3-file-email-topic"

SNS_MAX_BYTES = 262144          # 256 KB
SAFETY_BUFFER = 1024            # leave room for headers
MAX_MESSAGE_BYTES = SNS_MAX_BYTES - SAFETY_BUFFER


def lambda_handler(event, context):
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"]
        )

        # Read file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")

        subject = build_subject(key)
        message = build_sns_message(content, bucket, key)

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )

        return {
            "statusCode": 200,
            "body": "SNS message published successfully"
        }

    except Exception as e:
        print("ERROR:", str(e))
        raise e


def build_subject(object_key):
    """
    SNS subject max length is 100 characters
    """
    filename = object_key.split("/")[-1]
    return filename[:100]


def build_sns_message(content, bucket, key):
    """
    Ensure SNS message does not exceed size limits
    """
    content_bytes = content.encode("utf-8")

    if len(content_bytes) <= MAX_MESSAGE_BYTES:
        return content

    truncated_content = content_bytes[:MAX_MESSAGE_BYTES].decode(
        "utf-8", errors="ignore"
    )

    return (
        "MESSAGE TRUNCATED DUE TO SNS SIZE LIMIT\n"
        "-------------------------------------\n"
        f"S3 Location: s3://{bucket}/{key}\n\n"
        f"{truncated_content}\n\n"
        "[Truncated]"
    )
