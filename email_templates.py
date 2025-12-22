import json
import boto3
import urllib.parse

s3 = boto3.client("s3")
ses = boto3.client("ses")

SENDER_EMAIL = "sender@example.com"     # Must be SES verified
RECIPIENTS = ["user1@example.com", "user2@example.com"]

def lambda_handler(event, context):
    try:
        # Get S3 info
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"]
        )

        # Read file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response["Body"].read().decode("utf-8")

        # Build email subject dynamically
        subject = build_subject(file_content, object_key)

        # Send email
        send_email(subject, file_content)

        return {
            "statusCode": 200,
            "body": json.dumps("Email sent successfully")
        }

    except Exception as e:
        print("ERROR:", str(e))
        raise e


def build_subject(content, filename):
    """
    Customize subject based on file content
    """

    first_line = content.splitlines()[0] if content else "New Notification"

    if "URGENT" in content.upper():
        return "🚨 URGENT ALERT"
    elif "INFO" in content.upper():
        return "ℹ️ Information Update"
    elif first_line:
        return first_line[:100]   # SES subject length safety
    else:
        return f"New File Uploaded: {filename}"


def send_email(subject, body):
    ses.send_email(
        Source=SENDER_EMAIL,
        Destination={
            "ToAddresses": RECIPIENTS
        },
        Message={
            "Subject": {
                "Data": subject,
                "Charset": "UTF-8"
            },
            "Body": {
                "Text": {
                    "Data": body,
                    "Charset": "UTF-8"
                }
            }
        }
    )
