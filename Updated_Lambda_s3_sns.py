def build_sns_message(content, bucket, key):
    header = (
        f"S3 Bucket : {bucket}\n"
        f"S3 Object : {key}\n"
        f"S3 URI    : s3://{bucket}/{key}\n"
        "------------------------------------------------------------\n\n"
    )

    header_bytes = header.encode("utf-8")
    content_bytes = content.encode("utf-8")

    remaining_bytes = MAX_MESSAGE_BYTES - len(header_bytes)

    if remaining_bytes <= 0:
        return header[:MAX_MESSAGE_BYTES]

    if len(content_bytes) <= remaining_bytes:
        return header + content

    truncated_content = content_bytes[:remaining_bytes].decode(
        "utf-8", errors="ignore"
    )

    return (
        header
        + truncated_content
        + "\n\n[Content truncated due to SNS size limit]"
    )
