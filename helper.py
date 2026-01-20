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


---
# ===== SUMMARY: read TXT content into body =====
agency_prefix_summary = agency_prefix_ranked.replace(BASE_PREFIX_RANKED, BASE_PREFIX_SUMMARY, 1)

summary_sections: List[str] = []
bypass_prefixes_s = list_child_prefixes(BUCKET, agency_prefix_summary)
for bypass_prefix in bypass_prefixes_s:
    ipv_prefixes_s = list_child_prefixes(BUCKET, bypass_prefix)
    for ipv_prefix in ipv_prefixes_s:
        ip_field_prefixes_s = list_child_prefixes(BUCKET, ipv_prefix)
        for ipf_prefix in ip_field_prefixes_s:
            target_prefix_s = f"{ipf_prefix}{CADENCE_PREFIX}start_date={start_date}/"

            # ✅ now look for .txt files instead of .csv
            txt_keys_s = list_keys_with_suffix(BUCKET, target_prefix_s, ".txt")

            for txt_key in txt_keys_s:
                label = txt_key.replace(agency_prefix_summary, "").rstrip("/")
                txt_content = read_text_file(BUCKET, txt_key)
                if txt_content:
                    summary_sections.append(
                        f"\nSUMMARY TXT: {label}\n{SEP}\n{txt_content}\n{SEP}\n"
                    )

if summary_sections:
    agency_summary_text[agency_folder] = "\n".join(summary_sections).strip()
else:
    agency_summary_text[agency_folder] = ""
    agencies_no_summary.append(agency_folder)
