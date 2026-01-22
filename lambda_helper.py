# -------------------------
# System outage detection:
# NO ranked CSVs anywhere AND NO summary TXT anywhere
# -------------------------
all_no_ranked = (len(agencies) > 0 and len(agencies_no_ranked) == len(agencies))
all_no_summary = (len(agencies) > 0 and len(agencies_no_summary) == len(agencies))
system_outage = all_no_ranked and all_no_summary

if system_outage:
    if DEFAULT_EMAIL_TO_LIST:
        subject = f"DNS Service Bypass Weekly Report - NO DATA (ALL AGENCIES) start_date={start_date}"
        body = (
            "NO DATA DETAILS (SYSTEM OUTAGE):\n"
            f"No ranked-traffic CSV and no summary TXT were found for ANY agency for start_date={start_date}.\n\n"
            f"Ranked path checked:\n"
            f"s3://{BUCKET}/{BASE_PREFIX_RANKED}\n\n"
            f"Summary path checked:\n"
            f"s3://{BUCKET}/{BASE_PREFIX_SUMMARY}\n"
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
            f"[EMAIL SENT] type=NO_DATA_ALL_OUTAGE | to={MAIL_FROM} | "
            f"bcc={','.join(DEFAULT_EMAIL_TO_LIST)} | start_date={start_date}"
        )
    else:
        print("[WARN] System outage detected but DEFAULT_EMAIL_TO is empty; cannot notify default recipients.")

    # ✅ Suppress all other emails (treat as outage)
    return {
        "status": "no_data_all_outage",
        "start_date": start_date,
        "test_mode": TEST_MODE,
        "agencies_total": len(agencies),
        "agencies_no_ranked": len(agencies_no_ranked),
        "agencies_no_summary": len(agencies_no_summary),
        "note": "Suppressed all agency-specific NO DATA emails due to system outage condition."
    }
