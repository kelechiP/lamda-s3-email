# =========================
# FULL CORRECTED EMAIL LOGIC
# (drop-in section after S3 scanning is complete)
# Ensures: when NO_DATA_ALL fires, NO agency-specific NO DATA emails are sent.
# =========================

sent_reports = 0
sent_agency_no_data = 0

# -------------------------
# System outage / NO_DATA_ALL detection:
# Condition: NO ranked CSVs anywhere AND NO summary TXT anywhere
# -------------------------
all_no_ranked = (len(agencies) > 0 and len(agencies_no_ranked) == len(agencies))
all_no_summary = (len(agencies) > 0 and len(agencies_no_summary) == len(agencies))
no_data_all_case = all_no_ranked and all_no_summary

# -------------------------
# 3) If nothing found across ALL agencies (no ranked CSVs anywhere AND no summary TXT anywhere),
#    send ONLY the NO_DATA_ALL email to default and STOP (suppress all other NO DATA emails).
# -------------------------
if no_data_all_case:
    if DEFAULT_EMAIL_TO_LIST:
        subject = f"DNS Service Bypass Weekly Report - NO DATA (ALL AGENCIES) start_date={start_date}"
        body = (
            "NO DATA DETAILS (SYSTEM OUTAGE):\n"
            f"No ranked-traffic CSV and no summary TXT were found for ANY agency for start_date={start_date}.\n\n"
            "Paths checked:\n"
            f"- Ranked (attachments): s3://{BUCKET}/{BASE_PREFIX_RANKED}\n"
            f"- Summary (body):      s3://{BUCKET}/{BASE_PREFIX_SUMMARY}\n"
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
            f"[EMAIL SENT] type=NO_DATA_ALL | to={MAIL_FROM} | "
            f"bcc={','.join(DEFAULT_EMAIL_TO_LIST)} | start_date={start_date}"
        )
    else:
        print("[WARN] NO_DATA_ALL detected but DEFAULT_EMAIL_TO is empty; cannot notify default recipients.")

    # ✅ Critical: suppress all other emails (treat as outage)
    return {
        "status": "no_data_all",
        "start_date": start_date,
        "test_mode": TEST_MODE,
        "agencies_total": len(agencies),
        "agencies_no_ranked": len(agencies_no_ranked),
        "agencies_no_summary": len(agencies_no_summary),
        "note": "NO_DATA_ALL fired; suppressed report emails and all agency-specific NO DATA emails."
    }

# -------------------------
# 1) REPORT emails (per agency):
#    Send only for agencies that HAVE ranked CSV attachments.
#    Body = SUMMARY REPORTS (summary TXT content if present) + footer
# -------------------------
for agency_folder, attachments in agency_attachments.items():
    if not attachments:
        continue

    bcc_list = get_report_recipients_for_agency(agency_folder, active_map)
    if not bcc_list:
        print(f"[WARN] No recipients for {agency_folder} and DEFAULT_EMAIL_TO is empty. Skipping REPORT email.")
        continue

    agency_short = normalize_agency_for_subject(agency_folder)
    subject = f"DNS Service Bypass Weekly Report {agency_short}"

    summary_text = agency_summary_text.get(agency_folder, "").strip()
    body = ""
    if summary_text:
        body += "SUMMARY REPORTS (from substat=summary):\n" + summary_text + "\n"
    body += EMAIL_FOOTER.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

    send_email_with_failover(
        subject=subject,
        body=body,
        to_addr=MAIL_FROM,
        bcc_addrs=bcc_list,
        attachments=attachments
    )

    print(
        f"[EMAIL SENT] type=REPORT | agency={agency_folder} | to={MAIL_FROM} | "
        f"bcc={','.join(bcc_list)} | attachments={len(attachments)} | start_date={start_date}"
    )
    sent_reports += 1

# -------------------------
# 2) AGENCY "NO DATA" emails (per agency):
#    Condition:
#      - agency has an explicit DL configured
#      - NO ranked CSV attachments for that agency for the week
#    Body rules:
#      - No ranked + no summary -> "There is no DNS bypass traffic report data for this week and no summary report."
#      - No ranked + summary exists -> same intent, include summary TXT in body
# -------------------------
for agency_folder in agencies_no_ranked:
    explicit_dl = get_explicit_dl_for_agency(agency_folder, active_map)
    if not explicit_dl:
        # No explicit DL -> do not send agency-specific NO DATA email
        continue

    agency_short = normalize_agency_for_subject(agency_folder)
    subject = f"DNS Service Bypass Weekly Report - NO DATA {agency_short}"

    summary_text = agency_summary_text.get(agency_folder, "").strip()
    if summary_text:
        body = (
            "There is no DNS bypass traffic report data for this week. "
            "A summary report is included below.\n\n"
            "SUMMARY REPORTS (from substat=summary):\n"
            f"{summary_text}\n"
        )
    else:
        body = "There is no DNS bypass traffic report data for this week and no summary report.\n"

    body += EMAIL_FOOTER.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

    send_email_with_failover(
        subject=subject,
        body=body,
        to_addr=MAIL_FROM,
        bcc_addrs=explicit_dl,
        attachments=[]
    )

    print(
        f"[EMAIL SENT] type=NO_DATA_AGENCY | agency={agency_folder} | to={MAIL_FROM} | "
        f"bcc={','.join(explicit_dl)} | start_date={start_date} | summary_included={bool(summary_text)}"
    )
    sent_agency_no_data += 1

# -------------------------
# OPTIONAL: DEFAULT "NO DATA LIST" email (to default recipients)
# - Only if at least one agency is missing ranked CSV (partial missing)
# - Includes missing ranked agencies AND missing summary agencies
# -------------------------
sent_default_missing_list = 0

if DEFAULT_EMAIL_TO_LIST and agencies_no_ranked:
    ranked_missing = ", ".join([normalize_agency_for_subject(a) for a in agencies_no_ranked])
    summary_missing = ", ".join([normalize_agency_for_subject(a) for a in agencies_no_summary]) if agencies_no_summary else "None"

    subject = f"DNS Service Bypass Weekly Report - NO DATA (start_date={start_date})"
    body = (
        "NO DATA DETAILS:\n"
        f"Agencies missing ranked-traffic CSV attachments for start_date={start_date}:\n"
        f"{ranked_missing}\n\n"
        "SUMMARY NO DATA DETAILS:\n"
        f"Agencies missing summary TXT for start_date={start_date}:\n"
        f"{summary_missing}\n"
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
        f"[EMAIL SENT] type=NO_DATA_DEFAULT_LIST | to={MAIL_FROM} | "
        f"bcc={','.join(DEFAULT_EMAIL_TO_LIST)} | start_date={start_date} | "
        f"missing_ranked_count={len(agencies_no_ranked)} | missing_summary_count={len(agencies_no_summary)}"
    )
    sent_default_missing_list = 1

return {
    "status": "ok",
    "start_date": start_date,
    "test_mode": TEST_MODE,
    "report_emails_sent": sent_reports,
    "agency_no_data_emails_sent": sent_agency_no_data,
    "default_missing_list_sent": sent_default_missing_list,
    "agencies_total": len(agencies),
    "agencies_no_ranked": len(agencies_no_ranked),
    "agencies_no_summary": len(agencies_no_summary),
}
