# 2) AGENCY "NO DATA" emails (per agency):
# Condition:
#   - agency has an explicit DL configured
#   - NO ranked CSV attachments for that agency for the week
# Behavior:
#   - If no summary: send minimal NO DATA email (no headers/sections)
#   - If summary exists: include it with same section headers as report emails (DIPS then SIPS)
for agency_folder in agencies_no_ranked:
    explicit_dl = get_explicit_dl_for_agency(agency_folder, active_map)
    if not explicit_dl:
        continue

    agency_short = normalize_agency_for_subject(agency_folder)
    subject = f"DNS {agency_short} Service Bypass Weekly Report - NO DATA {date_range}"

    summary_items = agency_summary_items.get(agency_folder, [])

    def contains(name: str, token: str) -> bool:
        return token.lower() in (name or "").lower()

    dips_txt_blocks = [
        (it.get("content") or "").strip()
        for it in summary_items
        if contains(it.get("label", ""), "DIPS") and (it.get("content") or "").strip()
    ]
    sips_txt_blocks = [
        (it.get("content") or "").strip()
        for it in summary_items
        if contains(it.get("label", ""), "SIPS") and (it.get("content") or "").strip()
    ]

    dips_summary_text = "\n\n".join(dips_txt_blocks).strip()
    sips_summary_text = "\n".join(sips_txt_blocks).strip()

    has_any_summary = bool(dips_summary_text or sips_summary_text)

    # ---- CASE A: no ranked + no summary ----
    if not has_any_summary:
        body = (
            "Dear ma/sir\n\n"
            "Greetings,\n\n"
        )

        # <CUSTOM_BODY_1>\n\n\n
        if EMAIL_BODY:
            body += EMAIL_BODY + "\n\n\n"
        else:
            body += "\n\n\n"

        body += "NO DNS TRAFFIC WAS OBSERVED FOR YOUR AGENCY DURING THIS REPORTING PERIOD.\n\n"

    # ---- CASE B: no ranked + summary exists ----
    else:
        HDR_DEST = "DNS Traffic by Destination"
        HDR_SRC = "DNS Traffic by Source"

        body = (
            "Dear ma/sir\n\n"
            "Greetings\n\n"
        )

        # <CUSTOM_BODY_1>\n\n\n
        if EMAIL_BODY:
            body += EMAIL_BODY + "\n\n\n"
        else:
            body += "\n\n\n"

        # Destination header + DIPS summary
        body += f"{HDR_DEST}\n\n"
        body += (dips_summary_text if dips_summary_text else "(No DIPS summary report content found for this week.)")
        body += "\n\n"

        # Source header + SIPS summary
        body += f"{HDR_SRC}\n\n"
        body += (sips_summary_text if sips_summary_text else "(No SIPS summary report content found for this week.)")
        body += "\n\n"

    # Footer always at end
    body += EMAIL_FOOTER.format(sender=MAIL_FROM, disclaimer=DISCLAIMER_TEXT)

    send_email_with_failover(
        subject=subject,
        body=body,
        to_addr="",               # ✅ BCC-only (shows only From)
        bcc_addrs=explicit_dl,
        attachments=[]
    )

    print(
        f"[EMAIL SENT] type=NO_DATA_AGENCY | agency={agency_folder} | to={MAIL_FROM} | "
        f"bcc={','.join(explicit_dl)} | start_date={start_date} | summary_included={has_any_summary}"
    )
    sent_agency_no_data += 1
