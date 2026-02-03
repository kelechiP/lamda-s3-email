def format_report_email_body_final(
    summary_items: List[Dict[str, str]],
    attachments: List[Tuple[str, bytes]],
) -> str:
    """
    Email format:

    Dear ma/sir

    Greetings

    <EMAIL_BODY_1>\n\n\n

    <DNS Traffic by Destination> (bold-ish header) \n\n

    <EMAIL_BODY_2>\n\n

    <DIPS summary txt content>
    Attached CSV files <DIPS csv list> <CUSTOM_BODY_3>\n\n

    <CUSTOM_BODY_4>\n\n\n

    <DNS Traffic by Source> (bold-ish header) \n\n
    <CUSTOM_BODY_5>\n\n
    <SIPS summary txt content>\n\n
    Attached CSV files <SIPS csv list> <CUSTOM_BODY_6>\n\n

    <footer appended by caller>
    """

    def contains(name: str, token: str) -> bool:
        return token.lower() in (name or "").lower()

    # --- Split summary TXT by DIPS/SIPS (content only) ---
    dips_txt_blocks = [
        it.get("content", "").strip()
        for it in summary_items
        if contains(it.get("label", ""), "DIPS") and it.get("content", "").strip()
    ]
    sips_txt_blocks = [
        it.get("content", "").strip()
        for it in summary_items
        if contains(it.get("label", ""), "SIPS") and it.get("content", "").strip()
    ]

    dips_summary_text = "\n\n".join(dips_txt_blocks).strip()
    sips_summary_text = "\n\n".join(sips_txt_blocks).strip()

    # --- Split attachment filenames by DIPS/SIPS ---
    dips_csv_names = sorted([fn for (fn, _data) in attachments if contains(fn, "DIPS")])
    sips_csv_names = sorted([fn for (fn, _data) in attachments if contains(fn, "SIPS")])

    dips_csv_list = ", ".join(dips_csv_names)
    sips_csv_list = ", ".join(sips_csv_names)

    # ----------------------------
    # Hardcoded body parts (to avoid env var size limits)
    # ----------------------------

    # “Bold” in plain text emails: most clients won’t truly bold, so we use emphasis markers.
    # If you want HTML email for real bold, tell me and I’ll convert safely.
    HDR_DEST = "**DNS Traffic by Destination**"
    HDR_SRC  = "**DNS Traffic by Source**"

    CUSTOM_BODY_3 = (
        "Please review the attached destination-based DIPS CSV files for deeper breakdowns.\n"
        "These files provide detailed DNS bypass traffic insights by destination.\n"
        "Use the data to identify top destinations and unusual patterns.\n"
        "If you see unexpected changes, compare against previous weeks.\n"
        "This section focuses on destination traffic only.\n"
        "For source-based insights, refer to the next section.\n"
        "If data seems missing, confirm cadence/start_date alignment in S3.\n"
        "Thank you."
    )

    CUSTOM_BODY_4 = (
        "Note: Destination metrics may fluctuate due to traffic routing changes, reporting delays, "
        "or upstream DNS policy updates."
    )

    CUSTOM_BODY_5 = (
        "Below is the source-based summary for the same reporting period.\n"
        "This section focuses on SIPS and highlights DNS bypass traffic by source.\n"
        "Use it to identify source-side anomalies and trends."
    )

    CUSTOM_BODY_6 = (
        "Please review the attached source-based SIPS CSV files for full details.\n"
        "If you have questions, reply to this email and we will follow up."
    )

    # ----------------------------
    # Compose body exactly in the requested order
    # ----------------------------
    body = (
        "Dear ma/sir\n\n"
        "Greetings\n\n"
    )

    if EMAIL_BODY_1:
        body += EMAIL_BODY_1 + "\n\n\n"
    else:
        body += "\n\n\n"  # keep spacing consistent even if blank

    body += f"{HDR_DEST}\n\n"

    if EMAIL_BODY_2:
        body += EMAIL_BODY_2 + "\n\n"
    else:
        body += "\n\n"

    # DIPS section
    if dips_summary_text:
        body += dips_summary_text + "\n"
    else:
        body += "(No DIPS summary report content found for this week.)\n"

    body += "Attached CSV files "
    body += (dips_csv_list if dips_csv_list else "(No DIPS CSV attachments found)") 
    body += "  " + CUSTOM_BODY_3 + "\n\n"

    body += CUSTOM_BODY_4 + "\n\n\n"

    # SIPS section
    body += f"{HDR_SRC}\n\n"
    body += CUSTOM_BODY_5 + "\n\n"

    if sips_summary_text:
        body += sips_summary_text + "\n\n"
    else:
        body += "(No SIPS summary report content found for this week.)\n\n"

    body += "Attached CSV files "
    body += (sips_csv_list if sips_csv_list else "(No SIPS CSV attachments found)")
    body += "  " + CUSTOM_BODY_6 + "\n\n"

    return body.strip() + "\n"
