def format_report_email_body_v2(
    summary_items: List[Dict[str, str]],
    attachments: List[Tuple[str, bytes]],
) -> str:
    """
    Formats REPORT email body to match requested layout:

    Dear ma/sir
    Greetings
    BODY_1

    DNS Traffic by Destination
    BODY_2

    <DIPS SUMMARY TXT CONTENT>
    Attached CSV files <DIPS CSV NAMES ONLY> <BODY_3>

    BODY_4

    DNS Traffic by Source
    BODY_5

    <SIPS SUMMARY TXT CONTENT>
    Attached CSV files <SIPS CSV NAMES ONLY> <BODY_3>

    Footer appended by caller
    """

    def contains(name: str, token: str) -> bool:
        return token.lower() in (name or "").lower()

    # --- Summary TXT content split (content only, no labels) ---
    dips_txt_contents = [
        it.get("content", "").strip()
        for it in summary_items
        if contains(it.get("label", ""), "DIPS") and it.get("content", "").strip()
    ]
    sips_txt_contents = [
        it.get("content", "").strip()
        for it in summary_items
        if contains(it.get("label", ""), "SIPS") and it.get("content", "").strip()
    ]

    dips_summary = "\n\n".join(dips_txt_contents).strip()
    sips_summary = "\n\n".join(sips_txt_contents).strip()

    # --- CSV filenames split (for listing only; attachments remain ALL csv) ---
    csv_names = [fn for (fn, _data) in attachments]
    dips_csv = sorted([fn for fn in csv_names if contains(fn, "DIPS")])
    sips_csv = sorted([fn for fn in csv_names if contains(fn, "SIPS")])

    dips_csv_list = ", ".join(dips_csv)
    sips_csv_list = ", ".join(sips_csv)

    # --- "Bold header" in plain text: use markdown-ish emphasis or caps ---
    hdr_dest = "**DNS Traffic by Destination**"
    hdr_src = "**DNS Traffic by Source**"

    body_parts: List[str] = []

    # Greeting block
    body_parts.append("Dear ma/sir")
    body_parts.append("")
    body_parts.append("Greetings")
    body_parts.append("")
    if EMAIL_BODY_1:
        body_parts.append(EMAIL_BODY_1)
    body_parts.append("")  # corresponds to \n\n\n effect when joined with blank lines
    body_parts.append("")

    # Destination block header + body2
    body_parts.append(hdr_dest)
    body_parts.append("")
    if EMAIL_BODY_2:
        body_parts.append(EMAIL_BODY_2)
    body_parts.append("")

    # DIPS section: summary + dips csv list + body3
    if dips_summary:
        body_parts.append(dips_summary)
    else:
        body_parts.append("(No DIPS summary report found.)")

    # "Attached CSV files ..." line (DIPS only)
    dips_line = "Attached CSV files"
    if dips_csv_list:
        dips_line += f"  {dips_csv_list}"
    else:
        dips_line += "  (No DIPS CSV attachments found.)"
    if EMAIL_BODY_3:
        dips_line += f"  {EMAIL_BODY_3}"
    body_parts.append(dips_line)
    body_parts.append("")

    # Body4 block
    if EMAIL_BODY_4:
        body_parts.append(EMAIL_BODY_4)
    body_parts.append("")
    body_parts.append("")  # \n\n\n effect
    body_parts.append("")

    # Source block header + body5
    body_parts.append(hdr_src)
    body_parts.append("")
    if EMAIL_BODY_5:
        body_parts.append(EMAIL_BODY_5)
    body_parts.append("")

    # SIPS section: summary + sips csv list + body3
    if sips_summary:
        body_parts.append(sips_summary)
    else:
        body_parts.append("(No SIPS summary report found.)")

    sips_line = "Attached CSV files"
    if sips_csv_list:
        sips_line += f"  {sips_csv_list}"
    else:
        sips_line += "  (No SIPS CSV attachments found.)"
    if EMAIL_BODY_3:
        sips_line += f"  {EMAIL_BODY_3}"
    body_parts.append(sips_line)
    body_parts.append("")

    return "\n".join(body_parts).rstrip() + "\n"
