def _contains_token(name: str, token: str) -> bool:
    return token.lower() in (name or "").lower()


def format_report_email_body(
    agency_folder: str,
    summary_items: List[Dict[str, str]],
    attachments: List[Tuple[str, bytes]],
    sep: str,
) -> str:
    """
    REPORT email body format:
      - Summary TXT (DIPS)
      - List attached CSV names containing DIPS
      - Summary TXT (SIPS)
      - List attached CSV names containing SIPS
      - Footer appended later by caller
    """

    # Split summary items by token
    dips_txt = [it for it in summary_items if _contains_token(it.get("label", ""), "DIPS")]
    sips_txt = [it for it in summary_items if _contains_token(it.get("label", ""), "SIPS")]

    # Split attachment filenames by token
    dips_csv_names = [fn for (fn, _data) in attachments if _contains_token(fn, "DIPS")]
    sips_csv_names = [fn for (fn, _data) in attachments if _contains_token(fn, "SIPS")]

    def render_txt(items: List[Dict[str, str]], title: str) -> str:
        if not items:
            return f"{title}\n{sep}\n(no matching summary txt files)\n{sep}\n"
        blocks = []
        for it in items:
            blocks.append(
                f"{title}\n"
                f"FILE: {it['label']}\n"
                f"{sep}\n"
                f"{it['content']}\n"
                f"{sep}\n"
            )
        return "\n".join(blocks).strip() + "\n"

    def render_csv_list(names: List[str], title: str) -> str:
        if not names:
            return f"{title}\n{sep}\n(no matching DIPS/SIPS csv attachments)\n{sep}\n"
        lines = "\n".join([f"- {n}" for n in sorted(names)])
        return f"{title}\n{sep}\n{lines}\n{sep}\n"

    body = ""
    body += render_txt(dips_txt, "SUMMARY REPORTS - DIPS")
    body += "\n"
    body += render_csv_list(dips_csv_names, "ATTACHED CSV FILES - DIPS")
    body += "\n"
    body += render_txt(sips_txt, "SUMMARY REPORTS - SIPS")
    body += "\n"
    body += render_csv_list(sips_csv_names, "ATTACHED CSV FILES - SIPS")

    return body.strip() + "\n"
