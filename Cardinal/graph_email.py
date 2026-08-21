"""Send the final ETL summary through Microsoft Graph."""

from __future__ import annotations

import html
from typing import Any
from urllib.parse import quote

import msal
import requests


def _summary_html(summary: dict[str, Any]) -> str:
    rows = []
    for result in summary["results"]:
        details = "; ".join(
            value
            for value in [
                result.get("file_action"),
                result.get("error"),
                *result.get("warnings", []),
            ]
            if value
        )
        rows.append(
            "<tr>"
            f"<td>{html.escape(result['loader'])}</td>"
            f"<td>{html.escape(result['target'])}</td>"
            f"<td>{html.escape(result['status'])}</td>"
            f"<td>{result.get('rows', 0):,}</td>"
            f"<td>{html.escape(details)}</td>"
            "</tr>"
        )
    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif">
      <h2>ETL run {html.escape(summary['status'])}</h2>
      <p><b>Run ID:</b> {html.escape(summary['run_id'])}<br>
         <b>Started:</b> {html.escape(summary['started_at'])}<br>
         <b>Finished:</b> {html.escape(summary['finished_at'])}</p>
      <table border="1" cellpadding="6" style="border-collapse:collapse">
        <tr><th>Loader</th><th>Target</th><th>Status</th><th>Rows</th><th>Details</th></tr>
        {''.join(rows)}
      </table>
    </body></html>
    """


def send_summary_email(email_config: dict[str, Any], summary: dict[str, Any]) -> None:
    """Send one app-only Graph email. Returns immediately when disabled."""

    if not email_config.get("enabled", False):
        return

    required = ["tenant_id", "client_id", "client_secret", "sender", "recipients"]
    missing = [name for name in required if not email_config.get(name)]
    if missing:
        raise ValueError(f"Email configuration is missing: {', '.join(missing)}")

    app = msal.ConfidentialClientApplication(
        client_id=email_config["client_id"],
        client_credential=email_config["client_secret"],
        authority=f"https://login.microsoftonline.com/{email_config['tenant_id']}",
    )
    token_result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    token = token_result.get("access_token")
    if not token:
        message = token_result.get("error_description") or token_result.get("error")
        raise RuntimeError(f"Could not obtain Microsoft Graph token: {message}")

    outcome = summary["status"]
    payload = {
        "message": {
            "subject": (
                f"{email_config.get('subject_prefix', 'ETL summary')} - "
                f"{outcome} - {summary['run_id']}"
            ),
            "body": {"contentType": "HTML", "content": _summary_html(summary)},
            "toRecipients": [
                {"emailAddress": {"address": address}}
                for address in email_config["recipients"]
            ],
            "ccRecipients": [
                {"emailAddress": {"address": address}}
                for address in email_config.get("cc", [])
            ],
        },
        "saveToSentItems": email_config.get("save_to_sent_items", False),
    }
    sender = quote(email_config["sender"], safe="")
    response = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=email_config.get("timeout_seconds", 30),
    )
    if response.status_code != 202:
        raise RuntimeError(
            f"Microsoft Graph sendMail returned HTTP {response.status_code}: "
            f"{response.text[:500]}"
        )
