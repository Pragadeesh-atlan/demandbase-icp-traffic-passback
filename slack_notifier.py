import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

SLACK_POST_URL = "https://slack.com/api/chat.postMessage"


def notify_success(bot_token, channel_id, fetched, skipped, appended, sheet_id):
    """Send a success notification to Slack."""
    now = datetime.now(timezone.utc).strftime("%B %d, %Y — %H:%M UTC")
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "✅ Demandbase ICP Traffic Passback — Success"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Date:*\n{now}"},
                {"type": "mrkdwn", "text": f"*Fetched from HubSpot:*\n{fetched} leads"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Skipped (dupes):*\n{skipped}"},
                {"type": "mrkdwn", "text": f"*New rows appended:*\n{appended}"},
            ],
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"<{sheet_url}|📊 Google Sheet>",
                }
            ],
        },
    ]

    _send(bot_token, channel_id, f"✅ Demandbase ICP Traffic Passback: {appended} appended, {skipped} skipped", blocks)


def notify_no_leads(bot_token, channel_id, fetched, skipped):
    """Send a notification when no new leads were found."""
    now = datetime.now(timezone.utc).strftime("%B %d, %Y — %H:%M UTC")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "ℹ️ Demandbase ICP Traffic Passback — No New Leads"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Date:*\n{now}"},
                {"type": "mrkdwn", "text": f"*Fetched from HubSpot:*\n{fetched} leads"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Skipped (already in sheet):*\n{skipped}"},
                {"type": "mrkdwn", "text": "*New rows appended:*\n0"},
            ],
        },
    ]

    _send(bot_token, channel_id, f"ℹ️ Demandbase ICP Traffic Passback: No new leads today ({fetched} fetched, {skipped} dupes)", blocks)


def notify_error(bot_token, channel_id, error_message):
    """Send an error notification to Slack."""
    now = datetime.now(timezone.utc).strftime("%B %d, %Y — %H:%M UTC")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "❌ Demandbase ICP Traffic Passback — Error"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Date:* {now}\n\n*Error:*\n```{error_message}```",
            },
        },
    ]

    _send(bot_token, channel_id, f"❌ Demandbase ICP Traffic Passback Error: {error_message}", blocks)


def _send(bot_token, channel_id, fallback_text, blocks):
    """Send a Slack message with blocks."""
    try:
        resp = requests.post(
            SLACK_POST_URL,
            headers={
                "Authorization": f"Bearer {bot_token}",
                "Content-Type": "application/json",
            },
            json={
                "channel": channel_id,
                "text": fallback_text,
                "blocks": blocks,
            },
            timeout=10,
        )
        data = resp.json()
        if data.get("ok"):
            logger.info("Slack notification sent")
        else:
            logger.warning("Slack notification failed: %s", data.get("error"))
    except Exception as e:
        logger.warning("Failed to send Slack notification: %s", e)
