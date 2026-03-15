import logging
import os
import traceback
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

import config
from hubspot_client import fetch_new_workable_leads
from sheets_client import get_existing_gclids, append_leads
from slack_notifier import notify_success, notify_no_leads, notify_error


def setup_logging():
    """Configure logging to both console and rotating file."""
    os.makedirs("logs", exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler — rotate at 5MB, keep 3 backups
    file_handler = RotatingFileHandler(
        "logs/passback.log", maxBytes=5_000_000, backupCount=3
    )
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("=== Demandbase ICP Traffic Passback Started ===")

    try:
        # 1. Calculate lookback window
        since_dt = (datetime.now(timezone.utc) - timedelta(days=config.LOOKBACK_DAYS)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        since_ms = int(since_dt.timestamp() * 1000)
        logger.info("Looking back %d days (since %s)", config.LOOKBACK_DAYS, since_dt.isoformat())

        # 2. Get existing GCLIDs from Google Sheet for dedup
        logger.info("Reading existing GCLIDs from Google Sheet...")
        existing_gclids = get_existing_gclids(
            config.GOOGLE_SHEET_ID,
            config.GOOGLE_CLIENT_ID,
            config.GOOGLE_CLIENT_SECRET,
            config.GOOGLE_REFRESH_TOKEN,
            config.GOOGLE_SHEET_TAB_NAME,
        )

        # 3. Fetch new workable leads from HubSpot
        logger.info("Fetching workable leads from HubSpot...")
        leads = fetch_new_workable_leads(config.HUBSPOT_ACCESS_TOKEN, since_ms)

        # 4. Dedup — filter out leads already in the sheet
        new_leads = [lead for lead in leads if lead["gclid"] not in existing_gclids]
        skipped = len(leads) - len(new_leads)

        if skipped > 0:
            logger.info("Skipped %d leads already in sheet", skipped)

        if not new_leads:
            logger.info("No new leads to add. Done.")
            # Notify Slack — no new leads
            if config.SLACK_BOT_TOKEN and config.SLACK_CHANNEL_ID:
                notify_no_leads(config.SLACK_BOT_TOKEN, config.SLACK_CHANNEL_ID, len(leads), skipped)
            return

        # 5. Append new leads to Google Sheet
        logger.info("Appending %d new leads to Google Sheet...", len(new_leads))
        count = append_leads(
            config.GOOGLE_SHEET_ID,
            config.GOOGLE_CLIENT_ID,
            config.GOOGLE_CLIENT_SECRET,
            config.GOOGLE_REFRESH_TOKEN,
            config.GOOGLE_SHEET_TAB_NAME,
            new_leads,
            config.CONVERSION_NAME,
        )

        logger.info(
            "=== Done: %d fetched, %d skipped (dupes), %d appended ===",
            len(leads),
            skipped,
            count,
        )

        # Notify Slack — success
        if config.SLACK_BOT_TOKEN and config.SLACK_CHANNEL_ID:
            notify_success(
                config.SLACK_BOT_TOKEN,
                config.SLACK_CHANNEL_ID,
                len(leads),
                skipped,
                count,
                config.GOOGLE_SHEET_ID,
            )

    except Exception as e:
        logger.error("Script failed: %s", e)
        logger.error(traceback.format_exc())

        # Notify Slack — error
        if config.SLACK_BOT_TOKEN and config.SLACK_CHANNEL_ID:
            notify_error(config.SLACK_BOT_TOKEN, config.SLACK_CHANNEL_ID, str(e))

        raise


if __name__ == "__main__":
    main()
