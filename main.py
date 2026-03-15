import logging
import os
import traceback
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

import config
from snowflake_client import fetch_icp_traffic
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
        # 1. Determine the start date for the query
        since_date = config.SINCE_DATE
        logger.info("Fetching ICP traffic since %s", since_date)

        # 2. Get existing GCLIDs from Google Sheet for dedup
        logger.info("Reading existing GCLIDs from Google Sheet...")
        existing_gclids = get_existing_gclids(
            config.GOOGLE_SHEET_ID,
            config.GOOGLE_CLIENT_ID,
            config.GOOGLE_CLIENT_SECRET,
            config.GOOGLE_REFRESH_TOKEN,
            config.GOOGLE_SHEET_TAB_NAME,
        )

        # 3. Fetch ICP traffic from Snowflake
        logger.info("Fetching ICP traffic from Snowflake...")
        leads = fetch_icp_traffic(
            config.SNOWFLAKE_ACCOUNT,
            config.SNOWFLAKE_USER,
            config.SNOWFLAKE_PASSWORD,
            config.SNOWFLAKE_WAREHOUSE,
            since_date,
        )

        # 4. Dedup — filter out leads already in the sheet
        new_leads = [lead for lead in leads if lead["gclid"] not in existing_gclids]
        skipped = len(leads) - len(new_leads)

        if skipped > 0:
            logger.info("Skipped %d GCLIDs already in sheet", skipped)

        if not new_leads:
            logger.info("No new GCLIDs to add. Done.")
            if config.SLACK_BOT_TOKEN and config.SLACK_CHANNEL_ID:
                notify_no_leads(config.SLACK_BOT_TOKEN, config.SLACK_CHANNEL_ID, len(leads), skipped)
            return

        # 5. Append new leads to Google Sheet
        logger.info("Appending %d new GCLIDs to Google Sheet...", len(new_leads))
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
