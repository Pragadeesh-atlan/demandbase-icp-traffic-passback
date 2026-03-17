import logging
import os
import traceback
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler


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


def _try_slack_error(error_detail):
    """Best-effort Slack error notification, even if config is partially loaded."""
    try:
        from slack_notifier import notify_error

        slack_token = os.getenv("SLACK_BOT_TOKEN")
        slack_channel = os.getenv("SLACK_CHANNEL_ID")
        if slack_token and slack_channel:
            if len(error_detail) > 2900:
                error_detail = error_detail[:2900] + "\n... (truncated)"
            notify_error(slack_token, slack_channel, error_detail)
    except Exception:
        pass  # Slack is best-effort; don't mask the original error


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("=== Demandbase ICP Traffic Passback Started ===")

    try:
        # Import config here so validation errors are caught by try/except
        import config
        from snowflake_client import fetch_icp_traffic
        from sheets_client import get_existing_gclids, append_leads, clean_old_rows
        from slack_notifier import notify_success, notify_no_leads, notify_error

        # 1. Determine the start date — rolling window based on LOOKBACK_DAYS,
        #    but never earlier than SINCE_DATE (the absolute floor).
        lookback_date = (
            datetime.now(timezone.utc) - timedelta(days=config.LOOKBACK_DAYS)
        ).strftime("%Y-%m-%d")
        since_date = max(lookback_date, config.SINCE_DATE)
        logger.info(
            "Fetching ICP traffic since %s (lookback=%d days, floor=%s)",
            since_date, config.LOOKBACK_DAYS, config.SINCE_DATE,
        )

        # 1b. Clean old rows from sheet (before SINCE_DATE)
        logger.info("Cleaning sheet rows before %s...", config.SINCE_DATE)
        deleted = clean_old_rows(
            config.GOOGLE_SHEET_ID,
            config.GOOGLE_CLIENT_ID,
            config.GOOGLE_CLIENT_SECRET,
            config.GOOGLE_REFRESH_TOKEN,
            config.GOOGLE_SHEET_TAB_NAME,
            config.SINCE_DATE,
        )
        if deleted:
            logger.info("Cleaned %d old rows from sheet", deleted)

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
        tb = traceback.format_exc()
        logger.error(tb)

        # Notify Slack — error (include traceback for debuggability)
        _try_slack_error(f"{e}\n\n{tb}")

        raise


if __name__ == "__main__":
    main()
