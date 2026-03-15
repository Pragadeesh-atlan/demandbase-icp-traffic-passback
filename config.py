import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Snowflake
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

# Google Sheets
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_SHEETS_REFRESH_TOKEN", os.getenv("GOOGLE_REFRESH_TOKEN"))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "16XXrXoZO7AKMqZ0ggKHrhZNsNvcFim9p4TG8krRJhTo")
GOOGLE_SHEET_TAB_NAME = os.getenv("GOOGLE_SHEET_TAB_NAME", "Sheet1")

# Pipeline settings
CONVERSION_NAME = os.getenv("CONVERSION_NAME", "ICP Traffic")
SINCE_DATE = os.getenv("SINCE_DATE", "2026-03-01")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "2"))

# Slack (optional)
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

# Google Sheet column headers
SHEET_HEADERS = ["gclid", "conversion date & timestamp", "conversion name"]

# Validate required config
_required = {
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    "GOOGLE_CLIENT_ID": GOOGLE_CLIENT_ID,
    "GOOGLE_CLIENT_SECRET": GOOGLE_CLIENT_SECRET,
    "GOOGLE_REFRESH_TOKEN": GOOGLE_REFRESH_TOKEN,
}

_missing = [name for name, value in _required.items() if not value]
if _missing:
    print(f"ERROR: Missing required environment variables: {', '.join(_missing)}")
    print("Copy .env.example to .env and fill in the values.")
    sys.exit(1)
