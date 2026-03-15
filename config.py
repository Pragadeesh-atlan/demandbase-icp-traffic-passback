import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Required environment variables
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1l_7s70iSeruuGEtfKm4VHLL-scdmeVMKVsv_gSBtpfQ")
GOOGLE_SHEET_TAB_NAME = os.getenv("GOOGLE_SHEET_TAB_NAME", "Sheet1")
CONVERSION_NAME = os.getenv("CONVERSION_NAME", "Demandbase ICP Traffic")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "2"))
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

# HubSpot properties to fetch
HUBSPOT_PROPERTIES = ["email", "hs_google_click_id", "date_entered_workable_yes"]

# Google Sheet column headers
SHEET_HEADERS = ["gclid", "email", "conversion date & timestamp", "conversion name"]

# Validate required config
_required = {
    "HUBSPOT_ACCESS_TOKEN": HUBSPOT_ACCESS_TOKEN,
    "GOOGLE_CLIENT_ID": GOOGLE_CLIENT_ID,
    "GOOGLE_CLIENT_SECRET": GOOGLE_CLIENT_SECRET,
    "GOOGLE_REFRESH_TOKEN": GOOGLE_REFRESH_TOKEN,
}

_missing = [name for name, value in _required.items() if not value]
if _missing:
    print(f"ERROR: Missing required environment variables: {', '.join(_missing)}")
    print("Copy .env.example to .env and fill in the values.")
    sys.exit(1)
