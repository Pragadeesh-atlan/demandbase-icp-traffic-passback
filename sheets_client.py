import logging
from datetime import datetime, timezone

import gspread
from google.oauth2.credentials import Credentials

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TOKEN_URI = "https://oauth2.googleapis.com/token"


def _get_worksheet(sheet_id, client_id, client_secret, refresh_token, tab_name):
    """Authenticate with OAuth2 refresh token and return the worksheet."""
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        token_uri=TOKEN_URI,
        scopes=SCOPES,
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.worksheet(tab_name)


def get_existing_gclids(sheet_id, client_id, client_secret, refresh_token, tab_name):
    """Read all GCLIDs from column A of the sheet for deduplication.

    Returns:
        A set of GCLID strings already present in the sheet.
    """
    worksheet = _get_worksheet(sheet_id, client_id, client_secret, refresh_token, tab_name)
    col_values = worksheet.col_values(1)  # Column A = gclid

    # Skip header row if present
    gclids = set()
    for val in col_values:
        stripped = val.strip()
        if stripped and stripped.lower() != "gclid":
            gclids.add(stripped)

    logger.info("Found %d existing GCLIDs in sheet", len(gclids))
    return gclids


def append_leads(sheet_id, client_id, client_secret, refresh_token, tab_name, leads, conversion_name):
    """Append new lead rows to the Google Sheet.

    Args:
        sheet_id: Google Sheet ID.
        client_id: Google OAuth2 client ID.
        client_secret: Google OAuth2 client secret.
        refresh_token: Google OAuth2 refresh token.
        tab_name: Sheet tab name.
        leads: List of dicts with keys: gclid, email, conversion_timestamp.
        conversion_name: Static value for the conversion name column.

    Returns:
        Number of rows appended.
    """
    if not leads:
        logger.info("No leads to append")
        return 0

    worksheet = _get_worksheet(sheet_id, client_id, client_secret, refresh_token, tab_name)

    rows = []
    for lead in leads:
        try:
            formatted_ts = _format_timestamp(lead["conversion_timestamp"])
        except (ValueError, TypeError, OSError) as e:
            logger.warning(
                "Skipping lead %s — could not parse timestamp %r: %s",
                lead.get("email"),
                lead.get("conversion_timestamp"),
                e,
            )
            continue
        rows.append([lead["gclid"], lead["email"], formatted_ts, conversion_name])

    if not rows:
        logger.warning("All %d leads skipped due to timestamp errors", len(leads))
        return 0

    worksheet.append_rows(rows, value_input_option="USER_ENTERED")
    logger.info("Appended %d rows to sheet", len(rows))
    return len(rows)


def _format_timestamp(raw_timestamp):
    """Convert HubSpot timestamp to UTC ISO 8601 format for Google Ads.

    Handles both formats HubSpot may return:
      - ISO 8601:  '2026-03-02T23:27:32.640Z'
      - Epoch ms:  '1709423252640'

    Output: '2026-03-02T23:27:32Z'
    """
    ts = str(raw_timestamp).strip()

    # Detect epoch milliseconds (purely numeric string)
    if ts.isdigit():
        dt_utc = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
    else:
        # Handle both 'Z' suffix and '+00:00' formats
        ts = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        dt_utc = dt.astimezone(timezone.utc)

    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
