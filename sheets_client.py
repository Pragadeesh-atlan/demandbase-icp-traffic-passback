import logging

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

    Columns: GCLID | Conversion Date & Timestamp | Conversion Name

    Args:
        sheet_id: Google Sheet ID.
        client_id: Google OAuth2 client ID.
        client_secret: Google OAuth2 client secret.
        refresh_token: Google OAuth2 refresh token.
        tab_name: Sheet tab name.
        leads: List of dicts with keys: gclid, conversion_timestamp.
        conversion_name: Static value for the conversion name column.

    Returns:
        Number of rows appended.
    """
    if not leads:
        logger.info("No leads to append")
        return 0

    worksheet = _get_worksheet(sheet_id, client_id, client_secret, refresh_token, tab_name)

    # Ensure header row exists
    headers = ["gclid", "activity date & timestamp", "conversion name"]
    row1 = worksheet.row_values(1)
    if row1 != headers:
        worksheet.update("A1:C1", [headers], value_input_option="USER_ENTERED")
        logger.info("Header row set")

    rows = []
    for lead in leads:
        rows.append([
            lead["gclid"],
            lead["conversion_timestamp"],
            conversion_name,
        ])

    worksheet.append_rows(rows, value_input_option="USER_ENTERED", table_range="A1")
    logger.info("Appended %d rows to sheet", len(rows))
    return len(rows)
