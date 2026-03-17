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


def clean_old_rows(sheet_id, client_id, client_secret, refresh_token, tab_name, cutoff_date):
    """Delete rows from the sheet where the activity date is before cutoff_date.

    Args:
        cutoff_date: Date string (YYYY-MM-DD). Rows with dates before this are removed.

    Returns:
        Number of rows deleted.
    """
    worksheet = _get_worksheet(sheet_id, client_id, client_secret, refresh_token, tab_name)
    all_values = worksheet.get_all_values()

    if len(all_values) <= 1:
        logger.info("Sheet has no data rows to clean")
        return 0

    rows_to_delete = []
    for i, row in enumerate(all_values[1:], start=2):  # skip header, 1-indexed
        if len(row) < 2 or not row[1]:
            continue
        # Column B is the activity date/timestamp (e.g. "2026-03-01T00:00:00Z")
        row_date = row[1][:10]  # extract YYYY-MM-DD
        if row_date < cutoff_date:
            rows_to_delete.append(i)

    if not rows_to_delete:
        logger.info("No old rows to clean (cutoff=%s)", cutoff_date)
        return 0

    # Group contiguous rows into ranges and delete from bottom up
    # to preserve row indices for earlier ranges
    ranges = []
    start = rows_to_delete[0]
    end = start
    for idx in rows_to_delete[1:]:
        if idx == end + 1:
            end = idx
        else:
            ranges.append((start, end))
            start = idx
            end = idx
    ranges.append((start, end))

    for start_row, end_row in reversed(ranges):
        worksheet.delete_rows(start_row, end_row)

    logger.info("Deleted %d rows with dates before %s", len(rows_to_delete), cutoff_date)
    return len(rows_to_delete)


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
