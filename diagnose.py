"""Diagnostic script to investigate missing data after 2026-03-15.

Run with: python diagnose.py

Requires .env file with Snowflake and Google Sheets credentials.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()


def check_snowflake():
    """Run step-by-step Snowflake checks to isolate the data gap."""
    import snowflake.connector

    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH"

    if not all([account, user, password]):
        print("ERROR: Missing Snowflake credentials in .env")
        return

    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=account, user=user, password=password, warehouse=warehouse
    )
    cur = conn.cursor()

    print("\n" + "=" * 70)
    print("CHECK 1: Demandbase table — latest data date")
    print("=" * 70)
    cur.execute("""
        SELECT MAX(DATE) AS max_date, MIN(DATE) AS min_date, COUNT(*) AS total_rows
        FROM DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS
        WHERE DATE >= '2026-03-01'
    """)
    row = cur.fetchone()
    print(f"  Date range: {row[1]} to {row[0]}")
    print(f"  Total rows since 2026-03-01: {row[2]}")
    if row[0] and str(row[0]) <= "2026-03-15":
        print("  >>> ISSUE FOUND: Demandbase table has NO data after 2026-03-15!")
        print("  >>> The upstream Demandbase data pipeline likely stopped.")
    else:
        print(f"  OK — Data goes up to {row[0]}")

    print("\n" + "=" * 70)
    print("CHECK 2: Demandbase table — row counts by date (last 14 days)")
    print("=" * 70)
    cur.execute("""
        SELECT DATE, COUNT(*) AS row_count
        FROM DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS
        WHERE DATE >= DATEADD(day, -14, CURRENT_DATE())
        GROUP BY DATE
        ORDER BY DATE DESC
    """)
    rows = cur.fetchall()
    if not rows:
        print("  >>> NO DATA in the last 14 days!")
    for r in rows:
        print(f"  {r[0]}: {r[1]:,} rows")

    print("\n" + "=" * 70)
    print("CHECK 3: Demandbase table — rows with GCLIDs (last 14 days)")
    print("=" * 70)
    cur.execute("""
        SELECT DATE, COUNT(*) AS gclid_rows
        FROM DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS
        WHERE DATE >= DATEADD(day, -14, CURRENT_DATE())
          AND LOWER(BASE_PAGE) LIKE '%%gclid%%'
        GROUP BY DATE
        ORDER BY DATE DESC
    """)
    rows = cur.fetchall()
    if not rows:
        print("  >>> NO rows with GCLIDs in the last 14 days!")
    for r in rows:
        print(f"  {r[0]}: {r[1]:,} rows with GCLIDs")

    print("\n" + "=" * 70)
    print("CHECK 4: Salesforce accounts table (ICP filter)")
    print("=" * 70)
    cur.execute("""
        SELECT COUNT(DISTINCT ACCOUNT_DOMAIN) AS domain_count
        FROM ATLAN_GROWTH_ANALYTICS.DBT_DEV.STG_SALESFORCE_ACCOUNTS
        WHERE EMPLOYEE_SIZE > 1000
    """)
    row = cur.fetchone()
    print(f"  Domains with 1000+ employees: {row[0]}")
    if row[0] == 0:
        print("  >>> ISSUE FOUND: STG_SALESFORCE_ACCOUNTS has NO matching accounts!")
        print("  >>> The dbt DEV model may have been dropped or rebuilt empty.")

    print("\n" + "=" * 70)
    print("CHECK 5: Full pipeline query (last 14 days)")
    print("=" * 70)
    since = (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT
            REGEXP_SUBSTR(m.BASE_PAGE, 'gclid=([^&#]+)', 1, 1, 'e') AS gclid,
            m.DATE AS visit_date,
            m.ACCOUNT_DOMAIN,
            a.EMPLOYEE_SIZE
        FROM DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS m
        JOIN (
            SELECT ACCOUNT_DOMAIN, MAX(EMPLOYEE_SIZE) AS EMPLOYEE_SIZE
            FROM ATLAN_GROWTH_ANALYTICS.DBT_DEV.STG_SALESFORCE_ACCOUNTS
            WHERE EMPLOYEE_SIZE > 1000
            GROUP BY ACCOUNT_DOMAIN
        ) a ON m.ACCOUNT_DOMAIN = a.ACCOUNT_DOMAIN
        WHERE m.DATE >= %s
          AND m.DATE < CURRENT_DATE()
          AND LOWER(m.BASE_PAGE) LIKE '%%gclid%%'
        ORDER BY m.DATE DESC
        LIMIT 20
    """, (since,))
    rows = cur.fetchall()
    if not rows:
        print(f"  >>> Full pipeline query returned 0 rows since {since}!")
    else:
        print(f"  Found {len(rows)} rows (showing up to 20):")
        for r in rows:
            gclid_display = r[0][:20] + "..." if r[0] and len(r[0]) > 20 else r[0]
            print(f"    {r[1]} | {r[2]} | emp={r[3]} | gclid={gclid_display}")

    print("\n" + "=" * 70)
    print("CHECK 6: EMPLOYEE_SIZE data type check")
    print("=" * 70)
    cur.execute("""
        SELECT TYPEOF(EMPLOYEE_SIZE), EMPLOYEE_SIZE
        FROM ATLAN_GROWTH_ANALYTICS.DBT_DEV.STG_SALESFORCE_ACCOUNTS
        WHERE EMPLOYEE_SIZE IS NOT NULL
        LIMIT 5
    """)
    rows = cur.fetchall()
    for r in rows:
        print(f"  type={r[0]}, value={r[1]}")
    if rows and rows[0][0] in ("VARCHAR", "TEXT"):
        print("  >>> WARNING: EMPLOYEE_SIZE is a string! The > 1000 comparison may be wrong.")
        print("  >>> 'WHERE EMPLOYEE_SIZE > 1000' does lexicographic comparison on strings.")
        print("  >>> Consider: WHERE TRY_CAST(EMPLOYEE_SIZE AS INT) > 1000")

    cur.close()
    conn.close()


def check_google_sheet():
    """Check what's currently in the Google Sheet."""
    import gspread
    from google.oauth2.credentials import Credentials

    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    refresh_token = os.getenv("GOOGLE_SHEETS_REFRESH_TOKEN", os.getenv("GOOGLE_REFRESH_TOKEN"))
    sheet_id = os.getenv("GOOGLE_SHEET_ID") or "16XXrXoZO7AKMqZ0ggKHrhZNsNvcFim9p4TG8krRJhTo"

    if not all([client_id, client_secret, refresh_token]):
        print("\nERROR: Missing Google Sheets credentials in .env")
        return

    print("\n" + "=" * 70)
    print("CHECK 7: Google Sheet contents")
    print("=" * 70)

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    ws = client.open_by_key(sheet_id).worksheet("Sheet1")

    all_values = ws.get_all_values()
    print(f"  Total rows (including header): {len(all_values)}")

    if len(all_values) <= 1:
        print("  >>> Sheet is empty (only header or no data)!")
        return

    # Analyze dates in column B
    dates = []
    for row in all_values[1:]:
        if len(row) >= 2 and row[1]:
            dates.append(row[1][:10])

    if dates:
        print(f"  Date range: {min(dates)} to {max(dates)}")
        print(f"  Data rows: {len(dates)}")

        # Count by date
        from collections import Counter
        date_counts = Counter(dates)
        print("  Rows by date (last 10 dates):")
        for d in sorted(date_counts.keys(), reverse=True)[:10]:
            print(f"    {d}: {date_counts[d]} GCLIDs")

        if max(dates) <= "2026-03-15":
            print("  >>> CONFIRMED: No data in Google Sheet after 2026-03-15!")
    else:
        print("  >>> No parseable dates in column B!")


def main():
    print("Demandbase ICP Traffic Passback — Diagnostic Report")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print()

    try:
        check_snowflake()
    except Exception as e:
        print(f"\nSnowflake check failed: {e}")

    try:
        check_google_sheet()
    except Exception as e:
        print(f"\nGoogle Sheet check failed: {e}")

    print("\n" + "=" * 70)
    print("SUMMARY OF LIKELY ROOT CAUSES (in order of probability)")
    print("=" * 70)
    print("""
1. UPSTREAM DATA PIPELINE STOPPED
   The Demandbase table (DB1_ACCOUNT_SITE_BASE_PAGE_METRICS) may have
   stopped receiving data after 3/15. Check if the Demandbase → GCS →
   Snowflake ingestion pipeline is still running.

2. dbt DEV MODEL STALE OR DROPPED
   The Salesforce accounts table (ATLAN_GROWTH_ANALYTICS.DBT_DEV.
   STG_SALESFORCE_ACCOUNTS) is in a dbt DEV schema. If this model was
   dropped, rebuilt empty, or its EMPLOYEE_SIZE values changed, the
   JOIN would return zero results. Consider using a PROD schema instead.

3. EMPLOYEE_SIZE DATA TYPE ISSUE
   If EMPLOYEE_SIZE is stored as VARCHAR, the comparison > 1000 does
   lexicographic comparison ("200" < "1000" is FALSE in string comparison).
   This could silently filter out all accounts.

4. WORKFLOW FAILURES (3/16 - 3/19)
   Multiple bug fixes were pushed between 3/16-3/19. During this period,
   the workflow may have been failing. Check GitHub Actions run history.
""")


if __name__ == "__main__":
    main()
