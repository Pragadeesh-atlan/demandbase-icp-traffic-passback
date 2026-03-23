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

    # ── CHECK 1: Demandbase table (confirmed OK — has data till yesterday) ──
    print("\n" + "=" * 70)
    print("CHECK 1: Demandbase table — GCLID rows by date (last 14 days)")
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

    # ── CHECK 1b: Total rows by date (with and without GCLID filter) ──
    print("\n" + "=" * 70)
    print("CHECK 1b: Demandbase table — ALL rows vs GCLID rows (last 14 days)")
    print("=" * 70)
    cur.execute("""
        SELECT
            DATE,
            COUNT(*) AS total_rows,
            COUNT(CASE WHEN LOWER(BASE_PAGE) LIKE '%%gclid%%' THEN 1 END) AS gclid_rows
        FROM DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS
        WHERE DATE >= DATEADD(day, -14, CURRENT_DATE())
        GROUP BY DATE
        ORDER BY DATE DESC
    """)
    rows = cur.fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]:,} total, {r[2]:,} with GCLIDs")
    if rows and rows[0][2] == 0:
        print("  >>> ISSUE: Table has data but NO GCLIDs in recent days!")
        print("  >>> Checking BASE_PAGE samples for recent dates...")
        cur.execute("""
            SELECT DATE, BASE_PAGE
            FROM DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS
            WHERE DATE = (SELECT MAX(DATE) FROM DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS)
            LIMIT 5
        """)
        for r in cur.fetchall():
            page = r[1][:120] + "..." if len(str(r[1])) > 120 else r[1]
            print(f"    {r[0]}: {page}")

    # ── CHECK 2: List all tables in GCS_TABLES schema ──
    print("\n" + "=" * 70)
    print("CHECK 2: All tables in DEMANDBASE_DB.GCS_TABLES schema")
    print("=" * 70)
    cur.execute("SHOW TABLES IN SCHEMA DEMANDBASE_DB.GCS_TABLES")
    tables = cur.fetchall()
    # SHOW TABLES returns: created_on, name, database_name, schema_name, ...
    print(f"  Found {len(tables)} tables:")
    for t in tables:
        table_name = t[1]
        print(f"    - {table_name}")
        if "PAGE_METRICS" in table_name.upper() and table_name != "DB1_ACCOUNT_SITE_BASE_PAGE_METRICS":
            print(f"      >>> POSSIBLE REPLACEMENT TABLE FOUND: {table_name}")

    # ── CHECK 3: STG_SALESFORCE_ACCOUNTS — does it exist and have data? ──
    print("\n" + "=" * 70)
    print("CHECK 3: Salesforce accounts table — existence & row count")
    print("=" * 70)
    try:
        cur.execute("""
            SELECT COUNT(*) AS total,
                   COUNT(DISTINCT ACCOUNT_DOMAIN) AS domains,
                   COUNT(CASE WHEN EMPLOYEE_SIZE IS NOT NULL THEN 1 END) AS has_emp_size
            FROM ATLAN_GROWTH_ANALYTICS.DBT_DEV.STG_SALESFORCE_ACCOUNTS
        """)
        row = cur.fetchone()
        print(f"  Total rows: {row[0]}")
        print(f"  Distinct domains: {row[1]}")
        print(f"  Rows with EMPLOYEE_SIZE: {row[2]}")
        if row[0] == 0:
            print("  >>> ISSUE FOUND: STG_SALESFORCE_ACCOUNTS is EMPTY!")
            print("  >>> The dbt DEV model was likely dropped or rebuilt empty.")
        elif row[2] == 0:
            print("  >>> ISSUE FOUND: EMPLOYEE_SIZE is NULL for all rows!")
    except Exception as e:
        print(f"  >>> ERROR querying table: {e}")
        print("  >>> Table may not exist or you lack access!")

    # ── CHECK 4: EMPLOYEE_SIZE data type & filter ──
    print("\n" + "=" * 70)
    print("CHECK 4: EMPLOYEE_SIZE data type & ICP filter results")
    print("=" * 70)
    try:
        cur.execute("""
            SELECT TYPEOF(EMPLOYEE_SIZE), EMPLOYEE_SIZE
            FROM ATLAN_GROWTH_ANALYTICS.DBT_DEV.STG_SALESFORCE_ACCOUNTS
            WHERE EMPLOYEE_SIZE IS NOT NULL
            LIMIT 5
        """)
        rows = cur.fetchall()
        for r in rows:
            print(f"  type={r[0]}, value={r[1]}")
        if rows and str(rows[0][0]).upper() in ("VARCHAR", "TEXT", "STRING"):
            print("  >>> WARNING: EMPLOYEE_SIZE is a STRING!")
            print("  >>> 'WHERE EMPLOYEE_SIZE > 1000' does lexicographic comparison.")
            print("  >>> e.g. '500' > '1000' = TRUE (wrong), '2' > '1000' = TRUE (wrong)")

        # Count with numeric filter vs string filter
        cur.execute("""
            SELECT
                COUNT(DISTINCT CASE WHEN EMPLOYEE_SIZE > 1000 THEN ACCOUNT_DOMAIN END) AS current_filter,
                COUNT(DISTINCT CASE WHEN TRY_CAST(EMPLOYEE_SIZE AS INT) > 1000 THEN ACCOUNT_DOMAIN END) AS safe_filter
            FROM ATLAN_GROWTH_ANALYTICS.DBT_DEV.STG_SALESFORCE_ACCOUNTS
        """)
        row = cur.fetchone()
        print(f"  Domains matching 'EMPLOYEE_SIZE > 1000': {row[0]}")
        print(f"  Domains matching 'TRY_CAST(EMPLOYEE_SIZE AS INT) > 1000': {row[1]}")
        if row[0] != row[1]:
            print(f"  >>> MISMATCH! String vs numeric filter returns different results.")
        if row[0] == 0 and row[1] == 0:
            print("  >>> ISSUE FOUND: NO accounts match the ICP filter!")
    except Exception as e:
        print(f"  >>> ERROR: {e}")

    # ── CHECK 5: List tables in DBT_DEV schema (look for renames) ──
    print("\n" + "=" * 70)
    print("CHECK 5: All tables in ATLAN_GROWTH_ANALYTICS.DBT_DEV schema")
    print("=" * 70)
    try:
        cur.execute("SHOW TABLES IN SCHEMA ATLAN_GROWTH_ANALYTICS.DBT_DEV")
        tables = cur.fetchall()
        sf_tables = [t for t in tables if "SALESFORCE" in t[1].upper() or "ACCOUNT" in t[1].upper()]
        print(f"  Found {len(tables)} total tables, {len(sf_tables)} matching 'SALESFORCE' or 'ACCOUNT':")
        for t in sf_tables:
            print(f"    - {t[1]}")
    except Exception as e:
        print(f"  >>> ERROR listing tables: {e}")
        print("  >>> Schema may not exist!")

    # ── CHECK 6: JOIN domain overlap ──
    print("\n" + "=" * 70)
    print("CHECK 6: Domain overlap between Demandbase & Salesforce tables")
    print("=" * 70)
    try:
        cur.execute("""
            WITH demandbase_domains AS (
                SELECT DISTINCT ACCOUNT_DOMAIN
                FROM DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS
                WHERE DATE >= DATEADD(day, -14, CURRENT_DATE())
                  AND LOWER(BASE_PAGE) LIKE '%%gclid%%'
            ),
            sf_domains AS (
                SELECT DISTINCT ACCOUNT_DOMAIN
                FROM ATLAN_GROWTH_ANALYTICS.DBT_DEV.STG_SALESFORCE_ACCOUNTS
                WHERE TRY_CAST(EMPLOYEE_SIZE AS INT) > 1000
            )
            SELECT
                (SELECT COUNT(*) FROM demandbase_domains) AS demandbase_count,
                (SELECT COUNT(*) FROM sf_domains) AS sf_count,
                (SELECT COUNT(*) FROM demandbase_domains d JOIN sf_domains s ON d.ACCOUNT_DOMAIN = s.ACCOUNT_DOMAIN) AS overlap
        """)
        row = cur.fetchone()
        print(f"  Demandbase domains (with GCLIDs, last 14d): {row[0]}")
        print(f"  Salesforce ICP domains (1000+ employees): {row[1]}")
        print(f"  Overlapping domains (= pipeline output): {row[2]}")
        if row[2] == 0 and row[0] > 0 and row[1] > 0:
            print("  >>> ISSUE FOUND: No domain overlap! Check ACCOUNT_DOMAIN format mismatch.")
            # Sample domains from each side
            cur.execute("""
                SELECT ACCOUNT_DOMAIN FROM DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS
                WHERE DATE >= DATEADD(day, -14, CURRENT_DATE()) AND LOWER(BASE_PAGE) LIKE '%%gclid%%'
                LIMIT 5
            """)
            print("  Sample Demandbase domains:")
            for r in cur.fetchall():
                print(f"    '{r[0]}'")
            cur.execute("""
                SELECT ACCOUNT_DOMAIN FROM ATLAN_GROWTH_ANALYTICS.DBT_DEV.STG_SALESFORCE_ACCOUNTS
                WHERE TRY_CAST(EMPLOYEE_SIZE AS INT) > 1000
                LIMIT 5
            """)
            print("  Sample Salesforce domains:")
            for r in cur.fetchall():
                print(f"    '{r[0]}'")
        elif row[2] == 0:
            print("  >>> No overlap — one or both sides are empty.")
    except Exception as e:
        print(f"  >>> ERROR: {e}")

    # ── CHECK 7: Full pipeline query ──
    print("\n" + "=" * 70)
    print("CHECK 7: Full pipeline query (last 14 days)")
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
    print("SUMMARY — Demandbase table confirmed OK (has data till yesterday)")
    print("Likely root causes for missing data (in order of probability):")
    print("=" * 70)
    print("""
1. dbt DEV MODEL STALE OR DROPPED
   The Salesforce accounts table (ATLAN_GROWTH_ANALYTICS.DBT_DEV.
   STG_SALESFORCE_ACCOUNTS) is in a dbt DEV schema. If this model was
   dropped, rebuilt empty, or its EMPLOYEE_SIZE values changed, the
   JOIN would return zero results. Consider using a PROD schema instead.

2. EMPLOYEE_SIZE DATA TYPE ISSUE
   If EMPLOYEE_SIZE is stored as VARCHAR, the comparison > 1000 does
   lexicographic comparison (broken for strings). Use TRY_CAST.

3. DOMAIN FORMAT MISMATCH
   If ACCOUNT_DOMAIN values differ between the two tables (e.g.
   'example.com' vs 'www.example.com'), the JOIN returns zero matches.

4. WORKFLOW FAILURES (3/16 - 3/19)
   Multiple bug fixes were pushed between 3/16-3/19. The workflow may
   have been failing. Check GitHub Actions run history.
""")


if __name__ == "__main__":
    main()
