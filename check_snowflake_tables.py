"""
Script to check which Snowflake tables have access to gclid, account_domain, and account_name columns.

Usage:
    pip install snowflake-connector-python python-dotenv
    python check_snowflake_tables.py

Requires these env vars (or a .env file):
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE
"""

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

TARGET_COLUMNS = ['gclid', 'account_domain', 'account_name']

def main():
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    )
    cur = conn.cursor()

    # Search across all databases using ACCOUNT_USAGE (requires ACCOUNTADMIN or appropriate grants)
    print("Searching SNOWFLAKE.ACCOUNT_USAGE.COLUMNS for target columns...\n")
    try:
        cur.execute("""
            SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
            WHERE LOWER(COLUMN_NAME) IN (%s)
              AND DELETED IS NULL
            ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        """ % ','.join(f"'{c}'" for c in TARGET_COLUMNS))

        results = cur.fetchall()
        if results:
            print(f"Found {len(results)} matching columns:\n")
            print(f"{'DATABASE':<30} {'SCHEMA':<25} {'TABLE':<40} {'COLUMN':<20} {'TYPE':<15}")
            print("-" * 130)
            for row in results:
                print(f"{row[0]:<30} {row[1]:<25} {row[2]:<40} {row[3]:<20} {row[4]:<15}")
        else:
            print("No matching columns found in ACCOUNT_USAGE.")
    except Exception as e:
        print(f"ACCOUNT_USAGE query failed ({e}). Falling back to per-database search...\n")
        _search_per_database(cur)

    # Also find tables that have BOTH gclid AND (account_domain or account_name)
    print("\n\n=== Tables with BOTH gclid AND account_domain/account_name ===\n")
    try:
        cur.execute("""
            SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,
                   LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY COLUMN_NAME) AS COLUMNS
            FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
            WHERE LOWER(COLUMN_NAME) IN (%s)
              AND DELETED IS NULL
            GROUP BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
            HAVING COUNT(DISTINCT CASE WHEN LOWER(COLUMN_NAME) = 'gclid' THEN 1 END) > 0
               AND COUNT(DISTINCT CASE WHEN LOWER(COLUMN_NAME) IN ('account_domain', 'account_name') THEN 1 END) > 0
            ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
        """ % ','.join(f"'{c}'" for c in TARGET_COLUMNS))

        results = cur.fetchall()
        if results:
            print(f"{'DATABASE':<30} {'SCHEMA':<25} {'TABLE':<40} {'MATCHING COLUMNS':<40}")
            print("-" * 135)
            for row in results:
                print(f"{row[0]:<30} {row[1]:<25} {row[2]:<40} {row[3]:<40}")
        else:
            print("No tables found with both gclid and account_domain/account_name.")
    except Exception:
        pass

    conn.close()


def _search_per_database(cur):
    """Fallback: iterate through each accessible database and search INFORMATION_SCHEMA."""
    cur.execute("SHOW DATABASES")
    databases = [row[1] for row in cur.fetchall()]
    print(f"Accessible databases: {databases}\n")

    for db in databases:
        try:
            cur.execute(f"""
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
                FROM "{db}".INFORMATION_SCHEMA.COLUMNS
                WHERE LOWER(COLUMN_NAME) IN ({','.join(f"'{c}'" for c in TARGET_COLUMNS)})
                ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
            """)
            results = cur.fetchall()
            if results:
                print(f"\n--- Database: {db} ---")
                print(f"{'SCHEMA':<25} {'TABLE':<40} {'COLUMN':<20} {'TYPE':<15}")
                print("-" * 100)
                for row in results:
                    print(f"{row[0]:<25} {row[1]:<40} {row[2]:<20} {row[3]:<15}")
        except Exception as e:
            print(f"  Skipping {db}: {e}")


if __name__ == '__main__':
    main()
