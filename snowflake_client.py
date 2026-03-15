import logging

import snowflake.connector

logger = logging.getLogger(__name__)


def fetch_icp_traffic(account, user, password, warehouse, since_date):
    """Fetch ICP traffic with GCLIDs from Snowflake.

    Queries Demandbase page-view data joined with Salesforce account data
    to find visits from companies with 1,000+ employees that arrived via
    Google Ads (GCLID present in URL).

    Args:
        account: Snowflake account identifier.
        user: Snowflake username.
        password: Snowflake password.
        warehouse: Snowflake warehouse name.
        since_date: Start date string (YYYY-MM-DD). Rows on or after this
            date are returned.

    Returns:
        List of dicts with keys: gclid, conversion_timestamp.
    """
    query = """
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
        ORDER BY m.DATE ASC
    """

    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
    )

    try:
        cur = conn.cursor()
        logger.info("Querying ICP traffic since %s...", since_date)
        cur.execute(query, (since_date,))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    leads = []
    seen_gclids = set()
    skipped_dupes = 0

    for gclid, visit_date, domain, emp_size in rows:
        if not gclid:
            continue
        gclid = gclid.strip()
        if not gclid:
            continue

        # Deduplicate within the Snowflake result set (same GCLID can appear
        # across multiple page views)
        if gclid in seen_gclids:
            skipped_dupes += 1
            continue
        seen_gclids.add(gclid)

        # Format date as ISO 8601 timestamp (midnight UTC)
        conversion_ts = visit_date.strftime("%Y-%m-%dT00:00:00Z")

        leads.append(
            {
                "gclid": gclid,
                "conversion_timestamp": conversion_ts,
            }
        )

    logger.info(
        "Snowflake returned %d rows, %d unique GCLIDs extracted (%d intra-query dupes skipped)",
        len(rows),
        len(leads),
        skipped_dupes,
    )
    return leads
