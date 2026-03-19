# Demandbase ICP Traffic Passback

Skill for managing the Demandbase ICP Traffic Passback pipeline — fetches GCLID data from Snowflake (Demandbase page-view metrics joined with Salesforce accounts with 1000+ employees) and appends new entries to Google Sheets for Google Ads offline conversion tracking.

## Trigger the Daily Passback Workflow

Run the daily passback GitHub Action manually:

```bash
gh workflow run "Daily Demandbase ICP Traffic Passback" --repo Pragadeesh-atlan/demandbase-icp-traffic-passback
```

## Trigger the Backfill Workflow

Run a backfill with custom date range:

```bash
gh workflow run "One-time Backfill (Demandbase ICP Traffic)" \
  --repo Pragadeesh-atlan/demandbase-icp-traffic-passback \
  -f since_date=2026-03-01 \
  -f lookback_days=30
```

## Check Latest Workflow Runs

```bash
gh run list --repo Pragadeesh-atlan/demandbase-icp-traffic-passback --limit 5
```

## View a Specific Run's Logs

```bash
gh run view <run-id> --repo Pragadeesh-atlan/demandbase-icp-traffic-passback --log
```

## Architecture

- **`main.py`**: Orchestrator — calculates date window, cleans old rows, deduplicates, appends new GCLIDs
- **`snowflake_client.py`**: Queries `DEMANDBASE_DB.GCS_TABLES.DB1_ACCOUNT_SITE_BASE_PAGE_METRICS` joined with `ATLAN_GROWTH_ANALYTICS.DBT_DEV.STG_SALESFORCE_ACCOUNTS`
- **`sheets_client.py`**: Google Sheets read/write via OAuth2 refresh token
- **`slack_notifier.py`**: Slack notifications for success/no-leads/error
- **`config.py`**: Environment config loader with validation

## Key Configuration

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `SINCE_DATE` | 2026-03-01 | Absolute floor date — rows older than this are cleaned |
| `LOOKBACK_DAYS` | 14 | Rolling window for Snowflake query (accounts for 2-3 day Demandbase ingestion lag) |
| `CONVERSION_NAME` | ICP Traffic | Static label for the conversion name column |

## Google Sheet

Sheet ID: `16XXrXoZO7AKMqZ0ggKHrhZNsNvcFim9p4TG8krRJhTo`
Columns: `gclid | activity date & timestamp | conversion name`

## Troubleshooting

**"No New GCLIDs" every day:**
- Demandbase has a 2-3 day data ingestion lag into Snowflake
- Check if `LOOKBACK_DAYS` is large enough (currently 14)
- Run the backfill workflow with a larger `lookback_days` to catch up

**Data gap after a specific date:**
- Run backfill with `lookback_days` covering the gap period
- Check Snowflake table `DB1_ACCOUNT_SITE_BASE_PAGE_METRICS` for data freshness
