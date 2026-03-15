# Demandbase ICP Traffic Passback

## Project Overview

Python data pipeline that fetches "workable" leads from HubSpot, deduplicates against a Google Sheet, appends new leads, and notifies Slack. Used to pass GCLID conversion data back to Google Ads.

## Tech Stack

- Python 3.12
- Libraries: requests, gspread, google-auth, python-dotenv
- CI/CD: GitHub Actions (daily-passback.yml, backfill.yml)

## Project Structure

- `main.py` — Entry point and orchestrator
- `config.py` — Environment variable loader with validation
- `hubspot_client.py` — HubSpot API client (fetches workable leads)
- `sheets_client.py` — Google Sheets client (read existing GCLIDs, append new leads)
- `slack_notifier.py` — Slack notification utility
- `get_refresh_token.py` — Google OAuth token refresh utility

## Development

### Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # Fill in credentials
```

### Run

```bash
python main.py
```

### Environment Variables

All required variables are documented in `.env.example`. Key ones:
- `HUBSPOT_ACCESS_TOKEN` — HubSpot private app token
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REFRESH_TOKEN` — Google OAuth2
- `GOOGLE_SHEET_ID` — Target Google Sheet
- `SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID` — Optional Slack notifications

### Linting

```bash
pip install ruff
ruff check .
ruff format --check .
```

## Key Notes

- No test suite currently exists
- Logs rotate at 5MB with 3 backups in `logs/`
- `.env` files and `credentials/` directory must never be committed
- The daily workflow runs at 00:30 UTC (6:00 AM IST)
- Lookback window is configurable via `LOOKBACK_DAYS` (default: 2 days)
