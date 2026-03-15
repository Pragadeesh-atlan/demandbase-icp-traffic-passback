import time
import logging
import requests

logger = logging.getLogger(__name__)

HUBSPOT_SEARCH_URL = "https://api.hubapi.com/crm/v3/objects/contacts/search"


def fetch_new_workable_leads(access_token, since_timestamp_ms):
    """Fetch workable leads with GCLID from HubSpot since a given timestamp.

    Args:
        access_token: HubSpot private app token.
        since_timestamp_ms: Unix timestamp in milliseconds. Only leads with
            date_entered_workable_yes >= this value are returned.

    Returns:
        List of dicts with keys: gclid, email, conversion_timestamp.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "workable_leads",
                        "operator": "EQ",
                        "value": "yes",
                    },
                    {
                        "propertyName": "hs_google_click_id",
                        "operator": "HAS_PROPERTY",
                    },
                    {
                        "propertyName": "date_entered_workable_yes",
                        "operator": "GTE",
                        "value": str(since_timestamp_ms),
                    },
                ]
            }
        ],
        "properties": ["email", "hs_google_click_id", "date_entered_workable_yes"],
        "sorts": [
            {
                "propertyName": "date_entered_workable_yes",
                "direction": "ASCENDING",
            }
        ],
        "limit": 100,
    }

    leads = []
    after = None

    while True:
        if after:
            payload["after"] = after

        response = _request_with_retry(headers, payload)
        results = response.get("results", [])

        for contact in results:
            props = contact.get("properties", {})
            gclid = props.get("hs_google_click_id")
            email = props.get("email")
            conversion_ts = props.get("date_entered_workable_yes")

            if not gclid:
                continue
            gclid = gclid.strip()
            if not email:
                logger.warning(
                    "Skipping lead with GCLID %s — missing email", gclid[:20]
                )
                continue
            if not conversion_ts:
                logger.warning(
                    "Skipping lead %s — missing date_entered_workable_yes", email
                )
                continue

            leads.append(
                {
                    "gclid": gclid,
                    "email": email,
                    "conversion_timestamp": conversion_ts,
                }
            )

        # Handle pagination
        paging = response.get("paging")
        if paging and paging.get("next", {}).get("after"):
            after = paging["next"]["after"]
            time.sleep(0.25)  # Stay under 5 req/s rate limit
        else:
            break

    total = response.get("total", len(leads))
    logger.info("HubSpot returned %d total results, %d valid leads extracted", total, len(leads))
    return leads


def _request_with_retry(headers, payload, max_retries=3):
    """POST to HubSpot search API with exponential backoff on errors."""
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                HUBSPOT_SEARCH_URL,
                headers=headers,
                json=payload,
                timeout=30,
            )

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 429:
                wait = 2**attempt
                logger.warning("Rate limited (429). Retrying in %ds...", wait)
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = 2**attempt
                logger.warning(
                    "HubSpot server error (%d). Retrying in %ds...",
                    resp.status_code,
                    wait,
                )
                time.sleep(wait)
                continue

            # 4xx errors (except 429) — don't retry
            resp.raise_for_status()

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = 2**attempt
                logger.warning("Request failed: %s. Retrying in %ds...", e, wait)
                time.sleep(wait)
            else:
                raise

    raise RuntimeError(f"HubSpot API failed after {max_retries} retries")
