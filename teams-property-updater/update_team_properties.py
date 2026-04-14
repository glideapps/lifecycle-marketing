"""
update_team_properties.py
-------------------------
Reads Teams custom object property definitions from a Google Sheet and updates
each property's description in HubSpot via the CRM Properties API.

Expected sheet columns (row 1 is a header and is skipped):
  A: Display name       (ignored – for human reference only)
  B: Internal name      (HubSpot property key, e.g. "team_region")
  C: Description        (the text to write into HubSpot)

Required environment variables:
  HUBSPOT_API_KEY              – HubSpot private app token
                                 (must have crm.schemas.custom.write scope)
  GOOGLE_SERVICE_ACCOUNT_JSON  – service account key as a JSON string or file path
  TEAMS_SPREADSHEET_ID         – ID of the Google Sheet containing property definitions
  HUBSPOT_TEAMS_OBJECT_TYPE    – HubSpot object type identifier for the Teams custom
                                 object (e.g. "p_12345678_teams" or "2-12345678")
  TEAMS_SHEET_NAME             – (optional) tab name to read; defaults to first sheet
"""

import json
import logging
import os
import sys
import time

import gspread
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

GOOGLE_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

HUBSPOT_BASE_URL = "https://api.hubapi.com"


def _hubspot_headers() -> dict:
    token = os.environ.get("HUBSPOT_API_KEY", "")
    if not token:
        raise RuntimeError("HUBSPOT_API_KEY is not set")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _teams_object_type() -> str:
    object_type = os.environ.get("HUBSPOT_TEAMS_OBJECT_TYPE", "")
    if not object_type:
        raise RuntimeError(
            "HUBSPOT_TEAMS_OBJECT_TYPE is not set. "
            "Set it to the fully qualified name of the Teams custom object "
            "(e.g. 'p_12345678_teams') or its numeric type ID (e.g. '2-12345678'). "
            "Find this in HubSpot under Settings → Objects → Custom Objects."
        )
    return object_type


def update_team_property(property_name: str, description: str) -> None:
    """PATCH /crm/v3/properties/{objectType}/{propertyName} with the given description."""
    object_type = _teams_object_type()
    url = f"{HUBSPOT_BASE_URL}/crm/v3/properties/{object_type}/{property_name}"
    payload = {"description": description}

    for attempt in range(1, 5):
        resp = requests.patch(url, json=payload, headers=_hubspot_headers(), timeout=15)

        if resp.status_code == 200:
            log.info("  updated: %s", property_name)
            return

        if resp.status_code == 429:
            wait = 2 ** attempt
            log.warning("  rate-limited – waiting %ss before retry (%s/4)", wait, attempt)
            time.sleep(wait)
            continue

        # Non-retryable error
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise RuntimeError(f"HTTP {resp.status_code} – {detail}")

    raise RuntimeError("exceeded retry limit after rate-limiting")


def _load_service_account() -> dict:
    sa_env = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not sa_env:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")
    if sa_env.strip().startswith("{"):
        return json.loads(sa_env)
    with open(sa_env) as fh:
        return json.load(fh)


def read_properties_from_sheet() -> list[tuple[str, str]]:
    """Return a list of (internal_name, description) pairs from the sheet."""
    spreadsheet_id = os.environ.get("TEAMS_SPREADSHEET_ID", "")
    if not spreadsheet_id:
        raise RuntimeError("TEAMS_SPREADSHEET_ID is not set")

    sheet_name = os.environ.get("TEAMS_SHEET_NAME", "")

    sa_info = _load_service_account()
    creds = Credentials.from_service_account_info(sa_info, scopes=GOOGLE_SCOPES)
    gc = gspread.Client(auth=creds)

    spreadsheet = gc.open_by_key(spreadsheet_id)
    ws = spreadsheet.worksheet(sheet_name) if sheet_name else spreadsheet.sheet1

    rows = ws.get_all_values()
    if not rows:
        log.warning("Sheet is empty – nothing to process")
        return []

    data_rows = rows[1:]  # skip header row
    results = []
    for i, row in enumerate(data_rows, start=2):
        while len(row) < 3:
            row.append("")

        internal_name = row[1].strip()
        description = row[2].strip()

        if not internal_name:
            log.debug("Row %d: no internal name – skipping", i)
            continue
        if not description:
            log.warning("Row %d (%s): description is empty – skipping", i, internal_name)
            continue

        results.append((internal_name, description))

    return results


def main() -> None:
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"
    if dry_run:
        log.info("DRY RUN – no changes will be written to HubSpot")

    log.info("Teams object type: %s", os.environ.get("HUBSPOT_TEAMS_OBJECT_TYPE", "(not set)"))
    log.info("Reading Teams property definitions from Google Sheet...")
    properties = read_properties_from_sheet()

    if not properties:
        log.info("No properties to update – exiting")
        sys.exit(0)

    log.info("Found %d properties to update", len(properties))

    updated = 0
    skipped = 0
    for internal_name, description in properties:
        if dry_run:
            log.info("  [DRY RUN] would update: %s → %r", internal_name, description[:80])
            updated += 1
            continue
        try:
            update_team_property(internal_name, description)
            updated += 1
        except Exception as exc:
            log.error("Unexpected error for %s: %s", internal_name, exc)
            skipped += 1

    log.info("Done. Updated: %d  |  Errors: %d", updated, skipped)
    if skipped:
        sys.exit(1)


if __name__ == "__main__":
    main()
