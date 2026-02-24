"""
Airtable API client for product sync.
Patterns adapted from tools/10. Script for Product All v8.py (lines 98-157).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

from sync.config import (
    AIRTABLE_API_BASE,
    AIRTABLE_BASE_ID,
    AIRTABLE_BATCH_SIZE,
    AIRTABLE_FIELD_LAST_SYNCED,
    AIRTABLE_FIELD_PRIORITY_UDATE,
    AIRTABLE_MAX_RETRIES,
    AIRTABLE_PRODUCTS_TABLE_NAME,
    AIRTABLE_REQUEST_TIMEOUT,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TOKEN,
)
from sync.field_mapping import AIRTABLE_FIELDS_TO_FETCH

logger = logging.getLogger(__name__)


class AirtableClient:
    """Client for reading and writing Airtable product records."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {AIRTABLE_TOKEN}",
            "Content-Type": "application/json",
        })
        self.session.timeout = AIRTABLE_REQUEST_TIMEOUT
        self._base_url = f"{AIRTABLE_API_BASE}/{AIRTABLE_BASE_ID}/{quote(AIRTABLE_PRODUCTS_TABLE_NAME)}"

    # ── Read ─────────────────────────────────────────────────────────────

    def fetch_changed_records(self) -> list[dict[str, Any]]:
        """
        Fetch records from the 'Airtable > Priority API Sync' view.
        This view only returns records where Priority Sync Needed = "Yes".

        Uses explicit field selection to fetch only the 31 fields we need
        (not all 400+ columns).

        Returns list of raw Airtable record dicts with 'id' and 'fields'.
        """
        records: list[dict[str, Any]] = []
        offset: str | None = None

        # Build params with explicit field list
        base_params: dict[str, Any] = {"view": AIRTABLE_SYNC_VIEW}
        for field_name in AIRTABLE_FIELDS_TO_FETCH:
            base_params.setdefault("fields[]", [])
            # requests handles list params correctly
        # Use a list of tuples for repeated 'fields[]' params
        field_params = [("fields[]", f) for f in AIRTABLE_FIELDS_TO_FETCH]

        for attempt in range(AIRTABLE_MAX_RETRIES):
            try:
                while True:
                    params = [("view", AIRTABLE_SYNC_VIEW)] + field_params
                    if offset:
                        params.append(("offset", offset))

                    logger.debug(
                        "Fetching Airtable records (offset: %s)",
                        offset or "start",
                    )

                    response = self.session.get(
                        self._base_url,
                        params=params,
                        timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

                    # Handle rate limiting (from v8 script pattern)
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 30))
                        logger.warning(
                            "Airtable rate limited. Waiting %ds...", retry_after
                        )
                        time.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    data = response.json()

                    new_records = data.get("records", [])
                    records.extend(new_records)
                    logger.debug("Retrieved %d records", len(new_records))

                    offset = data.get("offset")
                    if not offset:
                        break

                    # Small delay to be respectful to the API
                    time.sleep(0.2)

                break  # Success, exit retry loop

            except requests.exceptions.RequestException as e:
                if attempt == AIRTABLE_MAX_RETRIES - 1:
                    logger.error(
                        "Failed to fetch Airtable records after %d attempts: %s",
                        AIRTABLE_MAX_RETRIES,
                        e,
                    )
                    raise
                wait_time = 2 ** attempt
                logger.warning(
                    "Attempt %d failed, retrying in %ds: %s",
                    attempt + 1,
                    wait_time,
                    e,
                )
                time.sleep(wait_time)

        return records

    # ── Write ────────────────────────────────────────────────────────────

    def batch_update_timestamps(
        self,
        updates: list[dict[str, Any]],
    ) -> int:
        """
        Batch-update 'Last Synced to Priority' and 'Priority UDATE' timestamps.

        Args:
            updates: list of dicts with keys:
                - record_id: Airtable record ID
                - synced_at: ISO 8601 UTC datetime string
                - priority_udate: ISO 8601 UTC datetime string (or None)

        Returns:
            Number of successfully updated records.
        """
        success_count = 0

        # Process in batches of AIRTABLE_BATCH_SIZE (10)
        for i in range(0, len(updates), AIRTABLE_BATCH_SIZE):
            batch = updates[i : i + AIRTABLE_BATCH_SIZE]

            records_payload = []
            for update in batch:
                fields: dict[str, Any] = {
                    AIRTABLE_FIELD_LAST_SYNCED: update["synced_at"],
                }
                if update.get("priority_udate"):
                    fields[AIRTABLE_FIELD_PRIORITY_UDATE] = update["priority_udate"]

                records_payload.append({
                    "id": update["record_id"],
                    "fields": fields,
                })

            payload = {"records": records_payload}

            for attempt in range(AIRTABLE_MAX_RETRIES):
                try:
                    response = self.session.patch(
                        self._base_url,
                        json=payload,
                        timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 30))
                        logger.warning(
                            "Airtable rate limited on timestamp update. Waiting %ds...",
                            retry_after,
                        )
                        time.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    success_count += len(batch)
                    logger.debug(
                        "Updated timestamps for batch %d/%d (%d records)",
                        (i // AIRTABLE_BATCH_SIZE) + 1,
                        (len(updates) + AIRTABLE_BATCH_SIZE - 1) // AIRTABLE_BATCH_SIZE,
                        len(batch),
                    )
                    break

                except requests.exceptions.RequestException as e:
                    if attempt == AIRTABLE_MAX_RETRIES - 1:
                        logger.error(
                            "Failed to update Airtable timestamps (batch starting at %d): %s",
                            i,
                            e,
                        )
                    else:
                        wait_time = 2 ** attempt
                        logger.warning(
                            "Timestamp update attempt %d failed, retrying in %ds: %s",
                            attempt + 1,
                            wait_time,
                            e,
                        )
                        time.sleep(wait_time)

            # Small delay between batches
            time.sleep(0.2)

        return success_count
