"""
Airtable API client — generic for all sync workflows.
Parameterized by table name, key field, sync view, and timestamp field names.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

from sync.core.config import (
    AIRTABLE_API_BASE,
    AIRTABLE_BASE_ID,
    AIRTABLE_BATCH_SIZE,
    AIRTABLE_COMMENTS_ENABLED,
    AIRTABLE_MAX_RETRIES,
    AIRTABLE_REQUEST_TIMEOUT,
    AIRTABLE_TOKEN,
)
from sync.core.utils import clean

logger = logging.getLogger(__name__)


class AirtableClient:
    """
    Generic client for reading and writing Airtable records.

    Parameterized by table name, key field, sync view, and timestamp fields
    so the same client works for Products, Vendors, Customers, etc.
    """

    def __init__(
        self,
        table_name: str = "Products",
        key_field: str = "SKU Trim (EDI)",
        key_field_writable: str | None = None,
        sync_view: str = "Airtable > Priority API Sync",
        timestamp_fields: dict[str, str] | None = None,
        base_id_override: str | None = None,
        token_override: str | None = None,
        field_id_map: dict[str, str] | None = None,
    ) -> None:
        """
        Args:
            table_name: Airtable table name (e.g., "Products", "Vendors").
            key_field: Airtable field used for lookups (may be formula/read-only).
            key_field_writable: Writable field for creating new records (e.g., "SKU").
                               Defaults to key_field if not set.
            sync_view: View name that filters records needing sync.
            timestamp_fields: Dict of timestamp field names:
                - "last_synced_to": e.g., "Last Synced to Priority"
                - "last_synced_from": e.g., "Last Synced from Priority"
                - "priority_udate": e.g., "Priority UDATE"
            base_id_override: Optional base ID for testing.
            token_override: Optional token for testing.
            field_id_map: Optional mapping of field name → field ID.
                         When provided, API calls use stable field IDs instead
                         of field names. Response parsing is unaffected (Airtable
                         returns field names by default).
        """
        self.table_name = table_name
        self.key_field = key_field
        self.key_field_writable = key_field_writable or key_field
        self.sync_view = sync_view
        self.ts = timestamp_fields or {
            "last_synced_to": "Last Synced to Priority",
            "last_synced_from": "Last Synced from Priority",
            "priority_udate": "Priority UDATE",
        }
        self._field_id_map = field_id_map

        self._base_id = base_id_override or AIRTABLE_BASE_ID
        token = token_override or AIRTABLE_TOKEN
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        self.session.timeout = AIRTABLE_REQUEST_TIMEOUT
        self._base_url = f"{AIRTABLE_API_BASE}/{self._base_id}/{quote(table_name)}"

    # ── Field ID helpers ─────────────────────────────────────────────────

    def _to_id(self, field_name: str) -> str:
        """Translate a field name to its stable field ID if available."""
        if self._field_id_map:
            return self._field_id_map.get(field_name, field_name)
        return field_name

    def _fields_to_ids(self, fields: dict[str, Any]) -> dict[str, Any]:
        """Translate all field-name keys in a dict to field IDs."""
        if not self._field_id_map:
            return fields
        return {self._to_id(k): v for k, v in fields.items()}

    # ── Read ─────────────────────────────────────────────────────────────

    def fetch_changed_records(
        self,
        fields_to_fetch: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch records from the sync view.
        This view only returns records where sync is needed.

        Args:
            fields_to_fetch: Airtable field names to include. If None, fetches all.

        Returns list of raw Airtable record dicts with 'id' and 'fields'.
        """
        records: list[dict[str, Any]] = []
        offset: str | None = None

        field_params = [("fields[]", self._to_id(f)) for f in fields_to_fetch] if fields_to_fetch else []

        for attempt in range(AIRTABLE_MAX_RETRIES):
            try:
                while True:
                    params = [("view", self.sync_view)] + field_params
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

                    # Handle rate limiting
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

    def fetch_records_from_view(
        self,
        view_name: str,
        fields: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch all records from a specific view (different from the sync view).
        Useful for pre-fetching related data from alternative views on the
        same table (e.g., bank details, shelf lives).

        Args:
            view_name: Airtable view name to fetch from.
            fields: Field names to include. If None, fetches all.

        Returns list of raw Airtable record dicts with 'id' and 'fields'.
        """
        records: list[dict[str, Any]] = []
        offset: str | None = None

        field_params = [("fields[]", self._to_id(f)) for f in fields] if fields else []

        for attempt in range(AIRTABLE_MAX_RETRIES):
            try:
                while True:
                    params = [("view", view_name)] + field_params
                    if offset:
                        params.append(("offset", offset))

                    logger.debug(
                        "Fetching from view '%s' (offset: %s)",
                        view_name,
                        offset or "start",
                    )

                    response = self.session.get(
                        self._base_url,
                        params=params,
                        timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

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
                    logger.debug("Retrieved %d records from view '%s'", len(new_records), view_name)

                    offset = data.get("offset")
                    if not offset:
                        break

                    time.sleep(0.2)

                break  # Success

            except requests.exceptions.RequestException as e:
                if attempt == AIRTABLE_MAX_RETRIES - 1:
                    logger.error(
                        "Failed to fetch from view '%s' after %d attempts: %s",
                        view_name,
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

    def fetch_record_by_key(
        self,
        key_value: str,
        fields_to_fetch: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch a single record by key, bypassing the sync view filter.
        Used for --sku testing so we can re-sync a specific record
        even if it's not currently flagged as needing sync.

        Args:
            key_value: Key value to fetch.
            fields_to_fetch: Optional custom field list.

        Returns list of raw Airtable record dicts (0 or 1 items).
        """
        field_params = [("fields[]", self._to_id(f)) for f in fields_to_fetch] if fields_to_fetch else []
        key_ref = self._to_id(self.key_field)
        formula = f'{{{key_ref}}}="{key_value}"'
        params = [("filterByFormula", formula)] + field_params

        for attempt in range(AIRTABLE_MAX_RETRIES):
            try:
                response = self.session.get(
                    self._base_url,
                    params=params,
                    timeout=AIRTABLE_REQUEST_TIMEOUT,
                )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 30))
                    logger.warning("Airtable rate limited. Waiting %ds...", retry_after)
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                data = response.json()
                records = data.get("records", [])
                logger.debug("Fetched %d records for key %s", len(records), key_value)
                return records

            except requests.exceptions.RequestException as e:
                if attempt == AIRTABLE_MAX_RETRIES - 1:
                    logger.error("Failed to fetch key %s: %s", key_value, e)
                    raise
                wait_time = 2 ** attempt
                logger.warning("Attempt %d failed, retrying in %ds: %s", attempt + 1, wait_time, e)
                time.sleep(wait_time)

        return []

    # ── Write ────────────────────────────────────────────────────────────

    def batch_update_timestamps(
        self,
        updates: list[dict[str, Any]],
    ) -> int:
        """
        Batch-update 'Last Synced to Priority', 'Priority UDATE', and
        optionally 'Sync Comments' fields.

        Args:
            updates: list of dicts with keys:
                - record_id: Airtable record ID
                - synced_at: ISO 8601 UTC datetime string
                - priority_udate: ISO 8601 UTC datetime string (or None)
                - sync_comment: optional string for the Sync Comments field

        Returns:
            Number of successfully updated records.
        """
        success_count = 0

        for i in range(0, len(updates), AIRTABLE_BATCH_SIZE):
            batch = updates[i : i + AIRTABLE_BATCH_SIZE]

            records_payload = []
            for update in batch:
                fields: dict[str, Any] = {}
                if update.get("synced_at"):
                    fields[self._to_id(self.ts["last_synced_to"])] = update["synced_at"]
                if update.get("priority_udate"):
                    fields[self._to_id(self.ts["priority_udate"])] = update["priority_udate"]
                if update.get("sync_comment") and self.ts.get("sync_comments"):
                    fields[self._to_id(self.ts["sync_comments"])] = update["sync_comment"]

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

            time.sleep(0.2)

        return success_count

    # ── P→A Read operations ───────────────────────────────────────────────

    def fetch_all_keys(
        self, fields_to_fetch: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Fetch all records from Airtable, indexed by key field.

        Used by P→A sync to compare existing records with Priority data.
        Paginated — fetches ALL records from the table.

        Args:
            fields_to_fetch: Airtable field names to include. Always includes key field.

        Returns:
            dict mapping key → {"record_id": str, "fields": dict}
        """
        # Ensure key field is always fetched
        fields = list(dict.fromkeys(
            [self.key_field] + (fields_to_fetch or [])
        ))
        field_params = [("fields[]", self._to_id(f)) for f in fields]

        records: list[dict[str, Any]] = []
        offset: str | None = None

        for attempt in range(AIRTABLE_MAX_RETRIES):
            try:
                while True:
                    params = list(field_params)  # Copy
                    if offset:
                        params.append(("offset", offset))

                    logger.debug(
                        "Fetching all Airtable keys (offset: %s)",
                        offset or "start",
                    )

                    response = self.session.get(
                        self._base_url,
                        params=params,
                        timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

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

                    time.sleep(0.2)

                break  # Success

            except requests.exceptions.RequestException as e:
                if attempt == AIRTABLE_MAX_RETRIES - 1:
                    logger.error(
                        "Failed to fetch all Airtable keys after %d attempts: %s",
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

        # Index by key
        by_key: dict[str, dict[str, Any]] = {}
        for record in records:
            fields_data = record.get("fields", {})
            key = clean(fields_data.get(self.key_field))
            if key:
                by_key[key] = {
                    "record_id": record["id"],
                    "fields": fields_data,
                }

        logger.info("Fetched %d Airtable records (%d with key)", len(records), len(by_key))
        return by_key

    # ── Linked Record Lookups ─────────────────────────────────────────────

    def fetch_linked_record_map(
        self,
        table_id: str,
        match_field_id: str,
    ) -> dict[str, str]:
        """
        Fetch all records from a target Airtable table and build a map of
        {match_field_value: record_id}.

        Used for P→A linked record resolution: e.g., fetch the Vendors table
        and build {"4157": "recXXXX", "10042": "recYYYY"} so we can convert
        a Priority vendor code to an Airtable record ID.
        """
        url = f"{AIRTABLE_API_BASE}/{self._base_id}/{table_id}"
        result: dict[str, str] = {}
        offset: str | None = None

        while True:
            params: dict[str, str] = {
                "fields[]": match_field_id,
            }
            if offset:
                params["offset"] = offset

            try:
                response = self.session.get(
                    url, params=params, timeout=AIRTABLE_REQUEST_TIMEOUT,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.error("Failed to fetch linked record table %s: %s", table_id, exc)
                return result

            data = response.json()
            for record in data.get("records", []):
                rec_id = record["id"]
                fields = record.get("fields", {})
                # The API returns data keyed by field ID when we request by ID
                # but may also return by name — handle both
                match_val = None
                for v in fields.values():
                    match_val = v
                    break
                if match_val is not None:
                    match_val_str = str(match_val).strip()
                    if match_val_str:
                        result[match_val_str] = rec_id

            offset = data.get("offset")
            if not offset:
                break

        logger.info(
            "Loaded %d linked records from table %s", len(result), table_id,
        )
        return result

    def create_linked_records(
        self,
        table_id: str,
        records: list[dict[str, Any]],
        codes: list[str],
    ) -> dict[str, str]:
        """
        Create new records in a target Airtable table and return a map of
        {code: record_id} for the created records.

        Used to auto-create missing linked records (e.g., create a Vendor
        record so a product's Preferred Vendor linked record can be populated).

        Args:
            table_id: Target Airtable table ID.
            records: List of dicts, each with key "fields" containing
                    {field_id: value} mappings.  Must be 1:1 with codes.
            codes: List of source codes corresponding to each record.
                  Used as keys in the returned map (avoids parsing Airtable
                  response which returns field names, not IDs).

        Returns:
            Dict mapping source codes to newly created Airtable record IDs.
        """
        url = f"{AIRTABLE_API_BASE}/{self._base_id}/{table_id}"
        created_map: dict[str, str] = {}

        for i in range(0, len(records), AIRTABLE_BATCH_SIZE):
            batch = records[i : i + AIRTABLE_BATCH_SIZE]
            batch_codes = codes[i : i + AIRTABLE_BATCH_SIZE]
            payload = {"records": batch, "typecast": True}

            for attempt in range(AIRTABLE_MAX_RETRIES):
                try:
                    response = self.session.post(
                        url, json=payload, timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 30))
                        logger.warning(
                            "Airtable rate limited on linked record create. "
                            "Waiting %ds...", retry_after,
                        )
                        time.sleep(retry_after)
                        continue

                    if response.status_code == 422:
                        try:
                            error_msg = response.json().get(
                                "error", {},
                            ).get("message", response.text)
                        except Exception:
                            error_msg = response.text
                        logger.error(
                            "Airtable rejected linked record create (422): %s",
                            error_msg,
                        )

                    response.raise_for_status()

                    # Map codes to created record IDs using 1:1 correspondence
                    resp_data = response.json()
                    created_records = resp_data.get("records", [])
                    for code, rec in zip(batch_codes, created_records):
                        created_map[code] = rec["id"]

                    break

                except requests.RequestException as e:
                    if attempt == AIRTABLE_MAX_RETRIES - 1:
                        logger.error(
                            "Failed to create linked records in table %s "
                            "(batch starting at %d): %s",
                            table_id, i, e,
                        )
                    else:
                        wait_time = 2 ** attempt
                        logger.warning(
                            "Linked record create attempt %d failed, "
                            "retrying in %ds: %s",
                            attempt + 1, wait_time, e,
                        )
                        time.sleep(wait_time)

            time.sleep(0.2)

        if created_map:
            logger.info(
                "Created %d linked records in table %s", len(created_map), table_id,
            )
        return created_map

    # ── Cross-table batch operations ─────────────────────────────────────

    def batch_create_to_table(
        self,
        table_id: str,
        records: list[dict[str, Any]],
    ) -> int:
        """
        Create records in any Airtable table (not just the configured one).

        Uses ``typecast: true`` to auto-create missing singleSelect options.

        Args:
            table_id: Target Airtable table ID.
            records: List of dicts, each with key ``"fields"`` containing
                     ``{field_name_or_id: value}`` mappings.

        Returns:
            Number of successfully created records.
        """
        url = f"{AIRTABLE_API_BASE}/{self._base_id}/{table_id}"
        success_count = 0

        for i in range(0, len(records), AIRTABLE_BATCH_SIZE):
            batch = records[i : i + AIRTABLE_BATCH_SIZE]
            payload = {"records": batch, "typecast": True}

            for attempt in range(AIRTABLE_MAX_RETRIES):
                try:
                    response = self.session.post(
                        url, json=payload, timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 30))
                        logger.warning(
                            "Rate limited on table %s create. Waiting %ds...",
                            table_id, retry_after,
                        )
                        time.sleep(retry_after)
                        continue

                    if response.status_code == 422:
                        try:
                            error_msg = response.json().get(
                                "error", {},
                            ).get("message", response.text)
                        except Exception:
                            error_msg = response.text
                        logger.error(
                            "Airtable rejected create on table %s (422): %s",
                            table_id, error_msg,
                        )

                    response.raise_for_status()
                    success_count += len(batch)
                    break

                except requests.RequestException as e:
                    if attempt == AIRTABLE_MAX_RETRIES - 1:
                        logger.error(
                            "Failed to create records in table %s "
                            "(batch starting at %d): %s",
                            table_id, i, e,
                        )
                    else:
                        wait_time = 2 ** attempt
                        logger.warning(
                            "Table %s create attempt %d failed, retrying in %ds: %s",
                            table_id, attempt + 1, wait_time, e,
                        )
                        time.sleep(wait_time)

            time.sleep(0.2)

        if success_count:
            logger.info("Created %d records in table %s", success_count, table_id)
        return success_count

    def batch_update_to_table(
        self,
        table_id: str,
        updates: list[dict[str, Any]],
    ) -> int:
        """
        Update records in any Airtable table (not just the configured one).

        Uses ``typecast: true`` to auto-create missing singleSelect options.

        Args:
            table_id: Target Airtable table ID.
            updates: List of dicts, each with ``"id"`` (record ID) and
                     ``"fields"`` (changed field values).

        Returns:
            Number of successfully updated records.
        """
        url = f"{AIRTABLE_API_BASE}/{self._base_id}/{table_id}"
        success_count = 0

        for i in range(0, len(updates), AIRTABLE_BATCH_SIZE):
            batch = updates[i : i + AIRTABLE_BATCH_SIZE]
            payload = {"records": batch, "typecast": True}

            for attempt in range(AIRTABLE_MAX_RETRIES):
                try:
                    response = self.session.patch(
                        url, json=payload, timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 30))
                        logger.warning(
                            "Rate limited on table %s update. Waiting %ds...",
                            table_id, retry_after,
                        )
                        time.sleep(retry_after)
                        continue

                    if response.status_code == 422:
                        try:
                            error_msg = response.json().get(
                                "error", {},
                            ).get("message", response.text)
                        except Exception:
                            error_msg = response.text
                        logger.error(
                            "Airtable rejected update on table %s (422): %s",
                            table_id, error_msg,
                        )

                    response.raise_for_status()
                    success_count += len(batch)
                    break

                except requests.RequestException as e:
                    if attempt == AIRTABLE_MAX_RETRIES - 1:
                        logger.error(
                            "Failed to update records in table %s "
                            "(batch starting at %d): %s",
                            table_id, i, e,
                        )
                    else:
                        wait_time = 2 ** attempt
                        logger.warning(
                            "Table %s update attempt %d failed, retrying in %ds: %s",
                            table_id, attempt + 1, wait_time, e,
                        )
                        time.sleep(wait_time)

            time.sleep(0.2)

        if success_count:
            logger.info("Updated %d records in table %s", success_count, table_id)
        return success_count

    # ── P→A Write operations ──────────────────────────────────────────────

    def batch_create_records(
        self,
        records: list[dict[str, Any]],
    ) -> int:
        """
        Create new Airtable records in batches of 10.

        Args:
            records: list of dicts, each with key "fields" containing
                    the Airtable field name → value mapping.

        Returns:
            Number of successfully created records.
        """
        success_count = 0

        for i in range(0, len(records), AIRTABLE_BATCH_SIZE):
            batch = records[i : i + AIRTABLE_BATCH_SIZE]
            # Translate field name keys to IDs if available
            id_batch = [
                {"fields": self._fields_to_ids(r["fields"])}
                for r in batch
            ]
            # typecast=True lets Airtable auto-create singleSelect options
            payload = {"records": id_batch, "typecast": True}

            for attempt in range(AIRTABLE_MAX_RETRIES):
                try:
                    response = self.session.post(
                        self._base_url,
                        json=payload,
                        timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 30))
                        logger.warning(
                            "Airtable rate limited on create. Waiting %ds...",
                            retry_after,
                        )
                        time.sleep(retry_after)
                        continue

                    if response.status_code == 422:
                        try:
                            error_body = response.json()
                            error_msg = error_body.get("error", {}).get("message", response.text)
                        except Exception:
                            error_msg = response.text
                        logger.error(
                            "Airtable rejected create (422): %s", error_msg,
                        )

                    response.raise_for_status()
                    success_count += len(batch)
                    logger.debug(
                        "Created %d records (batch %d/%d)",
                        len(batch),
                        (i // AIRTABLE_BATCH_SIZE) + 1,
                        (len(records) + AIRTABLE_BATCH_SIZE - 1) // AIRTABLE_BATCH_SIZE,
                    )
                    break

                except requests.exceptions.RequestException as e:
                    if attempt == AIRTABLE_MAX_RETRIES - 1:
                        logger.error(
                            "Failed to create Airtable records (batch starting at %d): %s",
                            i,
                            e,
                        )
                    else:
                        wait_time = 2 ** attempt
                        logger.warning(
                            "Create attempt %d failed, retrying in %ds: %s",
                            attempt + 1,
                            wait_time,
                            e,
                        )
                        time.sleep(wait_time)

            time.sleep(0.2)

        return success_count

    def batch_update_records(
        self,
        updates: list[dict[str, Any]],
    ) -> int:
        """
        Batch PATCH existing Airtable records in batches of 10.

        Args:
            updates: list of dicts, each with keys:
                - "id": Airtable record ID
                - "fields": dict of Airtable field name → new value

        Returns:
            Number of successfully updated records.
        """
        success_count = 0

        for i in range(0, len(updates), AIRTABLE_BATCH_SIZE):
            batch = updates[i : i + AIRTABLE_BATCH_SIZE]
            # Translate field name keys to IDs if available
            id_batch = [
                {"id": u["id"], "fields": self._fields_to_ids(u["fields"])}
                for u in batch
            ]
            # typecast=True lets Airtable auto-create singleSelect options
            payload = {"records": id_batch, "typecast": True}

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
                            "Airtable rate limited on update. Waiting %ds...",
                            retry_after,
                        )
                        time.sleep(retry_after)
                        continue

                    if response.status_code == 422:
                        try:
                            error_body = response.json()
                            error_msg = error_body.get("error", {}).get("message", response.text)
                        except Exception:
                            error_msg = response.text
                        logger.error(
                            "Airtable rejected update (422): %s", error_msg,
                        )

                    response.raise_for_status()
                    success_count += len(batch)
                    logger.debug(
                        "Updated %d records (batch %d/%d)",
                        len(batch),
                        (i // AIRTABLE_BATCH_SIZE) + 1,
                        (len(updates) + AIRTABLE_BATCH_SIZE - 1) // AIRTABLE_BATCH_SIZE,
                    )
                    break

                except requests.exceptions.RequestException as e:
                    if attempt == AIRTABLE_MAX_RETRIES - 1:
                        logger.error(
                            "Failed to update Airtable records (batch starting at %d): %s",
                            i,
                            e,
                        )
                    else:
                        wait_time = 2 ** attempt
                        logger.warning(
                            "Update attempt %d failed, retrying in %ds: %s",
                            attempt + 1,
                            wait_time,
                            e,
                        )
                        time.sleep(wait_time)

            time.sleep(0.2)

        return success_count

    def batch_update_p2a_timestamps(
        self,
        updates: list[dict[str, Any]],
    ) -> int:
        """
        Batch-update 'Last Synced from Priority', 'Priority UDATE', and
        optionally 'Sync Comments' fields.

        Used after P→A sync to mark records as recently synced from Priority.
        This timestamp is critical for loop prevention.

        Args:
            updates: list of dicts with keys:
                - record_id: Airtable record ID
                - synced_at: ISO 8601 UTC datetime string
                - priority_udate: ISO 8601 UTC datetime string (or None)
                - sync_comment: optional string for the Sync Comments field

        Returns:
            Number of successfully updated records.
        """
        success_count = 0

        for i in range(0, len(updates), AIRTABLE_BATCH_SIZE):
            batch = updates[i : i + AIRTABLE_BATCH_SIZE]

            records_payload = []
            for update in batch:
                fields: dict[str, Any] = {
                    self._to_id(self.ts["last_synced_from"]): update["synced_at"],
                }
                if update.get("priority_udate"):
                    fields[self._to_id(self.ts["priority_udate"])] = update["priority_udate"]
                if update.get("sync_comment") and self.ts.get("sync_comments"):
                    fields[self._to_id(self.ts["sync_comments"])] = update["sync_comment"]

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
                            "Airtable rate limited on P→A timestamp update. Waiting %ds...",
                            retry_after,
                        )
                        time.sleep(retry_after)
                        continue

                    if response.status_code == 422:
                        try:
                            error_body = response.json()
                            error_msg = error_body.get("error", {}).get(
                                "message", response.text,
                            )
                        except Exception:
                            error_msg = response.text
                        logger.error(
                            "Airtable rejected P→A timestamp update (422): %s",
                            error_msg,
                        )
                        logger.error(
                            "Payload sent: %s",
                            payload,
                        )

                    response.raise_for_status()
                    success_count += len(batch)
                    logger.debug(
                        "Updated P→A timestamps for batch %d/%d (%d records)",
                        (i // AIRTABLE_BATCH_SIZE) + 1,
                        (len(updates) + AIRTABLE_BATCH_SIZE - 1) // AIRTABLE_BATCH_SIZE,
                        len(batch),
                    )
                    break

                except requests.exceptions.RequestException as e:
                    if attempt == AIRTABLE_MAX_RETRIES - 1:
                        logger.error(
                            "Failed to update P→A timestamps (batch starting at %d): %s",
                            i,
                            e,
                        )
                    else:
                        wait_time = 2 ** attempt
                        logger.warning(
                            "P→A timestamp update attempt %d failed, retrying in %ds: %s",
                            attempt + 1,
                            wait_time,
                            e,
                        )
                        time.sleep(wait_time)

            time.sleep(0.2)

        return success_count

    # ── Record Comments ───────────────────────────────────────────────────

    def post_record_comments(
        self,
        comments: list[dict[str, str]],
    ) -> int:
        """
        Post a comment to each record's comment thread.

        The Airtable Comments API is per-record (no batching).
        Comment failures are logged as warnings but never raise.

        Args:
            comments: list of dicts with keys:
                - record_id: Airtable record ID (e.g., "recXXX")
                - text: Comment text to post

        Returns:
            Number of successfully posted comments.
        """
        if not AIRTABLE_COMMENTS_ENABLED:
            logger.debug(
                "Airtable comments disabled — skipping %d comments.",
                len(comments),
            )
            return 0

        success_count = 0

        for item in comments:
            record_id = item.get("record_id", "")
            text = item.get("text", "")

            if not record_id or not text:
                continue

            url = f"{self._base_url}/{record_id}/comments"
            payload = {"text": text}

            try:
                response = self.session.post(
                    url,
                    json=payload,
                    timeout=AIRTABLE_REQUEST_TIMEOUT,
                )

                if response.status_code == 429:
                    retry_after = int(
                        response.headers.get("Retry-After", 30)
                    )
                    logger.warning(
                        "Airtable rate limited on comment POST. "
                        "Waiting %ds...",
                        retry_after,
                    )
                    time.sleep(retry_after)
                    # Retry once after waiting
                    response = self.session.post(
                        url,
                        json=payload,
                        timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

                response.raise_for_status()
                success_count += 1

            except requests.exceptions.RequestException as e:
                logger.warning(
                    "Failed to post comment on record %s: %s",
                    record_id,
                    e,
                )

            # Rate limit: ~4 req/s (under 5 req/s limit)
            time.sleep(0.25)

        return success_count
