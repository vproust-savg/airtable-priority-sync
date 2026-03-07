"""
Priority ERP REST API client — generic for all entity types.
Parameterized by entity name and key field.
API docs: https://prioritysoftware.github.io/restapi/
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any

import requests

from sync.core.config import (
    PRIORITY_API_URL,
    PRIORITY_MAX_CALLS_PER_MINUTE,
    PRIORITY_MAX_RETRIES,
    PRIORITY_PAGE_SIZE,
    PRIORITY_PASS,
    PRIORITY_REQUEST_TIMEOUT,
    PRIORITY_USER,
)
from sync.core.utils import values_equal

logger = logging.getLogger(__name__)


def extract_priority_error(response: requests.Response) -> str:
    """
    Extract a human-readable error message from Priority's response.
    Priority returns errors in a JSON-wrapped XML format like:
    {"FORM": {"InterfaceErrors": {"text": "Line 1- error message here"}}}
    """
    try:
        data = response.json()
        # Standard OData error format
        if "error" in data:
            return data["error"].get("message", response.text[:200])
        # Priority's InterfaceErrors format
        if "FORM" in data:
            form = data["FORM"]
            if "InterfaceErrors" in form:
                return form["InterfaceErrors"].get("text", response.text[:200])
        return response.text[:200]
    except Exception:
        return response.text[:200] if response.text else f"HTTP {response.status_code}"


class PriorityClient:
    """
    Generic client for reading and writing Priority ERP records.

    Parameterized by entity name and key field so the same client works
    for LOGPART, FNCPART, PRDPART, SUPPLIERS, CUSTOMERS, etc.
    """

    def __init__(
        self,
        entity: str = "LOGPART",
        key_field: str = "PARTNAME",
        use_filter_lookup: bool = False,
        api_url_override: str | None = None,
    ) -> None:
        self.entity = entity
        self.key_field = key_field
        # Some entities (FNCPART, FNCSUP) use an internal integer key in the URL,
        # not PARTNAME/SUPNAME. For those, use $filter for lookups instead.
        self.use_filter_lookup = use_filter_lookup
        # Allow overriding API URL for environment switching (sandbox/uat/production)
        self.api_url = api_url_override or PRIORITY_API_URL
        self.session = requests.Session()
        self.session.auth = (PRIORITY_USER, PRIORITY_PASS)
        self.session.headers.update({
            "Content-Type": "application/json",
            "IEEE754Compatible": "true",
        })
        # Rolling window of request timestamps for rate limiting
        self._request_times: deque[float] = deque()

    # ── Rate Limiting ────────────────────────────────────────────────────

    def _throttle(self) -> None:
        """
        Ensure we stay under PRIORITY_MAX_CALLS_PER_MINUTE.
        Uses a rolling 60-second window of request timestamps.
        """
        now = time.monotonic()

        # Remove timestamps older than 60 seconds
        while self._request_times and (now - self._request_times[0]) > 60:
            self._request_times.popleft()

        # If at limit, sleep until the oldest request expires
        if len(self._request_times) >= PRIORITY_MAX_CALLS_PER_MINUTE:
            sleep_time = 60 - (now - self._request_times[0]) + 0.1
            if sleep_time > 0:
                logger.debug("Rate limit: sleeping %.1fs", sleep_time)
                time.sleep(sleep_time)

        self._request_times.append(time.monotonic())

    # ── HTTP helpers ─────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        url: str,
        json_body: dict[str, Any] | None = None,
        allow_404: bool = False,
    ) -> requests.Response | None:
        """
        Execute an HTTP request with retry logic and rate limiting.

        Args:
            method: GET, POST, or PATCH
            url: Full URL
            json_body: JSON payload for POST/PATCH
            allow_404: If True, return None for 404 instead of raising

        Returns:
            Response object, or None if 404 and allow_404=True
        """
        for attempt in range(PRIORITY_MAX_RETRIES):
            self._throttle()

            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    json=json_body,
                    timeout=PRIORITY_REQUEST_TIMEOUT,
                )

                # Handle 404
                if response.status_code == 404 and allow_404:
                    return None

                # Handle rate limiting
                if response.status_code == 429:
                    wait = min(60, 2 ** (attempt + 2))
                    logger.warning(
                        "Priority rate limited (429). Waiting %ds...", wait
                    )
                    time.sleep(wait)
                    continue

                # Handle server errors with retry
                if response.status_code >= 500:
                    if attempt < PRIORITY_MAX_RETRIES - 1:
                        wait = 2 ** (attempt + 1)
                        logger.warning(
                            "Priority server error (%d). Retrying in %ds...",
                            response.status_code,
                            wait,
                        )
                        time.sleep(wait)
                        continue

                # For client errors (400-499), raise with Priority's error message
                if 400 <= response.status_code < 500:
                    error_msg = extract_priority_error(response)
                    logger.error(
                        "Priority %d error for %s %s: %s",
                        response.status_code,
                        method,
                        url,
                        error_msg,
                    )
                    raise requests.HTTPError(
                        error_msg, response=response
                    )

                response.raise_for_status()
                return response

            except requests.exceptions.ConnectionError as e:
                if attempt < PRIORITY_MAX_RETRIES - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "Connection error, retrying in %ds: %s", wait, e
                    )
                    time.sleep(wait)
                else:
                    raise

            except requests.exceptions.Timeout as e:
                if attempt < PRIORITY_MAX_RETRIES - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "Timeout, retrying in %ds: %s", wait, e
                    )
                    time.sleep(wait)
                else:
                    raise

        return None

    # ── Read operations ──────────────────────────────────────────────────

    def fetch_all_keys(self) -> set[str]:
        """
        Fetch all existing key values from Priority via paginated GET.
        Uses $select={key_field} for minimal payload.

        Returns:
            Set of all key values in Priority.
        """
        keys: set[str] = set()
        skip = 0

        while True:
            url = (
                f"{self.api_url}{self.entity}"
                f"?$select={self.key_field}"
                f"&$top={PRIORITY_PAGE_SIZE}"
                f"&$skip={skip}"
            )

            logger.debug("Fetching %s keys (skip=%d)", self.entity, skip)
            response = self._request("GET", url)
            if response is None:
                break

            data = response.json()
            records = data.get("value", [])

            if not records:
                break

            for record in records:
                key = record.get(self.key_field, "").strip()
                if key:
                    keys.add(key)

            logger.debug(
                "Fetched %d keys (total so far: %d)",
                len(records),
                len(keys),
            )

            # If we got fewer than page size, we're done
            if len(records) < PRIORITY_PAGE_SIZE:
                break

            skip += PRIORITY_PAGE_SIZE

        # Warn if we hit Priority's MAXAPILINES limit (default 2000)
        if len(keys) >= 2000 and len(keys) % PRIORITY_PAGE_SIZE == 0:
            logger.warning(
                "Loaded exactly %d keys — may have hit Priority MAXAPILINES "
                "limit. Records beyond this will use 409 fallback.",
                len(keys),
            )

        return keys

    def fetch_changed_records(
        self,
        since_udate: str | None = None,
        select_fields: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch records from Priority, optionally filtered by UDATE.

        Used by the P→A sync to get changed records.
        - If since_udate is set, only returns records updated after that timestamp.
        - If since_udate is None (first run), returns ALL records.
        - Uses $select to minimize payload (only mapped fields).

        Args:
            since_udate: ISO timestamp string. Records with UDATE > this are returned.
                         None means fetch all records.
            select_fields: List of Priority field names to fetch (for $select).
                          Always includes key_field. If None, no $select (all fields).

        Returns:
            List of record dicts from Priority.
        """
        results: list[dict[str, Any]] = []
        skip = 0

        # Build $select
        select_clause = ""
        if select_fields:
            # Ensure key_field is always included
            fields = list(dict.fromkeys([self.key_field] + select_fields))
            select_clause = f"$select={','.join(fields)}"

        # Build $filter
        filter_clause = ""
        if since_udate:
            filter_clause = f"$filter=UDATE gt '{since_udate}'"

        while True:
            # Build URL with OData params
            params = [f"$top={PRIORITY_PAGE_SIZE}", f"$skip={skip}"]
            if select_clause:
                params.append(select_clause)
            if filter_clause:
                params.append(filter_clause)

            url = f"{self.api_url}{self.entity}?{'&'.join(params)}"

            logger.debug(
                "Fetching %s records (skip=%d, filter=%s)",
                self.entity,
                skip,
                "UDATE" if since_udate else "none",
            )
            response = self._request("GET", url)
            if response is None:
                break

            data = response.json()
            records = data.get("value", [])

            if not records:
                break

            results.extend(records)
            logger.debug(
                "Fetched %d records (total so far: %d)",
                len(records),
                len(results),
            )

            # If fewer than page size, we've reached the end
            if len(records) < PRIORITY_PAGE_SIZE:
                break

            skip += PRIORITY_PAGE_SIZE

        logger.info(
            "Fetched %d %s records from Priority%s",
            len(results),
            self.entity,
            f" (since UDATE {since_udate})" if since_udate else " (all)",
        )
        return results

    def get_record(self, key_value: str) -> dict[str, Any] | None:
        """
        Fetch a single record by key value.

        For entities with use_filter_lookup=True (e.g., FNCPART), uses
        $filter instead of key-in-URL because the URL key is an internal
        integer, not the human-readable PARTNAME.

        Returns:
            Record dict, or None if not found.
        """
        if self.use_filter_lookup:
            url = (
                f"{self.api_url}{self.entity}"
                f"?$filter={self.key_field} eq '{key_value}'"
            )
            response = self._request("GET", url)
            if response is None:
                return None
            data = response.json()
            records = data.get("value", [])
            return records[0] if records else None

        url = f"{self.api_url}{self.entity}('{key_value}')"
        response = self._request("GET", url, allow_404=True)

        if response is None:
            return None

        return response.json()

    # ── Lookup tables ──────────────────────────────────────────────────

    def fetch_lookup_table(
        self,
        entity: str,
        code_field: str,
        desc_field: str,
    ) -> dict[str, str]:
        """
        Fetch a Priority lookup/linked-table and return a code → description dict.

        Many Priority fields are linked tables that store a code (e.g., "1")
        but display a description (e.g., "Beverages").  This method fetches
        the full lookup table so the P→A sync can write the human-readable
        description to Airtable instead of the numeric code.

        Args:
            entity: Priority entity name (e.g., "FAMILY_FNC").
            code_field: The code/key field (e.g., "FAMILYNAME").
            desc_field: The description field (e.g., "FAMILYDESC").

        Returns:
            Dict mapping code strings to description strings.
            E.g. {"1": "Beverages", "2": "Butter", ...}
        """
        url = (
            f"{self.api_url}{entity}"
            f"?$select={code_field},{desc_field}"
        )
        response = self._request("GET", url)
        if response is None:
            logger.warning("Failed to fetch lookup table %s", entity)
            return {}

        data = response.json()
        records = data.get("value", [])
        lookup = {
            str(r[code_field]).strip(): str(r[desc_field]).strip()
            for r in records
            if r.get(code_field) is not None
        }
        logger.info(
            "Loaded %d entries from lookup table %s",
            len(lookup),
            entity,
        )
        return lookup

    # ── Write operations ─────────────────────────────────────────────────

    def create_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Create a new record in Priority (POST).

        Args:
            payload: Dict of Priority field names → values.

        Returns:
            The created record from Priority.

        Raises:
            requests.HTTPError on failure.
        """
        url = f"{self.api_url}{self.entity}"
        logger.debug(
            "Creating %s %s with %d fields",
            self.entity,
            payload.get(self.key_field, "?"),
            len(payload),
        )

        response = self._request("POST", url, json_body=payload)
        if response is None:
            raise requests.HTTPError(f"No response from Priority on POST {self.entity}")

        return response.json()

    def update_record(
        self, key_value: str, patch_body: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Update an existing record in Priority (PATCH).

        For entities with use_filter_lookup=True (e.g., FNCPART), first looks up
        the internal integer key via $filter, then PATCHes using that key.

        Args:
            key_value: The record's human-readable key value (e.g., SKU).
            patch_body: Dict of only the changed fields.

        Returns:
            The updated record from Priority.

        Raises:
            requests.HTTPError on failure.
        """
        if self.use_filter_lookup:
            # Look up the internal integer key first
            record = self.get_record(key_value)
            if record is None:
                raise requests.HTTPError(
                    f"{self.entity} record '{key_value}' not found for PATCH"
                )
            # Use the internal PARTPARAM/PART/KLINE etc. key
            # Priority entities use their AUTOUNIQUE field as URL key
            internal_key = record.get("PARTPARAM") or record.get("PART")
            if internal_key is None:
                raise requests.HTTPError(
                    f"Could not determine internal key for {self.entity}('{key_value}')"
                )
            url = f"{self.api_url}{self.entity}({internal_key})"
        else:
            url = f"{self.api_url}{self.entity}('{key_value}')"

        logger.debug(
            "Updating %s %s: %d fields (%s)",
            self.entity,
            key_value,
            len(patch_body),
            ", ".join(patch_body.keys()),
        )

        response = self._request("PATCH", url, json_body=patch_body)
        if response is None:
            raise requests.HTTPError(f"No response from Priority on PATCH {self.entity}")

        return response.json()

    # ── Sub-form operations ─────────────────────────────────────────────

    def get_subform(
        self, key_value: str, subform_name: str
    ) -> list[dict[str, Any]]:
        """
        Fetch all sub-form records for a parent entity.

        Handles two Priority response formats:
        - Multi-record sub-forms: {"value": [record1, record2, ...]}
        - Single-entity sub-forms: {field1: val1, field2: val2, ...}
          (indicated by '$entity' in @odata.context)

        Args:
            key_value: The parent entity's key value.
            subform_name: e.g. 'SAVR_ALLERGENS_SUBFORM', 'SAVR_PARTSHELF_SUBFORM'

        Returns:
            List of sub-form record dicts.
        """
        url = f"{self.api_url}{self.entity}('{key_value}')/{subform_name}"
        response = self._request("GET", url, allow_404=True)

        if response is None:
            return []

        data = response.json()

        # Multi-record sub-form: {"value": [...]}
        if "value" in data:
            return data["value"]

        # Single-entity sub-form: fields returned directly
        # (e.g. SAVR_ALLERGENS_SUBFORM returns $entity, not value array)
        context = data.get("@odata.context", "")
        if "$entity" in context or any(
            k for k in data if not k.startswith("@")
        ):
            # Strip OData metadata keys, return as single-item list
            record = {k: v for k, v in data.items() if not k.startswith("@")}
            if record:
                return [record]

        return []

    def post_subform(
        self,
        key_value: str,
        subform_name: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Create a new sub-form record (POST).

        Returns:
            The created sub-form record from Priority.
        """
        url = f"{self.api_url}{self.entity}('{key_value}')/{subform_name}"
        logger.debug(
            "POST sub-form %s for %s: %s",
            subform_name, key_value, list(payload.keys()),
        )

        response = self._request("POST", url, json_body=payload)
        if response is None:
            raise requests.HTTPError(
                f"No response from Priority on POST {subform_name}"
            )

        return response.json()

    def patch_subform(
        self,
        key_value: str,
        subform_name: str,
        key_field: str,
        sub_key_value: str,
        patch_body: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Update an existing sub-form record (PATCH).

        Args:
            key_value: The parent entity's key value.
            subform_name: e.g. 'SAVR_ALLERGENS_SUBFORM'
            key_field: The sub-form's key field name (e.g. 'TYPE', 'PLNAME').
            sub_key_value: The value of the key field to target.
            patch_body: Dict of changed fields.

        Returns:
            The updated sub-form record from Priority.
        """
        url = (
            f"{self.api_url}{self.entity}('{key_value}')/"
            f"{subform_name}('{sub_key_value}')"
        )
        logger.debug(
            "PATCH sub-form %s[%s=%s] for %s: %s",
            subform_name, key_field, sub_key_value, key_value, list(patch_body.keys()),
        )

        response = self._request("PATCH", url, json_body=patch_body)
        if response is None:
            raise requests.HTTPError(
                f"No response from Priority on PATCH {subform_name}"
            )

        return response.json()

    def upsert_single_subform(
        self,
        key_value: str,
        subform_name: str,
        payload: dict[str, Any],
    ) -> dict[str, str]:
        """
        Upsert a single-record sub-form (Pattern A).

        Strategy:
        - GET existing records
        - If none exist → POST
        - If exists → compare and PATCH only changed fields

        Returns:
            {"action": "created"|"updated"|"skipped", "fields_changed": int}
        """
        if not payload:
            return {"action": "skipped", "fields_changed": "0"}

        existing = self.get_subform(key_value, subform_name)

        if not existing:
            # No sub-form record yet → POST
            self.post_subform(key_value, subform_name, payload)
            return {"action": "created", "fields_changed": str(len(payload))}

        # Compare with first existing record
        current = existing[0]
        changed: dict[str, Any] = {}

        for field, new_value in payload.items():
            old_value = current.get(field)
            if not values_equal(new_value, old_value):
                changed[field] = new_value

        if not changed:
            return {"action": "skipped", "fields_changed": "0"}

        # PATCH the single-entity sub-form directly (no key needed in URL)
        self.patch_subform_direct(key_value, subform_name, changed)
        return {"action": "updated", "fields_changed": str(len(changed))}

    def patch_subform_direct(
        self,
        key_value: str,
        subform_name: str,
        patch_body: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Update a single-entity sub-form directly (Pattern A — no key in URL).
        """
        url = f"{self.api_url}{self.entity}('{key_value}')/{subform_name}"
        logger.debug(
            "PATCH sub-form %s for %s (single record): %s",
            subform_name, key_value, list(patch_body.keys()),
        )

        response = self._request("PATCH", url, json_body=patch_body)
        if response is None:
            raise requests.HTTPError(
                f"No response from Priority on PATCH {subform_name}"
            )

        return response.json()

    def deep_patch_subform(
        self,
        key_value: str,
        subform_name: str,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Update sub-form records using deep PATCH on the parent entity (Pattern C).

        Some Priority sub-forms don't expose individual record access via URL keys.
        For these, we PATCH the parent entity with nested sub-form data.

        Args:
            key_value: The parent entity's key value.
            subform_name: e.g. 'PARTINCUSTPLISTS_SUBFORM'
            records: List of sub-form record dicts to set.

        Returns:
            The parent entity response from Priority.
        """
        url = f"{self.api_url}{self.entity}('{key_value}')"
        payload = {subform_name: records}

        logger.debug(
            "Deep PATCH %s for %s: %d records",
            subform_name, key_value, len(records),
        )

        response = self._request("PATCH", url, json_body=payload)
        if response is None:
            raise requests.HTTPError(
                f"No response from Priority on deep PATCH {subform_name}"
            )

        return response.json()

    def sync_multi_subform(
        self,
        key_value: str,
        subform_name: str,
        match_field: str,
        desired_records: list[dict[str, Any]],
        url_key_field: str | None = None,
    ) -> dict[str, Any]:
        """
        Sync a multi-record sub-form (Pattern B).

        Strategy:
        - GET existing records
        - Match by match_field
        - For each desired record:
          - If not in existing → POST
          - If in existing → compare and PATCH if changed

        Args:
            key_value: The parent entity's key value.
            subform_name: e.g. 'SAVR_PARTSHELF_SUBFORM'
            match_field: Field to match on (e.g. 'TYPE' for shelf lives)
            desired_records: List of payload dicts from Airtable mapping.
            url_key_field: If provided, use this field's value from the
                *existing* Priority record as the URL key for PATCH.
                E.g. 'SHELFLIFE' integer for shelf lives.
                If None, uses match_field value in the URL (string).

        Returns:
            {"created": int, "updated": int, "skipped": int}
        """
        result = {"created": 0, "updated": 0, "skipped": 0}

        if not desired_records:
            return result

        existing = self.get_subform(key_value, subform_name)

        # Index existing by match field
        existing_by_key: dict[str, dict[str, Any]] = {}
        for record in existing:
            key = str(record.get(match_field, "")).strip()
            if key:
                existing_by_key[key] = record

        for desired in desired_records:
            match_value = str(desired.get(match_field, "")).strip()
            if not match_value:
                continue

            if match_value not in existing_by_key:
                # New record → POST
                self.post_subform(key_value, subform_name, desired)
                result["created"] += 1
            else:
                # Existing → compare and PATCH
                current = existing_by_key[match_value]
                changed: dict[str, Any] = {}

                for field, new_value in desired.items():
                    if field == match_field:
                        continue  # Don't patch the key itself
                    old_value = current.get(field)
                    if not values_equal(new_value, old_value):
                        changed[field] = new_value

                if changed:
                    # Determine the URL key for PATCH
                    if url_key_field:
                        # Use the existing record's url_key_field value (e.g. SHELFLIFE integer)
                        url_key = current.get(url_key_field)
                        if url_key is None:
                            logger.warning(
                                "No %s value for %s[%s=%s] — skipping PATCH",
                                url_key_field, subform_name, match_field, match_value,
                            )
                            result["skipped"] += 1
                            continue
                        # Use numeric key (no quotes in URL)
                        url = (
                            f"{self.api_url}{self.entity}('{key_value}')/"
                            f"{subform_name}({url_key})"
                        )
                    else:
                        # Use string key (with quotes in URL)
                        url = (
                            f"{self.api_url}{self.entity}('{key_value}')/"
                            f"{subform_name}('{match_value}')"
                        )

                    logger.debug(
                        "PATCH sub-form %s for %s: %s",
                        subform_name, key_value, list(changed.keys()),
                    )
                    response = self._request("PATCH", url, json_body=changed)
                    if response is None:
                        raise requests.HTTPError(
                            f"No response from Priority on PATCH {subform_name}"
                        )
                    result["updated"] += 1
                else:
                    result["skipped"] += 1

        return result
