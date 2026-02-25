"""
Customers All sync engine: CUSTOMERS + 5 sub-forms.

Subclasses BaseSyncEngine with CUSTOMERS-specific configuration.
Handles 5 accessible sub-forms from 4 Airtable tables:
  - Customer Contacts  → CUSTPERSONNEL_SUBFORM (separate table)
  - Customer Sites     → CUSTDESTS_SUBFORM (separate table)
  - Special Prices     → CUSTPARTPRICE_SUBFORM (separate table)
  - Price List         → CUSTPLIST_SUBFORM (Customers table, different view)
  - Delivery Days      → CUSTWEEKDAY_SUBFORM (Customers table, different view)
                         Requires row explosion + day abbreviation + time conversion

NOT accessible via API (return 404):
  - CUSTOMERSTEXT_SUBFORM (internal remarks)
  - CUSTSHIPTEXT_SUBFORM (shipment remarks)
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from sync.core.airtable_client import AirtableClient
from sync.core.base_engine import BaseSyncEngine
from sync.core.config import (
    AIRTABLE_API_BASE,
    AIRTABLE_MAX_RETRIES,
    AIRTABLE_REQUEST_TIMEOUT,
)
from sync.core.models import FieldMapping, SubformResult, SyncMode, SyncRecord
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.core.utils import abbreviate_day, clean, format_time_24h
from sync.workflows.customers.config import (
    AIRTABLE_CONTACTS_TABLE_ID,
    AIRTABLE_CONTACTS_VIEW,
    AIRTABLE_DELIVERY_DAYS_VIEW,
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_PRICE_LIST_VIEW,
    AIRTABLE_SITES_TABLE_ID,
    AIRTABLE_SITES_VIEW,
    AIRTABLE_SPECIAL_PRICES_TABLE_ID,
    AIRTABLE_SPECIAL_PRICES_VIEW,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    CONTACTS_SUBFORM_NAME,
    PRICE_LIST_SUBFORM_NAME,
    PRIORITY_ENTITY,
    PRIORITY_KEY_FIELD,
    SITES_SUBFORM_NAME,
    SPECIAL_PRICES_SUBFORM_NAME,
    TIMESTAMP_FIELDS,
    WEEKDAY_SUBFORM_NAME,
)
from sync.workflows.customers.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELDS_TO_FETCH,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
)
from sync.workflows.customers.subform_mapping import (
    CONTACTS_AIRTABLE_FIELDS,
    CONTACTS_FIELD_MAP,
    CONTACTS_MATCH_FIELD,
    DELIVERY_DAYS_AIRTABLE_FIELDS,
    PRICE_LIST_AIRTABLE_FIELDS,
    PRICE_LIST_FIELD_MAP,
    SITES_AIRTABLE_FIELDS,
    SITES_FIELD_MAP,
    SITES_MATCH_FIELD,
    SPECIAL_PRICES_AIRTABLE_FIELDS,
    SPECIAL_PRICES_FIELD_MAP,
    SPECIAL_PRICES_MATCH_FIELD,
)

logger = logging.getLogger(__name__)


class CustomerSyncEngine(BaseSyncEngine):
    """
    Sync engine for CUSTOMERS (Customers All).

    Multi-table architecture:
      - Main fields from Customers table
      - Contacts from Customer Contacts 2025 table → CUSTPERSONNEL_SUBFORM
      - Sites from Customer Sites table → CUSTDESTS_SUBFORM
      - Special Prices from Special Cust. Prices table → CUSTPARTPRICE_SUBFORM
      - Price Lists from Customers table (different view) → CUSTPLIST_SUBFORM
      - Delivery Days from Customers table (different view) → CUSTWEEKDAY_SUBFORM

    Uses standard string keys (CUSTOMERS('C00001') works directly).
    """

    # ── Client factories ─────────────────────────────────────────────────

    def _create_airtable_client(
        self,
        base_id_override: str | None,
        token_override: str | None,
    ) -> AirtableClient:
        return AirtableClient(
            table_name=AIRTABLE_TABLE_NAME,
            key_field=AIRTABLE_KEY_FIELD,
            key_field_writable=AIRTABLE_KEY_FIELD_WRITABLE,
            sync_view=AIRTABLE_SYNC_VIEW,
            timestamp_fields=TIMESTAMP_FIELDS,
            base_id_override=base_id_override,
            token_override=token_override,
        )

    def _create_priority_client(self) -> PriorityClient:
        return PriorityClient(
            entity=PRIORITY_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
        )

    def _create_sync_log_client(self) -> SyncLogClient:
        return SyncLogClient(entity_label="Customer ID")

    # ── Field mapping accessors ──────────────────────────────────────────

    def _get_a2p_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        return A2P_FIELD_MAP

    def _get_a2p_fields_to_fetch(self, mode: SyncMode) -> list[str]:
        return AIRTABLE_FIELDS_TO_FETCH

    def _get_p2a_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        return P2A_FIELD_MAP

    def _get_p2a_airtable_fields(self, mode: SyncMode) -> list[str]:
        return P2A_AIRTABLE_FIELDS_TO_FETCH

    def _get_p2a_priority_select(self, mode: SyncMode) -> list[str]:
        return P2A_PRIORITY_SELECT

    # ── Key field accessors ──────────────────────────────────────────────

    def _get_key_field_name(self) -> str:
        return PRIORITY_KEY_FIELD

    def _get_airtable_key_field(self) -> str:
        return AIRTABLE_KEY_FIELD

    def _get_airtable_key_field_writable(self) -> str:
        return AIRTABLE_KEY_FIELD_WRITABLE

    # ── Sub-form: Pre-fetch from related Airtable tables ──────────────────

    def _pre_a2p_batch(
        self, records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Pre-fetch sub-form data from separate tables and additional views:
          - Customer Contacts (separate table)
          - Customer Sites (separate table)
          - Special Cust. Prices (separate table)
          - Price Lists (Customers table, different view)
          - Delivery Days (Customers table, different view)
        """
        context: dict[str, Any] = {}

        # 1. Contacts (separate table)
        logger.info("Pre-fetching customer contacts...")
        contacts_raw = self._fetch_from_table(
            AIRTABLE_CONTACTS_TABLE_ID,
            AIRTABLE_CONTACTS_VIEW,
            CONTACTS_AIRTABLE_FIELDS,
        )
        contacts_by_cust = self._group_by_key(
            contacts_raw,
            key_field="Priority Cust. ID (from Customers)",
        )
        logger.info("Loaded contacts for %d customers.", len(contacts_by_cust))
        context["contacts_by_cust"] = contacts_by_cust

        # 2. Sites (separate table)
        logger.info("Pre-fetching customer sites...")
        sites_raw = self._fetch_from_table(
            AIRTABLE_SITES_TABLE_ID,
            AIRTABLE_SITES_VIEW,
            SITES_AIRTABLE_FIELDS,
        )
        sites_by_cust = self._group_by_key(
            sites_raw,
            key_field="Priority Cust. ID",
        )
        logger.info("Loaded sites for %d customers.", len(sites_by_cust))
        context["sites_by_cust"] = sites_by_cust

        # 3. Special Prices (separate table)
        logger.info("Pre-fetching special customer prices...")
        prices_raw = self._fetch_from_table(
            AIRTABLE_SPECIAL_PRICES_TABLE_ID,
            AIRTABLE_SPECIAL_PRICES_VIEW,
            SPECIAL_PRICES_AIRTABLE_FIELDS,
        )
        prices_by_cust = self._group_by_key(
            prices_raw,
            key_field="Cust. IDs",
        )
        logger.info("Loaded special prices for %d customers.", len(prices_by_cust))
        context["prices_by_cust"] = prices_by_cust

        # 4. Price Lists (same Customers table, different view)
        logger.info("Pre-fetching price list assignments...")
        plist_raw = self.airtable.fetch_records_from_view(
            view_name=AIRTABLE_PRICE_LIST_VIEW,
            fields=PRICE_LIST_AIRTABLE_FIELDS,
        )
        plist_by_cust: dict[str, list[dict[str, Any]]] = {}
        for rec in plist_raw:
            fields = rec.get("fields", {})
            cust_id = clean(fields.get("Priority Cust. ID"))
            if cust_id:
                plist_by_cust.setdefault(cust_id, []).append(fields)
        logger.info("Loaded price lists for %d customers.", len(plist_by_cust))
        context["plist_by_cust"] = plist_by_cust

        # 5. Delivery Days (same Customers table, different view)
        logger.info("Pre-fetching delivery days...")
        days_raw = self.airtable.fetch_records_from_view(
            view_name=AIRTABLE_DELIVERY_DAYS_VIEW,
            fields=DELIVERY_DAYS_AIRTABLE_FIELDS,
        )
        days_by_cust: dict[str, list[dict[str, Any]]] = {}
        for rec in days_raw:
            fields = rec.get("fields", {})
            cust_id = clean(fields.get("Priority Cust. ID"))
            if cust_id:
                days_by_cust.setdefault(cust_id, []).append(fields)
        logger.info("Loaded delivery days for %d customers.", len(days_by_cust))
        context["days_by_cust"] = days_by_cust

        return context

    def _sync_subforms(
        self,
        key: str,
        airtable_fields: dict[str, Any],
        context: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Sync 5 sub-forms for a customer after main CUSTOMERS sync.

        1. Contacts → CUSTPERSONNEL_SUBFORM
        2. Sites → CUSTDESTS_SUBFORM
        3. Special Prices → CUSTPARTPRICE_SUBFORM
        4. Price Lists → CUSTPLIST_SUBFORM
        5. Delivery Days → CUSTWEEKDAY_SUBFORM (with row explosion)
        """
        # 1. Contacts
        contacts = context.get("contacts_by_cust", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=CONTACTS_SUBFORM_NAME,
            airtable_records=contacts,
            field_map=CONTACTS_FIELD_MAP,
            result=result,
            dry_run=dry_run,
            label="contacts",
        )

        # 2. Sites
        sites = context.get("sites_by_cust", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=SITES_SUBFORM_NAME,
            airtable_records=sites,
            field_map=SITES_FIELD_MAP,
            result=result,
            dry_run=dry_run,
            label="sites",
        )

        # 3. Special Prices
        prices = context.get("prices_by_cust", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=SPECIAL_PRICES_SUBFORM_NAME,
            airtable_records=prices,
            field_map=SPECIAL_PRICES_FIELD_MAP,
            result=result,
            dry_run=dry_run,
            label="special prices",
        )

        # 4. Price Lists
        plists = context.get("plist_by_cust", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=PRICE_LIST_SUBFORM_NAME,
            airtable_records=plists,
            field_map=PRICE_LIST_FIELD_MAP,
            result=result,
            dry_run=dry_run,
            label="price lists",
        )

        # 5. Delivery Days (with row explosion)
        days_records = context.get("days_by_cust", {}).get(key, [])
        self._sync_delivery_days(key, days_records, result, dry_run)

    # ── Delivery Days: special handling ──────────────────────────────────

    def _sync_delivery_days(
        self,
        key: str,
        days_records: list[dict[str, Any]],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Sync delivery days to CUSTWEEKDAY_SUBFORM.

        Special processing:
        - "Days of Business" is comma-separated → one sub-form row per day
        - Day names abbreviated: Monday → Mon
        - Times converted: 12h AM/PM → 24h
        """
        if not days_records:
            return

        payloads: list[dict[str, Any]] = []

        for record in days_records:
            days_str = clean(record.get("Days of Business"))
            deliver_after = clean(record.get("Deliver After"))
            deliver_before = clean(record.get("Deliver Before"))

            if not days_str:
                continue

            # Row explosion: split comma-separated days
            day_list = [d.strip() for d in days_str.split(",") if d.strip()]

            for day in day_list:
                payload: dict[str, Any] = {
                    "WEEKDAY": abbreviate_day(day),
                }
                if deliver_after:
                    payload["FROMTIME"] = format_time_24h(deliver_after)
                if deliver_before:
                    payload["TOTIME"] = format_time_24h(deliver_before)

                payloads.append(payload)

        if not payloads:
            return

        if dry_run:
            result.subform_results.append(
                SubformResult(
                    subform=WEEKDAY_SUBFORM_NAME,
                    action="DRY_RUN",
                    detail=f"Would sync {len(payloads)} delivery day entries",
                )
            )
            return

        try:
            self.priority.deep_patch_subform(
                key_value=key,
                subform_name=WEEKDAY_SUBFORM_NAME,
                records=payloads,
            )
            result.subform_results.append(
                SubformResult(
                    subform=WEEKDAY_SUBFORM_NAME,
                    action="UPDATED",
                    detail=f"Synced {len(payloads)} delivery days",
                )
            )
        except Exception as e:
            logger.error(
                "Failed to sync delivery days for customer %s: %s", key, e,
            )
            result.subform_results.append(
                SubformResult(
                    subform=WEEKDAY_SUBFORM_NAME,
                    action="ERROR",
                    detail=str(e),
                )
            )

    # ── Generic sub-form sync ────────────────────────────────────────────

    def _sync_one_subform(
        self,
        key: str,
        subform_name: str,
        airtable_records: list[dict[str, Any]],
        field_map: dict[str, str],
        result: SyncRecord,
        dry_run: bool,
        label: str,
    ) -> None:
        """
        Build payloads from Airtable records and sync to a Priority sub-form
        using deep PATCH on the parent CUSTOMERS entity.
        """
        if not airtable_records:
            return

        payloads: list[dict[str, Any]] = []
        for at_record in airtable_records:
            payload: dict[str, Any] = {}
            for at_field, p_field in field_map.items():
                raw = at_record.get(at_field)
                value = self._clean_lookup(raw)
                if value:
                    payload[p_field] = value
            if payload:
                payloads.append(payload)

        if not payloads:
            return

        if dry_run:
            result.subform_results.append(
                SubformResult(
                    subform=subform_name,
                    action="DRY_RUN",
                    detail=f"Would sync {len(payloads)} {label}",
                )
            )
            return

        try:
            self.priority.deep_patch_subform(
                key_value=key,
                subform_name=subform_name,
                records=payloads,
            )
            result.subform_results.append(
                SubformResult(
                    subform=subform_name,
                    action="UPDATED",
                    detail=f"Synced {len(payloads)} {label}",
                )
            )
        except Exception as e:
            logger.error(
                "Failed to sync %s for customer %s: %s", label, key, e,
            )
            result.subform_results.append(
                SubformResult(
                    subform=subform_name,
                    action="ERROR",
                    detail=str(e),
                )
            )

    # ── Helpers ──────────────────────────────────────────────────────────

    def _fetch_from_table(
        self,
        table_id: str,
        view_name: str,
        fields: list[str],
    ) -> list[dict[str, Any]]:
        """
        Fetch all records from a separate Airtable table (not the main Customers table).
        """
        url = f"{AIRTABLE_API_BASE}/{self.airtable._base_id}/{table_id}"
        field_params = [("fields[]", f) for f in fields]
        records: list[dict[str, Any]] = []
        offset: str | None = None

        for attempt in range(AIRTABLE_MAX_RETRIES):
            try:
                while True:
                    params = [("view", view_name)] + field_params
                    if offset:
                        params.append(("offset", offset))

                    response = self.airtable.session.get(
                        url, params=params, timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 30))
                        logger.warning("Rate limited. Waiting %ds...", retry_after)
                        time.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    data = response.json()
                    new_records = data.get("records", [])
                    records.extend(new_records)

                    offset = data.get("offset")
                    if not offset:
                        break
                    time.sleep(0.2)

                break

            except requests.exceptions.RequestException as e:
                if attempt == AIRTABLE_MAX_RETRIES - 1:
                    logger.error(
                        "Failed to fetch from table %s after %d attempts: %s",
                        table_id, AIRTABLE_MAX_RETRIES, e,
                    )
                    raise
                wait_time = 2 ** attempt
                logger.warning("Attempt %d failed, retrying in %ds: %s", attempt + 1, wait_time, e)
                time.sleep(wait_time)

        return records

    def _group_by_key(
        self,
        raw_records: list[dict[str, Any]],
        key_field: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Group fetched Airtable records by customer ID.
        Handles both plain strings and lookup (list) fields.
        """
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in raw_records:
            fields = record.get("fields", {})
            raw_key = fields.get(key_field)

            if isinstance(raw_key, list):
                cust_id = clean(raw_key[0]) if raw_key else None
            else:
                cust_id = clean(raw_key)

            if cust_id:
                grouped.setdefault(cust_id, []).append(fields)

        return grouped

    @staticmethod
    def _clean_lookup(value: Any) -> str | None:
        """Clean a value that might be a lookup (list)."""
        if isinstance(value, list):
            if not value:
                return None
            value = value[0]
        return clean(value)
