"""
Vendors All sync engine: SUPPLIERS + 3 sub-forms (contacts, parts, sites).

Subclasses BaseSyncEngine with SUPPLIERS-specific configuration.
Handles 3 sub-forms sourced from separate Airtable tables:
  - Vendor Contacts  → SUPPERSONNEL_SUBFORM
  - Vendor Products  → SUPPART_SUBFORM
  - Vendor Sites     → SUPDESTCODES_SUBFORM
  - Vendor Remarks   → SUPPLIERSTEXT_SUBFORM (NOT accessible via API — 404)

Multi-table architecture:
  - Main vendor data comes from the "Vendors" table via the sync view.
  - Sub-form data comes from 3 separate Airtable tables fetched in _pre_a2p_batch.
  - P→A is limited — many Airtable fields are formulas or AI-generated (aiText).
"""

from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import quote

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
from sync.core.utils import clean
from sync.workflows.vendors.config import (
    AIRTABLE_CONTACTS_TABLE_ID,
    AIRTABLE_CONTACTS_VIEW,
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_PRODUCTS_TABLE_ID,
    AIRTABLE_PRODUCTS_VIEW,
    AIRTABLE_SITES_TABLE_ID,
    AIRTABLE_SITES_VIEW,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    CONTACTS_SUBFORM_NAME,
    PARTS_SUBFORM_NAME,
    PRIORITY_ENTITY,
    PRIORITY_KEY_FIELD,
    SITES_SUBFORM_NAME,
    TIMESTAMP_FIELDS,
)
from sync.workflows.vendors.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELDS_TO_FETCH,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
)
from sync.workflows.vendors.subform_mapping import (
    CONTACTS_AIRTABLE_FIELDS,
    CONTACTS_FIELD_MAP,
    CONTACTS_MATCH_FIELD,
    PRODUCTS_AIRTABLE_FIELDS,
    PRODUCTS_FIELD_MAP,
    PRODUCTS_MATCH_FIELD,
    SITES_AIRTABLE_FIELDS,
    SITES_FIELD_MAP,
    SITES_MATCH_FIELD,
)

logger = logging.getLogger(__name__)


class VendorSyncEngine(BaseSyncEngine):
    """
    Sync engine for SUPPLIERS (Vendors All).

    Multi-table architecture:
      - Main fields from Vendors table
      - Contacts from Vendor Contacts table → SUPPERSONNEL_SUBFORM
      - Products from Vendor Products table → SUPPART_SUBFORM
      - Sites from Vendor Sites table → SUPDESTCODES_SUBFORM

    Uses standard string keys (SUPPLIERS('2993') works directly).
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

    def _create_priority_client(self, api_url_override: str | None = None) -> PriorityClient:
        return PriorityClient(
            entity=PRIORITY_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            api_url_override=api_url_override,
            # SUPPLIERS uses string keys — standard URL access
        )

    def _create_sync_log_client(self) -> SyncLogClient:
        return SyncLogClient(entity_label="Vendor ID")

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
        Pre-fetch sub-form data from 3 separate Airtable tables:
          - Vendor Contacts
          - Vendor Products
          - Vendor Sites

        Returns a context dict with each indexed by vendor ID.
        """
        context: dict[str, Any] = {}

        # Contacts
        logger.info("Pre-fetching vendor contacts from '%s'...", AIRTABLE_CONTACTS_VIEW)
        contacts_raw = self._fetch_from_table(
            AIRTABLE_CONTACTS_TABLE_ID,
            AIRTABLE_CONTACTS_VIEW,
            CONTACTS_AIRTABLE_FIELDS,
        )
        contacts_by_vendor = self._group_by_vendor_key(
            contacts_raw,
            key_field="Priority Vendor ID (from Vendor_ID)",
        )
        logger.info("Loaded contacts for %d vendors.", len(contacts_by_vendor))
        context["contacts_by_vendor"] = contacts_by_vendor

        # Products
        logger.info("Pre-fetching vendor products from '%s'...", AIRTABLE_PRODUCTS_VIEW)
        products_raw = self._fetch_from_table(
            AIRTABLE_PRODUCTS_TABLE_ID,
            AIRTABLE_PRODUCTS_VIEW,
            PRODUCTS_AIRTABLE_FIELDS,
        )
        products_by_vendor = self._group_by_vendor_key(
            products_raw,
            key_field="V-Vendor ID (from Preferred Vendor) (from Savory SKU)",
        )
        logger.info("Loaded products for %d vendors.", len(products_by_vendor))
        context["products_by_vendor"] = products_by_vendor

        # Sites
        logger.info("Pre-fetching vendor sites from '%s'...", AIRTABLE_SITES_VIEW)
        sites_raw = self._fetch_from_table(
            AIRTABLE_SITES_TABLE_ID,
            AIRTABLE_SITES_VIEW,
            SITES_AIRTABLE_FIELDS,
        )
        sites_by_vendor = self._group_by_vendor_key(
            sites_raw,
            key_field="Priority Vendor ID (from Vendor)",
        )
        logger.info("Loaded sites for %d vendors.", len(sites_by_vendor))
        context["sites_by_vendor"] = sites_by_vendor

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
        Sync 3 sub-forms for a vendor after main SUPPLIERS sync.

        1. Contacts → SUPPERSONNEL_SUBFORM (deep PATCH)
        2. Products → SUPPART_SUBFORM (deep PATCH)
        3. Sites → SUPDESTCODES_SUBFORM (deep PATCH)
        """
        # 1. Contacts
        contacts = context.get("contacts_by_vendor", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=CONTACTS_SUBFORM_NAME,
            airtable_records=contacts,
            field_map=CONTACTS_FIELD_MAP,
            match_field=CONTACTS_MATCH_FIELD,
            result=result,
            dry_run=dry_run,
            label="contacts",
        )

        # 2. Products
        products = context.get("products_by_vendor", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=PARTS_SUBFORM_NAME,
            airtable_records=products,
            field_map=PRODUCTS_FIELD_MAP,
            match_field=PRODUCTS_MATCH_FIELD,
            result=result,
            dry_run=dry_run,
            label="parts",
        )

        # 3. Sites
        sites = context.get("sites_by_vendor", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=SITES_SUBFORM_NAME,
            airtable_records=sites,
            field_map=SITES_FIELD_MAP,
            match_field=SITES_MATCH_FIELD,
            result=result,
            dry_run=dry_run,
            label="sites",
        )

    # ── Helpers ──────────────────────────────────────────────────────────

    def _sync_one_subform(
        self,
        key: str,
        subform_name: str,
        airtable_records: list[dict[str, Any]],
        field_map: dict[str, str],
        match_field: str,
        result: SyncRecord,
        dry_run: bool,
        label: str,
    ) -> None:
        """
        Build payloads from Airtable records and sync to a Priority sub-form
        using deep PATCH on the parent SUPPLIERS entity.
        """
        if not airtable_records:
            logger.debug("No %s found for vendor %s, skipping.", label, key)
            return

        # Build sub-form payloads
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
            logger.debug("No %s data to sync for vendor %s.", label, key)
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
                "Failed to sync %s for vendor %s: %s", label, key, e,
            )
            result.subform_results.append(
                SubformResult(
                    subform=subform_name,
                    action="ERROR",
                    detail=str(e),
                )
            )

    def _fetch_from_table(
        self,
        table_id: str,
        view_name: str,
        fields: list[str],
    ) -> list[dict[str, Any]]:
        """
        Fetch all records from a separate Airtable table (not the main Vendors table).
        Uses the same auth session as the main AirtableClient.
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

                break  # Success

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

    def _group_by_vendor_key(
        self,
        raw_records: list[dict[str, Any]],
        key_field: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Group fetched Airtable records by vendor ID.
        The key_field is often a lookup (list type), so we handle both
        plain strings and lists.
        """
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in raw_records:
            fields = record.get("fields", {})
            raw_key = fields.get(key_field)

            # Handle lookup fields that return a list
            if isinstance(raw_key, list):
                vendor_id = clean(raw_key[0]) if raw_key else None
            else:
                vendor_id = clean(raw_key)

            if vendor_id:
                grouped.setdefault(vendor_id, []).append(fields)

        return grouped

    @staticmethod
    def _clean_lookup(value: Any) -> str | None:
        """
        Clean a value that might be a lookup (list).
        Returns the first item if it's a list, otherwise clean the value.
        """
        if isinstance(value, list):
            if not value:
                return None
            value = value[0]
        return clean(value)
