"""
Vendors sync engine: SUPPLIERS + FNCSUP + sub-forms.

Subclasses BaseSyncEngine with SUPPLIERS-specific configuration.
Merged workflow syncs both SUPPLIERS (main entity) and FNCSUP (secondary entity).

Handles 3 sub-forms on SUPPLIERS + 1 on FNCSUP:
  - Vendor Contacts  → SUPPERSONNEL_SUBFORM
  - Vendor Products  → SUPPART_SUBFORM
  - Vendor Sites     → SUPDESTCODES_SUBFORM
  - Bank Accounts    → ACCOUNTBANK_SUBFORM (on FNCSUP)
  - Vendor Remarks   → SUPPLIERSTEXT_SUBFORM (NOT accessible via API — 404)

Multi-table architecture:
  - Main vendor data comes from the "Vendors" table via the sync view.
  - Sub-form data comes from 3 separate Airtable tables fetched in _pre_a2p_batch.
  - Bank account data comes from a secondary view on the same Vendors table.
  - P→A is limited — many Airtable fields are formulas or AI-generated (aiText).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

from sync.core.airtable_client import AirtableClient
from sync.core.base_engine import BaseSyncEngine, build_field_id_map, map_airtable_to_priority
from sync.core.config import (
    AIRTABLE_API_BASE,
    AIRTABLE_MAX_RETRIES,
    AIRTABLE_REQUEST_TIMEOUT,
)
from sync.core.models import FieldMapping, SubformResult, SyncError, SyncMode, SyncRecord
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.core.utils import clean
from sync.workflows.vendors.config import (
    ACCOUNTBANK_SUBFORM_NAME,
    AIRTABLE_BANK_VIEW,
    AIRTABLE_CONTACTS_TABLE_ID,
    AIRTABLE_CONTACTS_VIEW,
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_ID,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_KEY_FIELD_WRITABLE_ID,
    AIRTABLE_PRODUCTS_TABLE_ID,
    AIRTABLE_PRODUCTS_VIEW,
    AIRTABLE_SITES_TABLE_ID,
    AIRTABLE_SITES_VIEW,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    CONTACTS_SUBFORM_NAME,
    PARTS_SUBFORM_NAME,
    PRIORITY_ENTITY,
    PRIORITY_FNCSUP_ENTITY,
    PRIORITY_KEY_FIELD,
    SITES_SUBFORM_NAME,
    TIMESTAMP_FIELDS,
    TIMESTAMP_FIELD_IDS,
)
from sync.workflows.vendors.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELDS_TO_FETCH,
    FNCSUP_A2P_FIELD_MAP,
    FNCSUP_BANK_A2P_FIELD_MAP,
    FNCSUP_BANK_P2A_FIELD_MAP,
    FNCSUP_P2A_FIELD_MAP,
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
    Sync engine for Vendors (SUPPLIERS + FNCSUP).

    Merged workflow syncs:
      - SUPPLIERS (main entity) with 3 sub-forms
      - FNCSUP (secondary entity — financial parameters) with bank sub-form

    Multi-table architecture:
      - Main fields from Vendors table
      - Contacts from Vendor Contacts table → SUPPERSONNEL_SUBFORM
      - Products from Vendor Products table → SUPPART_SUBFORM
      - Sites from Vendor Sites table → SUPDESTCODES_SUBFORM
      - Bank details from Vendors table (secondary view) → ACCOUNTBANK_SUBFORM

    Uses standard string keys (SUPPLIERS('2993') works directly).
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Secondary Priority client: FNCSUP (Financial Parameters for Vendors)
        self.priority_fncsup = PriorityClient(
            entity=PRIORITY_FNCSUP_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            api_url_override=self.priority_url_override,
            # FNCSUP uses string keys — standard URL access
        )

    # ── Client factories ─────────────────────────────────────────────────

    def _create_airtable_client(
        self,
        base_id_override: str | None,
        token_override: str | None,
    ) -> AirtableClient:
        field_id_map = build_field_id_map(
            A2P_FIELD_MAP, P2A_FIELD_MAP,
            FNCSUP_A2P_FIELD_MAP, FNCSUP_P2A_FIELD_MAP,
            FNCSUP_BANK_A2P_FIELD_MAP, FNCSUP_BANK_P2A_FIELD_MAP,
            extra={
                AIRTABLE_KEY_FIELD: AIRTABLE_KEY_FIELD_ID,
                AIRTABLE_KEY_FIELD_WRITABLE: AIRTABLE_KEY_FIELD_WRITABLE_ID,
                **{v: TIMESTAMP_FIELD_IDS[k] for k, v in TIMESTAMP_FIELDS.items()},
            },
        )
        return AirtableClient(
            table_name=AIRTABLE_TABLE_NAME,
            key_field=AIRTABLE_KEY_FIELD,
            key_field_writable=AIRTABLE_KEY_FIELD_WRITABLE,
            sync_view=AIRTABLE_SYNC_VIEW,
            timestamp_fields=TIMESTAMP_FIELDS,
            field_id_map=field_id_map,
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
        Pre-fetch sub-form data from 3 separate Airtable tables + bank view:
          - Vendor Contacts
          - Vendor Products
          - Vendor Sites
          - Bank Account Details (same Vendors table, different view)

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

        # Bank account details (same Vendors table, different view — for FNCSUP sub-form)
        logger.info("Pre-fetching bank account details from '%s'...", AIRTABLE_BANK_VIEW)
        from sync.workflows.fncsup.field_mapping import BANK_AIRTABLE_FIELDS
        bank_records = self.airtable.fetch_records_from_view(
            view_name=AIRTABLE_BANK_VIEW,
            fields=BANK_AIRTABLE_FIELDS,
        )
        bank_by_vendor: dict[str, dict[str, Any]] = {}
        for rec in bank_records:
            fields = rec.get("fields", {})
            vendor_id = clean(fields.get("Priority Vendor ID"))
            if vendor_id:
                bank_by_vendor[vendor_id] = fields
        logger.info("Found bank details for %d vendors.", len(bank_by_vendor))
        context["bank_by_vendor"] = bank_by_vendor

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
        Sync sub-forms + secondary entity for a vendor after main SUPPLIERS sync.

        1. Contacts → SUPPERSONNEL_SUBFORM (deep PATCH)
        2. Products → SUPPART_SUBFORM (deep PATCH)
        3. Sites → SUPDESTCODES_SUBFORM (deep PATCH)
        4. FNCSUP (Financial Parameters — secondary entity)
        5. Bank Accounts → ACCOUNTBANK_SUBFORM on FNCSUP (deep PATCH)
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

        # 4. FNCSUP (Financial Parameters — secondary entity)
        if not dry_run:
            self._sync_secondary_entity(
                self.priority_fncsup, key, airtable_fields, result,
                FNCSUP_A2P_FIELD_MAP, "fncsup",
            )

        # 5. Bank Accounts → ACCOUNTBANK_SUBFORM on FNCSUP
        self._sync_bank_subform(key, context, result, dry_run)

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

    # ── Secondary entity sync (FNCSUP) ────────────────────────────────────

    def _sync_secondary_entity(
        self,
        client: PriorityClient,
        key: str,
        airtable_fields: dict[str, Any],
        result: SyncRecord,
        field_map: list[FieldMapping],
        label: str,
    ) -> None:
        """
        Sync fields to a secondary Priority entity (FNCSUP).

        Secondary entities are auto-created by Priority when the parent entity
        is created — so this only does UPDATE (PATCH), never CREATE.
        Errors are isolated and don't fail the main sync.
        """
        try:
            payload = map_airtable_to_priority(airtable_fields, field_map=field_map)
            # Remove the key field — it's for lookup only, not for patching
            payload.pop(client.key_field, None)
            if not payload:
                return

            existing = client.get_record(key)
            if not existing:
                result.subform_results.append(SubformResult(
                    subform=label,
                    action="skipped",
                    detail="not found in Priority",
                ))
                return

            # Build diff — only changed fields
            patch: dict[str, Any] = {}
            for k, v in payload.items():
                current = existing.get(k)
                if str(current) != str(v):
                    patch[k] = v

            if not patch:
                return  # No changes — silently skip

            client.update_record(key, patch)
            result.subform_results.append(SubformResult(
                subform=label,
                action="updated",
                detail=f"{len(patch)} fields",
            ))
            logger.debug("%s for %s: updated %d fields", label.upper(), key, len(patch))

        except Exception as e:
            logger.error("%s sync error for %s: %s", label.upper(), key, e)
            result.subform_results.append(SubformResult(
                subform=label,
                action="error",
                detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                entity_key=key,
                action="SECONDARY",
                message=f"{label}: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    # ── Bank account sub-form (on FNCSUP) ─────────────────────────────────

    def _sync_bank_subform(
        self,
        key: str,
        context: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Sync bank account details to ACCOUNTBANK_SUBFORM on FNCSUP.
        Bank data is pre-fetched from a secondary Airtable view in _pre_a2p_batch.
        """
        bank_by_vendor = context.get("bank_by_vendor", {})
        bank_fields = bank_by_vendor.get(key)

        if not bank_fields:
            logger.debug("No bank details found for vendor %s, skipping ACCOUNTBANK.", key)
            return

        # Build the bank payload from Airtable fields
        bank_payload: dict[str, Any] = {}
        for mapping in FNCSUP_BANK_A2P_FIELD_MAP:
            raw_value = bank_fields.get(mapping.airtable_field)
            cleaned = clean(raw_value)
            if cleaned:
                bank_payload[mapping.priority_field] = cleaned

        if not bank_payload:
            logger.debug("No bank data to sync for vendor %s.", key)
            return

        if dry_run:
            result.subform_results.append(
                SubformResult(
                    subform=ACCOUNTBANK_SUBFORM_NAME,
                    action="DRY_RUN",
                    detail=f"Would sync bank: {bank_payload}",
                )
            )
            return

        try:
            self.priority_fncsup.deep_patch_subform(
                key_value=key,
                subform_name=ACCOUNTBANK_SUBFORM_NAME,
                records=[bank_payload],
            )
            result.subform_results.append(
                SubformResult(
                    subform=ACCOUNTBANK_SUBFORM_NAME,
                    action="updated",
                    detail=f"Bank details synced: {list(bank_payload.keys())}",
                )
            )
        except Exception as e:
            logger.error("Failed to sync bank for vendor %s: %s", key, e)
            result.subform_results.append(
                SubformResult(
                    subform=ACCOUNTBANK_SUBFORM_NAME,
                    action="error",
                    detail=str(e)[:100],
                )
            )

    # ── P→A extra fields (FNCSUP + bank) ──────────────────────────────────

    def _get_p2a_extra_fields(
        self,
        key: str,
        priority_record: dict[str, Any],
        is_status: bool,
    ) -> dict[str, Any]:
        """
        Fetch extra fields from Priority for P→A direction (full mode only).

        Sources:
        1. FNCSUP (Financial Parameters) — secondary entity
        2. Bank account details (ACCOUNTBANK_SUBFORM on FNCSUP)
        """
        if is_status:
            return {}

        extra: dict[str, Any] = {}

        # 1. FNCSUP fields → Airtable
        try:
            fncsup_data = self.priority_fncsup.get_record(key)
            if fncsup_data:
                from sync.core.base_engine import map_priority_to_airtable
                fncsup_mapped = map_priority_to_airtable(
                    fncsup_data, FNCSUP_P2A_FIELD_MAP, is_create=False,
                )
                extra.update(fncsup_mapped)
        except Exception as e:
            logger.warning("Failed to fetch FNCSUP for %s: %s", key, e)

        # 2. Bank account details from ACCOUNTBANK_SUBFORM
        try:
            bank_records = self.priority_fncsup.get_subform(key, ACCOUNTBANK_SUBFORM_NAME)
            if bank_records:
                # Use first bank record (vendors typically have one bank account)
                bank_record = bank_records[0]
                for mapping in FNCSUP_BANK_P2A_FIELD_MAP:
                    value = bank_record.get(mapping.priority_field)
                    if value:
                        cleaned = clean(value)
                        if cleaned:
                            extra[mapping.airtable_field] = cleaned
        except Exception as e:
            logger.warning("Failed to fetch bank details for %s: %s", key, e)

        return extra
