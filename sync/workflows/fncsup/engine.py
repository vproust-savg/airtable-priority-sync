"""
FNCSUP sync engine: Financial Parameters for Vendors.

Subclasses BaseSyncEngine with FNCSUP-specific configuration.
Has 1 sub-form: ACCOUNTBANK_SUBFORM (bank account details).

The bank account data lives in the same Vendors table but in a different view
(EDI Vend. Financial 2 - Bank Account Details). The engine pre-fetches bank
data and syncs it as a sub-form on FNCSUP.
"""

from __future__ import annotations

import logging
from typing import Any

from sync.core.airtable_client import AirtableClient
from sync.core.base_engine import BaseSyncEngine
from sync.core.models import FieldMapping, SubformResult, SyncMode, SyncRecord
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.core.utils import clean
from sync.workflows.fncsup.config import (
    ACCOUNTBANK_SUBFORM_NAME,
    AIRTABLE_BANK_VIEW,
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    PRIORITY_ENTITY,
    PRIORITY_KEY_FIELD,
    TIMESTAMP_FIELDS,
)
from sync.workflows.fncsup.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELDS_TO_FETCH,
    BANK_A2P_FIELD_MAP,
    BANK_AIRTABLE_FIELDS,
    BANK_P2A_FIELD_MAP,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
)

logger = logging.getLogger(__name__)


class FncsupSyncEngine(BaseSyncEngine):
    """
    Sync engine for FNCSUP (Financial Parameters for Vendors).

    Has one sub-form: ACCOUNTBANK_SUBFORM for bank account details.
    Uses standard string keys (FNCSUP('2993') works directly).
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
            # FNCSUP uses string keys — standard URL access
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

    # ── Sub-form: Bank account details ───────────────────────────────────

    def _pre_a2p_batch(
        self, records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Pre-fetch bank account details from the secondary Airtable view.
        Returns a context dict with bank data indexed by vendor ID.
        """
        logger.info("Pre-fetching bank account details from '%s'...", AIRTABLE_BANK_VIEW)

        bank_records = self.airtable.fetch_records_from_view(
            view_name=AIRTABLE_BANK_VIEW,
            fields=BANK_AIRTABLE_FIELDS,
        )

        # Index bank records by vendor ID
        bank_by_vendor: dict[str, dict[str, Any]] = {}
        for rec in bank_records:
            fields = rec.get("fields", {})
            vendor_id = clean(fields.get("Priority Vendor ID"))
            if vendor_id:
                bank_by_vendor[vendor_id] = fields

        logger.info("Found bank details for %d vendors.", len(bank_by_vendor))
        return {"bank_by_vendor": bank_by_vendor}

    def _sync_subforms(
        self,
        key: str,
        airtable_fields: dict[str, Any],
        context: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Sync bank account details to ACCOUNTBANK_SUBFORM on FNCSUP.
        """
        bank_by_vendor = context.get("bank_by_vendor", {})
        bank_fields = bank_by_vendor.get(key)

        if not bank_fields:
            logger.debug("No bank details found for vendor %s, skipping ACCOUNTBANK.", key)
            return

        # Build the bank payload from Airtable fields
        bank_payload: dict[str, Any] = {}
        for mapping in BANK_A2P_FIELD_MAP:
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

        # Use deep PATCH on the parent FNCSUP entity to update bank account
        # This is the safest approach — works regardless of sub-form key pattern
        try:
            self.priority.deep_patch_subform(
                key_value=key,
                subform_name=ACCOUNTBANK_SUBFORM_NAME,
                records=[bank_payload],
            )
            result.subform_results.append(
                SubformResult(
                    subform=ACCOUNTBANK_SUBFORM_NAME,
                    action="UPDATED",
                    detail=f"Bank details synced: {list(bank_payload.keys())}",
                )
            )
        except Exception as e:
            logger.error("Failed to sync bank for vendor %s: %s", key, e)
            result.subform_results.append(
                SubformResult(
                    subform=ACCOUNTBANK_SUBFORM_NAME,
                    action="ERROR",
                    detail=str(e),
                )
            )

    def _get_p2a_extra_fields(
        self,
        key: str,
        priority_record: dict[str, Any],
        is_status: bool,
    ) -> dict[str, Any]:
        """
        Fetch bank account details from ACCOUNTBANK_SUBFORM for P→A direction.
        Maps bank sub-form fields back to Airtable field names.
        """
        if is_status:
            return {}

        try:
            bank_records = self.priority.get_subform(key, ACCOUNTBANK_SUBFORM_NAME)

            if not bank_records:
                return {}

            # Use first bank record (vendors typically have one bank account)
            bank_record = bank_records[0]

            extra_fields: dict[str, Any] = {}
            for mapping in BANK_P2A_FIELD_MAP:
                value = bank_record.get(mapping.priority_field)
                if value:
                    cleaned = clean(value)
                    if cleaned:
                        extra_fields[mapping.airtable_field] = cleaned

            return extra_fields

        except Exception as e:
            logger.warning("Failed to fetch bank details for vendor %s: %s", key, e)
            return {}
