"""
Vendor Price Lists sync engine.

Special architecture: This workflow doesn't follow the standard 1:1 entity pattern.
Instead, each Airtable record maps to a price entry within a PRICELIST's PARTPRICE2
sub-form. Multiple Airtable records with the same "Price List Code" go into the same
PRICELIST entity.

A→P flow:
  1. Fetch all records from Vendor Price List table
  2. Group by Price List Code
  3. For each Price List Code: deep PATCH PARTPRICE2_SUBFORM with price entries

P→A: Minimal — most Airtable fields are lookups/formulas.
"""

from __future__ import annotations

import logging
from typing import Any

from sync.core.airtable_client import AirtableClient
from sync.core.base_engine import BaseSyncEngine, build_field_id_map
from sync.core.models import FieldMapping, SubformResult, SyncMode, SyncRecord
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.core.utils import clean, format_price
from sync.workflows.vendor_prices.config import (
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_ID,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    PRIORITY_ENTITY,
    PRIORITY_ITEMS_SUBFORM,
    PRIORITY_KEY_FIELD,
    TIMESTAMP_FIELDS,
    TIMESTAMP_FIELD_IDS,
)
from sync.workflows.vendor_prices.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELD_IDS,
    AIRTABLE_FIELDS_TO_FETCH,
    ITEMS_FIELD_IDS,
    ITEMS_FIELD_MAP,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
)

logger = logging.getLogger(__name__)


class VendorPriceSyncEngine(BaseSyncEngine):
    """
    Sync engine for Vendor Price Lists (PRICELIST + PARTPRICE2).

    Groups Airtable records by Price List Code, then deep-patches
    the PARTPRICE2 sub-form on each PRICELIST entity.
    """

    # ── Client factories ─────────────────────────────────────────────────

    def _create_airtable_client(
        self,
        base_id_override: str | None,
        token_override: str | None,
    ) -> AirtableClient:
        field_id_map = build_field_id_map(
            A2P_FIELD_MAP, P2A_FIELD_MAP,
            extra={
                AIRTABLE_KEY_FIELD: AIRTABLE_KEY_FIELD_ID,
                **{v: TIMESTAMP_FIELD_IDS[k] for k, v in TIMESTAMP_FIELDS.items()},
                **AIRTABLE_FIELD_IDS,
                **ITEMS_FIELD_IDS,
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
        )

    def _create_sync_log_client(self) -> SyncLogClient:
        return SyncLogClient(entity_label="Price List")

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

    # ── Override: sub-form sync as the main A→P pattern ──────────────────

    def _sync_subforms(
        self,
        key: str,
        airtable_fields: dict[str, Any],
        context: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        The main sync logic for vendor price lists lives here.
        Each record maps to a PARTPRICE2 entry on the PRICELIST(key).

        We build the sub-form payload from the Airtable record's fields.
        The deep_patch_subform call adds/updates the price entry.
        """
        # Build the PARTPRICE2 payload from this record
        payload: dict[str, Any] = {}
        for at_field, p_field in ITEMS_FIELD_MAP.items():
            raw = airtable_fields.get(at_field)
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            if p_field == "PRICE":
                value = format_price(raw)
            else:
                value = clean(raw)
            if value is not None:
                payload[p_field] = value

        if not payload or "PARTNAME" not in payload:
            return

        if dry_run:
            result.subform_results.append(
                SubformResult(
                    subform=PRIORITY_ITEMS_SUBFORM,
                    action="DRY_RUN",
                    detail=f"Would sync price: {payload}",
                )
            )
            return

        try:
            self.priority.deep_patch_subform(
                key_value=key,
                subform_name=PRIORITY_ITEMS_SUBFORM,
                records=[payload],
            )
            result.subform_results.append(
                SubformResult(
                    subform=PRIORITY_ITEMS_SUBFORM,
                    action="UPDATED",
                    detail=f"Price entry: {payload.get('PARTNAME', '?')} = {payload.get('PRICE', '?')}",
                )
            )
        except Exception as e:
            logger.error(
                "Failed to sync price entry for %s/%s: %s",
                key, payload.get("PARTNAME", "?"), e,
            )
            result.subform_results.append(
                SubformResult(
                    subform=PRIORITY_ITEMS_SUBFORM,
                    action="ERROR",
                    detail=str(e),
                )
            )
