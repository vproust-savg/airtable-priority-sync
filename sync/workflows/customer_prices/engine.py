"""
Customer Price Lists sync engine.

Special architecture: Each Airtable record can produce up to 3 price entries
(Base, Whole, Lvl 3) via row explosion — one per price level with a non-empty
price field.

A→P flow:
  1. Fetch all records from Customer Price List v2 table
  2. For each record, check LVL 1/2/3 SALE PRICE fields
  3. For each non-empty price level: deep PATCH PARTPRICE2_SUBFORM
     on PRICELIST('{code}') with the SKU and price

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
from sync.workflows.customer_prices.config import (
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_ID,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    PRICE_LEVELS,
    PRIORITY_ENTITY,
    PRIORITY_ITEMS_SUBFORM,
    PRIORITY_KEY_FIELD,
    TIMESTAMP_FIELDS,
    TIMESTAMP_FIELD_IDS,
)
from sync.workflows.customer_prices.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELD_IDS,
    AIRTABLE_FIELDS_TO_FETCH,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
)

logger = logging.getLogger(__name__)


class CustomerPriceSyncEngine(BaseSyncEngine):
    """
    Sync engine for Customer Price Lists (PRICELIST + PARTPRICE2).

    Row explosion: Each Airtable record → up to 3 price entries
    (Base, Whole, Lvl 3) across different PRICELIST entities.
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

    # ── Override: row explosion sub-form sync ─────────────────────────────

    def _sync_subforms(
        self,
        key: str,
        airtable_fields: dict[str, Any],
        context: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Row explosion: For each price level (Base, Whole, Lvl 3),
        if the corresponding price field has a value, create a
        PARTPRICE2 entry on PRICELIST('{code}').

        The 'key' from the main entity processing is the Price List Code
        from the Airtable record, but we override it per level.
        """
        # Extract the SKU from the record
        raw_sku = airtable_fields.get("SKU Trim (EDI) (from Products)")
        if isinstance(raw_sku, list):
            raw_sku = raw_sku[0] if raw_sku else None
        sku = clean(raw_sku)
        if not sku:
            return

        quantity = clean(airtable_fields.get("Quantity"))

        for level in PRICE_LEVELS:
            price_field = level["price_field"]
            list_code = level["code"]

            raw_price = airtable_fields.get(price_field)
            price = format_price(raw_price)
            if price is None:
                continue  # Skip levels with no price

            payload: dict[str, Any] = {
                "PARTNAME": sku,
                "PRICE": price,
            }
            if quantity:
                payload["QUANT"] = quantity

            if dry_run:
                result.subform_results.append(
                    SubformResult(
                        subform=f"{PRIORITY_ITEMS_SUBFORM}@{list_code}",
                        action="DRY_RUN",
                        detail=f"Would sync {sku} = {price} on PRICELIST('{list_code}')",
                    )
                )
                continue

            try:
                self.priority.deep_patch_subform(
                    key_value=list_code,
                    subform_name=PRIORITY_ITEMS_SUBFORM,
                    records=[payload],
                )
                result.subform_results.append(
                    SubformResult(
                        subform=f"{PRIORITY_ITEMS_SUBFORM}@{list_code}",
                        action="UPDATED",
                        detail=f"{sku} = {price}",
                    )
                )
            except Exception as e:
                logger.error(
                    "Failed to sync price %s/%s on PRICELIST('%s'): %s",
                    sku, price, list_code, e,
                )
                result.subform_results.append(
                    SubformResult(
                        subform=f"{PRIORITY_ITEMS_SUBFORM}@{list_code}",
                        action="ERROR",
                        detail=str(e),
                    )
                )
