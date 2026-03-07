"""
Product-specific sync engine: subclasses BaseSyncEngine with product
field mappings, sub-form logic (allergens, shelf lives, price lists, bins),
and shelf-life fetching from a separate Airtable table.

This is the concrete implementation for syncing LOGPART (products) between
Airtable and Priority ERP. All orchestration (A->P and P->A loops,
timestamps, batch writes) lives in BaseSyncEngine; this class provides:
  - Client factories (AirtableClient, PriorityClient, SyncLogClient)
  - Field map selectors per direction/mode
  - Shelf-life pre-fetch from a separate Airtable table (A->P only)
  - Sub-form sync logic: allergens, shelf lives, price lists, bins
  - P->A allergen fetching for reverse sync
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from sync.core.airtable_client import AirtableClient
from sync.core.base_engine import BaseSyncEngine, build_field_id_map
from sync.core.config import (
    AIRTABLE_API_BASE,
    AIRTABLE_MAX_RETRIES,
    AIRTABLE_REQUEST_TIMEOUT,
)
from sync.core.logger_setup import print_detail, print_section
from sync.core.models import FieldMapping, SubformResult, SyncError, SyncMode, SyncRecord
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.core.utils import clean, values_equal
from sync.core.base_engine import map_airtable_to_priority
from sync.workflows.products.config import (
    AIRTABLE_FIELD_SKU,
    AIRTABLE_FIELD_SKU_ID,
    AIRTABLE_FIELD_SKU_WRITABLE,
    AIRTABLE_FIELD_SKU_WRITABLE_ID,
    AIRTABLE_PRODUCTS_TABLE_NAME,
    AIRTABLE_SHELF_LIVES_TABLE_ID,
    AIRTABLE_SHELF_LIVES_VIEW,
    AIRTABLE_SYNC_VIEW,
    PRIORITY_ENTITY,
    PRIORITY_FNCPART_ENTITY,
    PRIORITY_KEY_FIELD,
    PRIORITY_PRDPART_ENTITY,
    TIMESTAMP_FIELDS,
    TIMESTAMP_FIELD_IDS,
)
from sync.workflows.products.field_mapping import (
    AIRTABLE_FIELDS_TO_FETCH,
    FNCPART_A2P_FIELD_MAP,
    FNCPART_P2A_FIELD_MAP,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
    P2A_STATUS_AIRTABLE_FIELDS,
    P2A_STATUS_FIELD_MAP,
    P2A_STATUS_PRIORITY_SELECT,
    PRDPART_A2P_FIELD_MAP,
    PRDPART_P2A_FIELD_MAP,
    PRODUCT_FIELD_MAP,
    STATUS_FIELD_MAP,
    STATUS_FIELDS_TO_FETCH,
)
from sync.workflows.products.subform_mapping import (
    ALLERGEN_FIELD_MAP,
    ALLERGEN_SUBFORM_NAME,
    BIN_FIELD_MAP,
    BIN_SUBFORM_NAME,
    P2A_SHELF_LIFE_FIELD_IDS,
    P2A_SHELF_LIFE_FIELDS,
    PRICE_LIST_FIELD_IDS,
    PRICE_LIST_SHARED_FIELD_IDS,
    PRICE_LIST_SUBFORM_NAME,
    SHELF_LIFE_AIRTABLE_FIELDS,
    SHELF_LIFE_SUBFORM_NAME,
    map_allergens,
    map_allergens_to_airtable,
    map_bins,
    map_price_lists,
    map_shelf_lives,
)

logger = logging.getLogger(__name__)


class ProductSyncEngine(BaseSyncEngine):
    """
    Concrete sync engine for products (LOGPART + FNCPART + PRDPART).

    Extends BaseSyncEngine with:
    - Product-specific field mappings (full + status-only, both directions)
    - Sub-form sync (allergens, shelf lives, price lists, bins) for A->P
    - Secondary entity sync (FNCPART, PRDPART) for A->P and P->A
    - Shelf-life pre-fetch from a separate Airtable table
    - Allergen + FNCPART + PRDPART fetch from Priority for P->A
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Secondary Priority clients (FNCPART, PRDPART)
        self.priority_fncpart = PriorityClient(
            entity=PRIORITY_FNCPART_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            use_filter_lookup=True,
            api_url_override=self.priority_url_override,
        )
        self.priority_prdpart = PriorityClient(
            entity=PRIORITY_PRDPART_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            use_filter_lookup=True,
            api_url_override=self.priority_url_override,
        )

    # ── Abstract method implementations: client factories ────────────────

    def _create_airtable_client(
        self,
        base_id_override: str | None,
        token_override: str | None,
    ) -> AirtableClient:
        """Create an AirtableClient configured for the Products table."""
        # Skip ALL field IDs for test base — production IDs don't exist
        # in the duplicated base.  _to_id() falls back to field names.
        if base_id_override:
            field_id_map = None
        else:
            field_id_map = build_field_id_map(
                PRODUCT_FIELD_MAP, STATUS_FIELD_MAP,
                P2A_FIELD_MAP, P2A_STATUS_FIELD_MAP,
                FNCPART_A2P_FIELD_MAP, FNCPART_P2A_FIELD_MAP,
                PRDPART_A2P_FIELD_MAP, PRDPART_P2A_FIELD_MAP,
                ALLERGEN_FIELD_MAP, BIN_FIELD_MAP,
                extra={
                    AIRTABLE_FIELD_SKU: AIRTABLE_FIELD_SKU_ID,
                    AIRTABLE_FIELD_SKU_WRITABLE: AIRTABLE_FIELD_SKU_WRITABLE_ID,
                    **{v: TIMESTAMP_FIELD_IDS[k] for k, v in TIMESTAMP_FIELDS.items()},
                    **PRICE_LIST_FIELD_IDS,
                    **PRICE_LIST_SHARED_FIELD_IDS,
                },
            )
        return AirtableClient(
            table_name=AIRTABLE_PRODUCTS_TABLE_NAME,
            key_field=AIRTABLE_FIELD_SKU,
            key_field_writable=AIRTABLE_FIELD_SKU_WRITABLE,
            sync_view=AIRTABLE_SYNC_VIEW,
            timestamp_fields=TIMESTAMP_FIELDS,
            field_id_map=field_id_map,
            base_id_override=base_id_override,
            token_override=token_override,
        )

    def _create_priority_client(self, api_url_override: str | None = None) -> PriorityClient:
        """Create a PriorityClient configured for LOGPART."""
        return PriorityClient(
            entity=PRIORITY_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            api_url_override=api_url_override,
        )

    def _create_sync_log_client(self) -> SyncLogClient:
        """Create a SyncLogClient with 'SKU' as the entity label."""
        return SyncLogClient(entity_label="SKU")

    # ── Abstract method implementations: field map selectors ─────────────

    def _get_a2p_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        """Return the A->P field mapping list for the given mode."""
        if mode == SyncMode.STATUS:
            return STATUS_FIELD_MAP
        return PRODUCT_FIELD_MAP

    def _get_a2p_fields_to_fetch(self, mode: SyncMode) -> list[str]:
        """Return the list of Airtable fields to fetch for A->P sync."""
        if mode == SyncMode.STATUS:
            return STATUS_FIELDS_TO_FETCH
        return AIRTABLE_FIELDS_TO_FETCH

    def _get_p2a_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        """Return the P->A field mapping list for the given mode."""
        if mode == SyncMode.STATUS:
            return P2A_STATUS_FIELD_MAP
        return P2A_FIELD_MAP

    def _get_p2a_airtable_fields(self, mode: SyncMode) -> list[str]:
        """Return the Airtable fields to fetch for P->A comparison."""
        if mode == SyncMode.STATUS:
            return P2A_STATUS_AIRTABLE_FIELDS
        return P2A_AIRTABLE_FIELDS_TO_FETCH

    def _get_p2a_priority_select(self, mode: SyncMode) -> list[str]:
        """Return the Priority $select fields for P->A fetch."""
        if mode == SyncMode.STATUS:
            return P2A_STATUS_PRIORITY_SELECT
        return P2A_PRIORITY_SELECT

    def _get_key_field_name(self) -> str:
        """Return the Priority key field name."""
        return PRIORITY_KEY_FIELD

    def _get_airtable_key_field(self) -> str:
        """Return the Airtable key field name for reading (formula field)."""
        return AIRTABLE_FIELD_SKU

    def _get_airtable_key_field_writable(self) -> str:
        """Return the Airtable key field name for writing/creates."""
        return AIRTABLE_FIELD_SKU_WRITABLE

    # ── Hook overrides ───────────────────────────────────────────────────

    def _pre_a2p_batch(
        self, records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Pre-fetch shelf lives from a separate Airtable table before
        processing A->P records.

        In STATUS mode, shelf lives are not needed (no sub-form fields),
        so we return an empty context.

        Returns:
            {"shelf_lives_by_sku": {sku: [records...]}} or empty dict.
        """
        if self.mode == SyncMode.STATUS:
            print_section("Skipping shelf lives (status-only mode).")
            return {"shelf_lives_by_sku": {}}

        print_section("Loading shelf lives from Airtable...")
        try:
            shelf_lives_by_sku = self._fetch_shelf_lives()
            print_detail(f"Loaded shelf lives for {len(shelf_lives_by_sku)} SKUs.")
        except Exception as e:
            logger.error("Failed to fetch shelf lives: %s", e)
            shelf_lives_by_sku = {}
            print_detail(
                f"Warning: Could not load shelf lives ({e}). Continuing without."
            )

        return {"shelf_lives_by_sku": shelf_lives_by_sku}

    def _sync_subforms(
        self,
        key: str,
        airtable_fields: dict[str, Any],
        context: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Sync all sub-forms for a product after main LOGPART sync.
        Errors are logged but don't fail the overall product sync.

        Sub-forms synced (A->P direction):
        1. Allergens & Features (SAVR_ALLERGENS_SUBFORM) -- single record
        2. Shelf Lives (SAVR_PARTSHELF_SUBFORM) -- multi-record
        3. Price Lists (PARTINCUSTPLISTS_SUBFORM) -- multi-record, deep PATCH
        4. Bin Locations (PARTLOCATIONS_SUBFORM) -- single record, deep PATCH
        """
        shelf_lives_by_sku = context.get("shelf_lives_by_sku", {})
        shelf_life_records = shelf_lives_by_sku.get(key, [])

        if dry_run:
            # In dry run, just show what sub-forms would be synced
            allergen_payload = map_allergens(airtable_fields)
            if allergen_payload:
                result.subform_results.append(SubformResult(
                    subform="allergens",
                    action="dry_run",
                    detail=f"{len(allergen_payload)} fields",
                ))

            shelf_payloads = map_shelf_lives(shelf_life_records)
            if shelf_payloads:
                result.subform_results.append(SubformResult(
                    subform="shelf_lives",
                    action="dry_run",
                    detail=f"{len(shelf_payloads)} entries",
                ))

            price_payloads = map_price_lists(airtable_fields)
            if price_payloads:
                result.subform_results.append(SubformResult(
                    subform="price_lists",
                    action="dry_run",
                    detail=f"{len(price_payloads)} levels",
                ))

            bin_payload = map_bins(airtable_fields)
            if bin_payload:
                result.subform_results.append(SubformResult(
                    subform="bins",
                    action="dry_run",
                    detail=f"{len(bin_payload)} fields",
                ))
            return

        # 1. Allergens & Features (single-record sub-form)
        self._sync_allergens(key, airtable_fields, result)

        # 2. Shelf Lives (multi-record sub-form)
        self._sync_shelf_lives(key, shelf_life_records, result)

        # 3. Price Lists (multi-record sub-form, deep PATCH)
        self._sync_price_lists(key, airtable_fields, result)

        # 4. Bin Locations (single-record sub-form, deep PATCH)
        self._sync_bins(key, airtable_fields, result)

        # 5. FNCPART (Financial Parameters — secondary entity)
        self._sync_secondary_entity(
            self.priority_fncpart, key, airtable_fields, result,
            FNCPART_A2P_FIELD_MAP, "fncpart",
        )

        # 6. PRDPART (MRP Parameters — secondary entity)
        self._sync_secondary_entity(
            self.priority_prdpart, key, airtable_fields, result,
            PRDPART_A2P_FIELD_MAP, "prdpart",
        )

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
        Sync fields to a secondary Priority entity (FNCPART, PRDPART, etc.).

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
                if not values_equal(current, v):
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

    def _get_p2a_extra_field_map(self) -> list[FieldMapping]:
        """Include FNCPART + PRDPART field maps for P→A comparison."""
        return list(FNCPART_P2A_FIELD_MAP) + list(PRDPART_P2A_FIELD_MAP)

    def _get_p2a_extra_fields(
        self,
        key: str,
        priority_record: dict[str, Any],
        is_status: bool,
    ) -> dict[str, Any]:
        """
        Fetch extra fields from Priority for P->A direction (full mode only).

        Sources:
        1. Allergens sub-form (SAVR_ALLERGENS_SUBFORM)
        2. FNCPART (Financial Parameters) — secondary entity
        3. PRDPART (MRP Parameters) — secondary entity
        """
        if is_status:
            return {}

        extra: dict[str, Any] = {}

        # 1. Allergens sub-form
        try:
            allergen_data = self.priority.get_subform(key, ALLERGEN_SUBFORM_NAME)
            if allergen_data:
                extra.update(map_allergens_to_airtable(allergen_data[0]))
        except Exception as e:
            logger.warning("Failed to fetch allergens for %s: %s", key, e)

        # 2. FNCPART fields → Airtable
        try:
            fncpart_data = self.priority_fncpart.get_record(key)
            if fncpart_data:
                from sync.core.base_engine import map_priority_to_airtable
                # Fetch FNCPART lookups (cached on first call)
                if not hasattr(self, "_fncpart_lookups"):
                    self._fncpart_lookups = self._fetch_priority_lookups(
                        FNCPART_P2A_FIELD_MAP,
                    )
                fncpart_mapped = map_priority_to_airtable(
                    fncpart_data, FNCPART_P2A_FIELD_MAP, is_create=False,
                    lookups=self._fncpart_lookups,
                )
                extra.update(fncpart_mapped)
        except Exception as e:
            logger.warning("Failed to fetch FNCPART for %s: %s", key, e)

        # 3. PRDPART fields → Airtable
        try:
            prdpart_data = self.priority_prdpart.get_record(key)
            if prdpart_data:
                from sync.core.base_engine import map_priority_to_airtable
                prdpart_mapped = map_priority_to_airtable(
                    prdpart_data, PRDPART_P2A_FIELD_MAP, is_create=False,
                )
                extra.update(prdpart_mapped)
        except Exception as e:
            logger.warning("Failed to fetch PRDPART for %s: %s", key, e)

        return extra

    # ── Shelf lives fetch (product-specific, separate Airtable table) ────

    def _fetch_shelf_lives(self) -> dict[str, list[dict[str, Any]]]:
        """
        Fetch all shelf life records from the Shelf Lives table,
        grouped by SKU.

        Uses the 'EDI Parts 2 - Shelf Lives' view and the Shelf Lives
        table ID (separate from the Products table).

        Returns:
            dict mapping SKU -> list of Airtable shelf life records.
        """
        shelf_lives_url = (
            f"{AIRTABLE_API_BASE}/{self.airtable._base_id}/{AIRTABLE_SHELF_LIVES_TABLE_ID}"
        )
        field_params = [("fields[]", f) for f in SHELF_LIFE_AIRTABLE_FIELDS]
        records: list[dict[str, Any]] = []
        offset: str | None = None

        for attempt in range(AIRTABLE_MAX_RETRIES):
            try:
                while True:
                    params = [("view", AIRTABLE_SHELF_LIVES_VIEW)] + field_params
                    if offset:
                        params.append(("offset", offset))

                    logger.debug("Fetching shelf lives (offset: %s)", offset or "start")
                    response = self.airtable.session.get(
                        shelf_lives_url,
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

                    offset = data.get("offset")
                    if not offset:
                        break
                    time.sleep(0.2)

                break  # Success

            except Exception as e:
                if attempt == AIRTABLE_MAX_RETRIES - 1:
                    logger.error(
                        "Failed to fetch shelf lives after %d attempts: %s",
                        AIRTABLE_MAX_RETRIES,
                        e,
                    )
                    raise
                wait_time = 2 ** attempt
                logger.warning(
                    "Shelf lives attempt %d failed, retrying in %ds: %s",
                    attempt + 1,
                    wait_time,
                    e,
                )
                time.sleep(wait_time)

        # Group by SKU
        by_sku: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            fields = record.get("fields", {})
            # SKU comes from a linked field -- it's a list
            sku_raw = fields.get("SKU Trim (EDI) (from Products)")
            sku = clean(sku_raw)
            if sku:
                by_sku.setdefault(sku, []).append(record)

        logger.info(
            "Fetched %d shelf life records for %d SKUs",
            len(records),
            len(by_sku),
        )
        return by_sku

    # ── Sub-form sync methods (A->P direction) ───────────────────────────

    def _sync_allergens(
        self, sku: str, fields: dict[str, Any], result: SyncRecord
    ) -> None:
        """
        Sync allergens & features sub-form (Pattern A: single-record).

        Maps Airtable allergen/feature fields to Priority SAVR_ALLERGENS_SUBFORM
        and upserts (POST if no record exists, PATCH if changed).
        """
        try:
            payload = map_allergens(fields)
            if not payload:
                return

            res = self.priority.upsert_single_subform(
                sku, ALLERGEN_SUBFORM_NAME, payload
            )
            result.subform_results.append(SubformResult(
                subform="allergens",
                action=res["action"],
                detail=f"{res['fields_changed']} fields",
            ))
            logger.debug("Allergens for %s: %s", sku, res["action"])

        except Exception as e:
            logger.error("Allergen sub-form error for %s: %s", sku, e)
            result.subform_results.append(SubformResult(
                subform="allergens",
                action="error",
                detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                entity_key=sku,
                action="SUBFORM",
                message=f"allergens: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    def _sync_shelf_lives(
        self,
        sku: str,
        shelf_life_records: list[dict[str, Any]],
        result: SyncRecord,
    ) -> None:
        """
        Sync shelf lives sub-form (Pattern B: multi-record with URL keys).

        Maps Airtable Shelf Lives table records to Priority SAVR_PARTSHELF_SUBFORM.
        Matches records by TYPE field, PATCHes using SHELFLIFE integer key.
        """
        try:
            payloads = map_shelf_lives(shelf_life_records)
            if not payloads:
                return

            res = self.priority.sync_multi_subform(
                sku,
                SHELF_LIFE_SUBFORM_NAME,
                "TYPE",
                payloads,
                url_key_field="SHELFLIFE",  # Priority uses SHELFLIFE int as entity key
            )
            detail = f"c:{res['created']} u:{res['updated']} s:{res['skipped']}"
            result.subform_results.append(SubformResult(
                subform="shelf_lives",
                action="synced",
                detail=detail,
            ))
            logger.debug("Shelf lives for %s: %s", sku, detail)

        except Exception as e:
            logger.error("Shelf life sub-form error for %s: %s", sku, e)
            result.subform_results.append(SubformResult(
                subform="shelf_lives",
                action="error",
                detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                entity_key=sku,
                action="SUBFORM",
                message=f"shelf_lives: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    def _sync_price_lists(
        self, sku: str, fields: dict[str, Any], result: SyncRecord
    ) -> None:
        """
        Sync price lists sub-form (Pattern C: multi-record, no URL keys).

        PARTINCUSTPLISTS_SUBFORM doesn't expose individual record keys,
        so we compare locally and use deep PATCH on the parent LOGPART
        to push changes. Priority matches records internally by PLNAME.
        """
        try:
            payloads = map_price_lists(fields)
            if not payloads:
                return

            # GET existing price lists from Priority
            existing = self.priority.get_subform(sku, PRICE_LIST_SUBFORM_NAME)
            existing_by_plname: dict[str, dict[str, Any]] = {}
            for rec in existing:
                plname = str(rec.get("PLNAME", "")).strip()
                if plname:
                    existing_by_plname[plname] = rec

            # Determine what needs creating vs updating
            records_to_push: list[dict[str, Any]] = []
            created = 0
            updated = 0
            skipped = 0

            for desired in payloads:
                plname = str(desired.get("PLNAME", "")).strip()
                if not plname:
                    continue

                if plname not in existing_by_plname:
                    # New price list -> include in deep PATCH
                    records_to_push.append(desired)
                    created += 1
                else:
                    # Existing -> compare fields
                    current = existing_by_plname[plname]
                    has_changes = False
                    for field, new_value in desired.items():
                        if field == "PLNAME":
                            continue
                        old_value = current.get(field)
                        if not values_equal(new_value, old_value):
                            has_changes = True
                            break

                    if has_changes:
                        records_to_push.append(desired)
                        updated += 1
                    else:
                        skipped += 1

            if records_to_push:
                self.priority.deep_patch_subform(
                    sku, PRICE_LIST_SUBFORM_NAME, records_to_push
                )

            detail = f"c:{created} u:{updated} s:{skipped}"
            result.subform_results.append(SubformResult(
                subform="price_lists",
                action="synced",
                detail=detail,
            ))
            logger.debug("Price lists for %s: %s", sku, detail)

        except Exception as e:
            logger.error("Price list sub-form error for %s: %s", sku, e)
            result.subform_results.append(SubformResult(
                subform="price_lists",
                action="error",
                detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                entity_key=sku,
                action="SUBFORM",
                message=f"price_lists: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    def _sync_bins(
        self, sku: str, fields: dict[str, Any], result: SyncRecord
    ) -> None:
        """
        Sync bin locations sub-form (Pattern C: no URL keys, deep PATCH).

        PARTLOCATIONS_SUBFORM doesn't expose individual record keys,
        so we compare locally and use deep PATCH on the parent LOGPART.
        """
        try:
            payload = map_bins(fields)
            if not payload or (len(payload) == 1 and "WARHSNAME" in payload):
                # Only default warehouse, no actual bin data
                return

            # GET existing bins from Priority
            existing = self.priority.get_subform(sku, BIN_SUBFORM_NAME)

            # Check if bin data has changed
            if existing:
                current = existing[0]
                has_changes = False
                for field, new_value in payload.items():
                    old_value = current.get(field)
                    if not values_equal(new_value, old_value):
                        has_changes = True
                        break
                if not has_changes:
                    result.subform_results.append(SubformResult(
                        subform="bins",
                        action="skipped",
                        detail="no changes",
                    ))
                    return

            # Use deep PATCH to update bins
            self.priority.deep_patch_subform(
                sku, BIN_SUBFORM_NAME, [payload]
            )

            action = "created" if not existing else "updated"
            result.subform_results.append(SubformResult(
                subform="bins",
                action=action,
                detail=f"{len(payload)} fields",
            ))
            logger.debug("Bins for %s: %s", sku, action)

        except Exception as e:
            logger.error("Bin sub-form error for %s: %s", sku, e)
            result.subform_results.append(SubformResult(
                subform="bins",
                action="error",
                detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                entity_key=sku,
                action="SUBFORM",
                message=f"bins: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    # ── P→A sub-form sync: shelf lives → Airtable Shelf Lives table ──────

    def _post_p2a_sync(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """
        After main P→A product sync, sync shelf life sub-form data from
        Priority into the separate Airtable Shelf Lives table.

        Only runs in FULL mode (not STATUS).
        """
        if self.mode == SyncMode.STATUS:
            return

        try:
            self._sync_p2a_shelf_lives(priority_records, airtable_by_key)
        except Exception as e:
            logger.error("P→A shelf lives sync failed: %s", e)
            print_detail(f"⚠ Shelf lives sync error: {e}")

    def _sync_p2a_shelf_lives(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """
        Sync shelf lives from Priority SAVR_PARTSHELF_SUBFORM into the
        Airtable Shelf Lives table.

        For each product in priority_records:
        1. GET shelf life sub-form from Priority
        2. Compare with existing Airtable shelf life records
        3. CREATE new records / UPDATE changed records in Airtable

        Match key: SKU + Type (e.g., "API Test" + "Frozen")
        """
        print_section("Syncing shelf lives (P→A)...")

        # Step 1: Pre-fetch ALL existing Airtable shelf lives
        airtable_shelf_lives = self._fetch_airtable_shelf_lives_for_p2a()
        print_detail(
            f"Existing Airtable shelf lives: {sum(len(v) for v in airtable_shelf_lives.values())} "
            f"records across {len(airtable_shelf_lives)} SKUs"
        )

        # Step 2: Collect SKUs to process
        priority_key = self._get_key_field_name()
        skus_to_process = []
        for rec in priority_records:
            sku = str(rec.get(priority_key, "")).strip()
            if sku:
                skus_to_process.append(sku)

        creates: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        unchanged = 0

        # Step 3: For each product, fetch Priority sub-form and compare
        for sku in skus_to_process:
            # Get the Products record ID (needed for linked record on CREATE)
            products_record = airtable_by_key.get(sku)
            if not products_record:
                logger.debug("SKU %s not in Airtable — skipping shelf lives", sku)
                continue
            products_record_id = products_record["record_id"]

            # Fetch shelf lives from Priority
            try:
                priority_shelf_lives = self.priority.get_subform(
                    sku, SHELF_LIFE_SUBFORM_NAME,
                )
            except Exception as e:
                logger.warning("Failed to fetch shelf lives from Priority for %s: %s", sku, e)
                continue

            if not priority_shelf_lives:
                continue

            # Get existing Airtable shelf lives for this SKU
            existing_by_type = airtable_shelf_lives.get(sku, {})

            for p_record in priority_shelf_lives:
                p_type = str(p_record.get("TYPE", "")).strip()
                p_number = p_record.get("NUMBER")
                p_unit = str(p_record.get("TIMEUNIT", "")).strip()

                if not p_type:
                    continue

                existing = existing_by_type.get(p_type)

                if existing is None:
                    # CREATE: new shelf life record in Airtable
                    fields: dict[str, Any] = {
                        "Type": p_type,
                        "Products": [products_record_id],
                    }
                    if p_number is not None:
                        fields["Shelf Life Input"] = p_number
                    if p_unit:
                        fields["Shelf Life Unit Input"] = p_unit

                    creates.append({"fields": fields})
                else:
                    # Compare and UPDATE if changed
                    existing_fields = existing["fields"]
                    patch_fields: dict[str, Any] = {}

                    # Compare Number
                    existing_number = existing_fields.get("Shelf Life Input")
                    if p_number is not None and existing_number != p_number:
                        patch_fields["Shelf Life Input"] = p_number

                    # Compare Unit
                    existing_unit = str(existing_fields.get("Shelf Life Unit Input") or "").strip()
                    if p_unit and existing_unit != p_unit:
                        patch_fields["Shelf Life Unit Input"] = p_unit

                    if patch_fields:
                        updates.append({
                            "id": existing["record_id"],
                            "fields": patch_fields,
                        })
                    else:
                        unchanged += 1

        # Step 4: Batch write to Airtable Shelf Lives table
        created_count = 0
        updated_count = 0

        if creates and not self.dry_run:
            created_count = self.airtable.batch_create_to_table(
                AIRTABLE_SHELF_LIVES_TABLE_ID, creates,
            )

        if updates and not self.dry_run:
            updated_count = self.airtable.batch_update_to_table(
                AIRTABLE_SHELF_LIVES_TABLE_ID, updates,
            )

        summary = (
            f"Shelf lives P→A: "
            f"{created_count or len(creates)} created, "
            f"{updated_count or len(updates)} updated, "
            f"{unchanged} unchanged"
        )
        print_detail(summary)
        logger.info(summary)

    def _fetch_airtable_shelf_lives_for_p2a(
        self,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """
        Fetch ALL existing shelf life records from the Airtable Shelf Lives table.

        Returns:
            Nested dict: ``{sku: {type: {"record_id": str, "fields": dict}}}``
        """
        shelf_lives_url = (
            f"{AIRTABLE_API_BASE}/{self.airtable._base_id}/{AIRTABLE_SHELF_LIVES_TABLE_ID}"
        )
        field_params = [("fields[]", f) for f in P2A_SHELF_LIFE_FIELDS]
        records: list[dict[str, Any]] = []
        offset: str | None = None

        while True:
            # No view filter: fetch ALL shelf life records for comparison.
            # (The A→P view may exclude records created by P→A sync.)
            params = list(field_params)
            if offset:
                params.append(("offset", offset))

            response = self.airtable.session.get(
                shelf_lives_url,
                params=params,
                timeout=AIRTABLE_REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 30))
                logger.warning("Rate limited fetching shelf lives. Waiting %ds...", retry_after)
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            time.sleep(0.2)

        # Group by SKU → Type
        by_sku: dict[str, dict[str, dict[str, Any]]] = {}
        for record in records:
            fields = record.get("fields", {})
            record_id = record.get("id", "")

            # SKU comes from a lookup field — it's a list
            sku_raw = fields.get("SKU Trim (EDI) (from Products)")
            sku = clean(sku_raw)
            if not sku:
                continue

            shelf_type = str(fields.get("Type") or "").strip()
            if not shelf_type:
                continue

            by_sku.setdefault(sku, {})[shelf_type] = {
                "record_id": record_id,
                "fields": fields,
            }

        return by_sku
