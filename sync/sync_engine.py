"""
Core sync engine: orchestrates the Airtable → Priority product sync.
Designed for bidirectional sync — Phase 1 implements A→P direction.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from sync.airtable_client import AirtableClient
from sync.config import AIRTABLE_FIELD_SKU
from sync.field_mapping import (
    STATUS_FIELD_MAP,
    STATUS_FIELDS_TO_FETCH,
    build_patch_body,
    map_airtable_to_priority,
)
from sync.logger_setup import (
    print_banner,
    print_detail,
    print_record_line,
    print_section,
    print_summary,
)
from sync.models import (
    SubformResult,
    SyncAction,
    SyncDirection,
    SyncError,
    SyncMode,
    SyncRecord,
    SyncStats,
)
from sync.priority_client import PriorityClient
from sync.subform_mapping import (
    ALLERGEN_SUBFORM_NAME,
    BIN_SUBFORM_NAME,
    PRICE_LIST_SUBFORM_NAME,
    SHELF_LIFE_SUBFORM_NAME,
    map_allergens,
    map_bins,
    map_price_lists,
    map_shelf_lives,
)
from sync.sync_log_client import SyncLogClient
from sync.utils import clean

logger = logging.getLogger(__name__)


class ProductSyncEngine:
    """
    Orchestrates product data sync between Airtable and Priority ERP.

    Phase 1: Airtable → Priority (one-way)
    Phase 2 (future): Priority → Airtable
    Phase 3 (future): Bidirectional with conflict resolution
    """

    def __init__(
        self,
        direction: SyncDirection = SyncDirection.AIRTABLE_TO_PRIORITY,
        dry_run: bool = False,
        single_sku: str | None = None,
        trigger: str = "manual",
        mode: SyncMode = SyncMode.FULL,
    ) -> None:
        self.direction = direction
        self.dry_run = dry_run
        self.single_sku = single_sku
        self.trigger = trigger
        self.mode = mode
        self.airtable = AirtableClient()
        self.priority = PriorityClient()
        self.sync_log = SyncLogClient()
        self.stats = SyncStats()

    # ── Main entry point ─────────────────────────────────────────────────

    def run(self) -> SyncStats:
        """Execute the sync and return stats."""
        self.stats.start_time = datetime.now(timezone.utc)

        if self.direction == SyncDirection.AIRTABLE_TO_PRIORITY:
            self._sync_airtable_to_priority()
        else:
            raise NotImplementedError(
                f"Direction {self.direction.value} not yet implemented. "
                "Phase 2 will add Priority → Airtable."
            )

        self.stats.end_time = datetime.now(timezone.utc)

        # Log run to Airtable (non-blocking — failures don't affect sync result)
        if not self.dry_run:
            # Direction must match Airtable single-select options: "A→P" or "P→A"
            direction_label = "A→P" if self.direction == SyncDirection.AIRTABLE_TO_PRIORITY else "P→A"
            self.sync_log.log_run(self.stats, direction=direction_label, trigger=self.trigger)

        return self.stats

    # ── Airtable → Priority ──────────────────────────────────────────────

    def _sync_airtable_to_priority(self) -> None:
        """Full sync flow: Airtable → Priority."""

        is_status = self.mode == SyncMode.STATUS
        mode_suffix = " (STATUS ONLY)" if is_status else ""
        label = f"DRY RUN — Airtable → Priority{mode_suffix}" if self.dry_run else f"Airtable → Priority{mode_suffix}"
        print_banner(label)

        # In STATUS mode, only fetch status fields from Airtable
        fields_override = STATUS_FIELDS_TO_FETCH if is_status else None

        # Step 1: Fetch records from Airtable
        if self.single_sku:
            # When testing a specific SKU, fetch it directly (bypass sync view)
            print_section(f"Fetching SKU {self.single_sku} from Airtable...")
            airtable_records = self.airtable.fetch_record_by_sku(
                self.single_sku, fields_override=fields_override,
            )
            if not airtable_records:
                # Fallback: try the sync view in case SKU is flagged
                print_detail(f"SKU {self.single_sku} not found by direct lookup, trying sync view...")
                airtable_records = self.airtable.fetch_changed_records(
                    fields_override=fields_override,
                )
                airtable_records = [
                    r for r in airtable_records
                    if clean(r.get("fields", {}).get(AIRTABLE_FIELD_SKU)) == self.single_sku
                ]
        else:
            print_section("Fetching changed records from Airtable...")
            airtable_records = self.airtable.fetch_changed_records(
                fields_override=fields_override,
            )

        self.stats.total_fetched = len(airtable_records)

        if not airtable_records:
            print_detail("No changes detected. All products are up to date.")
            logger.info("No changed records found in Airtable.")
            return

        print_detail(f"Found {len(airtable_records)} records to sync.")
        print()

        # Step 2: Fetch all existing PARTNAMEs from Priority
        print_section("Loading existing products from Priority...")
        existing_partnames = self.priority.fetch_all_partnames()
        logger.info("Loaded %d existing PARTNAMEs from Priority", len(existing_partnames))

        # Classify records
        new_count = 0
        existing_count = 0
        for record in airtable_records:
            sku = clean(record.get("fields", {}).get(AIRTABLE_FIELD_SKU))
            if sku and sku not in existing_partnames:
                new_count += 1
            else:
                existing_count += 1

        self.stats.existing_in_priority = existing_count
        self.stats.new_to_priority = new_count
        print_detail(
            f"Loaded {len(existing_partnames):,} products from Priority. "
            f"{new_count} new, {existing_count} existing."
        )
        print()

        # Step 2b: Fetch shelf lives from separate Airtable table
        # (skipped in STATUS mode — status fields don't need sub-form data)
        if is_status:
            shelf_lives_by_sku: dict[str, list[dict[str, Any]]] = {}
            print_section("Skipping shelf lives (status-only mode).")
        else:
            print_section("Loading shelf lives from Airtable...")
            try:
                shelf_lives_by_sku = self.airtable.fetch_shelf_lives()
                print_detail(f"Loaded shelf lives for {len(shelf_lives_by_sku)} SKUs.")
            except Exception as e:
                logger.error("Failed to fetch shelf lives: %s", e)
                shelf_lives_by_sku = {}
                print_detail(f"Warning: Could not load shelf lives ({e}). Continuing without.")
        print()

        # Step 3: Process each record
        print_section("Syncing:")
        sync_results: list[SyncRecord] = []
        timestamp_updates: list[dict[str, Any]] = []

        for idx, record in enumerate(airtable_records, 1):
            result = self._process_record(
                record=record,
                existing_partnames=existing_partnames,
                shelf_lives_by_sku=shelf_lives_by_sku,
                index=idx,
                total=len(airtable_records),
            )
            sync_results.append(result)

            # Queue timestamp update for successful syncs and skips
            if result.action in (SyncAction.CREATE, SyncAction.UPDATE, SyncAction.SKIP):
                now_utc = datetime.now(timezone.utc).isoformat()
                timestamp_updates.append({
                    "record_id": result.airtable_record_id,
                    "synced_at": now_utc,
                    "priority_udate": result.priority_udate,
                })

        # Step 4: Update Airtable timestamps
        if timestamp_updates and not self.dry_run:
            print()
            total_batches = (len(timestamp_updates) + 9) // 10
            print_section("Updating Airtable timestamps...")
            updated = self.airtable.batch_update_timestamps(timestamp_updates)
            print_detail(f"{updated} records stamped across {total_batches} batches.")
        elif self.dry_run:
            print()
            print_section("DRY RUN — Skipping Airtable timestamp updates.")

        # Step 5: Print summary
        print_summary(
            created=self.stats.created,
            updated=self.stats.updated,
            skipped=self.stats.skipped,
            errors=self.stats.errors,
            duration=self.stats.duration_display,
        )

        # Print error details if any
        if self.stats.error_details:
            print("Errors:")
            for err in self.stats.error_details:
                status = f"HTTP {err.status_code}" if err.status_code else "Error"
                print(f"  - {err.sku}: {err.action} failed ({status}: {err.message})")
            print()

    # ── Single record processing ─────────────────────────────────────────

    def _process_record(
        self,
        record: dict[str, Any],
        existing_partnames: set[str],
        shelf_lives_by_sku: dict[str, list[dict[str, Any]]],
        index: int,
        total: int,
    ) -> SyncRecord:
        """Process a single Airtable record: map, compare, and sync to Priority."""

        fields = record.get("fields", {})
        record_id = record.get("id", "")
        sku = clean(fields.get(AIRTABLE_FIELD_SKU))

        result = SyncRecord(airtable_record_id=record_id, sku=sku or "")

        # Skip records with no SKU
        if not sku:
            result.action = SyncAction.ERROR
            result.error_message = "Empty SKU"
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                sku="(empty)",
                action="EXTRACT",
                message="Empty SKU — skipped",
                timestamp=datetime.now(timezone.utc),
            ))
            print_record_line(index, total, "(empty)", "ERROR", "empty SKU — skipped")
            logger.warning("Record %s has empty SKU, skipping", record_id)
            return result

        # Map Airtable fields → Priority fields
        # In STATUS mode, only map the 3 status fields + SKU
        field_map = STATUS_FIELD_MAP if self.mode == SyncMode.STATUS else None
        try:
            priority_payload = map_airtable_to_priority(fields, field_map=field_map)
        except Exception as e:
            result.action = SyncAction.ERROR
            result.error_message = f"Mapping error: {e}"
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                sku=sku,
                action="MAP",
                message=str(e),
                timestamp=datetime.now(timezone.utc),
            ))
            print_record_line(index, total, sku, "ERROR", f"mapping error: {e}")
            logger.error("Mapping error for %s: %s", sku, e)
            return result

        # Route: CREATE or UPDATE (main LOGPART fields)
        if sku not in existing_partnames:
            result = self._create_product(
                sku=sku,
                payload=priority_payload,
                result=result,
                index=index,
                total=total,
            )
        else:
            result = self._update_product(
                sku=sku,
                payload=priority_payload,
                result=result,
                index=index,
                total=total,
            )

        # Sub-form sync — only if main LOGPART sync succeeded
        # (skipped entirely in STATUS mode — no sub-form fields involved)
        if self.mode != SyncMode.STATUS and result.action in (SyncAction.CREATE, SyncAction.UPDATE, SyncAction.SKIP):
            shelf_lives = shelf_lives_by_sku.get(sku, [])
            self._sync_subforms(sku, fields, shelf_lives, result)

        return result

    # ── Sub-form sync ────────────────────────────────────────────────────

    def _sync_subforms(
        self,
        sku: str,
        airtable_fields: dict[str, Any],
        shelf_life_records: list[dict[str, Any]],
        result: SyncRecord,
    ) -> None:
        """
        Sync all sub-forms for a product after main LOGPART sync.
        Errors are logged but don't fail the overall product sync.
        """
        if self.dry_run:
            # In dry run, just show what sub-forms would be synced
            allergen_payload = map_allergens(airtable_fields)
            if allergen_payload:
                result.subform_results.append(SubformResult(
                    subform="allergens", action="dry_run",
                    detail=f"{len(allergen_payload)} fields",
                ))

            shelf_payloads = map_shelf_lives(shelf_life_records)
            if shelf_payloads:
                result.subform_results.append(SubformResult(
                    subform="shelf_lives", action="dry_run",
                    detail=f"{len(shelf_payloads)} entries",
                ))

            price_payloads = map_price_lists(airtable_fields)
            if price_payloads:
                result.subform_results.append(SubformResult(
                    subform="price_lists", action="dry_run",
                    detail=f"{len(price_payloads)} levels",
                ))

            bin_payload = map_bins(airtable_fields)
            if bin_payload:
                result.subform_results.append(SubformResult(
                    subform="bins", action="dry_run",
                    detail=f"{len(bin_payload)} fields",
                ))
            return

        # 1. Allergens & Features (single-record sub-form)
        self._sync_allergens(sku, airtable_fields, result)

        # 2. Shelf Lives (multi-record sub-form)
        self._sync_shelf_lives(sku, shelf_life_records, result)

        # 3. Price Lists (multi-record sub-form)
        self._sync_price_lists(sku, airtable_fields, result)

        # 4. Bin Locations (single-record sub-form)
        self._sync_bins(sku, airtable_fields, result)

    def _sync_allergens(
        self, sku: str, fields: dict[str, Any], result: SyncRecord
    ) -> None:
        """Sync allergens & features sub-form."""
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
                subform="allergens", action="error", detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                sku=sku, action="SUBFORM",
                message=f"allergens: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    def _sync_shelf_lives(
        self, sku: str, shelf_life_records: list[dict[str, Any]], result: SyncRecord
    ) -> None:
        """Sync shelf lives sub-form (multi-record, keyed by SHELFLIFE integer)."""
        try:
            payloads = map_shelf_lives(shelf_life_records)
            if not payloads:
                return

            res = self.priority.sync_multi_subform(
                sku, SHELF_LIFE_SUBFORM_NAME, "TYPE", payloads,
                url_key_field="SHELFLIFE",  # Priority uses SHELFLIFE int as entity key
            )
            detail = f"c:{res['created']} u:{res['updated']} s:{res['skipped']}"
            result.subform_results.append(SubformResult(
                subform="shelf_lives", action="synced", detail=detail,
            ))
            logger.debug("Shelf lives for %s: %s", sku, detail)

        except Exception as e:
            logger.error("Shelf life sub-form error for %s: %s", sku, e)
            result.subform_results.append(SubformResult(
                subform="shelf_lives", action="error", detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                sku=sku, action="SUBFORM",
                message=f"shelf_lives: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    def _sync_price_lists(
        self, sku: str, fields: dict[str, Any], result: SyncRecord
    ) -> None:
        """
        Sync price lists sub-form (multi-record, up to 3 levels).

        PARTINCUSTPLISTS_SUBFORM doesn't expose individual record keys,
        so we compare locally and use deep PATCH on the parent LOGPART
        to push changes.
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
                    # New price list → include in deep PATCH
                    records_to_push.append(desired)
                    created += 1
                else:
                    # Existing → compare fields
                    current = existing_by_plname[plname]
                    has_changes = False
                    for field, new_value in desired.items():
                        if field == "PLNAME":
                            continue
                        old_value = current.get(field)
                        if str(new_value).strip() != str(old_value or "").strip():
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
                subform="price_lists", action="synced", detail=detail,
            ))
            logger.debug("Price lists for %s: %s", sku, detail)

        except Exception as e:
            logger.error("Price list sub-form error for %s: %s", sku, e)
            result.subform_results.append(SubformResult(
                subform="price_lists", action="error", detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                sku=sku, action="SUBFORM",
                message=f"price_lists: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    def _sync_bins(
        self, sku: str, fields: dict[str, Any], result: SyncRecord
    ) -> None:
        """
        Sync bin locations sub-form.

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
                    if str(new_value).strip() != str(old_value or "").strip():
                        has_changes = True
                        break
                if not has_changes:
                    result.subform_results.append(SubformResult(
                        subform="bins", action="skipped", detail="no changes",
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
                subform="bins", action="error", detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                sku=sku, action="SUBFORM",
                message=f"bins: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    # ── Product create/update ─────────────────────────────────────────────

    def _create_product(
        self,
        sku: str,
        payload: dict[str, Any],
        result: SyncRecord,
        index: int,
        total: int,
    ) -> SyncRecord:
        """Create a new product in Priority."""

        if self.dry_run:
            result.action = SyncAction.CREATE
            result.fields_changed = list(payload.keys())
            self.stats.created += 1
            print_record_line(
                index, total, sku, "CREATE",
                f"[DRY RUN] {len(payload)} fields",
            )
            return result

        try:
            response_data = self.priority.create_product(payload)
            result.action = SyncAction.CREATE
            result.fields_changed = list(payload.keys())
            result.priority_udate = response_data.get("UDATE")
            self.stats.created += 1

            print_record_line(
                index, total, sku, "CREATE",
                f"new product ({len(payload)} fields)",
            )
            logger.info("Created product %s with %d fields", sku, len(payload))
            return result

        except (requests.HTTPError, requests.RequestException) as e:
            status_code = None
            message = str(e)
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                try:
                    err_body = e.response.json()
                    message = err_body.get("error", {}).get("message", str(e))
                except Exception:
                    message = e.response.text[:200] if e.response.text else str(e)

            result.action = SyncAction.ERROR
            result.error_message = message
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                sku=sku,
                action="CREATE",
                status_code=status_code,
                message=message,
                timestamp=datetime.now(timezone.utc),
            ))

            status_str = f"HTTP {status_code}" if status_code else "Error"
            print_record_line(index, total, sku, "ERROR", f"CREATE failed ({status_str})")
            logger.error("Failed to create %s: %s", sku, message)
            return result

    def _update_product(
        self,
        sku: str,
        payload: dict[str, Any],
        result: SyncRecord,
        index: int,
        total: int,
    ) -> SyncRecord:
        """Compare and update an existing product in Priority."""

        # Fetch current product from Priority for comparison
        try:
            priority_current = self.priority.get_product(sku)
        except (requests.HTTPError, requests.RequestException) as e:
            result.action = SyncAction.ERROR
            result.error_message = f"GET failed: {e}"
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                sku=sku,
                action="GET",
                message=str(e),
                timestamp=datetime.now(timezone.utc),
            ))
            print_record_line(index, total, sku, "ERROR", f"GET failed: {e}")
            logger.error("Failed to GET %s from Priority: %s", sku, e)
            return result

        if priority_current is None:
            # SKU was in the partnames set but GET returned 404 — treat as create
            logger.warning(
                "SKU %s was in partnames set but GET returned 404. Creating.", sku
            )
            return self._create_product(
                sku=sku,
                payload=payload,
                result=result,
                index=index,
                total=total,
            )

        # Build PATCH body (only changed fields)
        patch_body = build_patch_body(payload, priority_current)

        if not patch_body:
            # No changes detected
            result.action = SyncAction.SKIP
            result.priority_udate = priority_current.get("UDATE")
            self.stats.skipped += 1
            print_record_line(index, total, sku, "SKIP", "no changes")
            logger.debug("No changes for %s", sku)
            return result

        if self.dry_run:
            result.action = SyncAction.UPDATE
            result.fields_changed = list(patch_body.keys())
            self.stats.updated += 1
            field_names = ", ".join(patch_body.keys())
            print_record_line(
                index, total, sku, "UPDATE",
                f"[DRY RUN] {len(patch_body)} fields ({field_names})",
            )
            return result

        # Execute PATCH
        try:
            response_data = self.priority.update_product(sku, patch_body)
            result.action = SyncAction.UPDATE
            result.fields_changed = list(patch_body.keys())
            result.priority_udate = response_data.get("UDATE")
            self.stats.updated += 1

            field_names = ", ".join(patch_body.keys())
            print_record_line(
                index, total, sku, "UPDATE",
                f"{len(patch_body)} fields ({field_names})",
            )
            logger.info(
                "Updated %s: %d fields (%s)", sku, len(patch_body), field_names
            )
            return result

        except (requests.HTTPError, requests.RequestException) as e:
            status_code = None
            message = str(e)
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                try:
                    err_body = e.response.json()
                    message = err_body.get("error", {}).get("message", str(e))
                except Exception:
                    message = e.response.text[:200] if e.response.text else str(e)

            result.action = SyncAction.ERROR
            result.error_message = message
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                sku=sku,
                action="UPDATE",
                status_code=status_code,
                message=message,
                timestamp=datetime.now(timezone.utc),
            ))

            status_str = f"HTTP {status_code}" if status_code else "Error"
            print_record_line(index, total, sku, "ERROR", f"UPDATE failed ({status_str})")
            logger.error("Failed to update %s: %s", sku, message)
            return result
