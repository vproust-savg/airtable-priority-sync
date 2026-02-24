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
from sync.field_mapping import build_patch_body, map_airtable_to_priority
from sync.logger_setup import (
    print_banner,
    print_detail,
    print_record_line,
    print_section,
    print_summary,
)
from sync.models import SyncAction, SyncDirection, SyncError, SyncRecord, SyncStats
from sync.priority_client import PriorityClient
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
    ) -> None:
        self.direction = direction
        self.dry_run = dry_run
        self.single_sku = single_sku
        self.airtable = AirtableClient()
        self.priority = PriorityClient()
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
        return self.stats

    # ── Airtable → Priority ──────────────────────────────────────────────

    def _sync_airtable_to_priority(self) -> None:
        """Full sync flow: Airtable → Priority."""

        label = "DRY RUN — Airtable → Priority" if self.dry_run else "Airtable → Priority"
        print_banner(label)

        # Step 1: Fetch changed records from Airtable
        print_section("Fetching changed records from Airtable...")
        airtable_records = self.airtable.fetch_changed_records()

        # Filter to single SKU if specified (for testing)
        if self.single_sku:
            airtable_records = [
                r for r in airtable_records
                if clean(r.get("fields", {}).get(AIRTABLE_FIELD_SKU)) == self.single_sku
            ]

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

        # Step 3: Process each record
        print_section("Syncing:")
        sync_results: list[SyncRecord] = []
        timestamp_updates: list[dict[str, Any]] = []

        for idx, record in enumerate(airtable_records, 1):
            result = self._process_record(
                record=record,
                existing_partnames=existing_partnames,
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
        try:
            priority_payload = map_airtable_to_priority(fields)
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

        # Route: CREATE or UPDATE
        if sku not in existing_partnames:
            return self._create_product(
                sku=sku,
                payload=priority_payload,
                result=result,
                index=index,
                total=total,
            )
        else:
            return self._update_product(
                sku=sku,
                payload=priority_payload,
                result=result,
                index=index,
                total=total,
            )

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
