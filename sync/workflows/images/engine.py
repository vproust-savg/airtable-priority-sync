"""
Image sync engine: Airtable → Priority (one-direction only).

Downloads product images from Airtable, processes them with Pillow
(RGB, sRGB, smart compress to <150KB), and uploads as base64 to
Priority's LOGPART.EXTFILENAME field.
"""

from __future__ import annotations

import base64
import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests

from sync.core.airtable_client import AirtableClient
from sync.core.config import LA_TIMEZONE
from sync.core.logger_setup import (
    print_banner,
    print_detail,
    print_record_line,
    print_section,
    print_summary,
)
from sync.core.models import SyncAction, SyncDirection, SyncMode, SyncRecord, SyncStats
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.workflows.images.config import (
    AIRTABLE_IMAGE_FIELD,
    AIRTABLE_IMAGE_FIELD_ID,
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_ID,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    MAX_IMAGE_SIZE_KB,
    PRIORITY_ENTITY,
    PRIORITY_IMAGE_FIELD,
    PRIORITY_KEY_FIELD,
    TIMESTAMP_FIELDS,
    TIMESTAMP_FIELD_IDS,
)
from sync.workflows.images.image_processor import process_image

logger = logging.getLogger(__name__)


class ImageSyncEngine:
    """
    Syncs product images from Airtable to Priority ERP.

    Not a subclass of BaseSyncEngine — images don't use field mappings,
    P→A direction, or status mode. Shares the same constructor interface
    so run_sync.py and server.py can instantiate it uniformly.
    """

    def __init__(
        self,
        direction: SyncDirection = SyncDirection.AIRTABLE_TO_PRIORITY,
        dry_run: bool = False,
        single_key: str | None = None,
        trigger: str = "manual",
        mode: SyncMode = SyncMode.FULL,
        workflow_name: str = "images",
        base_id_override: str | None = None,
        token_override: str | None = None,
        priority_url_override: str | None = None,
    ) -> None:
        if direction != SyncDirection.AIRTABLE_TO_PRIORITY:
            raise ValueError("Image sync only supports Airtable → Priority direction.")

        self.dry_run = dry_run
        self.single_key = single_key
        self.trigger = trigger
        self.workflow_name = workflow_name
        self.stats = SyncStats()

        # Build field ID map for the Products table (images use same table)
        field_id_map: dict[str, str] = {
            AIRTABLE_KEY_FIELD: AIRTABLE_KEY_FIELD_ID,
            AIRTABLE_IMAGE_FIELD: AIRTABLE_IMAGE_FIELD_ID,
            **{v: TIMESTAMP_FIELD_IDS[k] for k, v in TIMESTAMP_FIELDS.items()},
        }

        self.airtable = AirtableClient(
            table_name=AIRTABLE_TABLE_NAME,
            key_field=AIRTABLE_KEY_FIELD,
            sync_view=AIRTABLE_SYNC_VIEW,
            timestamp_fields=TIMESTAMP_FIELDS,
            field_id_map=field_id_map,
            base_id_override=base_id_override,
            token_override=token_override,
        )
        self.priority = PriorityClient(
            entity=PRIORITY_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            api_url_override=priority_url_override,
        )
        self.sync_log = SyncLogClient(entity_label="SKU")

        # Reuse the Airtable session for image downloads (same auth)
        self._download_session = requests.Session()
        self._download_session.timeout = 60

    def run(self) -> SyncStats:
        """Execute the image sync and return stats."""
        self.stats.start_time = datetime.now(timezone.utc)

        self._sync_images()

        self.stats.end_time = datetime.now(timezone.utc)

        if not self.dry_run:
            self.sync_log.log_run(
                self.stats,
                direction="A->P",
                trigger=self.trigger,
                workflow=self.workflow_name,
            )

        return self.stats

    def _sync_images(self) -> None:
        """Main sync flow: fetch from Airtable, process, upload to Priority."""
        label = (
            f"DRY RUN -- Image Sync: Airtable → Priority"
            if self.dry_run
            else "Image Sync: Airtable → Priority"
        )
        print_banner(label)

        # Fetch records
        if self.single_key:
            print_section(f"Fetching single record: {self.single_key}")
            records = self.airtable.fetch_record_by_key(
                self.single_key,
                fields_to_fetch=[AIRTABLE_KEY_FIELD, AIRTABLE_IMAGE_FIELD],
            )
        else:
            print_section("Fetching records from Airtable")
            fields_to_fetch = [AIRTABLE_KEY_FIELD, AIRTABLE_IMAGE_FIELD]
            records = self.airtable.fetch_changed_records(fields_to_fetch)

        self.stats.total_fetched = len(records)
        print_detail(f"Records to process: {len(records)}")

        if not records:
            print_detail("No records to sync.")
            print_summary(
                created=self.stats.created,
                updated=self.stats.updated,
                skipped=self.stats.skipped,
                errors=self.stats.errors,
                duration=self.stats.duration_display,
            )
            return

        # Process each record, flushing timestamps every 50 records
        total = len(records)
        batch: list[SyncRecord] = []
        batch_size = 50
        for idx, record in enumerate(records, 1):
            result = self._process_record(record, idx, total)
            batch.append(result)

            if len(batch) >= batch_size:
                if not self.dry_run:
                    self._update_timestamps(batch)
                batch = []

        # Flush remaining results
        if batch and not self.dry_run:
            self._update_timestamps(batch)

        print_summary(
            created=self.stats.created,
            updated=self.stats.updated,
            skipped=self.stats.skipped,
            errors=self.stats.errors,
            duration=self.stats.duration_display,
        )

    def _process_record(
        self, record: dict[str, Any], idx: int, total: int,
    ) -> SyncRecord:
        """Process a single Airtable record: download, compress, upload."""
        fields = record.get("fields", {})
        record_id = record.get("id", "")
        sku = fields.get(AIRTABLE_KEY_FIELD, "").strip()

        result = SyncRecord(
            airtable_record_id=record_id,
            entity_key=sku,
        )

        if not sku:
            result.action = SyncAction.SKIP
            result.error_message = "No SKU"
            print_record_line(idx, total, sku or "???", "SKIP", "No SKU")
            self.stats.skipped += 1
            return result

        # Extract first image URL
        images = fields.get(AIRTABLE_IMAGE_FIELD, [])
        if not images or not isinstance(images, list):
            result.action = SyncAction.SKIP
            result.error_message = "No image"
            print_record_line(idx, total, sku, "SKIP", "No image attached")
            self.stats.skipped += 1
            return result

        image_url = images[0].get("url", "") if isinstance(images[0], dict) else ""
        if not image_url:
            result.action = SyncAction.SKIP
            result.error_message = "No image URL"
            print_record_line(idx, total, sku, "SKIP", "No image URL")
            self.stats.skipped += 1
            return result

        if self.dry_run:
            result.action = SyncAction.UPDATE
            result.fields_changed = [PRIORITY_IMAGE_FIELD]
            print_record_line(idx, total, sku, "UPDATE", f"Would upload image ({images[0].get('filename', '?')})")
            self.stats.updated += 1
            return result

        # Download image
        try:
            image_bytes = self._download_image(image_url)
        except Exception as e:
            result.action = SyncAction.ERROR
            result.error_message = f"Download failed: {e}"
            print_record_line(idx, total, sku, "ERROR", f"Download failed: {e}")
            self.stats.errors += 1
            return result

        # Process image (RGB, sRGB, compress)
        try:
            processed_bytes = process_image(image_bytes, max_size_kb=MAX_IMAGE_SIZE_KB)
        except Exception as e:
            result.action = SyncAction.ERROR
            result.error_message = f"Processing failed: {e}"
            print_record_line(idx, total, sku, "ERROR", f"Processing failed: {e}")
            self.stats.errors += 1
            return result

        # Base64 encode
        b64_data = base64.b64encode(processed_bytes).decode("ascii")
        data_uri = f"data:image/jpeg;base64,{b64_data}"

        # Upload to Priority
        try:
            self.priority.update_record(sku, {PRIORITY_IMAGE_FIELD: data_uri})
            original_kb = len(image_bytes) / 1024
            final_kb = len(processed_bytes) / 1024
            result.action = SyncAction.UPDATE
            result.fields_changed = [PRIORITY_IMAGE_FIELD]
            print_record_line(
                idx, total, sku, "UPDATE",
                f"Image uploaded ({original_kb:.0f}KB → {final_kb:.0f}KB)",
            )
            self.stats.updated += 1
        except Exception as e:
            result.action = SyncAction.ERROR
            result.error_message = f"Priority upload failed: {e}"
            print_record_line(idx, total, sku, "ERROR", f"Priority upload failed: {e}")
            self.stats.errors += 1

        return result

    def _download_image(self, url: str) -> bytes:
        """Download an image from Airtable CDN URL."""
        resp = self._download_session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.content

    def _update_timestamps(self, results: list[SyncRecord]) -> None:
        """Update Airtable timestamps and sync comments for processed records."""
        now_utc = datetime.now(timezone.utc).isoformat()
        now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
        now_short = now_la.strftime("%Y-%m-%d %H:%M")

        timestamp_updates: list[dict[str, Any]] = []

        for result in results:
            if not result.airtable_record_id:
                continue

            comment = self._compose_comment(result, now_short)

            if result.action == SyncAction.UPDATE:
                timestamp_updates.append({
                    "record_id": result.airtable_record_id,
                    "synced_at": now_utc,
                    "sync_comment": comment,
                    "_post_comment": True,
                })
            elif result.action == SyncAction.ERROR:
                # Write error comment but don't update sync timestamp
                timestamp_updates.append({
                    "record_id": result.airtable_record_id,
                    "synced_at": None,
                    "sync_comment": comment,
                    "_post_comment": True,
                })

        if timestamp_updates:
            print_section("Updating Airtable timestamps")
            updated = self.airtable.batch_update_timestamps(timestamp_updates)
            print_detail(f"Timestamps updated: {updated}")

            # Post record comments
            pending_comments = [
                {"record_id": u["record_id"], "text": u["sync_comment"]}
                for u in timestamp_updates
                if u.get("sync_comment") and u.get("_post_comment")
            ]
            if pending_comments:
                print_section("Posting record comments")
                comment_count = self.airtable.post_record_comments(
                    pending_comments,
                )
                print_detail(f"Comments posted: {comment_count}")

    @staticmethod
    def _compose_comment(result: SyncRecord, timestamp: str) -> str:
        """Compose a human-readable sync comment for the Airtable record."""
        if result.action == SyncAction.UPDATE:
            return f"Images: Uploaded to Priority ({timestamp})"
        elif result.action == SyncAction.SKIP:
            reason = result.error_message or "No changes"
            return f"Images: Skipped — {reason} ({timestamp})"
        elif result.action == SyncAction.ERROR:
            msg = result.error_message[:80] if result.error_message else "unknown"
            return f"Images: ERROR — {msg} ({timestamp})"
        return f"Images: {result.action.value} ({timestamp})"
