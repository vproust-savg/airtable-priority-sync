"""
Tech sheet sync engine: Airtable → Priority (one-direction only).

Downloads tech sheet PDFs from Airtable, base64-encodes them, and
uploads to Priority's LOGPART PARTEXTFILE_SUBFORM sub-form.

Sub-form pattern: C (multi-record, no URL key access).
Strategy: GET existing records → skip if EXTFILEDES match → POST if new.
"""

from __future__ import annotations

import base64
import logging
import mimetypes
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
from sync.workflows.techsheets.config import (
    AIRTABLE_KEY_FIELD,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    AIRTABLE_TECHSHEET_FIELD,
    EXTFILEDES_PREFIX,
    PRIORITY_ENTITY,
    PRIORITY_KEY_FIELD,
    PRIORITY_SUBFORM,
    TIMESTAMP_FIELDS,
)

logger = logging.getLogger(__name__)


class TechSheetSyncEngine:
    """
    Syncs product tech sheets from Airtable to Priority ERP.

    Not a subclass of BaseSyncEngine — tech sheets don't use field mappings,
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
        workflow_name: str = "techsheets",
        base_id_override: str | None = None,
        token_override: str | None = None,
        priority_url_override: str | None = None,
    ) -> None:
        if direction != SyncDirection.AIRTABLE_TO_PRIORITY:
            raise ValueError("Tech sheet sync only supports Airtable → Priority direction.")

        self.dry_run = dry_run
        self.single_key = single_key
        self.trigger = trigger
        self.workflow_name = workflow_name
        self.stats = SyncStats()

        self.airtable = AirtableClient(
            table_name=AIRTABLE_TABLE_NAME,
            key_field=AIRTABLE_KEY_FIELD,
            sync_view=AIRTABLE_SYNC_VIEW,
            timestamp_fields=TIMESTAMP_FIELDS,
            base_id_override=base_id_override,
            token_override=token_override,
        )
        self.priority = PriorityClient(
            entity=PRIORITY_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            api_url_override=priority_url_override,
        )
        self.sync_log = SyncLogClient(entity_label="SKU")

        self._download_session = requests.Session()
        self._download_session.timeout = 120  # Larger timeout for big PDFs

    def run(self) -> SyncStats:
        """Execute the tech sheet sync and return stats."""
        self.stats.start_time = datetime.now(timezone.utc)

        self._sync_techsheets()

        self.stats.end_time = datetime.now(timezone.utc)

        if not self.dry_run:
            self.sync_log.log_run(
                self.stats,
                direction="A->P",
                trigger=self.trigger,
                workflow=self.workflow_name,
            )

        return self.stats

    def _sync_techsheets(self) -> None:
        """Main sync flow: fetch from Airtable, upload to Priority."""
        label = (
            "DRY RUN -- Tech Sheet Sync: Airtable → Priority"
            if self.dry_run
            else "Tech Sheet Sync: Airtable → Priority"
        )
        print_banner(label)

        # Fetch records
        if self.single_key:
            print_section(f"Fetching single record: {self.single_key}")
            records = self.airtable.fetch_record_by_key(
                self.single_key,
                fields_to_fetch=[AIRTABLE_KEY_FIELD, AIRTABLE_TECHSHEET_FIELD],
            )
        else:
            print_section("Fetching records from Airtable")
            fields_to_fetch = [AIRTABLE_KEY_FIELD, AIRTABLE_TECHSHEET_FIELD]
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
        """Process a single Airtable record: download, base64 encode, upload."""
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

        # Extract first attachment
        attachments = fields.get(AIRTABLE_TECHSHEET_FIELD, [])
        if not attachments or not isinstance(attachments, list):
            result.action = SyncAction.SKIP
            result.error_message = "No tech sheet"
            print_record_line(idx, total, sku, "SKIP", "No tech sheet attached")
            self.stats.skipped += 1
            return result

        attachment = attachments[0] if isinstance(attachments[0], dict) else {}
        file_url = attachment.get("url", "")
        filename = attachment.get("filename", "techsheet.pdf")
        mime_type = attachment.get("type", "")

        if not file_url:
            result.action = SyncAction.SKIP
            result.error_message = "No file URL"
            print_record_line(idx, total, sku, "SKIP", "No file URL")
            self.stats.skipped += 1
            return result

        # Detect MIME type
        if not mime_type:
            mime_type, _ = mimetypes.guess_type(filename)
            mime_type = mime_type or "application/pdf"

        # Check if already uploaded to Priority (avoid duplicates)
        doc_label = f"{EXTFILEDES_PREFIX} - {sku}"
        if not self.dry_run:
            try:
                existing = self._get_existing_docs(sku)
                if any(r.get("EXTFILEDES") == doc_label for r in existing):
                    result.action = SyncAction.SKIP
                    result.error_message = "Already in Priority"
                    print_record_line(idx, total, sku, "SKIP", "Tech sheet already in Priority")
                    self.stats.skipped += 1
                    return result
            except Exception as e:
                logger.warning("Could not check existing docs for %s: %s", sku, e)

        if self.dry_run:
            result.action = SyncAction.CREATE
            result.fields_changed = [PRIORITY_SUBFORM]
            print_record_line(idx, total, sku, "CREATE", f"Would upload tech sheet ({filename})")
            self.stats.created += 1
            return result

        # Download file
        try:
            file_bytes = self._download_file(file_url)
        except Exception as e:
            result.action = SyncAction.ERROR
            result.error_message = f"Download failed: {e}"
            print_record_line(idx, total, sku, "ERROR", f"Download failed: {e}")
            self.stats.errors += 1
            return result

        # Base64 encode (no processing — upload as-is)
        b64_data = base64.b64encode(file_bytes).decode("ascii")
        data_uri = f"data:{mime_type};base64,{b64_data}"

        # Upload to Priority sub-form
        try:
            self._upload_to_priority(sku, data_uri, doc_label)
            size_kb = len(file_bytes) / 1024
            result.action = SyncAction.CREATE
            result.fields_changed = [PRIORITY_SUBFORM]
            print_record_line(
                idx, total, sku, "CREATE",
                f"Tech sheet uploaded ({size_kb:.0f}KB, {filename})",
            )
            self.stats.created += 1
        except Exception as e:
            result.action = SyncAction.ERROR
            result.error_message = f"Priority upload failed: {e}"
            print_record_line(idx, total, sku, "ERROR", f"Priority upload failed: {e}")
            self.stats.errors += 1

        return result

    def _download_file(self, url: str) -> bytes:
        """Download a file from Airtable CDN URL."""
        resp = self._download_session.get(url, timeout=120)
        resp.raise_for_status()
        return resp.content

    def _get_existing_docs(self, sku: str) -> list[dict[str, Any]]:
        """Get existing PARTEXTFILE_SUBFORM records for a SKU."""
        url = f"{self.priority.api_url}{PRIORITY_ENTITY}('{sku}')/{PRIORITY_SUBFORM}"
        resp = self.priority.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json().get("value", [])

    def _upload_to_priority(
        self, sku: str, data_uri: str, doc_label: str,
    ) -> None:
        """POST a new document to the PARTEXTFILE_SUBFORM."""
        url = f"{self.priority.api_url}{PRIORITY_ENTITY}('{sku}')/{PRIORITY_SUBFORM}"
        payload = {
            "EXTFILENAME": data_uri,
            "EXTFILEDES": doc_label,
        }
        resp = self.priority.session.post(
            url,
            json=payload,
            timeout=180,  # Large files may take time
        )
        resp.raise_for_status()

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

            if result.action in (SyncAction.CREATE, SyncAction.SKIP):
                timestamp_updates.append({
                    "record_id": result.airtable_record_id,
                    "synced_at": now_utc,
                    "sync_comment": comment,
                    "_post_comment": result.action == SyncAction.CREATE,
                })
            elif result.action == SyncAction.ERROR:
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
        if result.action == SyncAction.CREATE:
            return f"Tech Sheet: Uploaded to Priority ({timestamp})"
        elif result.action == SyncAction.SKIP:
            reason = result.error_message or "No changes"
            return f"Tech Sheet: Skipped — {reason} ({timestamp})"
        elif result.action == SyncAction.ERROR:
            msg = result.error_message[:80] if result.error_message else "unknown"
            return f"Tech Sheet: ERROR — {msg} ({timestamp})"
        return f"Tech Sheet: {result.action.value} ({timestamp})"
