"""
Writes sync run summaries and error details to the Sync Logs Airtable base.
Graceful degradation: if AIRTABLE_LOG_BASE_ID is not set, all methods are no-ops.
Log write failures never crash the sync.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

from sync.config import (
    AIRTABLE_API_BASE,
    AIRTABLE_LOG_BASE_ID,
    AIRTABLE_LOG_TOKEN,
    AIRTABLE_REQUEST_TIMEOUT,
    LA_TIMEZONE,
)
from sync.models import SyncStats

logger = logging.getLogger(__name__)

# Table IDs in the Sync Logs base (appr935iOTErWivM1)
_SYNC_RUNS_TABLE_ID = "tblSN1oQUP18mDq0K"
_SYNC_ERRORS_TABLE_ID = "tbljeM0YaEIWJRs63"


class SyncLogClient:
    """Writes sync run and error records to the Sync Logs Airtable base."""

    def __init__(self) -> None:
        self.enabled = AIRTABLE_LOG_BASE_ID is not None
        if not self.enabled:
            logger.info("Sync logging disabled (AIRTABLE_LOG_BASE_ID not set).")
            return

        self.base_url = f"{AIRTABLE_API_BASE}/{AIRTABLE_LOG_BASE_ID}"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {AIRTABLE_LOG_TOKEN}",
            "Content-Type": "application/json",
        })

    def log_run(
        self,
        stats: SyncStats,
        direction: str = "A→P",
        trigger: str = "manual",
    ) -> str | None:
        """
        Write a sync run record + error records to the log base.

        Args:
            stats: The completed SyncStats from the engine.
            direction: "A→P" or "P→A".
            trigger: "manual" or "webhook".

        Returns:
            The Airtable record ID of the run record, or None on failure.
        """
        if not self.enabled:
            return None

        try:
            run_record_id = self._write_run(stats, direction, trigger)

            if run_record_id and stats.error_details:
                self._write_errors(run_record_id, stats)

            return run_record_id

        except Exception as e:
            logger.error("Failed to write sync log: %s", e)
            return None

    def _write_run(
        self, stats: SyncStats, direction: str, trigger: str
    ) -> str | None:
        """Create the Sync Runs record."""
        run_id = _generate_run_id()

        # Determine status
        if stats.errors == 0:
            status = "success"
        elif stats.created + stats.updated > 0:
            status = "partial"
        else:
            status = "failed"

        # Build error summary
        error_summary = ""
        if stats.error_details:
            lines = []
            for err in stats.error_details[:20]:  # Cap at 20 lines
                status_str = f"HTTP {err.status_code}" if err.status_code else "Error"
                lines.append(f"{err.sku}: {err.action} ({status_str}) — {err.message}")
            error_summary = "\n".join(lines)
            if len(stats.error_details) > 20:
                error_summary += f"\n... and {len(stats.error_details) - 20} more"

        fields: dict[str, Any] = {
            "Run ID": run_id,
            "Direction": direction,
            "Trigger": trigger,
            "Status": status,
            "Fetched": stats.total_fetched,
            "Created": stats.created,
            "Updated": stats.updated,
            "Skipped": stats.skipped,
            "Errors": stats.errors,
        }

        if stats.start_time:
            fields["Start Time"] = stats.start_time.isoformat()
        if stats.end_time:
            fields["End Time"] = stats.end_time.isoformat()
            if stats.start_time:
                fields["Duration"] = int(stats.duration_seconds)

        if error_summary:
            fields["Error Summary"] = error_summary

        url = f"{self.base_url}/{_SYNC_RUNS_TABLE_ID}"
        response = self.session.post(
            url,
            json={"fields": fields},
            timeout=AIRTABLE_REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        record_id = response.json().get("id")
        logger.info("Logged sync run %s → Airtable record %s", run_id, record_id)
        return record_id

    def _write_errors(self, run_record_id: str, stats: SyncStats) -> None:
        """Write error records in batches of 10 (Airtable limit)."""
        records = []
        for err in stats.error_details:
            fields: dict[str, Any] = {
                "Error ID": str(uuid.uuid4())[:8],
                "Sync Run": [run_record_id],
                "SKU": err.sku,
                "Action": err.action,
                "Error Message": err.message,
                "Timestamp": err.timestamp.isoformat(),
            }
            if err.status_code:
                fields["HTTP Status"] = err.status_code
            records.append({"fields": fields})

        # Batch in groups of 10
        url = f"{self.base_url}/{_SYNC_ERRORS_TABLE_ID}"
        for i in range(0, len(records), 10):
            batch = records[i : i + 10]
            try:
                response = self.session.post(
                    url,
                    json={"records": batch},
                    timeout=AIRTABLE_REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                logger.debug(
                    "Wrote %d error records (batch %d)",
                    len(batch),
                    i // 10 + 1,
                )
            except Exception as e:
                logger.error("Failed to write error batch %d: %s", i // 10 + 1, e)


def _generate_run_id() -> str:
    """Generate a human-readable run ID like 'run-2026-02-24-14-30-05'."""
    now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
    return f"run-{now_la.strftime('%Y-%m-%d-%H-%M-%S')}"
