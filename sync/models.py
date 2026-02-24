"""
Pydantic models for the product sync engine.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────────────

class SyncDirection(str, Enum):
    AIRTABLE_TO_PRIORITY = "airtable-to-priority"
    PRIORITY_TO_AIRTABLE = "priority-to-airtable"
    BIDIRECTIONAL = "both"


class SyncAction(str, Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    SKIP = "SKIP"
    ERROR = "ERROR"


# ── Field Mapping ────────────────────────────────────────────────────────────

class FieldMapping(BaseModel):
    """Defines how one Airtable field maps to one Priority field."""

    airtable_field: str
    priority_field: str
    transform: Literal["clean", "format_price", "to_int"]
    required: bool = False
    field_type: Literal["str", "float", "int"] = "str"
    max_length: int | None = None
    create_only: bool = False  # If True, only sent on POST (create), not PATCH (update)


# ── Sync Records ─────────────────────────────────────────────────────────────

class SubformResult(BaseModel):
    """Tracks the sync result for a single sub-form."""

    subform: str  # e.g. "allergens", "shelf_lives", "price_lists", "bins"
    action: str = "skipped"  # "created", "updated", "skipped", "error"
    detail: str = ""


class SyncRecord(BaseModel):
    """Tracks the sync result for a single product."""

    airtable_record_id: str
    sku: str
    action: SyncAction = SyncAction.SKIP
    fields_changed: list[str] = Field(default_factory=list)
    subform_results: list[SubformResult] = Field(default_factory=list)
    error_message: str = ""
    priority_udate: str | None = None  # UDATE from Priority response


class SyncError(BaseModel):
    """Detailed error information for a failed sync."""

    sku: str
    action: str
    status_code: int | None = None
    message: str
    timestamp: datetime


# ── Sync Stats ───────────────────────────────────────────────────────────────

class SyncStats(BaseModel):
    """Aggregated statistics for a sync run."""

    total_fetched: int = 0
    existing_in_priority: int = 0
    new_to_priority: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0
    error_details: list[SyncError] = Field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None

    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def duration_display(self) -> str:
        secs = self.duration_seconds
        if secs < 60:
            return f"{secs:.0f}s"
        mins = int(secs // 60)
        remaining = int(secs % 60)
        return f"{mins}m {remaining}s"
