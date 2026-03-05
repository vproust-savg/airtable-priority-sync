"""
Pydantic models for the sync engine.
Generic models used by all sync workflows.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────────────

class SyncDirection(str, Enum):
    AIRTABLE_TO_PRIORITY = "airtable-to-priority"
    PRIORITY_TO_AIRTABLE = "priority-to-airtable"
    BIDIRECTIONAL = "both"


class SyncMode(str, Enum):
    FULL = "full"
    STATUS = "status"  # Only sync Catalog Status, Inventory Status, Priority Status


class SyncAction(str, Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    SKIP = "SKIP"
    ERROR = "ERROR"


class ConflictStrategy(str, Enum):
    """How to resolve field-level conflicts when both systems changed a record."""
    SOURCE_WINS = "source_wins"  # Default: sync source overwrites (current behavior)
    LOG_ONLY = "log_only"        # Skip conflicting fields, log for manual resolution
    SKIP_RECORD = "skip_record"  # Skip the entire record if any conflict detected


# ── Field Mapping ────────────────────────────────────────────────────────────

class LookupConfig(BaseModel):
    """Config for dynamically resolving a Priority linked-table code → description.

    Many Priority fields are linked tables that store a code (e.g., "1")
    but display a description (e.g., "Beverages").  At P→A sync time the
    engine fetches the lookup entity and builds a code→description dict
    so the human-readable name is written to Airtable.
    """

    entity: str         # Priority entity name (e.g., "FAMILY_FNC")
    code_field: str     # Code/key field (e.g., "FAMILYNAME")
    desc_field: str     # Description field (e.g., "FAMILYDESC")


class LinkedRecordAutoCreate(BaseModel):
    """Config for auto-creating missing linked records in the target Airtable table.

    When a Priority code has no matching Airtable record, this config tells the
    engine to create a stub record in the target table so the linked record
    field can still be populated.
    """

    writable_key_field_id: str              # Field ID to write the key value (e.g., Vendor_ID)
    extra_fields: dict[str, str] = Field(   # {airtable_field_id: priority_entity_field_name}
        default_factory=dict,               # e.g., {"fld4hIW7hG8eqrf2M": "SUPDES"}
    )
    priority_entity: str | None = None      # Priority entity to fetch extra data (e.g., "SUPPLIERS")
    priority_key_field: str | None = None   # Key field in that entity (e.g., "SUPNAME")


class LinkedRecordConfig(BaseModel):
    """Config for resolving a Priority code → Airtable linked record ID.

    Some Airtable fields are linked records (e.g., "Preferred Vendor" links to
    the Vendors table).  Writing to them requires the Airtable record ID of the
    target record, not a text value.  At P→A sync time the engine fetches the
    target table and builds a {match_value: record_id} map.
    """

    table_id: str          # Airtable table ID (e.g., "tblvenpZXbcgGz8Ry")
    match_field_id: str    # Field ID in target table to match on (e.g., "fldLkVY6ul00KnMJO")
    auto_create: LinkedRecordAutoCreate | None = None  # Auto-create missing records


class FieldMapping(BaseModel):
    """Defines how one Airtable field maps to one Priority field."""

    airtable_field: str                    # Human-readable name (for logging/display)
    airtable_field_id: str | None = None   # Stable Airtable field ID (for API calls)
    priority_field: str
    transform: Literal[
        "clean", "format_price", "to_int", "to_float", "priority_yn",
        "priority_lookup", "linked_record",
    ]
    required: bool = False
    field_type: Literal["str", "float", "int", "linked_record"] = "str"
    max_length: int | None = None
    create_only: bool = False  # If True, only sent on POST (create), not PATCH (update)
    lookup: LookupConfig | None = None  # For linked-table reverse lookups (P→A only)
    linked_record: LinkedRecordConfig | None = None  # For Airtable linked record fields (P→A only)


# ── Sync Records ─────────────────────────────────────────────────────────────

class SubformResult(BaseModel):
    """Tracks the sync result for a single sub-form."""

    subform: str  # e.g. "allergens", "shelf_lives", "price_lists", "bins"
    action: str = "skipped"  # "created", "updated", "skipped", "error"
    detail: str = ""


class SyncRecord(BaseModel):
    """Tracks the sync result for a single entity (product, vendor, etc.)."""

    airtable_record_id: str
    entity_key: str  # SKU for products, SUPNAME for vendors, etc.
    action: SyncAction = SyncAction.SKIP
    fields_changed: list[str] = Field(default_factory=list)
    subform_results: list[SubformResult] = Field(default_factory=list)
    error_message: str = ""
    priority_udate: str | None = None  # UDATE from Priority response


class SyncError(BaseModel):
    """Detailed error information for a failed sync."""

    entity_key: str  # SKU for products, SUPNAME for vendors, etc.
    action: str
    status_code: int | None = None
    message: str
    timestamp: datetime


# ── Conflict Detection ───────────────────────────────────────────────────────

class ConflictRecord(BaseModel):
    """A single field-level conflict detected during sync."""

    entity_key: str              # e.g., SKU "P00001"
    field_name: str              # e.g., "SPEC4" (Priority) or "Brand" (Airtable)
    source_value: Any = None     # Value from the sync source (what we want to write)
    target_value: Any = None     # Value currently in the target (what would be overwritten)
    direction: str               # "A->P" or "P->A"
    resolution: str = "pending"  # "source_wins", "skipped", "record_skipped"
    timestamp: str = ""          # ISO 8601 when conflict was detected


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
    conflicts: list[ConflictRecord] = Field(default_factory=list)
    max_priority_udate: str | None = None  # Highest UDATE seen in P→A run
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
