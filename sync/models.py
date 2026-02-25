"""
Backward-compatible re-export shim.
New code should import from sync.core.models.

NOTE: SyncRecord.sku has been renamed to SyncRecord.entity_key.
      SyncError.sku has been renamed to SyncError.entity_key.
"""
from sync.core.models import (  # noqa: F401
    FieldMapping,
    SubformResult,
    SyncAction,
    SyncDirection,
    SyncError,
    SyncMode,
    SyncRecord,
    SyncStats,
)
