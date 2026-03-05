"""Tech sheet sync configuration constants."""

# Airtable table/view for tech sheet sync
AIRTABLE_TABLE_NAME = "Products"
AIRTABLE_SYNC_VIEW = "Airtable > Priority Tech Sheet API Sync"

# Key fields
AIRTABLE_KEY_FIELD = "SKU Trim (EDI)"  # Formula field (read-only, used for lookups)
AIRTABLE_TECHSHEET_FIELD = "Tech Sheet"  # Attachment field

# Priority entity + sub-form
PRIORITY_ENTITY = "LOGPART"
PRIORITY_KEY_FIELD = "PARTNAME"
PRIORITY_SUBFORM = "PARTEXTFILE_SUBFORM"

# PARTEXTFILE_SUBFORM fields (discovered via API):
#   EXTFILENAME  — base64 data URI (data:application/pdf;base64,...)
#   EXTFILEDES   — description/label (settable on POST, used to identify our uploads)
#   SUFFIX       — file extension (auto-detected from data URI)
#   CURDATE      — date attached (auto-set)
#   EXTFILENUM   — internal key (auto-generated, NOT addressable via URL)
#   FILESIZE     — file size in bytes (auto-calculated)
#   ACTIVEFLAG   — "Y" by default
#
# Sub-form pattern: C (multi-record, no URL key access)
# - POST creates new records; duplicate POSTs create duplicate records
# - Deep PATCH also creates new records (does NOT match by EXTFILEDES)
# - Individual records cannot be accessed or deleted by key
# Strategy: GET existing → skip if EXTFILEDES match found → POST if new

# Label prefix for identifying our uploads in Priority
EXTFILEDES_PREFIX = "Tech Sheet"

# Timestamp fields (A→P only)
TIMESTAMP_FIELDS = {
    "last_synced_to": "Last Tech Sheet Sync in Priority",
    "sync_comments": "Sync Comments",
}

# ── Field IDs (stable Airtable identifiers) ─────────────────────────────
AIRTABLE_KEY_FIELD_ID = "fldiGjELA26tbBi8f"                  # SKU Trim (EDI)
AIRTABLE_TECHSHEET_FIELD_ID = "fldKejwzLXmMcMggz"            # Tech Sheet
AIRTABLE_LAST_TECHSHEET_SYNC_FIELD_ID = "fldaSoTHZpEcVETu1"  # Last Tech Sheet Sync in Priority
AIRTABLE_SYNC_COMMENTS_FIELD_ID = "fldi8arwqVkZmZzHs"        # Sync Comments

TIMESTAMP_FIELD_IDS = {
    "last_synced_to": AIRTABLE_LAST_TECHSHEET_SYNC_FIELD_ID,
    "sync_comments": AIRTABLE_SYNC_COMMENTS_FIELD_ID,
}
