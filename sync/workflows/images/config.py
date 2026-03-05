"""Image sync configuration constants."""

# Airtable table/view for image sync
AIRTABLE_TABLE_NAME = "Products"
AIRTABLE_SYNC_VIEW = "Airtable > Priority Image API Sync"

# Key fields
AIRTABLE_KEY_FIELD = "SKU Trim (EDI)"  # Formula field (read-only, used for lookups)
AIRTABLE_IMAGE_FIELD = "Images"  # Attachment field

# Image processing
MAX_IMAGE_SIZE_KB = 150  # Target max file size after compression

# Priority entity
PRIORITY_ENTITY = "LOGPART"
PRIORITY_KEY_FIELD = "PARTNAME"
PRIORITY_IMAGE_FIELD = "EXTFILENAME"  # Direct field, NOT a sub-form

# Timestamp fields (A→P only — no P→A for images)
TIMESTAMP_FIELDS = {
    "last_synced_to": "Last Image Sync in Priority",
    "sync_comments": "Sync Comments",
}

# ── Field IDs (stable Airtable identifiers) ─────────────────────────────
AIRTABLE_KEY_FIELD_ID = "fldiGjELA26tbBi8f"              # SKU Trim (EDI)
AIRTABLE_IMAGE_FIELD_ID = "fldR4D0kHPjBoicgz"            # Images
AIRTABLE_LAST_IMAGE_SYNC_FIELD_ID = "fldUqjDX3zwDeryIs"  # Last Image Sync in Priority
AIRTABLE_SYNC_COMMENTS_FIELD_ID = "fldi8arwqVkZmZzHs"    # Sync Comments

TIMESTAMP_FIELD_IDS = {
    "last_synced_to": AIRTABLE_LAST_IMAGE_SYNC_FIELD_ID,
    "sync_comments": AIRTABLE_SYNC_COMMENTS_FIELD_ID,
}
