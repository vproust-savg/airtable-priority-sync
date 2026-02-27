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
