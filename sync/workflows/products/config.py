"""Product-specific configuration constants."""

# Airtable table/view for Products
AIRTABLE_PRODUCTS_TABLE_NAME = "Products"
AIRTABLE_PRODUCTS_TABLE_ID = "tblAOeqWrVrBu16Tj"
AIRTABLE_SYNC_VIEW = "Airtable > Priority API Sync"

# Key fields
AIRTABLE_FIELD_SKU = "SKU Trim (EDI)"  # Formula field (read-only, used for lookups)
AIRTABLE_FIELD_SKU_WRITABLE = "SKU"    # Writable field (used for creating records)

# Timestamp fields
AIRTABLE_FIELD_LAST_MODIFIED = "Last Airtable Modified"
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY = "Last Synced from Priority"

# Shelf Lives table (for sub-form sync)
AIRTABLE_SHELF_LIVES_TABLE_ID = "tbluWqVkrpLFh0D1G"
AIRTABLE_SHELF_LIVES_VIEW = "EDI Parts 2 - Shelf Lives"

# Priority entities
PRIORITY_ENTITY = "LOGPART"
PRIORITY_KEY_FIELD = "PARTNAME"

# Secondary entities (merged from fncpart + prdpart workflows)
PRIORITY_FNCPART_ENTITY = "FNCPART"
PRIORITY_PRDPART_ENTITY = "PRDPART"

# Timestamp field names dict (for AirtableClient constructor)
TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
    "sync_comments": "Sync Comments",
}

# ── Field IDs (stable Airtable identifiers) ─────────────────────────────
AIRTABLE_FIELD_SKU_ID = "fldiGjELA26tbBi8f"             # SKU Trim (EDI)
AIRTABLE_FIELD_SKU_WRITABLE_ID = "flducYv1SA7vJm5QY"    # SKU
AIRTABLE_FIELD_LAST_MODIFIED_ID = "fldmVC26rpWfASDbA"    # Last Airtable Modified
AIRTABLE_FIELD_LAST_SYNCED_ID = "fldsDE7tq0eS9X4wD"     # Last Synced to Priority
AIRTABLE_FIELD_PRIORITY_UDATE_ID = "fldwSTqmSlXcL5V6z"  # Priority UDATE
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY_ID = "fldj64NlliBrSCtJO"  # Last Synced from Priority

TIMESTAMP_FIELD_IDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED_ID,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY_ID,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE_ID,
    "sync_comments": "fldi8arwqVkZmZzHs",  # Sync Comments
}
