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

# Priority entity
PRIORITY_ENTITY = "LOGPART"
PRIORITY_KEY_FIELD = "PARTNAME"

# Timestamp field names dict (for AirtableClient constructor)
TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
}
