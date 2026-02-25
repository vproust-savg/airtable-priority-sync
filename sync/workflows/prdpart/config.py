"""PRDPART-specific configuration constants (MRP Parameters for Parts)."""

# Airtable table/view for MRP Parameters for Parts
# Same table as Products — different view
AIRTABLE_TABLE_NAME = "Products"
AIRTABLE_SYNC_VIEW = "EDI MRP Parts 1 - MRP"

# Key fields (same as products — same table)
AIRTABLE_KEY_FIELD = "SKU Trim (EDI)"  # Formula field (read-only)
AIRTABLE_KEY_FIELD_WRITABLE = "SKU"    # Writable field

# Timestamp fields
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY = "Last Synced from Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"

# Priority entity
PRIORITY_ENTITY = "PRDPART"
PRIORITY_KEY_FIELD = "PARTNAME"

# Timestamp field names dict (for AirtableClient constructor)
TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
}
