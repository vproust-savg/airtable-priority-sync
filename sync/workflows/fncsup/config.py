"""FNCSUP-specific configuration constants (Financial Parameters for Vendors)."""

# Airtable table/view for Financial Parameters for Vendors
AIRTABLE_TABLE_NAME = "Vendors"
AIRTABLE_SYNC_VIEW = "EDI Vend. Financial 1 - Financial Parameters for Vendors"

# Secondary view for bank account details
AIRTABLE_BANK_VIEW = "EDI Vend. Financial 2 - Bank Account Details"

# Key fields
AIRTABLE_KEY_FIELD = "Priority Vendor ID"  # Formula field (read-only)
AIRTABLE_KEY_FIELD_WRITABLE = "Vendor_ID"  # Writable singleLineText field

# Timestamp fields
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY = "Last Synced from Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"

# Priority entity
PRIORITY_ENTITY = "FNCSUP"
PRIORITY_KEY_FIELD = "SUPNAME"

# Sub-form
ACCOUNTBANK_SUBFORM_NAME = "ACCOUNTBANK_SUBFORM"

# Timestamp field names dict (for AirtableClient constructor)
TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
    "sync_comments": "Sync Comments",
}
