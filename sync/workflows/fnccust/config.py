"""FNCCUST-specific configuration constants (Financial Parameters for Customers)."""

# Airtable table/view for Financial Parameters for Customers
AIRTABLE_TABLE_NAME = "Customers"
AIRTABLE_SYNC_VIEW = "EDI Cust. Financials 1"

# Key fields
AIRTABLE_KEY_FIELD = "Priority Cust. ID"  # Formula field (read-only)
AIRTABLE_KEY_FIELD_WRITABLE = "Account ID"  # Writable singleLineText field

# Timestamp fields
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY = "Last Synced from Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"

# Priority entity
PRIORITY_ENTITY = "FNCCUST"
PRIORITY_KEY_FIELD = "CUSTNAME"

# Timestamp field names dict (for AirtableClient constructor)
TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
}
