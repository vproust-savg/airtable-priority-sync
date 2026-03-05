"""
Vendor Price Lists configuration constants.

Uses the PRICELIST entity in Priority with PARTPRICE2_SUBFORM for individual items.
The Airtable source is the "Vendor Price List" table.
"""

# ── Airtable ─────────────────────────────────────────────────────────────────
AIRTABLE_TABLE_NAME = "Vendor Price List"
AIRTABLE_TABLE_ID = "tbleL8PK918UIukFv"
AIRTABLE_SYNC_VIEW = "Airtable > Priority API Sync"

# Key field: Price List Code + SKU form a composite key in the output,
# but for Airtable the record ID is the unique identifier.
# There's no single "sync needed" key — this is a batch export.
AIRTABLE_KEY_FIELD = "Price List Code"
AIRTABLE_KEY_FIELD_WRITABLE = "Price List Code"

# ── Priority ─────────────────────────────────────────────────────────────────
PRIORITY_ENTITY = "PRICELIST"
PRIORITY_KEY_FIELD = "PLNAME"
PRIORITY_ITEMS_SUBFORM = "PARTPRICE2_SUBFORM"

# ── Timestamp fields ─────────────────────────────────────────────────────────
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY = "Last Synced from Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"

TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
    "sync_comments": "Sync Comments",
}

# ── Field IDs ──────────────────────────────────────────────────────────────────
AIRTABLE_KEY_FIELD_ID = "fld98fqyqEboRCrU4"  # Price List Code
AIRTABLE_FIELD_LAST_SYNCED_ID = "fldvrN7JezJRBpBji"  # Last Synced to Priority
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY_ID = "fldYVbGkksNW334cv"  # Last Synced from Priority
AIRTABLE_FIELD_PRIORITY_UDATE_ID = "fldR2LIZoT17zzHbi"  # Priority UDATE

TIMESTAMP_FIELD_IDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED_ID,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY_ID,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE_ID,
    "sync_comments": "fld1ruqLsgy3QAnyB",  # Sync Comments
}
