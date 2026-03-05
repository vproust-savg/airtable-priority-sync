"""Vendors All configuration constants (SUPPLIERS + FNCSUP + sub-forms)."""

# ── Main Airtable table ─────────────────────────────────────────────────────
AIRTABLE_TABLE_NAME = "Vendors"
AIRTABLE_SYNC_VIEW = "Airtable > Priority API Sync"

# Key fields
AIRTABLE_KEY_FIELD = "Priority Vendor ID"  # Formula (read-only)
AIRTABLE_KEY_FIELD_WRITABLE = "Vendor_ID"  # singleLineText (writable)

# ── Related Airtable tables (sub-form sources) ──────────────────────────────
AIRTABLE_CONTACTS_TABLE_ID = "tblhSVfliCzSvNEuj"
AIRTABLE_CONTACTS_VIEW = "EDI Vendors 2 - Vendor Contacts"

AIRTABLE_PRODUCTS_TABLE_ID = "tblFU4YGTbTrHhbow"
AIRTABLE_PRODUCTS_VIEW = "EDI Vendors 3 - Vendor Parts"

AIRTABLE_SITES_TABLE_ID = "tbluYvxkejxx8MEK4"
AIRTABLE_SITES_VIEW = "EDI Vendors 4 - Vendor Sites"

AIRTABLE_REMARKS_VIEW = "EDI Vendors 5 - Remarks"

# ── Bank account view (same Vendors table, different view) ───────────────────
AIRTABLE_BANK_VIEW = "EDI Vend. Financial 2 - Bank Account Details"

# ── Timestamp fields ─────────────────────────────────────────────────────────
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY = "Last Synced from Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"

# ── Priority entities ────────────────────────────────────────────────────────
PRIORITY_ENTITY = "SUPPLIERS"
PRIORITY_KEY_FIELD = "SUPNAME"

# Secondary entity: FNCSUP (Financial Parameters for Vendors)
PRIORITY_FNCSUP_ENTITY = "FNCSUP"

# Sub-form names
CONTACTS_SUBFORM_NAME = "SUPPERSONNEL_SUBFORM"
PARTS_SUBFORM_NAME = "SUPPART_SUBFORM"
SITES_SUBFORM_NAME = "SUPDESTCODES_SUBFORM"
# SUPPLIERSTEXT_SUBFORM → returns 404, cannot sync via API

# Bank account sub-form (lives on FNCSUP)
ACCOUNTBANK_SUBFORM_NAME = "ACCOUNTBANK_SUBFORM"

TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
    "sync_comments": "Sync Comments",
}

# ── Field IDs ──────────────────────────────────────────────────────────────────
AIRTABLE_KEY_FIELD_ID = "fldLkVY6ul00KnMJO"  # Priority Vendor ID
AIRTABLE_KEY_FIELD_WRITABLE_ID = "fldQzGeL311gCqV6M"  # Vendor_ID
AIRTABLE_FIELD_LAST_SYNCED_ID = "fld45KwIRWg3jmY9R"  # Last Synced to Priority
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY_ID = "fldyYXhEBmDgRzJ2C"  # Last Synced from Priority
AIRTABLE_FIELD_PRIORITY_UDATE_ID = "fld4jzA0ORwkIWiOz"  # Priority UDATE

TIMESTAMP_FIELD_IDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED_ID,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY_ID,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE_ID,
    "sync_comments": "fldUlcPcylEWG3Nwj",  # Sync Comments
}
