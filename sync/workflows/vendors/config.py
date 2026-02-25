"""Vendors All configuration constants (SUPPLIERS + sub-forms)."""

# ── Main Airtable table ─────────────────────────────────────────────────────
AIRTABLE_TABLE_NAME = "Vendors"
AIRTABLE_SYNC_VIEW = "EDI Vendors 1 - Vendors"

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

# ── Timestamp fields ─────────────────────────────────────────────────────────
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY = "Last Synced from Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"

# ── Priority entity ─────────────────────────────────────────────────────────
PRIORITY_ENTITY = "SUPPLIERS"
PRIORITY_KEY_FIELD = "SUPNAME"

# Sub-form names
CONTACTS_SUBFORM_NAME = "SUPPERSONNEL_SUBFORM"
PARTS_SUBFORM_NAME = "SUPPART_SUBFORM"
SITES_SUBFORM_NAME = "SUPDESTCODES_SUBFORM"
# SUPPLIERSTEXT_SUBFORM → returns 404, cannot sync via API

TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
}
