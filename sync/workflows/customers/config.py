"""Customers All configuration constants (CUSTOMERS + sub-forms)."""

# ── Main Airtable table ─────────────────────────────────────────────────────
AIRTABLE_TABLE_NAME = "Customers"
AIRTABLE_SYNC_VIEW = "Airtable > Priority API Sync"

# Key fields
AIRTABLE_KEY_FIELD = "Priority Cust. ID"  # Formula (read-only)
AIRTABLE_KEY_FIELD_WRITABLE = "Account ID"  # singleLineText (writable)

# ── Related Airtable tables (sub-form sources) ──────────────────────────────
AIRTABLE_CONTACTS_TABLE_ID = "tblJb4wJxSucTBa2E"
AIRTABLE_CONTACTS_VIEW = "EDI Cust. 6 - Cust. Contacts"

AIRTABLE_SITES_TABLE_ID = "tblk4aTwDxjjzIzKP"
AIRTABLE_SITES_VIEW = "EDI Cust. 7 - Sites"

AIRTABLE_SPECIAL_PRICES_TABLE_ID = "tbl6xsKAS7MArVQse"
AIRTABLE_SPECIAL_PRICES_VIEW = "EDI Cust. 3 - Special Price"

# ── Additional views on the Customers table (sub-form data) ─────────────────
AIRTABLE_PRICE_LIST_VIEW = "EDI Cust. 2 - Price List"
AIRTABLE_DELIVERY_DAYS_VIEW = "EDI Cust. 5 - Delivery Days"

# ── Timestamp fields ─────────────────────────────────────────────────────────
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY = "Last Synced from Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"

# ── Priority entities ────────────────────────────────────────────────────────
PRIORITY_ENTITY = "CUSTOMERS"
PRIORITY_KEY_FIELD = "CUSTNAME"

# Secondary entity: FNCCUST (Financial Parameters for Customers)
PRIORITY_FNCCUST_ENTITY = "FNCCUST"

# Sub-form names
CONTACTS_SUBFORM_NAME = "CUSTPERSONNEL_SUBFORM"
PRICE_LIST_SUBFORM_NAME = "CUSTPLIST_SUBFORM"
SPECIAL_PRICES_SUBFORM_NAME = "CUSTPARTPRICE_SUBFORM"
WEEKDAY_SUBFORM_NAME = "CUSTWEEKDAY_SUBFORM"
SITES_SUBFORM_NAME = "CUSTDESTS_SUBFORM"
CREDIT_APP_SUBFORM_NAME = "CUSTEXTFILE_SUBFORM"
CREDIT_APP_EXTFILEDES_PREFIX = "Credit Application"
CREDIT_APP_AIRTABLE_FIELD = "Credit Application"  # Attachment field
# CUSTOMERSTEXT_SUBFORM → returns 404, cannot sync via API
# CUSTSHIPTEXT_SUBFORM → returns 404, cannot sync via API

# ── Field IDs ──────────────────────────────────────────────────────────────────
AIRTABLE_KEY_FIELD_ID = "fldsnRoMbOuU26Ezc"  # Priority Cust. ID
AIRTABLE_KEY_FIELD_WRITABLE_ID = "fldAn9q5PAOQOQBb3"  # Account ID
AIRTABLE_FIELD_LAST_SYNCED_ID = "fldsN7JczubWY49Ry"  # Last Synced to Priority
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY_ID = "fldVb2pIX4BLBJWgT"  # Last Synced from Priority
AIRTABLE_FIELD_PRIORITY_UDATE_ID = "fldWLCnJFrdiXGHEJ"  # Priority UDATE

TIMESTAMP_FIELD_IDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED_ID,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY_ID,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE_ID,
    "sync_comments": "fldCPYmJdtmi5440n",  # Sync Comments
}

TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
    "sync_comments": "Sync Comments",
}
