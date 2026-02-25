"""
Customer Price Lists configuration constants.

Uses the PRICELIST entity in Priority with PARTPRICE2_SUBFORM for individual items.
The Airtable source is the "Customer Price List v2" table.

Special: Each Airtable record can produce up to 3 rows (Base, Whole, Lvl 3)
via row explosion — one per price level.
"""

# ── Airtable ─────────────────────────────────────────────────────────────────
AIRTABLE_TABLE_NAME = "Customer Price List v2"
AIRTABLE_TABLE_ID = "tblSObjhLCOiaKTN6"
AIRTABLE_SYNC_VIEW = "EDI Cust. Price List 1"

AIRTABLE_KEY_FIELD = "Price List Code"
AIRTABLE_KEY_FIELD_WRITABLE = "Price List Code"

# ── Priority ─────────────────────────────────────────────────────────────────
PRIORITY_ENTITY = "PRICELIST"
PRIORITY_KEY_FIELD = "PLNAME"
PRIORITY_ITEMS_SUBFORM = "PARTPRICE2_SUBFORM"

# ── Price level mappings for row explosion ────────────────────────────────────
# Each Airtable record → up to 3 output rows, one per level
PRICE_LEVELS = [
    {
        "price_field": "LVL 1 SALE PRICE",
        "code": "Base",
        "description": "Base Price List",
    },
    {
        "price_field": "LVL 2 SALE PRICE",
        "code": "Whole",
        "description": "Wholesale Prices",
    },
    {
        "price_field": "LVL 3 SALE PRICE",
        "code": "Lvl 3",
        "description": "3. Distributor",
    },
]

# ── Timestamp fields ─────────────────────────────────────────────────────────
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY = "Last Synced from Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"

TIMESTAMP_FIELDS = {
    "last_synced_to": AIRTABLE_FIELD_LAST_SYNCED,
    "last_synced_from": AIRTABLE_FIELD_LAST_SYNCED_FROM_PRIORITY,
    "priority_udate": AIRTABLE_FIELD_PRIORITY_UDATE,
}
