"""
Field mapping for Vendor Price Lists.

Source of truth: tools/22. Script for Vendor Price Lists v2.py

This workflow is A→P only (Airtable → Priority).
Each Airtable record maps to a price entry in PRICELIST.PARTPRICE2_SUBFORM.

Airtable "Vendor Price List" table → PRICELIST('{code}')/PARTPRICE2_SUBFORM
"""

from __future__ import annotations

from sync.core.models import FieldMapping


# ═════════════════════════════════════════════════════════════════════════════
# A→P field mapping — PRICELIST + PARTPRICE2 sub-form
# ═════════════════════════════════════════════════════════════════════════════
# The parent PRICELIST record is identified by "Price List Code".
# The sub-form PARTPRICE2 contains price entries per SKU.

A2P_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Price List Code",
        priority_field="PLNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
]

# Sub-form fields: Airtable → PARTPRICE2 sub-form on PRICELIST
ITEMS_FIELD_MAP: dict[str, str] = {
    "SKU Trim (EDI) (from Products)": "PARTNAME",
    "Unit Price": "PRICE",
    "Quantity": "QUANT",
}

# Fields to fetch from Airtable
AIRTABLE_FIELDS_TO_FETCH: list[str] = [
    "Price List Code",
    "V-Vendor ID (from Vendor)",
    "Vendor Currency (from Vendor)",
    "SKU Trim (EDI) (from Products)",
    "Quantity",
    "Unit Price",
    "Price List Date",
    "Price List Description",
    "Vendor SKU Trim",
    "Vendor Name (from Vendor)",
    "List Currency",
]

# ═════════════════════════════════════════════════════════════════════════════
# P→A field mapping — Vendor Price Lists
# ═════════════════════════════════════════════════════════════════════════════
# Very limited: most Airtable fields are formulas/lookups.
# Only Discount% (percent) appears writable.

P2A_FIELD_MAP: list[FieldMapping] = []

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = [
    "Price List Code",
]

P2A_PRIORITY_SELECT: list[str] = ["PLNAME", "UDATE"]
