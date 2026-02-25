"""
Field mapping for Customer Price Lists.

Source of truth: tools/32. Script for Customer Price Lists v5.py

Special: Each Airtable record produces up to 3 output rows (Base, Whole, Lvl 3).
The "Price List Code" and "Price" are overridden per level.

A→P only — most Airtable fields are lookups/formulas.
"""

from __future__ import annotations

from sync.core.models import FieldMapping


# ═════════════════════════════════════════════════════════════════════════════
# A→P — the "key" is the Price List Code (Base, Whole, Lvl 3)
# ═════════════════════════════════════════════════════════════════════════════

A2P_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Price List Code",
        priority_field="PLNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
]

# Fields to fetch from Airtable
AIRTABLE_FIELDS_TO_FETCH: list[str] = [
    "Price List Code",
    "Price List Desc.",
    "Price List Date Output",
    "SKU Trim (EDI) (from Products)",
    "Price",
    "List Currency",
    "Part Currency",
    "Quantity",
    # LVL fields for row explosion (may or may not exist on each record)
    "LVL 1 SALE PRICE",
    "LVL 2 SALE PRICE",
    "LVL 3 SALE PRICE",
]

# ═════════════════════════════════════════════════════════════════════════════
# P→A — very limited
# ═════════════════════════════════════════════════════════════════════════════

P2A_FIELD_MAP: list[FieldMapping] = []

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = [
    "Price List Code",
]

P2A_PRIORITY_SELECT: list[str] = ["PLNAME", "UDATE"]
