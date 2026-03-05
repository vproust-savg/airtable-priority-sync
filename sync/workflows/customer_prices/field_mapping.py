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
        airtable_field_id="fldPympnf6DCHeZvo",
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
    "Last Synced to Priority",
    "Last Synced from Priority",
    "Priority UDATE",
]

AIRTABLE_FIELD_IDS: dict[str, str] = {
    "Price List Code": "fldPympnf6DCHeZvo",
    "Price List Desc.": "fld6Zu2fpICyJD6s2",
    "Price List Date Output": "fldfy9fduu8QnK38K",
    "SKU Trim (EDI) (from Products)": "fldz5TJfay7VpiA2C",
    "Price": "fldKDM53Gf1dQCQQA",
    "List Currency": "fldBfEC89HL9AqgJa",
    "Part Currency": "fldsmJHAIuzAKepn5",
    "Quantity": "fldxCPr9DPAW8aHTS",
    "LVL 1 SALE PRICE": "fld39uwa1u1Y6GLJs",
    "LVL 2 SALE PRICE": "fldgatgC8D0uJhzdH",
    "LVL 3 SALE PRICE": "fldV8RQyEGhjhQUCe",
    "Last Synced to Priority": "fld4TucAfGbT64SAY",
    "Last Synced from Priority": "fldt6jJZbwxlcd0xI",
    "Priority UDATE": "fldcFJReb7R6J7G6A",
}

# ═════════════════════════════════════════════════════════════════════════════
# P→A — very limited
# ═════════════════════════════════════════════════════════════════════════════

P2A_FIELD_MAP: list[FieldMapping] = []

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = [
    "Price List Code",
    "Last Synced from Priority",
    "Last Synced to Priority",
    "Priority UDATE",
]

P2A_PRIORITY_SELECT: list[str] = ["PLNAME", "UDATE"]
