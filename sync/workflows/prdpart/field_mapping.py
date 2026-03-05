"""
Bidirectional field mapping between Airtable and Priority ERP for PRDPART
(MRP Parameters for Parts).

Source of truth:
  - Priority interface: tools/4. SG MRP Parameters for Parts Priority Interface.txt
  - Airtable fields:   tools/12. Script for MRP for Parts v1.py

No STATUS mode for PRDPART — only 7 fields, always sync all.
"""

from __future__ import annotations

from sync.core.models import FieldMapping


# ═════════════════════════════════════════════════════════════════════════════
# A→P (Airtable → Priority) field mapping
# ═════════════════════════════════════════════════════════════════════════════

A2P_FIELD_MAP: list[FieldMapping] = [
    # ── Primary key ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="SKU Trim (EDI)",
        airtable_field_id="fldiGjELA26tbBi8f",
        priority_field="PARTNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
    # ── MRP parameters ───────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Purchase Lead Time",
        airtable_field_id="fldoSaWmwYoObgF3X",
        priority_field="PLEADTIME",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Shipping Days",
        airtable_field_id="fldUvBExbJD7L07lX",
        priority_field="SHIPMDAYS",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Safety Stock",
        airtable_field_id="fldnchhLM2lTsdEmS",
        priority_field="INVLEVEL",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Main Buyer Priority Code",
        airtable_field_id="fld3x0njsSny4q6hx",
        priority_field="USERLOGIN",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="V-Vendor ID (from Preferred Vendor)",
        airtable_field_id="fld1EMYPIfS1lW9GC",
        priority_field="SUPNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Min for Order",
        airtable_field_id="fldNCOEZH1OF0TCGk",
        priority_field="MINPURQUANT",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Increment for Order",
        airtable_field_id="fldAOemtBN449wYPU",
        priority_field="INCREMENT",
        transform="to_int",
        field_type="int",
    ),
]


# ═════════════════════════════════════════════════════════════════════════════
# P→A (Priority → Airtable) field mapping — REVERSE DIRECTION
# ═════════════════════════════════════════════════════════════════════════════
# Excludes read-only Airtable fields:
#   - "V-Vendor ID (from Preferred Vendor)" → multipleLookupValues (read-only)
#   - "Main Buyer Priority Code" → formula (read-only)

P2A_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Purchase Lead Time",
        airtable_field_id="fldoSaWmwYoObgF3X",
        priority_field="PLEADTIME",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Shipping Days",
        airtable_field_id="fldUvBExbJD7L07lX",
        priority_field="SHIPMDAYS",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Safety Stock",
        airtable_field_id="fldnchhLM2lTsdEmS",
        priority_field="INVLEVEL",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Min for Order",
        airtable_field_id="fldNCOEZH1OF0TCGk",
        priority_field="MINPURQUANT",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Increment for Order",
        airtable_field_id="fldAOemtBN449wYPU",
        priority_field="INCREMENT",
        transform="to_int",
        field_type="int",
    ),
]


# ── A→P: Airtable fields to fetch ──────────────────────────────────────────
# Includes mapped fields + timestamp fields for loop prevention.

AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in A2P_FIELD_MAP]
    + [
        "Last Synced to Priority",
        "Last Synced from Priority",
        "Priority UDATE",
    ]
)


# ── P→A: Airtable fields to fetch (for comparing current values) ───────────

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in P2A_FIELD_MAP]
    + [
        "SKU Trim (EDI)",
        "Last Synced from Priority",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
)


# ── P→A: Priority $select fields (for API call) ────────────────────────────

P2A_PRIORITY_SELECT: list[str] = (
    ["PARTNAME", "UDATE"]
    + [m.priority_field for m in P2A_FIELD_MAP if m.priority_field != "PARTNAME"]
)
