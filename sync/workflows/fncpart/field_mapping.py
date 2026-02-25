"""
Bidirectional field mapping between Airtable and Priority ERP for FNCPART
(Financial Parameters for Parts).

Source of truth:
  - Priority interface: tools/5. SG Financial Parameters Parts Priority Interface.txt
  - Airtable fields:   tools/11. Script for Financial Parameters for Parts v1.py

No STATUS mode for FNCPART -- only 10 fields, always sync all.
"""

from __future__ import annotations

from typing import Any, Callable

from sync.core.models import FieldMapping
from sync.core.utils import clean, format_price, to_float, to_int, priority_yn


# ── Transform function registry ──────────────────────────────────────────────

TRANSFORMS: dict[str, Callable[[Any], Any]] = {
    "clean": clean,
    "format_price": format_price,
    "to_int": to_int,
    "to_float": to_float,
    "priority_yn": priority_yn,
}


# ═════════════════════════════════════════════════════════════════════════════
# A->P (Airtable -> Priority) field mapping
# ═════════════════════════════════════════════════════════════════════════════

A2P_FIELD_MAP: list[FieldMapping] = [
    # ── Primary key ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="SKU Trim (EDI)",
        priority_field="PARTNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
    # ── Financial parameters ─────────────────────────────────────────────
    FieldMapping(
        airtable_field="Accounting Family",
        priority_field="ACCFAMILYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Filemaker Weight",
        priority_field="WEIGHT",
        transform="to_float",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="HTS Code",
        priority_field="CUSTOMSITEMNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Shipping Cost Type",
        priority_field="DUTYPERCENTTYPE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Tax_Group_Code",
        priority_field="TAXGROUPCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Country of Origin",
        priority_field="COUNTRYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Pallet TI",
        priority_field="SAVR_PALLETTI",
        transform="to_float",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Pallet HI",
        priority_field="SAVR_PALLETHI",
        transform="to_float",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Case / Pallet",
        priority_field="SAVR_CASEPERPALLET",
        transform="to_float",
        field_type="float",
    ),
]


# ═════════════════════════════════════════════════════════════════════════════
# P->A (Priority -> Airtable) field mapping -- REVERSE DIRECTION
# ═════════════════════════════════════════════════════════════════════════════
# All fields are writable in Airtable except SKU Trim (EDI) which is a formula.
# For creates, we use "SKU" (writable) instead -- handled by the engine.

P2A_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Accounting Family",
        priority_field="ACCFAMILYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Filemaker Weight",
        priority_field="WEIGHT",
        transform="to_float",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="HTS Code",
        priority_field="CUSTOMSITEMNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Shipping Cost Type",
        priority_field="DUTYPERCENTTYPE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Tax_Group_Code",
        priority_field="TAXGROUPCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Country of Origin",
        priority_field="COUNTRYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Pallet TI",
        priority_field="SAVR_PALLETTI",
        transform="to_float",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Pallet HI",
        priority_field="SAVR_PALLETHI",
        transform="to_float",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Case / Pallet",
        priority_field="SAVR_CASEPERPALLET",
        transform="to_float",
        field_type="float",
    ),
]


# ── Lookup helpers ───────────────────────────────────────────────────────────

_A2P_BY_AIRTABLE = {m.airtable_field: m for m in A2P_FIELD_MAP}
_A2P_BY_PRIORITY = {m.priority_field: m for m in A2P_FIELD_MAP}

_P2A_BY_AIRTABLE = {m.airtable_field: m for m in P2A_FIELD_MAP}
_P2A_BY_PRIORITY = {m.priority_field: m for m in P2A_FIELD_MAP}


# ── A->P: Airtable fields to fetch ──────────────────────────────────────────
# Includes mapped fields + timestamp fields for loop prevention.

AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in A2P_FIELD_MAP]
    + [
        "Last Synced to Priority",
        "Last Synced from Priority",
        "Priority UDATE",
    ]
)


# ── P->A: Airtable fields to fetch (for comparing current values) ───────────

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in P2A_FIELD_MAP]
    + [
        "SKU Trim (EDI)",
        "Last Synced from Priority",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
)


# ── P->A: Priority $select fields (for API call) ────────────────────────────

P2A_PRIORITY_SELECT: list[str] = (
    ["PARTNAME", "UDATE"]
    + [m.priority_field for m in P2A_FIELD_MAP if m.priority_field != "PARTNAME"]
)
