"""
Bidirectional field mapping for Vendors All (SUPPLIERS).

Source of truth:
  - Priority interface: tools/1. Vendors All Priority Interface.txt
  - Airtable fields:   tools/20. Script for Vendor All v3 - online.py

P→A is limited — many Airtable fields are formulas or AI-generated (aiText).
Only writable fields are included in P2A_FIELD_MAP.
"""

from __future__ import annotations

from sync.core.models import FieldMapping


# ═════════════════════════════════════════════════════════════════════════════
# A→P (Airtable → Priority) field mapping — Main SUPPLIERS entity
# ═════════════════════════════════════════════════════════════════════════════

A2P_FIELD_MAP: list[FieldMapping] = [
    # ── Primary key ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Priority Vendor ID",
        priority_field="SUPNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
    # ── Vendor fields ────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Company Name",
        priority_field="SUPDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Buyer Output",
        priority_field="OWNERLOGIN",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Status",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Clean Billing Address Line 1",
        priority_field="ADDRESS",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Clean Billing Address Line 2",
        priority_field="ADDRESS2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Clean Billing Address City",
        priority_field="STATEA",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address State",
        priority_field="STATECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Zip Code",
        priority_field="ZIP",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Country",
        priority_field="COUNTRYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Clean Website",
        priority_field="HOSTNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Company Email",
        priority_field="EMAIL",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Clean Phone Number",
        priority_field="PHONE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Company Type",
        priority_field="BUSINESSTYPE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Priority Shipping Code",
        priority_field="STCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Incoterms",
        priority_field="IMPTERMNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Vendor Group Code",
        priority_field="SUPTYPECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Payment Terms Code",
        priority_field="PAYCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Currency",
        priority_field="CODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Federal Tax ID (16)",
        priority_field="VATNUM",
        transform="clean",
        field_type="str",
    ),
]


# ═════════════════════════════════════════════════════════════════════════════
# P→A (Priority → Airtable) — REVERSE DIRECTION
# ═════════════════════════════════════════════════════════════════════════════
# Limited to writable Airtable fields only.
# Excluded (formula/aiText): Priority Vendor ID, Buyer Output, Clean Address*,
# Clean Website, Company Email, Clean Phone Number, Priority Shipping Code,
# Vendor Group Code, Payment Terms Code, Remarks

P2A_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Company Name",
        priority_field="SUPDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Status",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address State",
        priority_field="STATECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Zip Code",
        priority_field="ZIP",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Country",
        priority_field="COUNTRYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Incoterms",
        priority_field="IMPTERMNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Currency",
        priority_field="CODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Federal Tax ID (16)",
        priority_field="VATNUM",
        transform="clean",
        field_type="str",
    ),
]


# ── A→P: Airtable fields to fetch ──────────────────────────────────────────

AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in A2P_FIELD_MAP]
    + [
        "Last Synced to Priority",
        "Last Synced from Priority",
        "Priority UDATE",
    ]
)

# ── P→A: Airtable fields to fetch ──────────────────────────────────────────

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in P2A_FIELD_MAP]
    + [
        "Priority Vendor ID",
        "Last Synced from Priority",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
)

# ── P→A: Priority $select fields ───────────────────────────────────────────

P2A_PRIORITY_SELECT: list[str] = (
    ["SUPNAME", "UDATE"]
    + [m.priority_field for m in P2A_FIELD_MAP if m.priority_field != "SUPNAME"]
)
