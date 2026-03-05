"""
Bidirectional field mapping for Customers All (CUSTOMERS).

Source of truth:
  - Priority interface: tools/6. SG Cust All Priority Interface.txt
  - Airtable fields:   tools/30. Script for Customer All v14.py

P→A is limited — many Airtable fields are formulas or AI-generated (aiText).
Only writable fields are included in P2A_FIELD_MAP.
"""

from __future__ import annotations

from sync.core.models import FieldMapping

# ── FNCCUST field maps (imported for merged workflow) ────────────────────────
from sync.workflows.fnccust.field_mapping import (
    A2P_FIELD_MAP as FNCCUST_A2P_FIELD_MAP,
    P2A_FIELD_MAP as FNCCUST_P2A_FIELD_MAP,
)


# ═════════════════════════════════════════════════════════════════════════════
# A→P (Airtable → Priority) field mapping — Main CUSTOMERS entity
# ═════════════════════════════════════════════════════════════════════════════

A2P_FIELD_MAP: list[FieldMapping] = [
    # ── Primary key ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Priority Cust. ID",
        priority_field="CUSTNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
    # ── Customer fields ────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Business Name Output",
        priority_field="CUSTDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Sales Rep Number",
        priority_field="AGENTCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Cust Group Code",
        priority_field="CTYPECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Website Output",
        priority_field="HOSTNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Zone Code",
        priority_field="ZONECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Line 1 Output",
        priority_field="ADDRESS",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address_Line 2 Output",
        priority_field="ADDRESS2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address_City Output",
        priority_field="STATEA",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Full State Name",
        priority_field="STATENAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address_Zip Code",
        priority_field="ZIP",
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
        airtable_field="Credit Limit",
        priority_field="MAX_CREDIT",
        transform="to_float",
        field_type="float",
    ),
    # SPEC1-5: Custom fields
    FieldMapping(
        airtable_field="Payment Method (1)",
        priority_field="SPEC1",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Are you part of Hilton? (2)",
        priority_field="SPEC2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Hilton Inncode (3)",
        priority_field="SPEC3",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Rekki Output (4)",
        priority_field="SPEC4",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Carrier Account Number (5)",
        priority_field="SPEC5",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Curr",
        priority_field="CODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Country",
        priority_field="COUNTRYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Tax Code",
        priority_field="TAXCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Dist. Route Code",
        priority_field="DISTRLINECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Approval_Before_Charging (6)",
        priority_field="SPEC6",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Shipment Code",
        priority_field="STCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Priority Customer Status",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing_Legal Name Output",
        priority_field="CUSTDESLONG",
        transform="clean",
        field_type="str",
    ),
]


# ═════════════════════════════════════════════════════════════════════════════
# P→A (Priority → Airtable) — REVERSE DIRECTION
# ═════════════════════════════════════════════════════════════════════════════
# Limited to writable Airtable fields only.
#
# Excluded (formula — read-only):
#   Priority Cust. ID, Sales Rep Number, Cust Group Code, Payment Terms Code,
#   Curr, Country, Tax Code, Shipment Code, Rekki Output (4)
#
# Excluded (aiText — read-only):
#   Business Name Output, Website Output, Billing Address Line 1/2/City Output,
#   Full State Name, Billing Address_Zip Code, Billing_Legal Name Output
#
# Excluded (multipleLookupValues — read-only):
#   Zone Code, Dist. Route Code
#
# Writable fields:
#   - Priority Customer Status (singleSelect)
#   - Credit Limit (number)
#   - Payment Method (1) (singleSelect)
#   - Are you part of Hilton? (2) (singleSelect)
#   - Hilton Inncode (3) (singleLineText)
#   - Carrier Account Number (5) (multilineText)
#   - Approval_Before_Charging (singleSelect)

P2A_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Priority Customer Status",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Credit Limit",
        priority_field="MAX_CREDIT",
        transform="to_float",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Payment Method (1)",
        priority_field="SPEC1",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Are you part of Hilton? (2)",
        priority_field="SPEC2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Hilton Inncode (3)",
        priority_field="SPEC3",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Carrier Account Number (5)",
        priority_field="SPEC5",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Approval_Before_Charging (6)",
        priority_field="SPEC6",
        transform="clean",
        field_type="str",
    ),
]


# ── A→P: Airtable fields to fetch ──────────────────────────────────────────

AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in A2P_FIELD_MAP]
    # ── FNCCUST fields (Billing Cust, Account No, Invoice Type, etc.) ──
    + [m.airtable_field for m in FNCCUST_A2P_FIELD_MAP
       if m.airtable_field not in {"Priority Cust. ID", "Last Synced to Priority",
                                    "Last Synced from Priority", "Priority UDATE"}]
    + [
        "Last Synced to Priority",
        "Last Synced from Priority",
        "Priority UDATE",
    ]
)

# ── P→A: Airtable fields to fetch ──────────────────────────────────────────

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in P2A_FIELD_MAP]
    + [m.airtable_field for m in FNCCUST_P2A_FIELD_MAP]
    + [
        "Priority Cust. ID",
        "Last Synced from Priority",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
)

# ── P→A: Priority $select fields ───────────────────────────────────────────

P2A_PRIORITY_SELECT: list[str] = (
    ["CUSTNAME", "UDATE"]
    + [m.priority_field for m in P2A_FIELD_MAP if m.priority_field != "CUSTNAME"]
)
