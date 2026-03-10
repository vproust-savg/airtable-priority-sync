"""
Bidirectional field mapping between Airtable and Priority ERP for FNCCUST
(Financial Parameters for Customers).

Source of truth:
  - Priority interface: tools/7. SG Financial Parameters Cust Priority Interface.txt
  - Airtable fields:   tools/31. Script for Financial Parameters for Customers v2.py

NOTE: Most Airtable fields for customer financial parameters are formulas
(read-only). The P→A map is very thin — only Priority Customer Status is
writable. All 12 other fields can be READ from Airtable for A→P sync but
cannot be WRITTEN for P→A sync.

No STATUS mode for FNCCUST.
"""

from __future__ import annotations

from sync.core.models import FieldMapping


# ═════════════════════════════════════════════════════════════════════════════
# A→P (Airtable → Priority) field mapping
# ═════════════════════════════════════════════════════════════════════════════

A2P_FIELD_MAP: list[FieldMapping] = [
    # ── Primary key ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Priority Cust. ID",
        airtable_field_id="fldsnRoMbOuU26Ezc",
        priority_field="CUSTNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
    # ── Financial parameters ─────────────────────────────────────────────
    FieldMapping(
        airtable_field="Billing Priority Cust. ID (from Billing Cust. Number)",
        airtable_field_id="fldXpYyrOi3aLQr4a",
        priority_field="PCUSTNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Cust Group Code",
        airtable_field_id="fldA2clRU9QmVdXwy",
        priority_field="CTYPECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Priority Customer Status",
        airtable_field_id="fldLhntR45Memi1na",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Financial Parameters Assigned",
        airtable_field_id="fldBOeGZ4A33NNM1o",
        priority_field="OWNERLOGIN",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Financial Account No",
        airtable_field_id="fldAGfiflEytwOPlI",
        priority_field="ACCNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Tax Code",
        airtable_field_id="fldl7A0ajRvutueWk",
        priority_field="TAXCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Invoice Type",
        airtable_field_id="fldOZsMtsorhrXu0I",
        priority_field="IVTYPE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Payment Terms Code",
        airtable_field_id="fldaSfFLCnVHKAWti",
        priority_field="PAYCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Finance Charge Code",
        airtable_field_id="fldFaoGchkTHlStjK",
        priority_field="INTERESTNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Curr",
        airtable_field_id="fldoGuJdTrWu2SApq",
        priority_field="CODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="EDI_Y",
        airtable_field_id="fld1YPOQs19ILedr6",
        priority_field="PAYMENTLINK",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Pay to Bank Account",
        airtable_field_id="fldTXArpiuPDDBqyI",
        priority_field="CASHNAME",
        transform="clean",
        field_type="str",
    ),
]


# ═════════════════════════════════════════════════════════════════════════════
# P→A (Priority → Airtable) field mapping — REVERSE DIRECTION
# ═════════════════════════════════════════════════════════════════════════════
# Most Airtable financial parameter fields are FORMULAS (read-only).
# Writable fields: "Priority Customer Status" (singleSelect),
# "Annual Revenue" (currency). All other fields (Cust Group Code,
# Financial Account No, Tax Code, Invoice Type, Payment Terms Code,
# Finance Charge Code, Curr, EDI_Y, Pay to Bank Account, Financial
# Parameters Assigned, Billing Priority Cust. ID) are formulas or
# lookups and cannot be written.

P2A_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Priority Customer Status",
        airtable_field_id="fldLhntR45Memi1na",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Annual Revenue",
        airtable_field_id="fldIJdIiohJ6zJiTq",
        priority_field="YEARINCOME",
        transform="to_float",
        field_type="float",
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


# ── P→A: Airtable fields to fetch (for comparing current values) ───────────

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in P2A_FIELD_MAP]
    + [
        "Priority Cust. ID",
        "Last Synced from Priority",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
)


# ── P→A: Priority $select fields (for API call) ────────────────────────────

P2A_PRIORITY_SELECT: list[str] = (
    ["CUSTNAME", "UDATE"]
    + [m.priority_field for m in P2A_FIELD_MAP if m.priority_field != "CUSTNAME"]
)
