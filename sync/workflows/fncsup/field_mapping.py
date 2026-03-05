"""
Bidirectional field mapping between Airtable and Priority ERP for FNCSUP
(Financial Parameters for Vendors).

Source of truth:
  - Priority interface: tools/2.SG Financial Parameters Vend Priority Interface.txt
  - Airtable fields:   tools/21. Script for Financial Parameters for Vendors v1.py

P→A excludes read-only Airtable fields (formulas):
  - Priority Vendor ID (formula), Priority Vendor Account No (formula),
    Buyer Output (formula), VAT Code (formula), Payment Terms Code (formula),
    Maximum Tax Code (formula)

Writable P→A fields:
  - Currency (singleSelect), Status (singleSelect),
    FDA Registration # (singleLineText), Federal Tax ID (16) (singleLineText)
  - Bank fields: Bank Account, Bank Code, Routing Number, IBAN (all singleLineText)

No STATUS mode for FNCSUP.
"""

from __future__ import annotations

from sync.core.models import FieldMapping


# ═════════════════════════════════════════════════════════════════════════════
# A→P (Airtable → Priority) field mapping — Main FNCSUP entity
# ═════════════════════════════════════════════════════════════════════════════

A2P_FIELD_MAP: list[FieldMapping] = [
    # ── Primary key ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Priority Vendor ID",
        airtable_field_id="fldLkVY6ul00KnMJO",
        priority_field="SUPNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
    # ── Financial parameters ─────────────────────────────────────────────
    FieldMapping(
        airtable_field="Currency",
        airtable_field_id="fldsFN1BhXxYXusSh",
        priority_field="CODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Priority Vendor Account No",
        airtable_field_id="fldWCR3iNSo8gq4GK",
        priority_field="ACCNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Buyer Output",
        airtable_field_id="fldIKLZXHbvsnfhAh",
        priority_field="OWNERLOGIN",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Status",
        airtable_field_id="fldBcdMhERTIjDksV",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="FDA Registration #",
        airtable_field_id="fldExdRs6V7EKW09y",
        priority_field="COMPNUM",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Federal Tax ID (16)",
        airtable_field_id="fldpT73EmgEmdDPMN",
        priority_field="VATNUM",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="VAT Code",
        airtable_field_id="fldcfZ6ahghdgieNT",
        priority_field="TAXCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Payment Terms Code",
        airtable_field_id="fldxMIBEb84iKyoGy",
        priority_field="PAYCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Maximum Tax Code",
        airtable_field_id="fldUEbr759dY5mzyg",
        priority_field="WTAXCODE",
        transform="clean",
        field_type="str",
    ),
]


# ═════════════════════════════════════════════════════════════════════════════
# P→A (Priority → Airtable) field mapping — REVERSE DIRECTION
# ═════════════════════════════════════════════════════════════════════════════
# Only writable Airtable fields are included.
# Excludes: Priority Vendor ID (formula), Priority Vendor Account No (formula),
#           Buyer Output (formula), VAT Code (formula),
#           Payment Terms Code (formula), Maximum Tax Code (formula)

P2A_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Currency",
        airtable_field_id="fldsFN1BhXxYXusSh",
        priority_field="CODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Status",
        airtable_field_id="fldBcdMhERTIjDksV",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="FDA Registration #",
        airtable_field_id="fldExdRs6V7EKW09y",
        priority_field="COMPNUM",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Federal Tax ID (16)",
        airtable_field_id="fldpT73EmgEmdDPMN",
        priority_field="VATNUM",
        transform="clean",
        field_type="str",
    ),
]


# ═════════════════════════════════════════════════════════════════════════════
# Bank account fields (ACCOUNTBANK_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════
# These are in the same Vendors table but fetched from a different view.
# All 4 bank fields are writable singleLineText fields.

BANK_A2P_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Bank Account",
        airtable_field_id="fldLNWU2m9KWgx309",
        priority_field="PAYACCOUNT",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Bank Code",
        airtable_field_id="fldHPr2Sh7pRxpc85",
        priority_field="BANKCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Routing Number",
        airtable_field_id="fldfmJD3BQe4VrEdZ",
        priority_field="BRANCH",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="IBAN",
        airtable_field_id="fldBkaMQOdH1QfrU1",
        priority_field="IBAN",
        transform="clean",
        field_type="str",
    ),
]

# P→A bank fields (same as A→P — all are writable)
BANK_P2A_FIELD_MAP: list[FieldMapping] = BANK_A2P_FIELD_MAP


# ── Airtable fields to fetch from the bank view ─────────────────────────

BANK_AIRTABLE_FIELDS: list[str] = [
    "Priority Vendor ID",
    "Bank Account",
    "Bank Code",
    "Routing Number",
    "IBAN",
]


# ── A→P: Airtable fields to fetch from main view ───────────────────────

AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in A2P_FIELD_MAP]
    + [
        "Last Synced to Priority",
        "Last Synced from Priority",
        "Priority UDATE",
    ]
)


# ── P→A: Airtable fields to fetch (for comparing current values) ───────

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in P2A_FIELD_MAP]
    + [m.airtable_field for m in BANK_P2A_FIELD_MAP]
    + [
        "Priority Vendor ID",
        "Last Synced from Priority",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
)


# ── P→A: Priority $select fields (for API call) ────────────────────────

P2A_PRIORITY_SELECT: list[str] = (
    ["SUPNAME", "UDATE"]
    + [m.priority_field for m in P2A_FIELD_MAP if m.priority_field != "SUPNAME"]
)
