"""
Bidirectional field mapping for Vendors All (SUPPLIERS).

Source of truth:
  - Priority interface: tools/1. Vendors All Priority Interface.txt
  - Airtable fields:   tools/20. Script for Vendor All v3 - online.py

P→A is limited — many Airtable fields are formulas or AI-generated (aiText).
Only writable fields are included in P2A_FIELD_MAP.
"""

from __future__ import annotations

from sync.core.models import FieldMapping, LookupConfig

# ── FNCSUP field maps (imported for merged workflow) ─────────────────────────
from sync.workflows.fncsup.field_mapping import (
    A2P_FIELD_MAP as FNCSUP_A2P_FIELD_MAP,
    BANK_A2P_FIELD_MAP as FNCSUP_BANK_A2P_FIELD_MAP,
    BANK_AIRTABLE_FIELDS as FNCSUP_BANK_AIRTABLE_FIELDS,
    BANK_P2A_FIELD_MAP as FNCSUP_BANK_P2A_FIELD_MAP,
    P2A_FIELD_MAP as FNCSUP_P2A_FIELD_MAP,
)


# ═════════════════════════════════════════════════════════════════════════════
# A→P (Airtable → Priority) field mapping — Main SUPPLIERS entity
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
    # ── Vendor fields ────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Company Name",
        airtable_field_id="fld4hIW7hG8eqrf2M",
        priority_field="SUPDES",
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
        airtable_field="Clean Billing Address Line 1",
        airtable_field_id="fldSjjhxPTMtL39kL",
        priority_field="ADDRESS",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Clean Billing Address Line 2",
        airtable_field_id="fld7i7o82A7ZILzpG",
        priority_field="ADDRESS2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Clean Billing Address City",
        airtable_field_id="fldJTRX0PyhPYPKcy",
        priority_field="STATEA",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address State",
        airtable_field_id="fldMTTGAa84xRJH8f",
        priority_field="STATECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Zip Code",
        airtable_field_id="fldFnilj6DXd4K3cA",
        priority_field="ZIP",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Country",
        airtable_field_id="fldKgqa2uowAWLOpq",
        priority_field="COUNTRYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Clean Website",
        airtable_field_id="fldThNxcdcjFctv8s",
        priority_field="HOSTNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Company Email",
        airtable_field_id="fldE1iJVBa1PvJCh0",
        priority_field="EMAIL",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Clean Phone Number",
        airtable_field_id="fld5lrm2NtF52O2Wg",
        priority_field="PHONE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Company Type",
        airtable_field_id="fldJBS8jZ0ZszvIYb",
        priority_field="BUSINESSTYPE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Priority Shipping Code",
        airtable_field_id="fldpEOWvLtkWbDFPi",
        priority_field="STCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Incoterms",
        airtable_field_id="fldxWNAjXoGgLdveN",
        priority_field="IMPTERMNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Vendor Group Code",
        airtable_field_id="fldVyy4yXmaSxR89f",
        priority_field="SUPTYPECODE",
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
        airtable_field="Currency",
        airtable_field_id="fldsFN1BhXxYXusSh",
        priority_field="CODE",
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
# P→A (Priority → Airtable) — REVERSE DIRECTION
# ═════════════════════════════════════════════════════════════════════════════
# Limited to writable Airtable fields only.
# Excluded (formula/aiText): Priority Vendor ID, Company Email,
# Clean Phone Number, Remarks

P2A_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(
        airtable_field="Company Name",
        airtable_field_id="fld4hIW7hG8eqrf2M",
        priority_field="SUPDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Main Buyer",
        airtable_field_id="fldxxPeJbGXQo50Z2",
        priority_field="OWNERLOGIN",
        transform="priority_lookup",
        field_type="str",
        lookup=LookupConfig(
            entity="USERLIST",
            code_field="USERLOGIN",
            desc_field="USERNAME",
        ),
    ),
    FieldMapping(
        airtable_field="Status",
        airtable_field_id="fldBcdMhERTIjDksV",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Line 1",
        airtable_field_id="fldAsnAqhq6jxr4b0",
        priority_field="ADDRESS",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Line 2",
        airtable_field_id="fldaTlABAKWbo3FMd",
        priority_field="ADDRESS2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address City",
        airtable_field_id="fldEZqsxZdSeryMp6",
        priority_field="STATEA",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address State",
        airtable_field_id="fldMTTGAa84xRJH8f",
        priority_field="STATECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Zip Code",
        airtable_field_id="fldFnilj6DXd4K3cA",
        priority_field="ZIP",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Country",
        airtable_field_id="fldKgqa2uowAWLOpq",
        priority_field="COUNTRYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Website",
        airtable_field_id="fldNzCV1WzV6lWd4x",
        priority_field="HOSTNAME",
        transform="clean",
        field_type="str",
        p2a_write_if_empty=True,
    ),
    FieldMapping(
        airtable_field="Shipping Method",
        airtable_field_id="fldW1IO2vKCoHpR2v",
        priority_field="STCODE",
        transform="priority_lookup",
        field_type="multiple_select",
        lookup=LookupConfig(
            entity="SHIPTYPES",
            code_field="STCODE",
            desc_field="STDES",
        ),
    ),
    FieldMapping(
        airtable_field="Incoterms",
        airtable_field_id="fldxWNAjXoGgLdveN",
        priority_field="IMPTERMNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Vendor Group (3)",
        airtable_field_id="flduerqqmwXvpBbVD",
        priority_field="SUPTYPECODE",
        transform="priority_lookup",
        field_type="str",
        lookup=LookupConfig(
            entity="SUPTYPES",
            code_field="SUPTYPECODE",
            desc_field="SUPTYPEDES",
        ),
    ),
    FieldMapping(
        airtable_field="Payment Terms",
        airtable_field_id="fldaJM70LfvtOy2HT",
        priority_field="PAYCODE",
        transform="priority_lookup",
        field_type="str",
        lookup=LookupConfig(
            entity="PAY",
            code_field="PAYCODE",
            desc_field="PAYDES",
        ),
    ),
    FieldMapping(
        airtable_field="Currency",
        airtable_field_id="fldsFN1BhXxYXusSh",
        priority_field="CODE",
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


# ── A→P: Airtable fields to fetch ──────────────────────────────────────────

AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in A2P_FIELD_MAP]
    # ── FNCSUP fields (Account No, FDA Reg, VAT Code, Max Tax Code, etc.) ──
    + [m.airtable_field for m in FNCSUP_A2P_FIELD_MAP
       if m.airtable_field not in {"Priority Vendor ID", "Last Synced to Priority",
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
    + [m.airtable_field for m in FNCSUP_P2A_FIELD_MAP]
    + [m.airtable_field for m in FNCSUP_BANK_P2A_FIELD_MAP]
    + [
        "Priority Vendor ID",
        "Last Synced from Priority",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
)

# ── P→A: Priority $select fields ───────────────────────────────────────────

P2A_PRIORITY_SELECT: list[str] = (
    # NOTE: SUPPLIERS has no UDATE field — P→A always fetches ALL records.
    ["SUPNAME"]
    + [m.priority_field for m in P2A_FIELD_MAP if m.priority_field != "SUPNAME"]
)
