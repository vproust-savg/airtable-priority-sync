"""
Bidirectional field mapping for Customers All (CUSTOMERS).

Source of truth:
  - Priority interface: tools/6. SG Cust All Priority Interface.txt
  - Airtable fields:   tools/30. Script for Customer All v14.py

P→A is limited — many Airtable fields are formulas or AI-generated (aiText).
Only writable fields are included in P2A_FIELD_MAP.
"""

from __future__ import annotations

from sync.core.models import FieldMapping, LookupConfig

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
        airtable_field_id="fldsnRoMbOuU26Ezc",
        priority_field="CUSTNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
    # ── Customer fields ────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Business Name Output",
        airtable_field_id="fldnfU0xfMgYKlGnn",
        priority_field="CUSTDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Sales Rep Number",
        airtable_field_id="fldd50oTgqvbTzyz1",
        priority_field="AGENTCODE",
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
        airtable_field="Website Output",
        airtable_field_id="fldae1KP8D0nPVWwF",
        priority_field="HOSTNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Zone Code",
        airtable_field_id="fldpF2C594eZpY57n",
        priority_field="ZONECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address Line 1 Output",
        airtable_field_id="fldngFA0BudqaLTDN",
        priority_field="ADDRESS",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address_Line 2 Output",
        airtable_field_id="fldqcujQ7lbKx3tIq",
        priority_field="ADDRESS2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address_City Output",
        airtable_field_id="fldf0T2zIzBJsqk7w",
        priority_field="STATEA",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Full State Name",
        airtable_field_id="fldctnXE8Hazi0efQ",
        priority_field="STATENAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Billing Address_Zip Code",
        airtable_field_id="fldDtmAoDNPudoVm8",
        priority_field="ZIP",
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
        airtable_field="Credit Limit",
        airtable_field_id="fld5MrUHgUTws2LEn",
        priority_field="MAX_CREDIT",
        transform="to_float",
        field_type="float",
    ),
    # SPEC1-5: Custom fields
    FieldMapping(
        airtable_field="Payment Method (1)",
        airtable_field_id="fldpdc7DvNi4yXmrf",
        priority_field="SPEC1",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Are you part of Hilton? (2)",
        airtable_field_id="fld84ysxqcV3cUvSe",
        priority_field="SPEC2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Hilton Inncode (3)",
        airtable_field_id="fldBKs6npT71SjCCz",
        priority_field="SPEC3",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Rekki Output (4)",
        airtable_field_id="flda9oG5ZrfDy0hrQ",
        priority_field="SPEC4",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Carrier Account Number (5)",
        airtable_field_id="fld8A7QdwG2Wo1T1f",
        priority_field="SPEC5",
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
        airtable_field="Country",
        airtable_field_id="fldWuHkO0YY00zgit",
        priority_field="COUNTRYNAME",
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
        airtable_field="Dist. Route Code",
        airtable_field_id="fld07nuAN3KRg69PK",
        priority_field="DISTRLINECODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Approval_Before_Charging (6)",
        airtable_field_id="fld3frlgCUVQG6Tg7",
        priority_field="SPEC6",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Shipment Code",
        airtable_field_id="fldjamRfwSrNJIKMw",
        priority_field="STCODE",
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
        airtable_field="Billing_Legal Name Output",
        airtable_field_id="fldV0OhCRmkBTM0sq",
        priority_field="CUSTDESLONG",
        transform="clean",
        field_type="str",
    ),
]


# ═════════════════════════════════════════════════════════════════════════════
# P→A (Priority → Airtable) — REVERSE DIRECTION
# ═════════════════════════════════════════════════════════════════════════════
# Writable fields — directly mapped from Priority to Airtable.
#
# Three categories:
# 1. Always-overwrite: Priority value replaces Airtable value
# 2. Linked-table lookups: code → description via LookupConfig (Priority superior)
# 3. Write-if-empty (p2a_write_if_empty=True): only populate if Airtable field empty
#
# Skipped (no need to sync): SPEC4 (Rekki), CODE (Curr), TAXCODE
# Deferred (harder): ZONECODE, DISTRLINECODE, STCODE
# Address fields: consolidated into Billing Address Input (in engine, not here)

P2A_FIELD_MAP: list[FieldMapping] = [
    # ── Always-overwrite fields ───────────────────────────────────────────
    FieldMapping(
        airtable_field="Priority Customer Status",
        airtable_field_id="fldLhntR45Memi1na",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Credit Limit",
        airtable_field_id="fld5MrUHgUTws2LEn",
        priority_field="MAX_CREDIT",
        transform="to_float",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Payment Method (1)",
        airtable_field_id="fldpdc7DvNi4yXmrf",
        priority_field="SPEC1",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Are you part of Hilton? (2)",
        airtable_field_id="fld84ysxqcV3cUvSe",
        priority_field="SPEC2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Hilton Inncode (3)",
        airtable_field_id="fldBKs6npT71SjCCz",
        priority_field="SPEC3",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Carrier Account Number (5)",
        airtable_field_id="fld8A7QdwG2Wo1T1f",
        priority_field="SPEC5",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Approval_Before_Charging (6)",
        airtable_field_id="fld3frlgCUVQG6Tg7",
        priority_field="SPEC6",
        transform="clean",
        field_type="str",
    ),
    # ── Linked-table lookups (Priority data is superior — always overwrite) ─
    # NOTE: AGENTS, CTYPE, PAY need API access enabled in Priority.
    # Until enabled, fetch_lookup_table() returns {} and the raw code is written.
    FieldMapping(
        airtable_field="Account Executive",
        airtable_field_id="fldNq5gGxwvcwgiWr",
        priority_field="AGENTCODE",
        transform="priority_lookup",
        field_type="str",
        lookup=LookupConfig(
            entity="AGENTS",
            code_field="AGENTCODE",
            desc_field="AGENTNAME",
        ),
    ),
    FieldMapping(
        airtable_field="Business Type",
        airtable_field_id="fldlwyGJn56BZjU3r",
        priority_field="CTYPECODE",
        transform="priority_lookup",
        field_type="str",
        lookup=LookupConfig(
            entity="CTYPE",
            code_field="CTYPECODE",
            desc_field="CTYPENAME",
        ),
    ),
    FieldMapping(
        airtable_field="Payment Terms",
        airtable_field_id="fldtJbcuxychfBkSs",
        priority_field="PAYCODE",
        transform="priority_lookup",
        field_type="str",
        lookup=LookupConfig(
            entity="PAY",
            code_field="PAYCODE",
            desc_field="PAYDES",
        ),
    ),
    # ── Write-if-empty fields (only populate if Airtable field is empty) ──
    FieldMapping(
        airtable_field="Business Name Input",
        airtable_field_id="fldd61qbZI7L3KCg5",
        priority_field="CUSTDES",
        transform="clean",
        field_type="str",
        p2a_write_if_empty=True,
    ),
    FieldMapping(
        airtable_field="Website Input",
        airtable_field_id="fldNoV5ycIyHJVIfP",
        priority_field="HOSTNAME",
        transform="clean",
        field_type="str",
        p2a_write_if_empty=True,
    ),
    FieldMapping(
        airtable_field="Billing_Legal Name Input",
        airtable_field_id="fldZaL0gBirbtwsB7",
        priority_field="CUSTDESLONG",
        transform="clean",
        field_type="str",
        p2a_write_if_empty=True,
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
        "Credit Application",  # Attachment field → CUSTEXTFILE_SUBFORM
        "Cleaned Delivery Instructions",  # aiText → CUSTSHIPTEXT_SUBFORM
        "Billing_Instructions_Output",  # aiText → CUSTOMERSTEXT_SUBFORM
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
        # Address consolidation target (for write-if-empty check)
        "Billing Address Input",
    ]
)

# ── P→A: Priority $select fields ───────────────────────────────────────────

P2A_PRIORITY_SELECT: list[str] = (
    # NOTE: CUSTOMERS has no UDATE field — P→A always fetches ALL records.
    ["CUSTNAME"]
    + [m.priority_field for m in P2A_FIELD_MAP if m.priority_field != "CUSTNAME"]
    # Address fields for consolidation into Billing Address Input
    + ["ADDRESS", "ADDRESS2", "STATEA", "STATENAME", "ZIP", "COUNTRYNAME"]
)
