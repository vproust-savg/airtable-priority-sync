"""
Validate API Matching table against actual field_mapping.py code.

Detects drift between documentation (Airtable) and runtime code (Python).
Reports:
- Fields in code but not in the API Matching table
- Fields in table but not in code
- Mismatches (different transform, field type, required, etc.)
"""

import os
import sys
import time

import requests
from dotenv import load_dotenv

# Add project root to path so we can import sync modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sync.core.models import FieldMapping

load_dotenv()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = "appjwOgR4HsXeGIda"
TABLE_ID = "tblxfbeMh5hnzZj74"
API_URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}


# ═════════════════════════════════════════════════════════════════════════════
# 1. Load all field mappings from code
# ═════════════════════════════════════════════════════════════════════════════

def load_code_mappings() -> dict[tuple[str, str, str], FieldMapping]:
    """
    Load all FieldMapping objects from all workflow field_mapping.py files.

    Returns dict keyed by (airtable_field_name, direction, priority_form_prefix).
    Direction is "A>P" or "P>A".
    """
    mappings: dict[tuple[str, str, str], FieldMapping] = {}

    # ── Products (LOGPART) ───────────────────────────────────────────────
    from sync.workflows.products.field_mapping import (
        PRODUCT_FIELD_MAP,
        P2A_FIELD_MAP as PRODUCTS_P2A,
    )
    for m in PRODUCT_FIELD_MAP:
        mappings[(m.airtable_field, "A>P", "LOGPART")] = m
    for m in PRODUCTS_P2A:
        mappings[(m.airtable_field, "P>A", "LOGPART")] = m

    # ── FNCPART ──────────────────────────────────────────────────────────
    from sync.workflows.fncpart.field_mapping import (
        A2P_FIELD_MAP as FNCPART_A2P,
        P2A_FIELD_MAP as FNCPART_P2A,
    )
    for m in FNCPART_A2P:
        mappings[(m.airtable_field, "A>P", "FNCPART")] = m
    for m in FNCPART_P2A:
        mappings[(m.airtable_field, "P>A", "FNCPART")] = m

    # ── PRDPART ──────────────────────────────────────────────────────────
    from sync.workflows.prdpart.field_mapping import (
        A2P_FIELD_MAP as PRDPART_A2P,
        P2A_FIELD_MAP as PRDPART_P2A,
    )
    for m in PRDPART_A2P:
        mappings[(m.airtable_field, "A>P", "PRDPART")] = m
    for m in PRDPART_P2A:
        mappings[(m.airtable_field, "P>A", "PRDPART")] = m

    # ── Vendors (SUPPLIERS) ──────────────────────────────────────────────
    from sync.workflows.vendors.field_mapping import (
        A2P_FIELD_MAP as VENDORS_A2P,
        P2A_FIELD_MAP as VENDORS_P2A,
    )
    for m in VENDORS_A2P:
        mappings[(m.airtable_field, "A>P", "SUPPLIERS")] = m
    for m in VENDORS_P2A:
        mappings[(m.airtable_field, "P>A", "SUPPLIERS")] = m

    # ── FNCSUP ───────────────────────────────────────────────────────────
    from sync.workflows.fncsup.field_mapping import (
        A2P_FIELD_MAP as FNCSUP_A2P,
        P2A_FIELD_MAP as FNCSUP_P2A,
    )
    for m in FNCSUP_A2P:
        mappings[(m.airtable_field, "A>P", "FNCSUP")] = m
    for m in FNCSUP_P2A:
        mappings[(m.airtable_field, "P>A", "FNCSUP")] = m

    # ── Customers (CUSTOMERS) ────────────────────────────────────────────
    from sync.workflows.customers.field_mapping import (
        A2P_FIELD_MAP as CUSTOMERS_A2P,
        P2A_FIELD_MAP as CUSTOMERS_P2A,
    )
    for m in CUSTOMERS_A2P:
        mappings[(m.airtable_field, "A>P", "CUSTOMERS")] = m
    for m in CUSTOMERS_P2A:
        mappings[(m.airtable_field, "P>A", "CUSTOMERS")] = m

    # ── FNCCUST ──────────────────────────────────────────────────────────
    from sync.workflows.fnccust.field_mapping import (
        A2P_FIELD_MAP as FNCCUST_A2P,
        P2A_FIELD_MAP as FNCCUST_P2A,
    )
    for m in FNCCUST_A2P:
        mappings[(m.airtable_field, "A>P", "FNCCUST")] = m
    for m in FNCCUST_P2A:
        mappings[(m.airtable_field, "P>A", "FNCCUST")] = m

    # ── Vendor Prices ────────────────────────────────────────────────────
    from sync.workflows.vendor_prices.field_mapping import (
        A2P_FIELD_MAP as VP_A2P,
        P2A_FIELD_MAP as VP_P2A,
    )
    for m in VP_A2P:
        mappings[(m.airtable_field, "A>P", "PRICELIST")] = m
    for m in VP_P2A:
        mappings[(m.airtable_field, "P>A", "PRICELIST")] = m

    # ── Customer Prices ──────────────────────────────────────────────────
    from sync.workflows.customer_prices.field_mapping import (
        A2P_FIELD_MAP as CP_A2P,
        P2A_FIELD_MAP as CP_P2A,
    )
    for m in CP_A2P:
        mappings[(m.airtable_field, "A>P", "PRICELIST")] = m
    for m in CP_P2A:
        mappings[(m.airtable_field, "P>A", "PRICELIST")] = m

    return mappings


# ═════════════════════════════════════════════════════════════════════════════
# 2. Load all records from API Matching table
# ═════════════════════════════════════════════════════════════════════════════

def load_table_records() -> list[dict]:
    """Fetch all records from the API Matching table."""
    all_records = []
    params: dict = {"pageSize": 100}
    while True:
        resp = requests.get(API_URL, headers=HEADERS, params=params)
        data = resp.json()
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
        time.sleep(0.25)
    return all_records


# ═════════════════════════════════════════════════════════════════════════════
# 3. Build comparable key from table record
# ═════════════════════════════════════════════════════════════════════════════

def record_to_key(fields: dict) -> tuple[str, str, str] | None:
    """
    Extract (airtable_field_name, direction, priority_form) from a table record.
    Returns None if essential fields are missing.
    """
    name = fields.get("Name", "")
    direction = fields.get("Direction", "")

    if not name or not direction:
        return None

    # Name format: "FORM - Airtable Field Name"
    # e.g., "LOGPART - Brand" → form="LOGPART", airtable_field="Brand"
    if " - " not in name:
        return None

    form, airtable_field = name.split(" - ", 1)
    return (airtable_field, direction, form)


def code_key_to_name(key: tuple[str, str, str]) -> str:
    """Convert a code key back to the API Matching Name format."""
    airtable_field, direction, form = key
    return f"{form} - {airtable_field}"


# ═════════════════════════════════════════════════════════════════════════════
# 4. Compare and report
# ═════════════════════════════════════════════════════════════════════════════

TRANSFORM_MAP = {
    "clean": "clean",
    "format_price": "format_price",
    "to_int": "to_int",
    "to_float": "to_float",
    "priority_yn": "priority_yn",
    "priority_lookup": "priority_lookup",
    "linked_record": "linked_record",
}


def compare(
    code_mappings: dict[tuple[str, str, str], FieldMapping],
    table_records: list[dict],
) -> tuple[list[str], list[str], list[str], list[str]]:
    """
    Compare code mappings against table records.
    Returns (in_code_not_table, in_table_not_code, mismatches, subform_only).
    """
    # Build table lookup
    table_lookup: dict[tuple[str, str, str], dict] = {}
    for r in table_records:
        fields = r.get("fields", {})
        key = record_to_key(fields)
        if key:
            table_lookup[key] = fields

    in_code_not_table: list[str] = []
    in_table_not_code: list[str] = []
    mismatches: list[str] = []

    # Check code → table
    for key, mapping in sorted(code_mappings.items()):
        name = code_key_to_name(key)
        direction = key[1]

        if key not in table_lookup:
            in_code_not_table.append(f"{name} ({direction})")
            continue

        # Compare fields
        table_fields = table_lookup[key]
        record_mismatches = []

        # Transform
        table_transform = table_fields.get("Transform", "")
        if table_transform and table_transform != mapping.transform:
            record_mismatches.append(
                f"Transform: table={table_transform}, code={mapping.transform}"
            )

        # Field Type
        table_ft = table_fields.get("Field Type", "str")
        if table_ft and table_ft != mapping.field_type:
            record_mismatches.append(
                f"Field Type: table={table_ft}, code={mapping.field_type}"
            )

        # Required
        table_required = bool(table_fields.get("Required", False))
        if table_required != mapping.required:
            record_mismatches.append(
                f"Required: table={table_required}, code={mapping.required}"
            )

        # Lookup configs
        table_lookup_entity = table_fields.get("Lookup Entity", "")
        if mapping.lookup:
            if table_lookup_entity != mapping.lookup.entity:
                record_mismatches.append(
                    f"Lookup Entity: table={table_lookup_entity}, code={mapping.lookup.entity}"
                )
            table_code_field = table_fields.get("Lookup Code Field", "")
            if table_code_field != mapping.lookup.code_field:
                record_mismatches.append(
                    f"Lookup Code Field: table={table_code_field}, code={mapping.lookup.code_field}"
                )
            table_desc_field = table_fields.get("Lookup Desc Field", "")
            if table_desc_field != mapping.lookup.desc_field:
                record_mismatches.append(
                    f"Lookup Desc Field: table={table_desc_field}, code={mapping.lookup.desc_field}"
                )
        elif table_lookup_entity:
            record_mismatches.append(
                f"Lookup Entity: table={table_lookup_entity}, code=None"
            )

        # Linked Record configs
        table_lr_table = table_fields.get("Linked Record Table", "")
        if mapping.linked_record:
            if table_lr_table != mapping.linked_record.table_id:
                record_mismatches.append(
                    f"Linked Record Table: table={table_lr_table}, code={mapping.linked_record.table_id}"
                )
            table_lr_field = table_fields.get("Linked Record Match Field", "")
            if table_lr_field != mapping.linked_record.match_field_id:
                record_mismatches.append(
                    f"Linked Record Match Field: table={table_lr_field}, code={mapping.linked_record.match_field_id}"
                )
        elif table_lr_table:
            record_mismatches.append(
                f"Linked Record Table: table={table_lr_table}, code=None"
            )

        # Write Behavior — normalize naming differences
        # Table uses: "always-overwrite", "write-if-empty", "create-only"
        # Code uses: (default=always-overwrite), "p2a_write_if_empty", "create_only"
        WRITE_BEHAVIOR_NORMALIZE = {
            "always-overwrite": "",       # table explicit = code default
            "write-if-empty": "p2a_write_if_empty",
            "create-only": "create_only",
        }
        table_wb_raw = table_fields.get("Write Behavior", "")
        table_wb = WRITE_BEHAVIOR_NORMALIZE.get(table_wb_raw, table_wb_raw)
        code_wb = ""
        if mapping.create_only:
            code_wb = "create_only"
        elif mapping.p2a_write_if_empty:
            code_wb = "p2a_write_if_empty"
        if table_wb != code_wb:
            record_mismatches.append(
                f"Write Behavior: table={table_wb_raw or '(blank)'}, code={code_wb or '(default)'}"
            )

        if record_mismatches:
            mismatches.append(
                f"{name} ({direction}): " + "; ".join(record_mismatches)
            )

    # Check table → code (fields in table but not in code)
    # Sub-form and related table records are expected to not be in FieldMapping lists
    # (they're handled by separate sub-form/contact/site sync logic)
    SUBFORM_FORMS = {
        "Allergens", "Shelf Lives", "Price Lists", "Bins",
        "Delivery Days", "Shipment Remarks", "Internal Remarks",
        "Credit Application", "Special Prices",
        "Customer Contacts", "Customer Sites",
        "Vendor Contacts", "Vendor Sites", "Vendor Products",
        "Bank Accounts", "Price List",
    }
    in_table_not_code_main: list[str] = []
    in_table_not_code_subform: list[str] = []

    for key in sorted(table_lookup.keys()):
        if key not in code_mappings:
            name = code_key_to_name(key)
            direction = key[1]
            form = key[2]  # The form prefix
            # Check Claude Status — skip if "Not Needed"
            table_fields = table_lookup[key]
            status = table_fields.get("Claude Status", "")
            if status == "Not Needed":
                continue
            if form in SUBFORM_FORMS:
                in_table_not_code_subform.append(f"{name} ({direction})")
            else:
                in_table_not_code_main.append(f"{name} ({direction})")

    in_table_not_code = in_table_not_code_main  # Only report main entity gaps

    return in_code_not_table, in_table_not_code, mismatches, in_table_not_code_subform


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Loading field mappings from code...")
    code_mappings = load_code_mappings()
    print(f"  Found {len(code_mappings)} field mappings in code.\n")

    print("Fetching API Matching table records...")
    table_records = load_table_records()
    print(f"  Found {len(table_records)} records in table.\n")

    in_code_not_table, in_table_not_code, mismatches, in_table_not_code_subform = compare(
        code_mappings, table_records
    )

    # ── Report ─────────────────────────────────────────────────────────
    print("=" * 70)
    print("VALIDATION REPORT")
    print("=" * 70)

    # Info: sub-form/related table records (expected, not issues)
    if in_table_not_code_subform:
        print(f"ℹ️  Sub-form/related table records (not in main FieldMapping lists): {len(in_table_not_code_subform)}")
        print("   (These are handled by separate sync logic — not a problem.)\n")

    if not in_code_not_table and not in_table_not_code and not mismatches:
        print("  ✅ Everything matches! No drift detected.\n")
    else:
        if in_code_not_table:
            print(f"\n⚠️  IN CODE BUT NOT IN TABLE ({len(in_code_not_table)}):")
            for item in in_code_not_table:
                print(f"  - {item}")

        if in_table_not_code:
            print(f"\n⚠️  IN TABLE BUT NOT IN CODE — MAIN ENTITIES ({len(in_table_not_code)}):")
            for item in in_table_not_code:
                print(f"  - {item}")

        if mismatches:
            print(f"\n❌ MISMATCHES ({len(mismatches)}):")
            for item in mismatches:
                print(f"  - {item}")

        total = len(in_code_not_table) + len(in_table_not_code) + len(mismatches)
        print(f"\nTotal issues: {total}")

    print()
