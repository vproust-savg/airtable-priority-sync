"""
Populate metadata columns in the API Matching table + add change-tracking comments.

New columns:
- Required (checkbox)
- Field Type (singleSelect: str, float, int, linked_record)
- Max Length (number)
- Lookup Entity, Lookup Code Field, Lookup Desc Field (singleLineText)
- Linked Record Table, Linked Record Match Field (singleLineText)
- Sub-form Pattern (singleSelect: Pattern A, Pattern B, Pattern C)
"""

import os
import time
from datetime import date

import requests
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = "appjwOgR4HsXeGIda"
TABLE_ID = "tblxfbeMh5hnzZj74"
API_URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
COMMENT_URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}

TODAY = date.today().isoformat()


# ═════════════════════════════════════════════════════════════════════════════
# 1. Required — primary key fields only
# ═════════════════════════════════════════════════════════════════════════════

REQUIRED_FIELDS: set[tuple[str, str]] = {
    # Products
    ("LOGPART - SKU", "A>P"),
    ("FNCPART - SKU", "A>P"),
    ("PRDPART - SKU", "A>P"),
    # Vendors
    ("SUPPLIERS - Priority Vendor ID", "A>P"),
    ("FNCSUP - Priority Vendor ID", "A>P"),
    # Customers
    ("CUSTOMERS - Priority Cust. ID", "A>P"),
    ("FNCCUST - Priority Cust. ID", "A>P"),
    # Price Lists
    ("Vendor Prices - Price List Code", "A>P"),
    ("Customer Prices - Price List Code", "A>P"),
}


# ═════════════════════════════════════════════════════════════════════════════
# 2. Field Type — non-default types (default is "str")
# ═════════════════════════════════════════════════════════════════════════════

FIELD_TYPE_OVERRIDES: dict[tuple[str, str], str] = {
    # ── Products A→P (float) ──────────────────────────────────────────────
    ("LOGPART - Base Price", "A>P"): "float",
    ("LOGPART - Standard Cost", "A>P"): "float",
    ("LOGPART - LVL 2 SALE PRICE", "A>P"): "float",
    ("LOGPART - Conversion Ratio", "A>P"): "int",
    # ── Products P→A (float) ──────────────────────────────────────────────
    ("LOGPART - Case Pack", "P>A"): "float",
    ("LOGPART - Product Net Weight Input", "P>A"): "float",
    ("LOGPART - Preferred Vendor", "P>A"): "linked_record",
    # ── FNCPART A→P (float) ──────────────────────────────────────────────
    ("FNCPART - Filemaker Weight", "A>P"): "float",
    ("FNCPART - Pallet TI", "A>P"): "float",
    ("FNCPART - Pallet HI", "A>P"): "float",
    ("FNCPART - Case / Pallet", "A>P"): "float",
    # ── FNCPART P→A (float) ──────────────────────────────────────────────
    ("FNCPART - Pallet TI", "P>A"): "float",
    ("FNCPART - Pallet HI", "P>A"): "float",
    ("FNCPART - Case / Pallet", "P>A"): "float",
    # ── PRDPART A→P (int) ─────────────────────────────────────────────────
    ("PRDPART - Purchase Lead Time", "A>P"): "int",
    ("PRDPART - Shipping Days", "A>P"): "int",
    ("PRDPART - Safety Stock", "A>P"): "int",
    ("PRDPART - Min for Order", "A>P"): "int",
    ("PRDPART - Increment for Order", "A>P"): "int",
    # ── PRDPART P→A (int) ─────────────────────────────────────────────────
    ("PRDPART - Purchase Lead Time", "P>A"): "int",
    ("PRDPART - Shipping Days", "P>A"): "int",
    ("PRDPART - Safety Stock", "P>A"): "int",
    ("PRDPART - Min for Order", "P>A"): "int",
    ("PRDPART - Increment for Order", "P>A"): "int",
    # ── Shelf Lives (int) ─────────────────────────────────────────────────
    ("Shelf Lives - Shelf Life Input", "A>P"): "int",
    ("Shelf Lives - Shelf Life Input", "P>A"): "int",
    # ── Customers A→P (float) ─────────────────────────────────────────────
    ("CUSTOMERS - Credit Limit", "A>P"): "float",
    # ── Customers P→A (float) ─────────────────────────────────────────────
    ("CUSTOMERS - Credit Limit", "P>A"): "float",
    # ── Price Lists A→P (float for prices, int for quantity) ──────────────
    ("Price Lists - LVL 1 SALE PRICE", "A>P"): "float",
    ("Price Lists - LVL 2 SALE PRICE", "A>P"): "float",
    ("Price Lists - LVL 3 SALE PRICE", "A>P"): "float",
    ("Price Lists - EDI Price Quantity", "A>P"): "int",
}


# ═════════════════════════════════════════════════════════════════════════════
# 3. Lookup Configs — (Name, Direction) → (entity, code_field, desc_field)
# ═════════════════════════════════════════════════════════════════════════════

LOOKUP_CONFIGS: dict[tuple[str, str], tuple[str, str, str]] = {
    ("LOGPART - Product Type", "P>A"): ("FAMILY_LOG", "FAMILYNAME", "FAMILYDESC"),
    ("FNCPART - Kelsey_Types/Accounting Family", "P>A"): ("FAMILY_FNC", "FAMILYNAME", "FAMILYDESC"),
    ("CUSTOMERS - Account Executive", "P>A"): ("AGENTS", "AGENTCODE", "AGENTNAME"),
    ("CUSTOMERS - Business Type", "P>A"): ("CTYPE", "CTYPECODE", "CTYPENAME"),
    ("CUSTOMERS - Payment Terms", "P>A"): ("PAY", "PAYCODE", "PAYDES"),
}


# ═════════════════════════════════════════════════════════════════════════════
# 4. Linked Record Configs — (Name, Direction) → (table_id, match_field_id)
# ═════════════════════════════════════════════════════════════════════════════

LINKED_RECORD_CONFIGS: dict[tuple[str, str], tuple[str, str]] = {
    ("LOGPART - Preferred Vendor", "P>A"): ("tblvenpZXbcgGz8Ry", "fldLkVY6ul00KnMJO"),
}


# ═════════════════════════════════════════════════════════════════════════════
# 5. Sub-form Patterns — by Priority Form Name prefix
# ═════════════════════════════════════════════════════════════════════════════

SUBFORM_PATTERNS: dict[str, str] = {
    "SAVR_ALLERGENS_SUBFORM": "Pattern A",
    "CUSTSHIPTEXT_SUBFORM": "Pattern A",
    "CUSTOMERSTEXT_SUBFORM": "Pattern A",
    "SAVR_PARTSHELF_SUBFORM": "Pattern B",
    "PARTINCUSTPLISTS_SUBFORM": "Pattern C",
    "PARTLOCATIONS_SUBFORM": "Pattern C",
}

# Main entity forms — no sub-form pattern
MAIN_ENTITY_FORMS = {
    "LOGPART", "FNCPART", "PRDPART",
    "SUPPLIERS", "FNCSUP",
    "CUSTOMERS", "FNCCUST",
    "PRICELIST",
}


# ═════════════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════════════

def list_all_records() -> list[dict]:
    """Fetch all records from the table (handles pagination)."""
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


def batch_update(updates: list[dict], batch_size: int = 10) -> int:
    """Update records in batches of 10."""
    updated = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i : i + batch_size]
        payload = {"records": batch, "typecast": True}
        resp = requests.patch(API_URL, headers=HEADERS, json=payload)
        if resp.status_code == 200:
            updated += len(batch)
            print(f"  Batch {i // batch_size + 1}: updated {len(batch)} records ({updated}/{len(updates)} total)")
        else:
            print(f"  ERROR batch {i // batch_size + 1}: {resp.status_code} {resp.text}")
        time.sleep(0.25)
    return updated


def add_comment(record_id: str, text: str) -> bool:
    """Add a comment to an Airtable record."""
    url = f"{COMMENT_URL}/{record_id}/comments"
    resp = requests.post(url, headers=HEADERS, json={"text": text})
    if resp.status_code in (200, 201):
        return True
    else:
        print(f"  COMMENT ERROR {record_id}: {resp.status_code} {resp.text}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Fetching all records...")
    records = list_all_records()
    print(f"Found {len(records)} records.\n")

    updates = []
    comment_queue: list[tuple[str, str]] = []  # (record_id, comment_text)

    # Counters
    required_count = 0
    field_type_counts: dict[str, int] = {}
    lookup_count = 0
    linked_count = 0
    subform_count = 0

    for r in records:
        fields = r.get("fields", {})
        name = fields.get("Name", "")
        direction = fields.get("Direction", "")
        priority_form = fields.get("Priority Form Name", "")
        key = (name, direction)

        update_fields: dict = {}
        changes: list[str] = []

        # 1. Required
        is_required = key in REQUIRED_FIELDS
        if is_required:
            update_fields["Required"] = True
            changes.append("Required=true")
            required_count += 1

        # 2. Field Type
        ft = FIELD_TYPE_OVERRIDES.get(key, "str")
        update_fields["Field Type"] = ft
        field_type_counts[ft] = field_type_counts.get(ft, 0) + 1
        if ft != "str":
            changes.append(f"Field Type={ft}")

        # 3. Max Length (none found — leave blank)

        # 4. Lookup Config
        lc = LOOKUP_CONFIGS.get(key)
        if lc:
            update_fields["Lookup Entity"] = lc[0]
            update_fields["Lookup Code Field"] = lc[1]
            update_fields["Lookup Desc Field"] = lc[2]
            changes.append(f"Lookup={lc[0]}({lc[1]}→{lc[2]})")
            lookup_count += 1

        # 5. Linked Record Config
        lr = LINKED_RECORD_CONFIGS.get(key)
        if lr:
            update_fields["Linked Record Table"] = lr[0]
            update_fields["Linked Record Match Field"] = lr[1]
            changes.append(f"LinkedRecord=table:{lr[0]}")
            linked_count += 1

        # 6. Sub-form Pattern
        if priority_form in SUBFORM_PATTERNS:
            pattern = SUBFORM_PATTERNS[priority_form]
            update_fields["Sub-form Pattern"] = pattern
            changes.append(f"Sub-form={pattern}")
            subform_count += 1

        if update_fields:
            updates.append({"id": r["id"], "fields": update_fields})

        # Build comment
        if changes:
            comment = f"{TODAY}: Added metadata — {', '.join(changes)}."
        else:
            comment = f"{TODAY}: Added metadata — Field Type=str (default), no special configs."
        comment_queue.append((r["id"], comment))

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"Records to update: {len(updates)}")
    print(f"  Required fields: {required_count}")
    print(f"\n  Field Types:")
    for ft, c in sorted(field_type_counts.items(), key=lambda x: -x[1]):
        print(f"    {ft}: {c}")
    print(f"\n  Lookups: {lookup_count}")
    print(f"  Linked Records: {linked_count}")
    print(f"  Sub-form Patterns: {subform_count}")
    print()

    # ── Update records ────────────────────────────────────────────────────
    if updates:
        print("=== Updating records ===")
        batch_update(updates)
        print()

    # ── Add comments ──────────────────────────────────────────────────────
    print(f"=== Adding comments to {len(comment_queue)} records ===")
    comment_ok = 0
    comment_err = 0
    for i, (record_id, comment_text) in enumerate(comment_queue):
        if add_comment(record_id, comment_text):
            comment_ok += 1
        else:
            comment_err += 1
        # Rate limit: 5 req/sec → 0.22s between calls
        if (i + 1) % 50 == 0:
            print(f"  Comments: {i + 1}/{len(comment_queue)} ({comment_ok} ok, {comment_err} errors)")
        time.sleep(0.22)

    print(f"\n  Comments: {comment_ok} ok, {comment_err} errors")
    print("\nDone!")
