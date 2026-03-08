"""
Update API Matching table with:
1. Claude Status: rename "Pending Approval" → "Deferred"
2. Airtable Source Table: derived from Name prefix
3. Transform: derived from field_mapping.py files
4. Write Behavior: derived from direction + write mode
"""

import os
import time

import requests
from dotenv import load_dotenv

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
# 1. Airtable Source Table — derived from Name prefix
# ═════════════════════════════════════════════════════════════════════════════

# Order matters: more specific prefixes first
SOURCE_TABLE_MAP = [
    ("Vendor Prices -", "Vendor Price List"),
    ("LOGPART -", "Products"),
    ("FNCPART -", "Products"),
    ("PRDPART -", "Products"),
    ("Allergens -", "Products"),
    ("Shelf Lives -", "Shelf Lives"),
    ("Price Lists -", "Products"),
    ("Bins -", "Products"),
    ("SUPPLIERS -", "Vendors"),
    ("FNCSUP -", "Vendors"),
    ("Bank Accounts -", "Vendors"),
    ("Vendor Contacts -", "Vendor Contacts"),
    ("Vendor Products -", "Vendor Products"),
    ("Vendor Sites -", "Vendor Sites"),
    ("CUSTOMERS -", "Customers"),
    ("FNCCUST -", "Customers"),
    ("Customer Contacts -", "Customer Contacts 2025"),
    ("Customer Sites -", "Customer Sites"),
    ("Special Prices -", "Special Cust. Prices"),
    ("Price List -", "Customers"),
    ("Delivery Days -", "Customers"),
    ("Credit Application -", "Customers"),
    ("Shipment Remarks -", "Customers"),
    ("Internal Remarks -", "Customers"),
    ("Customer Prices -", "Customer Price List"),
]


def get_source_table(name: str) -> str:
    for prefix, table in SOURCE_TABLE_MAP:
        if name.startswith(prefix):
            return table
    return ""


# ═════════════════════════════════════════════════════════════════════════════
# 2. Transform — default is "clean", exceptions keyed by (Name, Direction)
# ═════════════════════════════════════════════════════════════════════════════

TRANSFORM_OVERRIDES: dict[tuple[str, str], str] = {
    # ── Products A→P ─────────────────────────────────────────────────────
    ("LOGPART - Base Price", "A>P"): "format_price",
    ("LOGPART - Standard Cost", "A>P"): "format_price",
    ("LOGPART - LVL 2 SALE PRICE", "A>P"): "format_price",
    ("LOGPART - Conversion Ratio", "A>P"): "to_int",
    # ── Products P→A ─────────────────────────────────────────────────────
    ("LOGPART - Case Pack", "P>A"): "to_float",
    ("LOGPART - Product Net Weight Input", "P>A"): "to_float",
    ("LOGPART - Product Type", "P>A"): "priority_lookup",
    ("LOGPART - Preferred Vendor", "P>A"): "linked_record",
    # ── FNCPART A→P ──────────────────────────────────────────────────────
    ("FNCPART - Filemaker Weight", "A>P"): "to_float",
    ("FNCPART - Pallet TI", "A>P"): "to_float",
    ("FNCPART - Pallet HI", "A>P"): "to_float",
    ("FNCPART - Case / Pallet", "A>P"): "to_float",
    # ── FNCPART P→A ──────────────────────────────────────────────────────
    ("FNCPART - Kelsey_Types/Accounting Family", "P>A"): "priority_lookup",
    ("FNCPART - Pallet TI", "P>A"): "to_float",
    ("FNCPART - Pallet HI", "P>A"): "to_float",
    ("FNCPART - Case / Pallet", "P>A"): "to_float",
    # ── PRDPART A→P ──────────────────────────────────────────────────────
    ("PRDPART - Purchase Lead Time", "A>P"): "to_int",
    ("PRDPART - Shipping Days", "A>P"): "to_int",
    ("PRDPART - Safety Stock", "A>P"): "to_int",
    ("PRDPART - Min for Order", "A>P"): "to_int",
    ("PRDPART - Increment for Order", "A>P"): "to_int",
    # ── PRDPART P→A ──────────────────────────────────────────────────────
    ("PRDPART - Purchase Lead Time", "P>A"): "to_int",
    ("PRDPART - Shipping Days", "P>A"): "to_int",
    ("PRDPART - Safety Stock", "P>A"): "to_int",
    ("PRDPART - Min for Order", "P>A"): "to_int",
    ("PRDPART - Increment for Order", "P>A"): "to_int",
    # ── Shelf Lives ──────────────────────────────────────────────────────
    ("Shelf Lives - Shelf Life Input", "A>P"): "to_int",
    ("Shelf Lives - Shelf Life Input", "P>A"): "to_int",
    # ── Price Lists A→P ──────────────────────────────────────────────────
    ("Price Lists - LVL 1 SALE PRICE", "A>P"): "format_price",
    ("Price Lists - LVL 2 SALE PRICE", "A>P"): "format_price",
    ("Price Lists - LVL 3 SALE PRICE", "A>P"): "format_price",
    ("Price Lists - EDI Price Quantity", "A>P"): "to_int",
    # ── Customers A→P ────────────────────────────────────────────────────
    ("CUSTOMERS - Credit Limit", "A>P"): "to_float",
    # ── Customers P→A ────────────────────────────────────────────────────
    ("CUSTOMERS - Credit Limit", "P>A"): "to_float",
    ("CUSTOMERS - Account Executive", "P>A"): "priority_lookup",
    ("CUSTOMERS - Business Type", "P>A"): "priority_lookup",
    ("CUSTOMERS - Payment Terms", "P>A"): "priority_lookup",
    # ── Customer Contacts P→A — flag fields ──────────────────────────────
    ("Customer Contacts - Main Contact", "P>A"): "priority_yn",
    ("Customer Contacts - Marketing", "P>A"): "priority_yn",
    ("Customer Contacts - Price Quote", "P>A"): "priority_yn",
    ("Customer Contacts - Sales Order", "P>A"): "priority_yn",
    ("Customer Contacts - Shipment", "P>A"): "priority_yn",
    ("Customer Contacts - Invoice", "P>A"): "priority_yn",
    ("Customer Contacts - Cust. Statement", "P>A"): "priority_yn",
    ("Customer Contacts - Outgoing Voucher", "P>A"): "priority_yn",
    # ── Customer Sites P→A ───────────────────────────────────────────────
    ("Customer Sites - Main", "P>A"): "priority_yn",
    ("Customer Sites - Address Remarks Input", "P>A"): "strip_html",
    ("Customer Sites - Shipping Zone", "P>A"): "zone_lookup",
    ("Customer Sites - Main Delivery Method", "P>A"): "shipper_lookup",
    # ── Delivery Days A→P ────────────────────────────────────────────────
    ("Delivery Days - Days of Business", "A>P"): "day_to_priority_int",
    ("Delivery Days - Deliver After", "A>P"): "format_time_24h",
    ("Delivery Days - Deliver Before", "A>P"): "format_time_24h",
    # ── Remarks A→P ──────────────────────────────────────────────────────
    ("Shipment Remarks - Cleaned Delivery Instructions", "A>P"): "strip_html",
    ("Internal Remarks - Billing_Instructions_Output", "A>P"): "strip_html",
}


def get_transform(name: str, direction: str) -> str:
    """Get transform for a record. Default: clean."""
    # Check exact match first
    override = TRANSFORM_OVERRIDES.get((name, direction))
    if override:
        return override

    # All Allergens P→A use priority_yn
    if name.startswith("Allergens -") and direction == "P>A":
        return "priority_yn"

    return "clean"


# ═════════════════════════════════════════════════════════════════════════════
# 3. Write Behavior — derived from direction + field-specific overrides
# ═════════════════════════════════════════════════════════════════════════════

# A→P create-only fields
A2P_CREATE_ONLY = {
    "LOGPART - Buy_Sell Unit (Priority)",
}

# P→A create-only fields
P2A_CREATE_ONLY = {
    "LOGPART - Product Title Priority Input",
}

# P→A write-if-empty fields
P2A_WRITE_IF_EMPTY = {
    # CUSTOMERS
    "CUSTOMERS - Business Name Input",
    "CUSTOMERS - Website Input",
    "CUSTOMERS - Billing_Legal Name Input",
    # Customer Contacts P→A — ALL 15 fields are write-if-empty
    "Customer Contacts - First Name Input",
    "Customer Contacts - Last Name Input",
    "Customer Contacts - Phone Number Input",
    "Customer Contacts - Cell Phone",
    "Customer Contacts - Email Input",
    "Customer Contacts - Position Input",
    "Customer Contacts - Status",
    "Customer Contacts - Main Contact",
    "Customer Contacts - Marketing",
    "Customer Contacts - Price Quote",
    "Customer Contacts - Sales Order",
    "Customer Contacts - Shipment",
    "Customer Contacts - Invoice",
    "Customer Contacts - Cust. Statement",
    "Customer Contacts - Outgoing Voucher",
    # Customer Sites P→A — all except Main (which is always-overwrite)
    "Customer Sites - Ship To Name Input",
    "Customer Sites - Address Remarks Input",
    "Customer Sites - Phone Input",
    "Customer Sites - Shipping Zone",
    "Customer Sites - Main Delivery Method",
}


def get_write_behavior(name: str, direction: str) -> str:
    """Get write behavior for a record."""
    if direction == "A>P":
        if name in A2P_CREATE_ONLY:
            return "create-only"
        return ""  # blank for standard A→P
    else:  # P>A
        if name in P2A_CREATE_ONLY:
            return "create-only"
        if name in P2A_WRITE_IF_EMPTY:
            return "write-if-empty"
        return "always-overwrite"


# ═════════════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════════════

def list_all_records() -> list[dict]:
    """Fetch all records from the table (handles pagination)."""
    all_records = []
    params = {"pageSize": 100}
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


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Fetching all records...")
    records = list_all_records()
    print(f"Found {len(records)} records.\n")

    updates = []
    no_source = []
    status_changes = 0

    for r in records:
        fields = r.get("fields", {})
        name = fields.get("Name", "")
        direction = fields.get("Direction", "")
        old_status = fields.get("Claude Status", "")

        update_fields = {}

        # 1. Claude Status: "Pending Approval" → "Deferred"
        if old_status == "Pending Approval":
            update_fields["Claude Status"] = "Deferred"
            status_changes += 1

        # 2. Airtable Source Table
        source_table = get_source_table(name)
        if source_table:
            update_fields["Airtable Source Table"] = source_table
        else:
            no_source.append(name)

        # 3. Transform
        transform = get_transform(name, direction)
        update_fields["Transform"] = transform

        # 4. Write Behavior
        write_behavior = get_write_behavior(name, direction)
        if write_behavior:
            update_fields["Write Behavior"] = write_behavior

        if update_fields:
            updates.append({"id": r["id"], "fields": update_fields})

    # Summary
    if no_source:
        print(f"WARNING: {len(no_source)} records with no source table match:")
        for n in no_source:
            print(f"  - {n}")
        print()

    print(f"Records to update: {len(updates)}")
    print(f"  Claude Status changes: {status_changes}")
    print(f"  Source Table populated: {len(updates) - len(no_source)}")

    # Count transforms
    transform_counts: dict[str, int] = {}
    write_counts: dict[str, int] = {}
    for u in updates:
        t = u["fields"].get("Transform", "")
        if t:
            transform_counts[t] = transform_counts.get(t, 0) + 1
        w = u["fields"].get("Write Behavior", "")
        if w:
            write_counts[w] = write_counts.get(w, 0) + 1

    print("\n  Transforms:")
    for t, c in sorted(transform_counts.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")

    print("\n  Write Behaviors:")
    for w, c in sorted(write_counts.items(), key=lambda x: -x[1]):
        print(f"    {w}: {c}")
    print()

    if updates:
        batch_update(updates)

    print("\nDone!")
