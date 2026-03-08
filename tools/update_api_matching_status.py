"""
Update API Matching table:
1. Set Claude Status = "Approved" on ALL records (Products + Vendors + Customers)
2. Mark excluded A→P-only records with Circle Back Later checkbox + Claude Status = "Pending Approval"
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
# Circle back later records — A→P fields with no P→A counterpart
# ═════════════════════════════════════════════════════════════════════════════

# Reasons keyed by record Name (A>P direction only)
CIRCLE_BACK_REASONS: dict[str, str] = {
    # ── Products ──────────────────────────────────────────────────────────
    "FNCPART - Filemaker Weight": "P→A excluded: AT field is formula (422 error). Needs alternate writable field.",
    "LOGPART - Base Price": "P→A excluded: AT formula/lookup field, read-only.",
    "LOGPART - Base Price Currency": "P→A excluded: AT formula/lookup field, read-only.",
    "LOGPART - Standard Cost": "P→A excluded: AT lookup from Price Import, read-only.",
    "LOGPART - LVL 2 SALE PRICE": "P→A excluded: AT formula/lookup field, read-only.",
    "LOGPART - Availability Priority Output": "P→A excluded: AT formula, read-only. A→P reads value (works), P→A can't write.",
    "LOGPART - Vendor SKU Trim": "P→A excluded: Writable field, but not yet needed P→A.",
    # Product Price Lists (9 records — A→P only, AT price fields are read-only)
    "Price Lists - Lvl 1 Price List Code": "P→A excluded: PARTINCUSTPLISTS_SUBFORM A→P only. AT price fields read-only.",
    "Price Lists - LVL 1 SALE PRICE": "P→A excluded: AT lookup field, read-only.",
    "Price Lists - Lvl 2 Price List Code": "P→A excluded: PARTINCUSTPLISTS_SUBFORM A→P only.",
    "Price Lists - LVL 2 SALE PRICE": "P→A excluded: AT lookup field, read-only.",
    "Price Lists - Lvl 3 Price List Code": "P→A excluded: PARTINCUSTPLISTS_SUBFORM A→P only.",
    "Price Lists - LVL 3 SALE PRICE": "P→A excluded: AT lookup field, read-only.",
    "Price Lists - EDI $": "P→A excluded: PARTINCUSTPLISTS_SUBFORM A→P only.",
    "Price Lists - EDI Price Quantity": "P→A excluded: PARTINCUSTPLISTS_SUBFORM A→P only.",
    "Price Lists - Buy_Sell Unit (Priority)": "P→A excluded: PARTINCUSTPLISTS_SUBFORM A→P only.",
    # Product Bins (2 records — A→P only for now)
    "Bins - EDI Main": "P→A excluded: PARTLOCATIONS_SUBFORM A→P only for now.",
    "Bins - Simplified Bin Location": "P→A excluded: PARTLOCATIONS_SUBFORM A→P only. AT lookup field.",

    # ── Vendors: SUPPLIERS excluded from P→A ──────────────────────────────
    "SUPPLIERS - Priority Vendor ID": "P→A excluded: AT primary key / formula.",
    "SUPPLIERS - Buyer Output": "P→A excluded: AT formula/aiText, read-only.",
    "SUPPLIERS - Clean Billing Address Line 1": "P→A excluded: AT formula/aiText, read-only.",
    "SUPPLIERS - Clean Billing Address Line 2": "P→A excluded: AT formula/aiText, read-only.",
    "SUPPLIERS - Clean Billing Address City": "P→A excluded: AT formula/aiText, read-only.",
    "SUPPLIERS - Clean Website": "P→A excluded: AT formula/aiText, read-only.",
    "SUPPLIERS - Company Email": "P→A excluded: AT formula/aiText, read-only.",
    "SUPPLIERS - Clean Phone Number": "P→A excluded: AT formula/aiText, read-only.",
    "SUPPLIERS - Company Type": "P→A excluded: Not needed P→A.",
    "SUPPLIERS - Priority Shipping Code": "P→A excluded: AT formula, read-only.",
    "SUPPLIERS - Vendor Group Code": "P→A excluded: AT formula, read-only.",
    "SUPPLIERS - Payment Terms Code": "P→A excluded: AT formula, read-only.",

    # ── Vendors: FNCSUP excluded from P→A ─────────────────────────────────
    "FNCSUP - Priority Vendor ID": "P→A excluded: AT formula / primary key.",
    "FNCSUP - Priority Vendor Account No": "P→A excluded: AT formula, read-only.",
    "FNCSUP - Buyer Output": "P→A excluded: AT formula, read-only.",
    "FNCSUP - VAT Code": "P→A excluded: AT formula, read-only.",
    "FNCSUP - Payment Terms Code": "P→A excluded: AT formula, read-only.",
    "FNCSUP - Maximum Tax Code": "P→A excluded: AT formula, read-only.",

    # ── Vendors: Sub-forms (all A→P only) ─────────────────────────────────
    # Vendor Contacts (9)
    "Vendor Contacts - Clean Full Name": "P→A excluded: SUPPERSONNEL_SUBFORM A→P only.",
    "Vendor Contacts - Clean_First Name": "P→A excluded: SUPPERSONNEL_SUBFORM A→P only.",
    "Vendor Contacts - Clean_Last Name": "P→A excluded: SUPPERSONNEL_SUBFORM A→P only.",
    "Vendor Contacts - Contact Status": "P→A excluded: SUPPERSONNEL_SUBFORM A→P only.",
    "Vendor Contacts - Clean Phone Number": "P→A excluded: SUPPERSONNEL_SUBFORM A→P only.",
    "Vendor Contacts - Clean Cell Phone": "P→A excluded: SUPPERSONNEL_SUBFORM A→P only.",
    "Vendor Contacts - Clean Email": "P→A excluded: SUPPERSONNEL_SUBFORM A→P only.",
    "Vendor Contacts - Clean_Job Title": "P→A excluded: SUPPERSONNEL_SUBFORM A→P only.",
    "Vendor Contacts - Buyer Output (from Vendor_ID)": "P→A excluded: SUPPERSONNEL_SUBFORM A→P only.",
    # Vendor Products (4)
    "Vendor Products - SKU Trim (EDI) (from Savory SKU)": "P→A excluded: SUPPART_SUBFORM A→P only.",
    "Vendor Products - Vendor Part Number": "P→A excluded: SUPPART_SUBFORM A→P only.",
    "Vendor Products - Purchase Lead Time (from Savory SKU)": "P→A excluded: SUPPART_SUBFORM A→P only.",
    "Vendor Products - Min for Order (from Savory SKU)": "P→A excluded: SUPPART_SUBFORM A→P only.",
    # Vendor Sites (8)
    "Vendor Sites - W": "P→A excluded: SUPDESTCODES_SUBFORM A→P only.",
    "Vendor Sites - Clean Site Name": "P→A excluded: SUPDESTCODES_SUBFORM A→P only.",
    "Vendor Sites - Clean Warehouse Address Line 1": "P→A excluded: SUPDESTCODES_SUBFORM A→P only.",
    "Vendor Sites - Clean Warehouse Address Line 2": "P→A excluded: SUPDESTCODES_SUBFORM A→P only.",
    "Vendor Sites - Clean Warehouse Address City": "P→A excluded: SUPDESTCODES_SUBFORM A→P only.",
    "Vendor Sites - Warehouse Address State": "P→A excluded: SUPDESTCODES_SUBFORM A→P only.",
    "Vendor Sites - Warehouse Address Zip Code": "P→A excluded: SUPDESTCODES_SUBFORM A→P only.",
    "Vendor Sites - Warehouse Address Country": "P→A excluded: SUPDESTCODES_SUBFORM A→P only.",
    # Vendor Prices (4)
    "Vendor Prices - Price List Code": "P→A excluded: PRICELIST A→P only. Most AT fields are formulas/lookups.",
    "Vendor Prices - SKU Trim (EDI) (from Products)": "P→A excluded: PARTPRICE2_SUBFORM A→P only.",
    "Vendor Prices - Unit Price": "P→A excluded: PARTPRICE2_SUBFORM A→P only.",
    "Vendor Prices - Quantity": "P→A excluded: PARTPRICE2_SUBFORM A→P only.",

    # ── Customers: CUSTOMERS excluded from P→A ───────────────────────────
    "CUSTOMERS - Priority Cust. ID": "P→A excluded: AT primary key.",
    "CUSTOMERS - Business Name Output": "P→A excluded: AT formula/aiText. P→A writes to 'Business Name Input' instead.",
    "CUSTOMERS - Sales Rep Number": "P→A excluded: AT formula. P→A uses 'Account Executive' lookup instead.",
    "CUSTOMERS - Cust Group Code": "P→A excluded: AT formula. P→A uses 'Business Type' lookup instead.",
    "CUSTOMERS - Website Output": "P→A excluded: AT formula/aiText. P→A writes to 'Website Input' instead.",
    "CUSTOMERS - Zone Code": "P→A deferred: Harder to implement (lookup table needed).",
    "CUSTOMERS - Billing Address Line 1 Output": "P→A excluded: AT formula/aiText. P→A consolidates into 'Billing Address Input'.",
    "CUSTOMERS - Billing Address_Line 2 Output": "P→A excluded: AT formula/aiText. P→A consolidates into 'Billing Address Input'.",
    "CUSTOMERS - Billing Address_City Output": "P→A excluded: AT formula/aiText. P→A consolidates into 'Billing Address Input'.",
    "CUSTOMERS - Full State Name": "P→A excluded: P→A consolidates address into 'Billing Address Input'.",
    "CUSTOMERS - Billing Address_Zip Code": "P→A excluded: P→A consolidates address into 'Billing Address Input'.",
    "CUSTOMERS - Payment Terms Code": "P→A excluded: AT formula. P→A uses 'Payment Terms' lookup instead.",
    "CUSTOMERS - Rekki Output (4)": "P→A skipped: No need to sync this field P→A.",
    "CUSTOMERS - Curr": "P→A skipped: No need to sync this field P→A.",
    "CUSTOMERS - Country": "P→A excluded: Not currently mapped P→A.",
    "CUSTOMERS - Tax Code": "P→A skipped: No need to sync this field P→A.",
    "CUSTOMERS - Dist. Route Code": "P→A deferred: Harder to implement.",
    "CUSTOMERS - Shipment Code": "P→A deferred: Harder to implement.",
    "CUSTOMERS - Billing_Legal Name Output": "P→A excluded: AT formula/aiText. P→A writes to 'Billing_Legal Name Input' instead.",

    # ── Customers: FNCCUST excluded from P→A ──────────────────────────────
    "FNCCUST - Priority Cust. ID": "P→A excluded: AT primary key.",
    "FNCCUST - Billing Priority Cust. ID (from Billing Cust. Number)": "P→A excluded: AT formula, read-only.",
    "FNCCUST - Cust Group Code": "P→A excluded: AT formula, read-only.",
    "FNCCUST - Financial Parameters Assigned": "P→A excluded: AT formula, read-only.",
    "FNCCUST - Financial Account No": "P→A excluded: AT formula, read-only.",
    "FNCCUST - Tax Code": "P→A excluded: AT formula, read-only.",
    "FNCCUST - Invoice Type": "P→A excluded: AT formula, read-only.",
    "FNCCUST - Payment Terms Code": "P→A excluded: AT formula, read-only.",
    "FNCCUST - Finance Charge Code": "P→A excluded: AT formula, read-only.",
    "FNCCUST - Curr": "P→A excluded: AT formula, read-only.",
    "FNCCUST - EDI_Y": "P→A excluded: AT formula, read-only.",
    "FNCCUST - Pay to Bank Account": "P→A excluded: AT formula, read-only.",

    # ── Customers: A→P-only sub-forms ─────────────────────────────────────
    # Customer Contacts A→P only (4 fields without P→A counterpart)
    "Customer Contacts - Consent to Receive Emails Output": "P→A excluded: A→P only, no writable counterpart.",
    "Customer Contacts - Clean Full Name": "P→A excluded: Match field only, not synced as data P→A.",
    "Customer Contacts - Linkedin": "P→A excluded: A→P only.",
    "Customer Contacts - Sign Up Priority": "P→A excluded: A→P only.",
    # Customer Sites A→P only (10 fields without P→A counterpart)
    "Customer Sites - Site Id": "P→A excluded: Match field only.",
    "Customer Sites - Address Line 1 Output": "P→A excluded: P→A consolidates address into 'Address Input'.",
    "Customer Sites - Address Line 2 Output": "P→A excluded: P→A consolidates address into 'Address Input'.",
    "Customer Sites - City Output": "P→A excluded: P→A consolidates address into 'Address Input'.",
    "Customer Sites - State": "P→A excluded: P→A consolidates address into 'Address Input'.",
    "Customer Sites - Zip": "P→A excluded: P→A consolidates address into 'Address Input'.",
    "Customer Sites - EDI_USA": "P→A excluded: P→A consolidates address into 'Address Input'.",
    "Customer Sites - Primary receiver (from Customers)": "P→A excluded: A→P only.",
    "Customer Sites - Zone Code": "P→A excluded: Covered by P→A 'Shipping Zone' with zone_lookup transform.",
    "Customer Sites - Dist. Route Code": "P→A excluded: A→P only.",
    "Customer Sites - EDI_Carrier Code": "P→A excluded: Covered by P→A 'Main Delivery Method' with shipper_lookup.",
    "Customer Sites - Carrier Account Number (5) (from Customers)": "P→A excluded: A→P only.",
    # Special Prices (4)
    "Special Prices - SKU": "P→A excluded: CUSTPARTPRICE_SUBFORM A→P only.",
    "Special Prices - Formatted Start Date": "P→A excluded: CUSTPARTPRICE_SUBFORM A→P only.",
    "Special Prices - Expiration Date": "P→A excluded: CUSTPARTPRICE_SUBFORM A→P only.",
    "Special Prices - Special Price": "P→A excluded: CUSTPARTPRICE_SUBFORM A→P only.",
    # Price List (1)
    "Price List - Price List Code": "P→A excluded: CUSTPLIST_SUBFORM A→P only.",
    # Delivery Days (3)
    "Delivery Days - Days of Business": "P→A excluded: CUSTWEEKDAY_SUBFORM A→P only.",
    "Delivery Days - Deliver After": "P→A excluded: CUSTWEEKDAY_SUBFORM A→P only.",
    "Delivery Days - Deliver Before": "P→A excluded: CUSTWEEKDAY_SUBFORM A→P only.",
    # Credit Application (1)
    "Credit Application - Credit Application": "P→A excluded: CUSTEXTFILE_SUBFORM A→P only.",
    # Shipment Remarks (1)
    "Shipment Remarks - Cleaned Delivery Instructions": "P→A excluded: CUSTSHIPTEXT_SUBFORM A→P only.",
    # Internal Remarks (1)
    "Internal Remarks - Billing_Instructions_Output": "P→A excluded: CUSTOMERSTEXT_SUBFORM A→P only.",
    # Customer Prices (1)
    "Customer Prices - Price List Code": "P→A excluded: PRICELIST A→P only.",
}

CIRCLE_BACK_NAMES = list(CIRCLE_BACK_REASONS.keys())


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


if __name__ == "__main__":
    print("Fetching all records...")
    records = list_all_records()
    print(f"Found {len(records)} records.\n")

    # Step 1: Set Claude Status = "Approved" on ALL records
    print("Step 1: Setting Claude Status = 'Approved' on all records...")
    all_updates = [
        {"id": r["id"], "fields": {"Claude Status": "Approved"}}
        for r in records
    ]
    batch_update(all_updates)

    # Step 2: Mark circle-back records
    print(f"\nStep 2: Marking up to {len(CIRCLE_BACK_NAMES)} 'Circle Back Later' records...")
    circle_back_updates = []
    for r in records:
        name = r.get("fields", {}).get("Name", "")
        direction = r.get("fields", {}).get("Direction", "")
        # Only flag A>P records (these are the ones without P>A counterparts)
        if name in CIRCLE_BACK_REASONS and direction == "A>P":
            existing_comments = r.get("fields", {}).get("Claude Comments", "")
            reason = CIRCLE_BACK_REASONS[name]
            # Append reason if not already there
            if reason not in existing_comments:
                new_comments = f"{existing_comments}\n{reason}".strip() if existing_comments else reason
            else:
                new_comments = existing_comments

            circle_back_updates.append({
                "id": r["id"],
                "fields": {
                    "Circle Back Later": True,
                    "Claude Status": "Pending Approval",
                    "Claude Comments": new_comments,
                },
            })

    if circle_back_updates:
        print(f"Found {len(circle_back_updates)} records to flag:")
        for u in circle_back_updates:
            rec = next(r for r in records if r["id"] == u["id"])
            print(f"  - [{rec['fields'].get('Category', '?')}] {rec['fields']['Name']}")
        batch_update(circle_back_updates)
    else:
        print("WARNING: No circle-back records found!")

    print("\nDone!")
