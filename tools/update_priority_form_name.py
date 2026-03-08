"""
Populate 'Priority Form Name' column and clean form name references from Claude Comments.
"""

import os
import re
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
# Name prefix → Priority Form Name mapping
# ═════════════════════════════════════════════════════════════════════════════

# Order matters: more specific prefixes first
PREFIX_MAP = [
    ("Vendor Prices - Price List Code", "PRICELIST"),
    ("Vendor Prices -", "PARTPRICE2_SUBFORM"),
    ("Customer Prices -", "PRICELIST"),
    ("LOGPART -", "LOGPART"),
    ("FNCPART -", "FNCPART"),
    ("PRDPART -", "PRDPART"),
    ("Allergens -", "SAVR_ALLERGENS_SUBFORM"),
    ("Shelf Lives -", "SAVR_PARTSHELF_SUBFORM"),
    ("Price Lists -", "PARTINCUSTPLISTS_SUBFORM"),
    ("Bins -", "PARTLOCATIONS_SUBFORM"),
    ("SUPPLIERS -", "SUPPLIERS"),
    ("FNCSUP -", "FNCSUP"),
    ("Bank Accounts -", "ACCOUNTBANK_SUBFORM"),
    ("Vendor Contacts -", "SUPPERSONNEL_SUBFORM"),
    ("Vendor Products -", "SUPPART_SUBFORM"),
    ("Vendor Sites -", "SUPDESTCODES_SUBFORM"),
    ("CUSTOMERS -", "CUSTOMERS"),
    ("FNCCUST -", "FNCCUST"),
    ("Customer Contacts -", "CUSTPERSONNEL_SUBFORM"),
    ("Customer Sites -", "CUSTDESTS_SUBFORM"),
    ("Special Prices -", "CUSTPARTPRICE_SUBFORM"),
    ("Price List -", "CUSTPLIST_SUBFORM"),
    ("Delivery Days -", "CUSTWEEKDAY_SUBFORM"),
    ("Credit Application -", "CUSTEXTFILE_SUBFORM"),
    ("Shipment Remarks -", "CUSTSHIPTEXT_SUBFORM"),
    ("Internal Remarks -", "CUSTOMERSTEXT_SUBFORM"),
]

# All form names to strip from Claude Comments
FORM_NAMES = [
    "SAVR_ALLERGENS_SUBFORM",
    "SAVR_PARTSHELF_SUBFORM",
    "PARTINCUSTPLISTS_SUBFORM",
    "PARTLOCATIONS_SUBFORM",
    "SUPPERSONNEL_SUBFORM",
    "SUPPART_SUBFORM",
    "SUPDESTCODES_SUBFORM",
    "ACCOUNTBANK_SUBFORM",
    "CUSTPERSONNEL_SUBFORM",
    "CUSTDESTS_SUBFORM",
    "CUSTPARTPRICE_SUBFORM",
    "CUSTPLIST_SUBFORM",
    "CUSTWEEKDAY_SUBFORM",
    "CUSTEXTFILE_SUBFORM",
    "CUSTSHIPTEXT_SUBFORM",
    "CUSTOMERSTEXT_SUBFORM",
    "PARTPRICE2_SUBFORM",
    "PRICELIST",
    "ACCOUNTBANK_SUBFORM",
]


def get_form_name(name: str) -> str:
    """Map record Name to Priority Form Name."""
    for prefix, form_name in PREFIX_MAP:
        if name == prefix.rstrip(" -") or name.startswith(prefix):
            return form_name
    return ""


def clean_comment(comment: str) -> str:
    """Remove form name references from Claude Comments."""
    if not comment:
        return comment

    # Build regex pattern: form name + optional extra words + period
    # e.g., "PRICELIST parent entity." or "SAVR_ALLERGENS_SUBFORM."
    form_pattern = "|".join(re.escape(f) for f in sorted(set(FORM_NAMES), key=len, reverse=True))

    # Strip leading form name references (start of comment or start of line)
    # Pattern: FORM_NAME followed by optional non-period chars, then period, then space
    cleaned = re.sub(
        rf"^(?:{form_pattern})\b[^.\n]*\.?\s*",
        "",
        comment,
    )

    # Strip inline form name references (mid-sentence)
    # e.g., "P→A excluded: PARTINCUSTPLISTS_SUBFORM A→P only." → "P→A excluded: A→P only."
    cleaned = re.sub(
        rf"(?:{form_pattern})\s+",
        "",
        cleaned,
    )

    # Clean up any double spaces or leading/trailing whitespace
    cleaned = re.sub(r"  +", " ", cleaned).strip()

    # Remove empty lines
    lines = [line.strip() for line in cleaned.split("\n") if line.strip()]
    return "\n".join(lines)


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

    updates = []
    unmatched = []

    for r in records:
        fields = r.get("fields", {})
        name = fields.get("Name", "")
        old_comment = fields.get("Claude Comments", "")

        form_name = get_form_name(name)
        new_comment = clean_comment(old_comment)

        if not form_name:
            unmatched.append(name)

        update_fields = {}
        if form_name:
            update_fields["Priority Form Name"] = form_name
        if new_comment != old_comment:
            update_fields["Claude Comments"] = new_comment

        if update_fields:
            updates.append({"id": r["id"], "fields": update_fields})

    if unmatched:
        print(f"WARNING: {len(unmatched)} records with no form name match:")
        for n in unmatched:
            print(f"  - {n}")
        print()

    print(f"Records to update: {len(updates)}")
    comment_changes = sum(1 for u in updates if "Claude Comments" in u["fields"])
    form_changes = sum(1 for u in updates if "Priority Form Name" in u["fields"])
    print(f"  Priority Form Name: {form_changes}")
    print(f"  Claude Comments cleaned: {comment_changes}")
    print()

    if updates:
        batch_update(updates)

    print("\nDone!")
