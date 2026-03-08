"""
Populate Max Length column in the API Matching table from Priority OData metadata.

Queries Priority's GetMetadataFor endpoint for each entity, extracts MaxLength
for string fields, and updates the API Matching table + adds comments.
"""

import os
import time
import xml.etree.ElementTree as ET
from datetime import date

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Priority API ─────────────────────────────────────────────────────────────
PRIORITY_USER = os.getenv("PRIORITY_USER")
PRIORITY_PASS = os.getenv("PRIORITY_PASS")
PRIORITY_API_URL = os.getenv("PRIORITY_API_URL")

# ── Airtable API ─────────────────────────────────────────────────────────────
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = "appjwOgR4HsXeGIda"
TABLE_ID = "tblxfbeMh5hnzZj74"
AIRTABLE_URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}

TODAY = date.today().isoformat()
NS = {"edm": "http://docs.oasis-open.org/odata/ns/edm"}

# ── Map API Matching "Priority Form Name" → Priority entity for metadata query ──
# Some forms are sub-forms that come from a parent entity's metadata response.
# We map each form name to (parent_entity_to_query, entity_type_name_in_response).
FORM_TO_ENTITY: dict[str, tuple[str, str]] = {
    # Products
    "LOGPART": ("LOGPART", "LOGPART"),
    "FNCPART": ("FNCPART", "FNCPART"),
    "PRDPART": ("PRDPART", "PRDPART"),
    # Product sub-forms — both nice names and raw _SUBFORM names
    "Allergens": ("LOGPART", "SAVR_ALLERGENS"),
    "SAVR_ALLERGENS_SUBFORM": ("LOGPART", "SAVR_ALLERGENS"),
    "Shelf Lives": ("LOGPART", "SAVR_PARTSHELF"),
    "SAVR_PARTSHELF_SUBFORM": ("LOGPART", "SAVR_PARTSHELF"),
    "Price Lists": ("LOGPART", "PARTINCUSTPLISTS"),
    "PARTINCUSTPLISTS_SUBFORM": ("LOGPART", "PARTINCUSTPLISTS"),
    "Bins": ("LOGPART", "PARTLOCATIONS"),
    "PARTLOCATIONS_SUBFORM": ("LOGPART", "PARTLOCATIONS"),
    # Vendors
    "SUPPLIERS": ("SUPPLIERS", "SUPPLIERS"),
    "FNCSUP": ("FNCSUP", "FNCSUP"),
    "Vendor Contacts": ("SUPPLIERS", "SUPPERSONNEL"),
    "SUPPERSONNEL_SUBFORM": ("SUPPLIERS", "SUPPERSONNEL"),
    "Vendor Sites": ("SUPPLIERS", "SUPPLIERSA"),
    "SUPDESTCODES_SUBFORM": ("SUPPLIERS", "SUPDESTCODES"),
    "Bank Accounts": ("FNCSUP", "ACCOUNTBANK"),
    "ACCOUNTBANK_SUBFORM": ("FNCSUP", "ACCOUNTBANK"),
    "Vendor Products": ("SUPPLIERS", "SUPPART"),
    "SUPPART_SUBFORM": ("SUPPLIERS", "SUPPART"),
    "Vendor Prices": ("PRICELIST", "PARTPRICE2"),
    "PARTPRICE2_SUBFORM": ("PRICELIST", "PARTPRICE2"),
    # Customers
    "CUSTOMERS": ("CUSTOMERS", "CUSTOMERS"),
    "FNCCUST": ("FNCCUST", "FNCCUST"),
    "Customer Contacts": ("CUSTOMERS", "CUSTPERSONNEL"),
    "CUSTPERSONNEL_SUBFORM": ("CUSTOMERS", "CUSTPERSONNEL"),
    "Customer Sites": ("CUSTOMERS", "CUSTOMERSA"),
    "CUSTDESTS_SUBFORM": ("CUSTOMERS", "CUSTDESTS"),
    "Delivery Days": ("CUSTOMERS", "CUSTWEEKDAY"),
    "CUSTWEEKDAY_SUBFORM": ("CUSTOMERS", "CUSTWEEKDAY"),
    "Shipment Remarks": ("CUSTOMERS", "CUSTSHIPTEXT"),
    "CUSTSHIPTEXT_SUBFORM": ("CUSTOMERS", "CUSTSHIPTEXT"),
    "Internal Remarks": ("CUSTOMERS", "CUSTOMERSTEXT"),
    "CUSTOMERSTEXT_SUBFORM": ("CUSTOMERS", "CUSTOMERSTEXT"),
    "Credit Application": ("CUSTOMERS", "CUSTOMERS"),
    "CUSTEXTFILE_SUBFORM": ("CUSTOMERS", "CUSTEXTFILE"),
    "Special Prices": ("CUSTOMERS", "CUSTPARTPRICE"),
    "CUSTPARTPRICE_SUBFORM": ("CUSTOMERS", "CUSTPARTPRICE"),
    "Customer Prices": ("PRICELIST", "PRICELIST"),
    "CUSTPLIST_SUBFORM": ("CUSTOMERS", "CUSTPLIST"),
    # Price Lists
    "Price List": ("PRICELIST", "PRICELIST"),
    "PRICELIST": ("PRICELIST", "PRICELIST"),
}


# ═════════════════════════════════════════════════════════════════════════════
# 1. Fetch Priority metadata
# ═════════════════════════════════════════════════════════════════════════════

def fetch_priority_metadata() -> dict[str, dict[str, int | None]]:
    """
    Query Priority GetMetadataFor for each parent entity.
    Returns: {entity_type_name: {field_name: max_length}} for all entity types.
    """
    session = requests.Session()
    session.auth = (PRIORITY_USER, PRIORITY_PASS)
    session.headers.update({
        "Content-Type": "application/json",
        "IEEE754Compatible": "true",
    })

    # Collect unique parent entities to query
    parents = set(parent for parent, _ in FORM_TO_ENTITY.values())
    all_metadata: dict[str, dict[str, int | None]] = {}

    for parent in sorted(parents):
        print(f"  Querying GetMetadataFor(entity='{parent}')...")
        try:
            resp = session.get(
                f"{PRIORITY_API_URL}GetMetadataFor(entity='{parent}')",
                timeout=60,
            )
            if resp.status_code != 200:
                print(f"    ERROR: {resp.status_code}")
                continue

            root = ET.fromstring(resp.content)

            for entity_type in root.findall(".//edm:EntityType", NS):
                etype_name = entity_type.get("Name")
                fields: dict[str, int | None] = {}

                for prop in entity_type.findall("edm:Property", NS):
                    pname = prop.get("Name")
                    ptype = prop.get("Type", "")
                    max_len = prop.get("MaxLength")

                    if max_len:
                        fields[pname] = int(max_len)
                    elif ptype in ("Edm.Decimal", "Edm.Int64"):
                        # Numeric fields — no MaxLength but we can note precision
                        precision = prop.get("Precision")
                        if precision:
                            fields[pname] = int(precision)
                    # Skip fields with no length info

                all_metadata[etype_name] = fields

            print(f"    Found {sum(1 for et in root.findall('.//edm:EntityType', NS))} entity types")

        except requests.exceptions.Timeout:
            print(f"    TIMEOUT for {parent}")
        except Exception as e:
            print(f"    ERROR: {e}")

        time.sleep(1)  # Rate limit

    return all_metadata


# ═════════════════════════════════════════════════════════════════════════════
# 2. Fetch API Matching records
# ═════════════════════════════════════════════════════════════════════════════

def fetch_api_matching_records() -> list[dict]:
    """Fetch all records from the API Matching table."""
    all_records = []
    params: dict = {"pageSize": 100}
    while True:
        resp = requests.get(AIRTABLE_URL, headers=AIRTABLE_HEADERS, params=params)
        data = resp.json()
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
        time.sleep(0.25)
    return all_records


# ═════════════════════════════════════════════════════════════════════════════
# 3. Map and update
# ═════════════════════════════════════════════════════════════════════════════

def add_comment(record_id: str, text: str) -> bool:
    """Add a comment to an Airtable record."""
    url = f"{AIRTABLE_URL}/{record_id}/comments"
    resp = requests.post(url, headers=AIRTABLE_HEADERS, json={"text": text})
    return resp.status_code in (200, 201)


def batch_update(updates: list[dict], batch_size: int = 10) -> int:
    """Update records in batches of 10."""
    updated = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i : i + batch_size]
        payload = {"records": batch, "typecast": True}
        resp = requests.patch(AIRTABLE_URL, headers=AIRTABLE_HEADERS, json=payload)
        if resp.status_code == 200:
            updated += len(batch)
        else:
            print(f"  ERROR batch {i // batch_size + 1}: {resp.status_code} {resp.text}")
        time.sleep(0.25)
    return updated


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("1. Fetching Priority metadata...")
    metadata = fetch_priority_metadata()
    print(f"   Total entity types with metadata: {len(metadata)}\n")

    print("2. Fetching API Matching records...")
    records = fetch_api_matching_records()
    print(f"   Found {len(records)} records.\n")

    print("3. Mapping Priority field lengths to API Matching records...")
    updates = []
    comment_queue: list[tuple[str, str]] = []
    matched = 0
    unmatched = 0
    already_set = 0

    for r in records:
        fields = r.get("fields", {})
        form_name = fields.get("Priority Form Name", "")
        priority_field = fields.get("Priority Field Name", "")

        # Skip if Max Length already set
        if fields.get("Max Length") is not None:
            already_set += 1
            continue

        if not form_name or not priority_field:
            continue

        # Resolve which entity type to look up
        mapping = FORM_TO_ENTITY.get(form_name)
        if not mapping:
            unmatched += 1
            continue

        _, entity_type = mapping
        entity_meta = metadata.get(entity_type, {})

        max_len = entity_meta.get(priority_field)
        if max_len is not None:
            updates.append({
                "id": r["id"],
                "fields": {"Max Length": max_len},
            })
            comment_queue.append((
                r["id"],
                f"{TODAY}: Max Length set to {max_len} (from Priority {entity_type} metadata).",
            ))
            matched += 1
        else:
            unmatched += 1

    print(f"   Matched: {matched}")
    print(f"   Unmatched (no metadata): {unmatched}")
    print(f"   Already set: {already_set}")
    print()

    # ── Update records ────────────────────────────────────────────────────
    if updates:
        print(f"4. Updating {len(updates)} records...")
        updated = batch_update(updates)
        print(f"   Updated: {updated}\n")

    # ── Add comments ──────────────────────────────────────────────────────
    if comment_queue:
        print(f"5. Adding comments to {len(comment_queue)} records...")
        ok = 0
        err = 0
        for i, (rid, text) in enumerate(comment_queue):
            if add_comment(rid, text):
                ok += 1
            else:
                err += 1
            if (i + 1) % 50 == 0:
                print(f"   Comments: {i + 1}/{len(comment_queue)} ({ok} ok, {err} errors)")
            time.sleep(0.22)
        print(f"   Comments: {ok} ok, {err} errors\n")

    print("Done!")
