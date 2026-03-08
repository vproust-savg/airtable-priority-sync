"""
Clean Claude Comments of redundant data + convert 4 fields to singleSelect.

1. Strip transform/write-behavior references from Claude Comments
   (now covered by dedicated Transform and Write Behavior columns).
2. Convert Priority Form Name, Priority Field Name, Airtable Source Table,
   Transform from singleLineText → singleSelect via Airtable Fields API.
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
META_URL = f"https://api.airtable.com/v0/meta/bases/{BASE_ID}/tables/{TABLE_ID}/fields"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}

# Fields to convert to singleSelect
FIELDS_TO_CONVERT = {
    "Priority Form Name": "fldxSq6PGdDsYl5cs",
    "Priority Field Name": "fldHnoIbRWkeHFryK",
    "Airtable Source Table": "fldoueig6syhKp3Sa",
    "Transform": "fldxSvtH9g0WFmxwz",
}


# ═════════════════════════════════════════════════════════════════════════════
# Comment cleaning
# ═════════════════════════════════════════════════════════════════════════════

def clean_comment(comment: str) -> str:
    """Remove redundant transform/write-behavior references from Claude Comments."""
    if not comment:
        return comment

    # 1. Transform references: "Transform: <word>." or "Transform: <word> (<explanation>)."
    comment = re.sub(
        r"Transform:\s+\S+\s*(?:\([^)]*\))?\.\s*",
        "",
        comment,
    )

    # 2. P→A transform references: "P→A transform: <word>." or with parens
    comment = re.sub(
        r"P→A transform:\s+\S+\s*(?:\([^)]*\))?\.\s*",
        "",
        comment,
    )

    # 3. Write behavior standalone references
    comment = re.sub(r"Always-overwrite\.\s*", "", comment)
    comment = re.sub(r"Write-if-empty\.\s*", "", comment)

    # 4. create_only references (with following explanation sentence)
    comment = re.sub(
        r"create_only=True\.\s*(?:Cannot be changed via PATCH\.\s*|Only set when creating new AT records\.\s*)?",
        "",
        comment,
    )

    # Clean up whitespace
    comment = re.sub(r"  +", " ", comment).strip()
    lines = [line.strip() for line in comment.split("\n") if line.strip()]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════════════

def list_all_records() -> list[dict]:
    """Fetch all records (handles pagination)."""
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


def convert_field_to_single_select(field_name: str, field_id: str, choices: list[str]) -> bool:
    """Convert a singleLineText field to singleSelect via Airtable Fields API."""
    url = f"{META_URL}/{field_id}"
    payload = {
        "type": "singleSelect",
        "options": {
            "choices": [{"name": c} for c in sorted(choices)],
        },
    }
    resp = requests.patch(url, headers=HEADERS, json=payload)
    if resp.status_code == 200:
        print(f"  Converted '{field_name}' to singleSelect ({len(choices)} options)")
        return True
    else:
        print(f"  ERROR converting '{field_name}': {resp.status_code} {resp.text}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Fetching all records...")
    records = list_all_records()
    print(f"Found {len(records)} records.\n")

    # ── Task 1: Clean comments ───────────────────────────────────────────
    print("=== Task 1: Clean Claude Comments ===")
    updates = []
    for r in records:
        fields = r.get("fields", {})
        old_comment = fields.get("Claude Comments", "")
        new_comment = clean_comment(old_comment)
        if new_comment != old_comment:
            updates.append({"id": r["id"], "fields": {"Claude Comments": new_comment}})

    print(f"Comments to clean: {len(updates)}")
    if updates:
        batch_update(updates)
    print()

    # ── Task 2: Collect unique values for field conversion ───────────────
    print("=== Task 2: Convert fields to singleSelect ===")
    unique_values: dict[str, set[str]] = {name: set() for name in FIELDS_TO_CONVERT}

    for r in records:
        fields = r.get("fields", {})
        for field_name in FIELDS_TO_CONVERT:
            val = fields.get(field_name, "")
            if val:
                unique_values[field_name].add(val)

    for field_name, values in unique_values.items():
        print(f"  {field_name}: {len(values)} unique values")

    print()

    # Convert each field
    for field_name, field_id in FIELDS_TO_CONVERT.items():
        choices = unique_values[field_name]
        if choices:
            convert_field_to_single_select(field_name, field_id, list(choices))
            time.sleep(0.5)  # Be gentle with the API
        else:
            print(f"  SKIP '{field_name}': no values found")

    print("\nDone!")
