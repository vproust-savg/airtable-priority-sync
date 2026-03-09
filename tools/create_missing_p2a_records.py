"""Create missing P→A records in API Matching table for user review."""

import os
import time

import requests
from datetime import date
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
URL = "https://api.airtable.com/v0/appjwOgR4HsXeGIda/tblxfbeMh5hnzZj74"
HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}
TODAY = date.today().isoformat()


def fetch_max_lengths() -> dict[tuple[str, str], int]:
    """Fetch Max Length from existing A→P records."""
    all_records: list = []
    params: dict = {"pageSize": 100, "filterByFormula": '{Direction} = "A>P"'}
    while True:
        resp = requests.get(URL, headers=HEADERS, params=params)
        data = resp.json()
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
        time.sleep(0.25)

    result: dict[tuple[str, str], int] = {}
    for r in all_records:
        f = r.get("fields", {})
        form = f.get("Priority Form Name", "")
        pfield = f.get("Priority Field Name", "")
        ml = f.get("Max Length")
        if ml:
            result[(form, pfield)] = int(ml)
    return result


# (category, form, priority_field, a2p_airtable_field, field_type, notes)
MISSING_FIELDS = [
    # Products — LOGPART
    ("Products", "LOGPART", "BASEPLPRICE", "Base Price", "float",
     "Formula refs Price Import lookup. No known writable Airtable field."),
    ("Products", "LOGPART", "BASEPLCODE", "Base Price Currency", "str",
     "Formula. No known writable Airtable field."),
    ("Products", "LOGPART", "PRICE", "Standard Cost", "float",
     "Lookup from Price Import. No known writable Airtable field."),
    ("Products", "LOGPART", "WSPLPRICE", "LVL 2 SALE PRICE", "float",
     "Lookup from Price Import. No known writable Airtable field."),
    ("Products", "LOGPART", "CONV", "Conversion Ratio", "int",
     "Formula (hardcoded 1). No known writable Airtable field."),
    ("Products", "LOGPART", "RESERVFLAG", "Allocate Inventory", "str",
     "Formula computed from Accounting Family. No known writable Airtable field."),
    ("Products", "LOGPART", "SPEC14", "Availability Priority Output", "str",
     "Formula computed from availability dates. No known writable Airtable field."),
    ("Products", "LOGPART", "SPEC16", "Vendor SKU Trim", "str",
     "Formula computed from Vendor SKU. No known writable Airtable field."),
    # Products — FNCPART
    ("Products", "FNCPART", "WEIGHT", "Filemaker Weight", "float",
     "Formula in Airtable — 422 on write. Needs alternate writable field."),
    # Products — PRDPART
    ("Products", "PRDPART", "USERLOGIN", "Main Buyer Priority Code", "str",
     "Formula in Airtable. May need lookup to convert code to name."),
    # Vendors — SUPPLIERS
    ("Vendors", "SUPPLIERS", "OWNERLOGIN", "Buyer Output", "str",
     "User confirmed: write to 'Main Buyer' column. Needs USERLIST lookup (code→name)."),
    ("Vendors", "SUPPLIERS", "ADDRESS", "Clean Billing Address Line 1", "str",
     "aiText in Airtable. Suggested writable: 'Billing Address Line 1'."),
    ("Vendors", "SUPPLIERS", "ADDRESS2", "Clean Billing Address Line 2", "str",
     "aiText in Airtable. Suggested writable: 'Billing Address Line 2'."),
    ("Vendors", "SUPPLIERS", "STATEA", "Clean Billing Address City", "str",
     "aiText in Airtable. Suggested writable: 'Billing Address City'."),
    ("Vendors", "SUPPLIERS", "HOSTNAME", "Clean Website", "str",
     "aiText in Airtable. Suggested writable: 'Website'."),
    ("Vendors", "SUPPLIERS", "EMAIL", "Company Email", "str",
     "aiText in Airtable. No known writable equivalent."),
    ("Vendors", "SUPPLIERS", "PHONE", "Clean Phone Number", "str",
     "aiText in Airtable. Suggested writable: 'Phone Number (20)'. "
     "WARNING: Priority autoformats phone numbers — comparison issue."),
    ("Vendors", "SUPPLIERS", "BUSINESSTYPE", "Company Type", "str",
     "Not in P→A. May be writable multipleSelects in Airtable."),
    ("Vendors", "SUPPLIERS", "STCODE", "Priority Shipping Code", "str",
     "Formula in Airtable. May need lookup to convert code to name."),
    ("Vendors", "SUPPLIERS", "SUPTYPECODE", "Vendor Group Code", "str",
     "Formula in Airtable. May need lookup to convert code to name."),
    ("Vendors", "SUPPLIERS", "PAYCODE", "Payment Terms Code", "str",
     "Formula in Airtable. Needs lookup like customers P→A (PAY entity)."),
    # Vendors — FNCSUP
    ("Vendors", "FNCSUP", "ACCNAME", "Priority Vendor Account No", "str",
     "Formula in Airtable. No known writable equivalent."),
    ("Vendors", "FNCSUP", "OWNERLOGIN", "Buyer Output", "str",
     "Formula in Airtable. Same as SUPPLIERS — may share the same writable target."),
    ("Vendors", "FNCSUP", "TAXCODE", "VAT Code", "str",
     "Formula in Airtable. No known writable equivalent."),
    ("Vendors", "FNCSUP", "PAYCODE", "Payment Terms Code", "str",
     "Formula in Airtable. Needs lookup like customers P→A."),
    ("Vendors", "FNCSUP", "WTAXCODE", "Maximum Tax Code", "str",
     "Formula in Airtable. No known writable equivalent."),
    # Customers — CUSTOMERS
    ("Customers", "CUSTOMERS", "COUNTRYNAME", "Country", "str",
     "Not in P→A. Field may be writable in Airtable."),
    ("Customers", "CUSTOMERS", "ZONECODE", "Zone Code", "str",
     "Deferred — needs lookup (DISTRLINES entity, code→name)."),
    ("Customers", "CUSTOMERS", "DISTRLINECODE", "Dist. Route Code", "str",
     "Deferred — needs lookup to convert code to route name."),
    ("Customers", "CUSTOMERS", "STCODE", "Shipment Code", "str",
     "Deferred — needs lookup to convert code to shipment method name."),
    ("Customers", "CUSTOMERS", "SPEC4", "Rekki Output (4)", "str",
     "Previously skipped ('no need to sync'). Review if P→A needed."),
    ("Customers", "CUSTOMERS", "CODE", "Curr", "str",
     "Previously skipped ('no need to sync'). Review if P→A needed."),
    ("Customers", "CUSTOMERS", "TAXCODE", "Tax Code", "str",
     "Previously skipped ('no need to sync'). Review if P→A needed."),
    # Customers — FNCCUST
    ("Customers", "FNCCUST", "PCUSTNAME",
     "Billing Priority Cust. ID (from Billing Cust. Number)", "str",
     "Formula/lookup in Airtable. No known writable equivalent."),
    ("Customers", "FNCCUST", "CTYPECODE", "Cust Group Code", "str",
     "Formula in Airtable. Already covered by CUSTOMERS P→A 'Business Type'?"),
    ("Customers", "FNCCUST", "OWNERLOGIN", "Financial Parameters Assigned", "str",
     "Formula in Airtable. No known writable equivalent."),
    ("Customers", "FNCCUST", "ACCNAME", "Financial Account No", "str",
     "Formula in Airtable. No known writable equivalent."),
    ("Customers", "FNCCUST", "TAXCODE", "Tax Code", "str",
     "Formula in Airtable. No known writable equivalent."),
    ("Customers", "FNCCUST", "IVTYPE", "Invoice Type", "str",
     "Formula in Airtable. No known writable equivalent."),
    ("Customers", "FNCCUST", "PAYCODE", "Payment Terms Code", "str",
     "Formula in Airtable. Already covered by CUSTOMERS P→A 'Payment Terms'?"),
    ("Customers", "FNCCUST", "INTERESTNAME", "Finance Charge Code", "str",
     "Formula in Airtable. No known writable equivalent."),
    ("Customers", "FNCCUST", "CODE", "Curr", "str",
     "Formula in Airtable. No known writable equivalent."),
    ("Customers", "FNCCUST", "PAYMENTLINK", "EDI_Y", "str",
     "Formula in Airtable. No known writable equivalent."),
    ("Customers", "FNCCUST", "CASHNAME", "Pay to Bank Account", "str",
     "Formula in Airtable. No known writable equivalent."),
]


def main():
    max_lengths = fetch_max_lengths()
    print(f"Loaded Max Length for {len(max_lengths)} A→P fields")

    records_to_create = []
    comments = []

    for category, form, pfield, at_field, ftype, notes in MISSING_FIELDS:
        ml = max_lengths.get((form, pfield))

        fields = {
            "Name": f"{form} - {at_field}",
            "Direction": "P>A",
            "Category": category,
            "Priority Form Name": form,
            "Priority Field Name": pfield,
            "Claude Status": "Needs Config",
            "Claude Comments": notes,
            "Transform": "clean",
            "Write Behavior": "always-overwrite",
            "Field Type": ftype,
        }
        if ml:
            fields["Max Length"] = ml

        comment = (
            f"{TODAY}: P→A record created with 'Needs Config' status. "
            f"Needs writable Airtable field name. "
            f"A→P currently reads from '{at_field}' (formula/aiText)."
        )
        records_to_create.append(fields)
        comments.append(comment)

    print(f"Creating {len(records_to_create)} records...")

    created_ids = []
    for i in range(0, len(records_to_create), 10):
        batch = [{"fields": f} for f in records_to_create[i : i + 10]]
        resp = requests.post(URL, headers=HEADERS, json={"records": batch, "typecast": True})
        if resp.status_code == 200:
            new_records = resp.json().get("records", [])
            created_ids.extend([r["id"] for r in new_records])
            print(f"  Batch {i // 10 + 1}/{(len(records_to_create) + 9) // 10}: OK ({len(new_records)} records)")
        else:
            print(f"  Batch {i // 10 + 1}: ERROR {resp.status_code} {resp.text[:200]}")
        time.sleep(0.25)

    print(f"\nCreated {len(created_ids)} records total.")

    print(f"Adding comments to {len(created_ids)} records...")
    ok = 0
    for idx, rid in enumerate(created_ids):
        resp = requests.post(
            f"{URL}/{rid}/comments", headers=HEADERS, json={"text": comments[idx]}
        )
        if resp.status_code in (200, 201):
            ok += 1
        else:
            print(f"  Comment error for {rid}: {resp.status_code}")
        time.sleep(0.22)

    print(f"Comments: {ok}/{len(created_ids)} ok")
    print("Done!")


if __name__ == "__main__":
    main()
