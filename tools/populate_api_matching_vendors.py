"""
Populate the API Matching table in Airtable with all Vendor workflow field mappings.
Uses batch creates (10 records per request) for efficiency.

Covers: SUPPLIERS, FNCSUP, Bank Accounts, Vendor Contacts, Vendor Products,
        Vendor Sites, Vendor Price Lists.
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

# Both bases have identical field IDs (test base was duplicated from production)


def rec(name, direction, at_field, at_id, priority_field, comment=""):
    """Build a single record dict."""
    return {
        "fields": {
            "Name": name,
            "Direction": direction,
            "Category": "Vendors",
            "Production Airtable Field Name": at_field,
            "API Test Airtable Field Name": at_field,
            "Production Airtable Field Id": at_id,
            "API Test Airtable Field Id": at_id,
            "Priority Field Name": priority_field,
            "Claude Comments": comment,
            "Victor Status": "Pending Approval",
        }
    }


# ═════════════════════════════════════════════════════════════════════════════
# SUPPLIERS A→P (20 fields)
# ═════════════════════════════════════════════════════════════════════════════

SUPPLIERS_A2P = [
    rec("SUPPLIERS - Priority Vendor ID", "A>P", "Priority Vendor ID", "fldLkVY6ul00KnMJO", "SUPNAME", "Primary key. Required."),
    rec("SUPPLIERS - Company Name", "A>P", "Company Name", "fld4hIW7hG8eqrf2M", "SUPDES", ""),
    rec("SUPPLIERS - Buyer Output", "A>P", "Buyer Output", "fldIKLZXHbvsnfhAh", "OWNERLOGIN", "Formula/aiText in AT."),
    rec("SUPPLIERS - Status", "A>P", "Status", "fldBcdMhERTIjDksV", "STATDES", ""),
    rec("SUPPLIERS - Clean Billing Address Line 1", "A>P", "Clean Billing Address Line 1", "fldSjjhxPTMtL39kL", "ADDRESS", "Formula/aiText in AT."),
    rec("SUPPLIERS - Clean Billing Address Line 2", "A>P", "Clean Billing Address Line 2", "fld7i7o82A7ZILzpG", "ADDRESS2", "Formula/aiText in AT."),
    rec("SUPPLIERS - Clean Billing Address City", "A>P", "Clean Billing Address City", "fldJTRX0PyhPYPKcy", "STATEA", "Formula/aiText in AT."),
    rec("SUPPLIERS - Billing Address State", "A>P", "Billing Address State", "fldMTTGAa84xRJH8f", "STATECODE", ""),
    rec("SUPPLIERS - Billing Address Zip Code", "A>P", "Billing Address Zip Code", "fldFnilj6DXd4K3cA", "ZIP", ""),
    rec("SUPPLIERS - Billing Address Country", "A>P", "Billing Address Country", "fldKgqa2uowAWLOpq", "COUNTRYNAME", ""),
    rec("SUPPLIERS - Clean Website", "A>P", "Clean Website", "fldThNxcdcjFctv8s", "HOSTNAME", "Formula/aiText in AT."),
    rec("SUPPLIERS - Company Email", "A>P", "Company Email", "fldE1iJVBa1PvJCh0", "EMAIL", "Formula/aiText in AT."),
    rec("SUPPLIERS - Clean Phone Number", "A>P", "Clean Phone Number", "fld5lrm2NtF52O2Wg", "PHONE", "Formula/aiText in AT."),
    rec("SUPPLIERS - Company Type", "A>P", "Company Type", "fldJBS8jZ0ZszvIYb", "BUSINESSTYPE", ""),
    rec("SUPPLIERS - Priority Shipping Code", "A>P", "Priority Shipping Code", "fldpEOWvLtkWbDFPi", "STCODE", "Formula in AT."),
    rec("SUPPLIERS - Incoterms", "A>P", "Incoterms", "fldxWNAjXoGgLdveN", "IMPTERMNAME", ""),
    rec("SUPPLIERS - Vendor Group Code", "A>P", "Vendor Group Code", "fldVyy4yXmaSxR89f", "SUPTYPECODE", "Formula in AT."),
    rec("SUPPLIERS - Payment Terms Code", "A>P", "Payment Terms Code", "fldxMIBEb84iKyoGy", "PAYCODE", "Formula in AT."),
    rec("SUPPLIERS - Currency", "A>P", "Currency", "fldsFN1BhXxYXusSh", "CODE", ""),
    rec("SUPPLIERS - Federal Tax ID (16)", "A>P", "Federal Tax ID (16)", "fldpT73EmgEmdDPMN", "VATNUM", ""),
]

# ═════════════════════════════════════════════════════════════════════════════
# SUPPLIERS P→A (8 fields)
# ═════════════════════════════════════════════════════════════════════════════

SUPPLIERS_P2A = [
    rec("SUPPLIERS - Company Name", "P>A", "Company Name", "fld4hIW7hG8eqrf2M", "SUPDES", ""),
    rec("SUPPLIERS - Status", "P>A", "Status", "fldBcdMhERTIjDksV", "STATDES", ""),
    rec("SUPPLIERS - Billing Address State", "P>A", "Billing Address State", "fldMTTGAa84xRJH8f", "STATECODE", ""),
    rec("SUPPLIERS - Billing Address Zip Code", "P>A", "Billing Address Zip Code", "fldFnilj6DXd4K3cA", "ZIP", ""),
    rec("SUPPLIERS - Billing Address Country", "P>A", "Billing Address Country", "fldKgqa2uowAWLOpq", "COUNTRYNAME", ""),
    rec("SUPPLIERS - Incoterms", "P>A", "Incoterms", "fldxWNAjXoGgLdveN", "IMPTERMNAME", ""),
    rec("SUPPLIERS - Currency", "P>A", "Currency", "fldsFN1BhXxYXusSh", "CODE", ""),
    rec("SUPPLIERS - Federal Tax ID (16)", "P>A", "Federal Tax ID (16)", "fldpT73EmgEmdDPMN", "VATNUM", ""),
]

# ═════════════════════════════════════════════════════════════════════════════
# FNCSUP A→P (10 fields, includes primary key)
# ═════════════════════════════════════════════════════════════════════════════

FNCSUP_A2P = [
    rec("FNCSUP - Priority Vendor ID", "A>P", "Priority Vendor ID", "fldLkVY6ul00KnMJO", "SUPNAME", "Primary key. Links to SUPPLIERS."),
    rec("FNCSUP - Currency", "A>P", "Currency", "fldsFN1BhXxYXusSh", "CODE", ""),
    rec("FNCSUP - Priority Vendor Account No", "A>P", "Priority Vendor Account No", "fldWCR3iNSo8gq4GK", "ACCNAME", "Formula in AT."),
    rec("FNCSUP - Buyer Output", "A>P", "Buyer Output", "fldIKLZXHbvsnfhAh", "OWNERLOGIN", "Formula/aiText in AT."),
    rec("FNCSUP - Status", "A>P", "Status", "fldBcdMhERTIjDksV", "STATDES", ""),
    rec("FNCSUP - FDA Registration #", "A>P", "FDA Registration #", "fldExdRs6V7EKW09y", "COMPNUM", ""),
    rec("FNCSUP - Federal Tax ID (16)", "A>P", "Federal Tax ID (16)", "fldpT73EmgEmdDPMN", "VATNUM", ""),
    rec("FNCSUP - VAT Code", "A>P", "VAT Code", "fldcfZ6ahghdgieNT", "TAXCODE", "Formula in AT."),
    rec("FNCSUP - Payment Terms Code", "A>P", "Payment Terms Code", "fldxMIBEb84iKyoGy", "PAYCODE", "Formula in AT."),
    rec("FNCSUP - Maximum Tax Code", "A>P", "Maximum Tax Code", "fldUEbr759dY5mzyg", "WTAXCODE", "Formula in AT."),
]

# ═════════════════════════════════════════════════════════════════════════════
# FNCSUP P→A (4 fields)
# ═════════════════════════════════════════════════════════════════════════════

FNCSUP_P2A = [
    rec("FNCSUP - Currency", "P>A", "Currency", "fldsFN1BhXxYXusSh", "CODE", ""),
    rec("FNCSUP - Status", "P>A", "Status", "fldBcdMhERTIjDksV", "STATDES", ""),
    rec("FNCSUP - FDA Registration #", "P>A", "FDA Registration #", "fldExdRs6V7EKW09y", "COMPNUM", ""),
    rec("FNCSUP - Federal Tax ID (16)", "P>A", "Federal Tax ID (16)", "fldpT73EmgEmdDPMN", "VATNUM", ""),
]

# ═════════════════════════════════════════════════════════════════════════════
# Bank Accounts A→P (4 fields — ACCOUNTBANK_SUBFORM on FNCSUP)
# ═════════════════════════════════════════════════════════════════════════════

BANK_A2P = [
    rec("Bank Accounts - Bank Account", "A>P", "Bank Account", "fldLNWU2m9KWgx309", "PAYACCOUNT", "ACCOUNTBANK_SUBFORM. All writable."),
    rec("Bank Accounts - Bank Code", "A>P", "Bank Code", "fldHPr2Sh7pRxpc85", "BANKCODE", "ACCOUNTBANK_SUBFORM."),
    rec("Bank Accounts - Routing Number", "A>P", "Routing Number", "fldfmJD3BQe4VrEdZ", "BRANCH", "ACCOUNTBANK_SUBFORM."),
    rec("Bank Accounts - IBAN", "A>P", "IBAN", "fldBkaMQOdH1QfrU1", "IBAN", "ACCOUNTBANK_SUBFORM."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Bank Accounts P→A (4 fields — same fields, all writable)
# ═════════════════════════════════════════════════════════════════════════════

BANK_P2A = [
    rec("Bank Accounts - Bank Account", "P>A", "Bank Account", "fldLNWU2m9KWgx309", "PAYACCOUNT", "ACCOUNTBANK_SUBFORM. All writable."),
    rec("Bank Accounts - Bank Code", "P>A", "Bank Code", "fldHPr2Sh7pRxpc85", "BANKCODE", "ACCOUNTBANK_SUBFORM."),
    rec("Bank Accounts - Routing Number", "P>A", "Routing Number", "fldfmJD3BQe4VrEdZ", "BRANCH", "ACCOUNTBANK_SUBFORM."),
    rec("Bank Accounts - IBAN", "P>A", "IBAN", "fldBkaMQOdH1QfrU1", "IBAN", "ACCOUNTBANK_SUBFORM."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Vendor Contacts A→P (9 fields — SUPPERSONNEL_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

CONTACTS_A2P = [
    rec("Vendor Contacts - Clean Full Name", "A>P", "Clean Full Name", "fldN2PlqhOeMtvuQX", "NAME", "SUPPERSONNEL_SUBFORM. Match field for upsert. A→P only."),
    rec("Vendor Contacts - Clean_First Name", "A>P", "Clean_First Name", "fldaPwANUcUPSa55z", "FIRSTNAME", "SUPPERSONNEL_SUBFORM. A→P only."),
    rec("Vendor Contacts - Clean_Last Name", "A>P", "Clean_Last Name", "fldLaYAHIPkB13MC5", "LASTNAME", "SUPPERSONNEL_SUBFORM. A→P only."),
    rec("Vendor Contacts - Contact Status", "A>P", "Contact Status", "fldSG8EWwbg8KANHh", "STATDES", "SUPPERSONNEL_SUBFORM. A→P only."),
    rec("Vendor Contacts - Clean Phone Number", "A>P", "Clean Phone Number", "fld10t6t3ARBlyPdC", "PHONENUM", "SUPPERSONNEL_SUBFORM. A→P only."),
    rec("Vendor Contacts - Clean Cell Phone", "A>P", "Clean Cell Phone", "fldn8nED0EuU6tSwk", "CELLPHONE", "SUPPERSONNEL_SUBFORM. A→P only."),
    rec("Vendor Contacts - Clean Email", "A>P", "Clean Email", "fldTugnqAPRwm9yad", "EMAIL", "SUPPERSONNEL_SUBFORM. A→P only."),
    rec("Vendor Contacts - Clean_Job Title", "A>P", "Clean_Job Title", "fldsZUDIq5RmoMknh", "POSITIONDES", "SUPPERSONNEL_SUBFORM. A→P only."),
    rec("Vendor Contacts - Buyer Output (from Vendor_ID)", "A>P", "Buyer Output (from Vendor_ID)", "fld5Djf8gfanTI0Fn", "OWNERLOGIN", "SUPPERSONNEL_SUBFORM. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Vendor Products A→P (4 fields — SUPPART_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

PRODUCTS_A2P = [
    rec("Vendor Products - SKU Trim (EDI) (from Savory SKU)", "A>P", "SKU Trim (EDI) (from Savory SKU)", "fldYpa9CkG9i2lS9m", "PARTNAME", "SUPPART_SUBFORM. Match field for upsert. A→P only."),
    rec("Vendor Products - Vendor Part Number", "A>P", "Vendor Part Number", "fldPpJklgX03JU7wL", "SUPPARTNAME", "SUPPART_SUBFORM. A→P only."),
    rec("Vendor Products - Purchase Lead Time (from Savory SKU)", "A>P", "Purchase Lead Time (from Savory SKU)", "fldQeeo78pzTBWhjt", "SUPTIME", "SUPPART_SUBFORM. A→P only."),
    rec("Vendor Products - Min for Order (from Savory SKU)", "A>P", "Min for Order (from Savory SKU)", "fldMzQ1rWqoQhJJ1N", "MINQUANT", "SUPPART_SUBFORM. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Vendor Sites A→P (8 fields — SUPDESTCODES_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

SITES_A2P = [
    rec("Vendor Sites - W", "A>P", "W", "fld8G0ygtwYt4dNBd", "CODE", "SUPDESTCODES_SUBFORM. Match field for upsert. A→P only."),
    rec("Vendor Sites - Clean Site Name", "A>P", "Clean Site Name", "fldyxptlFEVo2I8lm", "CODEDES", "SUPDESTCODES_SUBFORM. A→P only."),
    rec("Vendor Sites - Clean Warehouse Address Line 1", "A>P", "Clean Warehouse Address Line 1", "fldyAhq8z1hxPhcU9", "ADDRESS", "SUPDESTCODES_SUBFORM. A→P only."),
    rec("Vendor Sites - Clean Warehouse Address Line 2", "A>P", "Clean Warehouse Address Line 2", "fldWFbdsRfzHYBjVb", "ADDRESS2", "SUPDESTCODES_SUBFORM. A→P only."),
    rec("Vendor Sites - Clean Warehouse Address City", "A>P", "Clean Warehouse Address City", "flduTJTvfUezvbUjQ", "STATE", "SUPDESTCODES_SUBFORM. A→P only."),
    rec("Vendor Sites - Warehouse Address State", "A>P", "Warehouse Address State", "fldE8jueZrtGGE3P8", "STATECODE", "SUPDESTCODES_SUBFORM. A→P only."),
    rec("Vendor Sites - Warehouse Address Zip Code", "A>P", "Warehouse Address Zip Code", "fldDM3L797zDvfNqI", "ZIP", "SUPDESTCODES_SUBFORM. A→P only."),
    rec("Vendor Sites - Warehouse Address Country", "A>P", "Warehouse Address Country", "fldA79B7xXcRYpipR", "COUNTRYNAME", "SUPDESTCODES_SUBFORM. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Vendor Prices A→P (4 fields — PRICELIST + PARTPRICE2_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

VENDOR_PRICES_A2P = [
    rec("Vendor Prices - Price List Code", "A>P", "Price List Code", "fld98fqyqEboRCrU4", "PLNAME", "PRICELIST parent entity. Required. A→P only."),
    rec("Vendor Prices - SKU Trim (EDI) (from Products)", "A>P", "SKU Trim (EDI) (from Products)", "fldCtaPuLVJNtPHMv", "PARTNAME", "PARTPRICE2_SUBFORM. Match field. A→P only."),
    rec("Vendor Prices - Unit Price", "A>P", "Unit Price", "fldKTUxHTSmrGfBHF", "PRICE", "PARTPRICE2_SUBFORM. A→P only."),
    rec("Vendor Prices - Quantity", "A>P", "Quantity", "fldHMtsbjsCqVrfuX", "QUANT", "PARTPRICE2_SUBFORM. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Combine all records
# ═════════════════════════════════════════════════════════════════════════════

ALL_RECORDS = (
    SUPPLIERS_A2P + SUPPLIERS_P2A
    + FNCSUP_A2P + FNCSUP_P2A
    + BANK_A2P + BANK_P2A
    + CONTACTS_A2P
    + PRODUCTS_A2P
    + SITES_A2P
    + VENDOR_PRICES_A2P
)


def batch_create(records: list[dict], batch_size: int = 10) -> int:
    """Create records in batches of 10 (Airtable API limit)."""
    created = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        payload = {"records": batch, "typecast": True}
        resp = requests.post(API_URL, headers=HEADERS, json=payload)
        if resp.status_code == 200:
            created += len(batch)
            print(f"  Batch {i // batch_size + 1}: created {len(batch)} records ({created}/{len(records)} total)")
        else:
            print(f"  ERROR batch {i // batch_size + 1}: {resp.status_code} {resp.text}")
        time.sleep(0.25)
    return created


if __name__ == "__main__":
    print(f"Total records to create: {len(ALL_RECORDS)}")
    print(f"  SUPPLIERS A→P: {len(SUPPLIERS_A2P)}")
    print(f"  SUPPLIERS P→A: {len(SUPPLIERS_P2A)}")
    print(f"  FNCSUP A→P: {len(FNCSUP_A2P)}")
    print(f"  FNCSUP P→A: {len(FNCSUP_P2A)}")
    print(f"  Bank Accounts A→P: {len(BANK_A2P)}")
    print(f"  Bank Accounts P→A: {len(BANK_P2A)}")
    print(f"  Vendor Contacts A→P: {len(CONTACTS_A2P)}")
    print(f"  Vendor Products A→P: {len(PRODUCTS_A2P)}")
    print(f"  Vendor Sites A→P: {len(SITES_A2P)}")
    print(f"  Vendor Prices A→P: {len(VENDOR_PRICES_A2P)}")
    print()

    total = batch_create(ALL_RECORDS)
    print(f"\nDone! Created {total} records in API Matching table.")
