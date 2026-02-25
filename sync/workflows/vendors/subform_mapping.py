"""
Sub-form field mappings for Vendors All (SUPPLIERS).

Maps Airtable fields from related tables to Priority sub-form fields:
  - Vendor Contacts → SUPPERSONNEL_SUBFORM
  - Vendor Products → SUPPART_SUBFORM
  - Vendor Sites → SUPDESTCODES_SUBFORM
  - Vendor Remarks → SUPPLIERSTEXT_SUBFORM (NOT accessible via API — 404)
"""

from __future__ import annotations


# ═════════════════════════════════════════════════════════════════════════════
# Vendor Contacts → SUPPERSONNEL_SUBFORM
# ═════════════════════════════════════════════════════════════════════════════

CONTACTS_AIRTABLE_FIELDS: list[str] = [
    "Priority Vendor ID (from Vendor_ID)",
    "Clean Full Name",
    "Clean_First Name",
    "Clean_Last Name",
    "Contact Status",
    "Clean Phone Number",
    "Clean Cell Phone",
    "Clean Email",
    "Clean_Job Title",
    "Buyer Output (from Vendor_ID)",
]

# Airtable field → Priority sub-form field
CONTACTS_FIELD_MAP: dict[str, str] = {
    "Clean Full Name": "NAME",
    "Clean_First Name": "FIRSTNAME",
    "Clean_Last Name": "LASTNAME",
    "Contact Status": "STATDES",
    "Clean Phone Number": "PHONENUM",
    "Clean Cell Phone": "CELLPHONE",
    "Clean Email": "EMAIL",
    "Clean_Job Title": "POSITIONDES",
    "Buyer Output (from Vendor_ID)": "OWNERLOGIN",
}

# Match contacts by NAME for upsert
CONTACTS_MATCH_FIELD = "NAME"


# ═════════════════════════════════════════════════════════════════════════════
# Vendor Products → SUPPART_SUBFORM
# ═════════════════════════════════════════════════════════════════════════════

PRODUCTS_AIRTABLE_FIELDS: list[str] = [
    "V-Vendor ID (from Preferred Vendor) (from Savory SKU)",
    "SKU Trim (EDI) (from Savory SKU)",
    "Vendor Part Number",
    "Purchase Lead Time (from Savory SKU)",
    "Min for Order (from Savory SKU)",
]

PRODUCTS_FIELD_MAP: dict[str, str] = {
    "SKU Trim (EDI) (from Savory SKU)": "PARTNAME",
    "Vendor Part Number": "SUPPARTNAME",
    "Purchase Lead Time (from Savory SKU)": "SUPTIME",
    "Min for Order (from Savory SKU)": "MINQUANT",
}

# Match parts by PARTNAME for upsert
PRODUCTS_MATCH_FIELD = "PARTNAME"


# ═════════════════════════════════════════════════════════════════════════════
# Vendor Sites → SUPDESTCODES_SUBFORM
# ═════════════════════════════════════════════════════════════════════════════

SITES_AIRTABLE_FIELDS: list[str] = [
    "Priority Vendor ID (from Vendor)",
    "W",
    "Clean Site Name",
    "Clean Warehouse Address Line 1",
    "Clean Warehouse Address Line 2",
    "Clean Warehouse Address City",
    "Warehouse Address State",
    "Warehouse Address Zip Code",
    "Warehouse Address Country",
]

SITES_FIELD_MAP: dict[str, str] = {
    "W": "CODE",
    "Clean Site Name": "CODEDES",
    "Clean Warehouse Address Line 1": "ADDRESS",
    "Clean Warehouse Address Line 2": "ADDRESS2",
    "Clean Warehouse Address City": "STATE",
    "Warehouse Address State": "STATECODE",
    "Warehouse Address Zip Code": "ZIP",
    "Warehouse Address Country": "COUNTRYNAME",
}

# Match sites by CODE for upsert
SITES_MATCH_FIELD = "CODE"
