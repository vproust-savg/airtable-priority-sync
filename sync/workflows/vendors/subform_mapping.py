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

CONTACTS_FIELD_IDS: dict[str, str] = {
    "Priority Vendor ID (from Vendor_ID)": "fldaBCpI4fDYcwXS0",
    "Clean Full Name": "fldN2PlqhOeMtvuQX",
    "Clean_First Name": "fldaPwANUcUPSa55z",
    "Clean_Last Name": "fldLaYAHIPkB13MC5",
    "Contact Status": "fldSG8EWwbg8KANHh",
    "Clean Phone Number": "fld10t6t3ARBlyPdC",
    "Clean Cell Phone": "fldn8nED0EuU6tSwk",
    "Clean Email": "fldTugnqAPRwm9yad",
    "Clean_Job Title": "fldsZUDIq5RmoMknh",
    "Buyer Output (from Vendor_ID)": "fld5Djf8gfanTI0Fn",
}

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

PRODUCTS_FIELD_IDS: dict[str, str] = {
    "V-Vendor ID (from Preferred Vendor) (from Savory SKU)": "fldT1ZI0gMdd7VkfW",
    "SKU Trim (EDI) (from Savory SKU)": "fldYpa9CkG9i2lS9m",
    "Vendor Part Number": "fldPpJklgX03JU7wL",
    "Purchase Lead Time (from Savory SKU)": "fldQeeo78pzTBWhjt",
    "Min for Order (from Savory SKU)": "fldMzQ1rWqoQhJJ1N",
}

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

SITES_FIELD_IDS: dict[str, str] = {
    "Priority Vendor ID (from Vendor)": "fldYS31GIiXUnjfVS",
    "W": "fld8G0ygtwYt4dNBd",
    "Clean Site Name": "fldyxptlFEVo2I8lm",
    "Clean Warehouse Address Line 1": "fldyAhq8z1hxPhcU9",
    "Clean Warehouse Address Line 2": "fldWFbdsRfzHYBjVb",
    "Clean Warehouse Address City": "flduTJTvfUezvbUjQ",
    "Warehouse Address State": "fldE8jueZrtGGE3P8",
    "Warehouse Address Zip Code": "fldDM3L797zDvfNqI",
    "Warehouse Address Country": "fldA79B7xXcRYpipR",
}

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
