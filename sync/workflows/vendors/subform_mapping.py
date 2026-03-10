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
    "Main Contact",
    "Quotes",
    "Purchase Orders",
    "Invoice",
    "Shipping Voucher",
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
    "Main Contact": "fldyqAOO5lWr4xsy8",
    "Quotes": "fldw7Rdwl6GFv7qPl",
    "Purchase Orders": "fldJSPomvcj31cwuY",
    "Invoice": "flddom8RDIFXFc1uu",
    "Shipping Voucher": "fldTfaxRT3R5rN25W",
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
    "Main Contact": "MAINPHONE",
    "Quotes": "PDPROFFLAG",
    "Purchase Orders": "PORDFLAG",
    "Invoice": "PIVFLAG",
    "Shipping Voucher": "IMPORTFLAG",
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


# ═════════════════════════════════════════════════════════════════════════════
# P→A: Vendor Contacts — SUPPERSONNEL_SUBFORM → Vendor Contacts table
# ═════════════════════════════════════════════════════════════════════════════
# Text fields are write-if-empty: only populate if the Airtable field is empty.
# Flag fields (Y/N in Priority) need priority_yn transform → Yes/No in Airtable.

# Priority field → (Airtable writable field name, transform)
P2A_CONTACTS_FIELD_MAP: dict[str, tuple[str, str]] = {
    "FIRSTNAME": ("First Name", "clean"),
    "LASTNAME": ("Last Name", "clean"),
    "PHONENUM": ("Phone Number", "clean"),
    "CELLPHONE": ("Cell Phone", "clean"),
    "EMAIL": ("Email", "clean"),
    "POSITIONDES": ("Job Title", "clean"),
    "STATDES": ("Contact Status", "clean"),
    "MAINPHONE": ("Main Contact", "priority_yn"),
    "PDPROFFLAG": ("Quotes", "priority_yn"),
    "PORDFLAG": ("Purchase Orders", "priority_yn"),
    "PIVFLAG": ("Invoice", "priority_yn"),
    "IMPORTFLAG": ("Shipping Voucher", "priority_yn"),
}

P2A_CONTACTS_MATCH_FIELD = "NAME"  # Priority full name for matching
P2A_CONTACTS_AIRTABLE_MATCH_FIELD = "Clean Full Name"  # Airtable formula for matching
P2A_CONTACTS_LINK_FIELD = "Vendor_ID"  # Linked record field to parent vendor

# Airtable fields to fetch for P→A comparison
P2A_CONTACTS_AIRTABLE_FIELDS: list[str] = (
    [at_field for _, (at_field, _) in P2A_CONTACTS_FIELD_MAP.items()]
    + [
        P2A_CONTACTS_AIRTABLE_MATCH_FIELD,  # For matching by full name
        "Priority Vendor ID (from Vendor_ID)",  # To group by vendor
    ]
)


# ═════════════════════════════════════════════════════════════════════════════
# P→A: Vendor Products — SUPPART_SUBFORM → Vendor Products table
# ═════════════════════════════════════════════════════════════════════════════
# UPDATE-only. Only Vendor Item Number is writable; Lead Time and Min Order
# are lookups from Products table.

# Priority field → (Airtable writable field name, transform)
P2A_PRODUCTS_FIELD_MAP: dict[str, tuple[str, str]] = {
    "SUPPARTNAME": ("Vendor Item Number", "clean"),
}

P2A_PRODUCTS_MATCH_FIELD = "PARTNAME"  # Priority SKU for matching
P2A_PRODUCTS_AIRTABLE_MATCH_FIELD = "SKU Trim (EDI) (from Savory SKU)"  # Lookup
P2A_PRODUCTS_LINK_FIELD = "Vendor Id"  # Linked record field to parent vendor

# Airtable fields to fetch for P→A comparison
P2A_PRODUCTS_AIRTABLE_FIELDS: list[str] = (
    [at_field for _, (at_field, _) in P2A_PRODUCTS_FIELD_MAP.items()]
    + [
        P2A_PRODUCTS_AIRTABLE_MATCH_FIELD,  # For matching by SKU
        "V-Vendor ID (from Vendor)",  # To group by vendor
    ]
)


# ═════════════════════════════════════════════════════════════════════════════
# P→A: Vendor Sites — SUPDESTCODES_SUBFORM → Vendor Sites table
# ═════════════════════════════════════════════════════════════════════════════
# UPDATE-only. Config needed: writable "Site Code" field for matching.
# Address fields are consolidated into "Address Input" (write-if-empty).

# Priority field → (Airtable writable field name, transform)
P2A_SITES_FIELD_MAP: dict[str, tuple[str, str]] = {
    "CODEDES": ("Site Name Input", "clean"),
    # COUNTRYNAME is included in Address Input consolidation (not standalone)
}

P2A_SITES_MATCH_FIELD = "CODE"  # Priority site code for matching
P2A_SITES_AIRTABLE_MATCH_FIELD = "Site ID Input"  # Writable singleLineText for site code
P2A_SITES_ADDRESS_TARGET = "Address Input"  # Consolidated address (write-if-empty)
P2A_SITES_LINK_FIELD = "Vendor"  # Linked record field to parent vendor

# Airtable fields to fetch for P→A comparison
P2A_SITES_AIRTABLE_FIELDS: list[str] = (
    [at_field for _, (at_field, _) in P2A_SITES_FIELD_MAP.items()]
    + [
        P2A_SITES_AIRTABLE_MATCH_FIELD,  # For matching by site code
        P2A_SITES_ADDRESS_TARGET,          # For write-if-empty check
        "Priority Vendor ID (from Vendor)",  # To group by vendor
    ]
)
