"""
Sub-form field mappings for Customers All (CUSTOMERS).

Maps Airtable fields from related tables to Priority sub-form fields:
  - Customer Contacts → CUSTPERSONNEL_SUBFORM
  - Customer Sites → CUSTDESTS_SUBFORM
  - Special Cust. Prices → CUSTPARTPRICE_SUBFORM
  - Price List → CUSTPLIST_SUBFORM (single field)
  - Delivery Days → CUSTWEEKDAY_SUBFORM (requires row explosion + transforms)
  - Shipment Remarks → CUSTSHIPTEXT_SUBFORM (NOT accessible via API — 404)
  - Internal Remarks → CUSTOMERSTEXT_SUBFORM (NOT accessible via API — 404)
"""

from __future__ import annotations


# ═════════════════════════════════════════════════════════════════════════════
# Customer Contacts → CUSTPERSONNEL_SUBFORM
# ═════════════════════════════════════════════════════════════════════════════

CONTACTS_AIRTABLE_FIELDS: list[str] = [
    "Priority Cust. ID (from Customers)",
    "Clean First Name",
    "Clean Last Name",
    "Clean Phone Number",
    "Clean Cell Phone",
    "Email_ID",
    "Consent to Receive Emails Output",
    "Clean Position",
    "Status",
    "Linkedin",
    "Sign Up Priority",
    "Clean Full Name",
]

# Airtable field → Priority sub-form field
CONTACTS_FIELD_MAP: dict[str, str] = {
    "Clean First Name": "FIRSTNAME",
    "Clean Last Name": "LASTNAME",
    "Clean Phone Number": "PHONENUM",
    "Clean Cell Phone": "CELLPHONE",
    "Email_ID": "EMAIL",
    "Consent to Receive Emails Output": "MAILAPP",
    "Clean Full Name": "NAME",
    "Clean Position": "POSITIONDES",
    "Status": "STATDES",
    "Linkedin": "EXTFILENAME",
    "Sign Up Priority": "GWREGISTERED",
}

# Match contacts by NAME for upsert
CONTACTS_MATCH_FIELD = "NAME"


# ═════════════════════════════════════════════════════════════════════════════
# Customer Sites → CUSTDESTS_SUBFORM
# ═════════════════════════════════════════════════════════════════════════════

SITES_AIRTABLE_FIELDS: list[str] = [
    "Priority Cust. ID",
    "Site Id",
    "Ship To Name Output",
    "Main Output",
    "Address Line 1 Output",
    "Address Line 2 Output",
    "Address Remarks Output",
    "City Output",
    "State",
    "Zip",
    "EDI_USA",
    "Primary receiver (from Customers)",
    "Phone Output",
    "Zone Code",
    "Dist. Route Code",
    "EDI_Carrier Code",
    "Carrier Account Number (5) (from Customers)",
]

SITES_FIELD_MAP: dict[str, str] = {
    "Site Id": "CODE",
    "Ship To Name Output": "CODEDES",
    "Main Output": "MAINFLAG",
    "Address Line 1 Output": "ADDRESS",
    "Address Line 2 Output": "ADDRESS2",
    "Address Remarks Output": "ADDRESS3",
    "City Output": "STATE",
    "State": "STATECODE",
    "Zip": "ZIP",
    "EDI_USA": "COUNTRYNAME",
    "Primary receiver (from Customers)": "NAME",
    "Phone Output": "PHONE",
    "Zone Code": "ZONECODE",
    "Dist. Route Code": "DISTRLINECODE",
    "EDI_Carrier Code": "SHIPPERNAME",
    "Carrier Account Number (5) (from Customers)": "SHIPACCOUNTNUM",
}

# Match sites by CODE for upsert
SITES_MATCH_FIELD = "CODE"


# ═════════════════════════════════════════════════════════════════════════════
# Special Cust. Prices → CUSTPARTPRICE_SUBFORM
# ═════════════════════════════════════════════════════════════════════════════

SPECIAL_PRICES_AIRTABLE_FIELDS: list[str] = [
    "Cust. IDs",
    "SKU",
    "Formatted Start Date",
    "Expiration Date",
    "Special Price",
]

SPECIAL_PRICES_FIELD_MAP: dict[str, str] = {
    "SKU": "PARTNAME",
    "Formatted Start Date": "FROMDATE",
    "Expiration Date": "EXPIRYDATE",
    "Special Price": "PRICE",
}

# Match special prices by PARTNAME for upsert
SPECIAL_PRICES_MATCH_FIELD = "PARTNAME"


# ═════════════════════════════════════════════════════════════════════════════
# Price List → CUSTPLIST_SUBFORM (from Customers table view)
# ═════════════════════════════════════════════════════════════════════════════

PRICE_LIST_AIRTABLE_FIELDS: list[str] = [
    "Priority Cust. ID",
    "Price List Code",
]

PRICE_LIST_FIELD_MAP: dict[str, str] = {
    "Price List Code": "PLNAME",
}

PRICE_LIST_MATCH_FIELD = "PLNAME"


# ═════════════════════════════════════════════════════════════════════════════
# Delivery Days → CUSTWEEKDAY_SUBFORM (from Customers table view)
# ═════════════════════════════════════════════════════════════════════════════
# SPECIAL: "Days of Business" is comma-separated → one sub-form row per day
# Time fields need 12h→24h conversion
# Day names need abbreviation (Monday → Mon)

DELIVERY_DAYS_AIRTABLE_FIELDS: list[str] = [
    "Priority Cust. ID",
    "Days of Business",
    "Deliver After",
    "Deliver Before",
]

# Not a standard field map — handled with custom logic in the engine
# because row explosion + transforms are needed
