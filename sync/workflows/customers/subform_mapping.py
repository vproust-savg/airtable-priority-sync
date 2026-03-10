"""
Sub-form field mappings for Customers All (CUSTOMERS).

Maps Airtable fields from related tables to Priority sub-form fields:
  - Customer Contacts → CUSTPERSONNEL_SUBFORM
  - Customer Sites → CUSTDESTS_SUBFORM
  - Special Cust. Prices → CUSTPARTPRICE_SUBFORM
  - Price List → CUSTPLIST_SUBFORM (single field)
  - Delivery Days → CUSTWEEKDAY_SUBFORM (requires row explosion + transforms)
  - Shipment Remarks → CUSTSHIPTEXT_SUBFORM (Pattern A — single entity, TEXT field)
  - Internal Remarks → CUSTOMERSTEXT_SUBFORM (Pattern A — single entity, TEXT field)
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
    "Main Contact Output",
    "Marketing Output",
    "Price Quote Output",
    "Sales Order Output",
    "Shipment Output",
    "Invoice Output",
    "Cust. Statement Output",
    "Outgoing Voucher Output",
]

CONTACTS_FIELD_IDS: dict[str, str] = {
    "Priority Cust. ID (from Customers)": "fldCzWux3AdKYFYL9",
    "Clean First Name": "fldP8R2YkLs59r0wc",
    "Clean Last Name": "fldUXuN2tPvLznoCn",
    "Clean Phone Number": "fldAzbY4BolcbPQJu",
    "Clean Cell Phone": "fldszV8PW8VtGf8xR",
    "Email_ID": "fldxHFcsanzzWf8fn",
    "Consent to Receive Emails Output": "fldM1eDjhTQbGqRp5",
    "Clean Position": "fldti46pIk4p7cIXG",
    "Status": "fld9kBeWqtHTczqLj",
    "Linkedin": "fldamkilBbLhalu0X",
    "Sign Up Priority": "fldy25icq78YSMcUk",
    "Clean Full Name": "fldJgSoTTx2eRcgEg",
    "Main Contact Output": "fldlSY1amLBqTeFlr",
    "Marketing Output": "fldZlEv78f9iajYA3",
    "Price Quote Output": "fldijSOdUr8HwqcCn",
    "Sales Order Output": "fldvQDpD5IFeZ7kaT",
    "Shipment Output": "fldkU2Li0iLA2fUUw",
    "Invoice Output": "fldDrc3EDhQwMr6v1",
    "Cust. Statement Output": "fld9lL0as0ia7t9EK",
    "Outgoing Voucher Output": "fldxiahOYOBseWdbU",
}

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
    "Main Contact Output": "MAINPHONE",
    "Marketing Output": "DEALFLAG",
    "Price Quote Output": "CPROFFLAG",
    "Sales Order Output": "ORDFLAG",
    "Shipment Output": "DOCFLAG",
    "Invoice Output": "CIVFLAG",
    "Cust. Statement Output": "FNCFLAG",
    "Outgoing Voucher Output": "CONSINGEEFLAG",
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

SITES_FIELD_IDS: dict[str, str] = {
    "Priority Cust. ID": "fld7YjtzFv6eiJlwF",
    "Site Id": "fld9bYfmERdt0T3Az",
    "Ship To Name Output": "fldjOXa5Kg7lqGS46",
    "Main Output": "fldM2QoftKTXiTlVM",
    "Address Line 1 Output": "fldmg9e952h8XyPFE",
    "Address Line 2 Output": "fldg3vOpR8GKGb1ca",
    "Address Remarks Output": "fldYNN7GhBxP7IIZV",
    "City Output": "fldc0TVQC5G7kd9oE",
    "State": "fldsLpLJ0H4mR4gmo",
    "Zip": "fld25oyjjBA2C9i6x",
    "EDI_USA": "fldOWRPJ249yT1bjn",
    "Primary receiver (from Customers)": "fldQq0M0icXcpYU0L",
    "Phone Output": "fldSUThO49r0D8hWf",
    "Zone Code": "fld47Y12aa9P4VGul",
    "Dist. Route Code": "fldWy8cTBTkGnKp2L",
    "EDI_Carrier Code": "fldsKJxxcpSnBgNnb",
    "Carrier Account Number (5) (from Customers)": "fldl2Tg6PyAyM4MYm",
}

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

SPECIAL_PRICES_FIELD_IDS: dict[str, str] = {
    "Cust. IDs": "fldrN10isC0LHoCdt",
    "SKU": "fld50GU0eYjvT2CC2",
    "Formatted Start Date": "fldnogjeffyFmGV5y",
    "Expiration Date": "fldzbrfYiaWW8eaV9",
    "Special Price": "fldOg7NcP6Vckpxak",
}

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

PRICE_LIST_FIELD_IDS: dict[str, str] = {
    "Priority Cust. ID": "fldsnRoMbOuU26Ezc",
    "Price List Code": "fldxB83teEXqu6aMq",
}

PRICE_LIST_FIELD_MAP: dict[str, str] = {
    "Price List Code": "PLNAME",
}

PRICE_LIST_MATCH_FIELD = "PLNAME"


# ═════════════════════════════════════════════════════════════════════════════
# Delivery Days → CUSTWEEKDAY_SUBFORM (from Customers table view)
# ═════════════════════════════════════════════════════════════════════════════
# SPECIAL: "Days of Business" is comma-separated → one sub-form row per day
# Time fields need 12h→24h conversion
# Day names → Priority integer WEEKDAY (Sun=1, Mon=2, …, Sat=7)

DELIVERY_DAYS_AIRTABLE_FIELDS: list[str] = [
    "Priority Cust. ID",
    "Days of Business",
    "Deliver After",
    "Deliver Before",
]

DELIVERY_DAYS_FIELD_IDS: dict[str, str] = {
    "Priority Cust. ID": "fldsnRoMbOuU26Ezc",
    "Days of Business": "fldFJJ3w9sPLZlLZi",
    "Deliver After": "fldlUAvkUo7v9E4PG",
    "Deliver Before": "fldaHJIvSdQMcva5S",
}

# Not a standard field map — handled with custom logic in the engine
# because row explosion + transforms are needed


# ═════════════════════════════════════════════════════════════════════════════
# P→A: Customer Contacts — CUSTPERSONNEL_SUBFORM → Customer Contacts 2025
# ═════════════════════════════════════════════════════════════════════════════
# All fields are write-if-empty: only populate if the Airtable field is empty.
# Flag fields (Y/N in Priority) need priority_yn transform → Yes/No in Airtable.

# Priority field → (Airtable writable field name, transform)
P2A_CONTACTS_FIELD_MAP: dict[str, tuple[str, str]] = {
    "FIRSTNAME": ("First Name Input", "clean"),
    "LASTNAME": ("Last Name Input", "clean"),
    "PHONENUM": ("Phone Number Input", "clean"),
    "CELLPHONE": ("Cell Phone", "clean"),
    "EMAIL": ("Email Input", "clean"),
    "POSITIONDES": ("Position Input", "clean"),
    "STATDES": ("Status", "clean"),
    "MAINPHONE": ("Main Contact", "priority_yn"),
    "DEALFLAG": ("Marketing", "priority_yn"),
    "CPROFFLAG": ("Price Quote", "priority_yn"),
    "ORDFLAG": ("Sales Order", "priority_yn"),
    "DOCFLAG": ("Shipment", "priority_yn"),
    "CIVFLAG": ("Invoice", "priority_yn"),
    "FNCFLAG": ("Cust. Statement", "priority_yn"),
    "CONSINGEEFLAG": ("Outgoing Voucher", "priority_yn"),
}

P2A_CONTACTS_MATCH_FIELD = "NAME"  # Priority fallback match field (full name)
P2A_CONTACTS_EMAIL_MATCH_FIELD = "EMAIL"  # Priority primary match field (email)
P2A_CONTACTS_AIRTABLE_MATCH_FIELD = "Clean Full Name"  # Airtable name (fallback)
P2A_CONTACTS_AIRTABLE_EMAIL_MATCH_FIELD = "Email Input"  # Airtable email (primary)
P2A_CONTACTS_LINK_FIELD = "Customers"  # Linked record field to parent customer

# Airtable fields to fetch for P→A comparison
# Note: "Email Input" is already in P2A_CONTACTS_FIELD_MAP, so it's auto-included.
# "Clean Full Name" is a formula field used for name-based fallback matching.
P2A_CONTACTS_AIRTABLE_FIELDS: list[str] = (
    [at_field for _, (at_field, _) in P2A_CONTACTS_FIELD_MAP.items()]
    + [
        P2A_CONTACTS_AIRTABLE_MATCH_FIELD,  # "Clean Full Name" — for name fallback matching
        "Priority Cust. ID (from Customers)",  # To group by customer
    ]
)


# ═════════════════════════════════════════════════════════════════════════════
# P→A: Customer Sites — CUSTDESTS_SUBFORM → Customer Sites table
# ═════════════════════════════════════════════════════════════════════════════
# Most fields are write-if-empty; MAINFLAG is always-overwrite.
# ZONECODE and SHIPPERNAME need Priority lookup tables (code → description).
# Address fields (ADDRESS, ADDRESS2, STATE, STATECODE, ZIP) are consolidated
# into a single "Address Input" field (write-if-empty).

# Priority field → (Airtable writable field name, transform)
P2A_SITES_FIELD_MAP: dict[str, tuple[str, str]] = {
    "CODEDES": ("Ship To Name Input", "clean"),
    "MAINFLAG": ("Main", "priority_yn"),
    "ADDRESS3": ("Address Remarks Input", "strip_html"),
    "PHONE": ("Phone Input", "clean"),
    "ZONECODE": ("Shipping Zone", "zone_lookup"),
    "SHIPPERNAME": ("Main Delivery Method", "shipper_lookup"),
}

# MAINFLAG is the only field that always overwrites; all others are write-if-empty
P2A_SITES_OVERWRITE_FIELDS: set[str] = {"Main"}

P2A_SITES_MATCH_FIELD = "CODE"  # Priority site code for matching
P2A_SITES_AIRTABLE_MATCH_FIELD = "Site ID Input"  # Writable singleLineText for site code
P2A_SITES_LINK_FIELD = "Customers"  # Linked record field to parent customer

# Address consolidation target (write-if-empty)
P2A_SITES_ADDRESS_TARGET = "Address Input"

# Airtable fields to fetch for P→A comparison
P2A_SITES_AIRTABLE_FIELDS: list[str] = (
    [at_field for _, (at_field, _) in P2A_SITES_FIELD_MAP.items()]
    + [
        P2A_SITES_AIRTABLE_MATCH_FIELD,  # "Site ID Input" — for matching
        P2A_SITES_ADDRESS_TARGET,         # "Address Input" — for write-if-empty check
        "Priority Cust. ID",              # Formula — for grouping by customer
    ]
)
