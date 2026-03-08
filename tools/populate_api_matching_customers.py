"""
Populate the API Matching table in Airtable with all Customer workflow field mappings.
Uses batch creates (10 records per request) for efficiency.

Covers: CUSTOMERS, FNCCUST, Customer Contacts, Customer Sites, Special Prices,
        Price List, Delivery Days, Credit Application, Shipment Remarks,
        Internal Remarks, Customer Price Lists.
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
            "Category": "Customers",
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
# CUSTOMERS A→P (26 fields)
# ═════════════════════════════════════════════════════════════════════════════

CUSTOMERS_A2P = [
    rec("CUSTOMERS - Priority Cust. ID", "A>P", "Priority Cust. ID", "fldsnRoMbOuU26Ezc", "CUSTNAME", "Primary key. Required."),
    rec("CUSTOMERS - Business Name Output", "A>P", "Business Name Output", "fldnfU0xfMgYKlGnn", "CUSTDES", "Formula/aiText in AT. P→A uses 'Business Name Input' instead."),
    rec("CUSTOMERS - Sales Rep Number", "A>P", "Sales Rep Number", "fldd50oTgqvbTzyz1", "AGENTCODE", "Formula in AT. P→A uses 'Account Executive' lookup instead."),
    rec("CUSTOMERS - Cust Group Code", "A>P", "Cust Group Code", "fldA2clRU9QmVdXwy", "CTYPECODE", "Formula in AT. P→A uses 'Business Type' lookup instead."),
    rec("CUSTOMERS - Website Output", "A>P", "Website Output", "fldae1KP8D0nPVWwF", "HOSTNAME", "Formula/aiText in AT. P→A uses 'Website Input' instead."),
    rec("CUSTOMERS - Zone Code", "A>P", "Zone Code", "fldpF2C594eZpY57n", "ZONECODE", "P→A deferred (harder)."),
    rec("CUSTOMERS - Billing Address Line 1 Output", "A>P", "Billing Address Line 1 Output", "fldngFA0BudqaLTDN", "ADDRESS", "Formula/aiText in AT. P→A consolidates into 'Billing Address Input'."),
    rec("CUSTOMERS - Billing Address_Line 2 Output", "A>P", "Billing Address_Line 2 Output", "fldqcujQ7lbKx3tIq", "ADDRESS2", "Formula/aiText in AT. P→A consolidates into 'Billing Address Input'."),
    rec("CUSTOMERS - Billing Address_City Output", "A>P", "Billing Address_City Output", "fldf0T2zIzBJsqk7w", "STATEA", "Formula/aiText in AT. P→A consolidates into 'Billing Address Input'."),
    rec("CUSTOMERS - Full State Name", "A>P", "Full State Name", "fldctnXE8Hazi0efQ", "STATENAME", "P→A consolidates into 'Billing Address Input'."),
    rec("CUSTOMERS - Billing Address_Zip Code", "A>P", "Billing Address_Zip Code", "fldDtmAoDNPudoVm8", "ZIP", "P→A consolidates into 'Billing Address Input'."),
    rec("CUSTOMERS - Payment Terms Code", "A>P", "Payment Terms Code", "fldaSfFLCnVHKAWti", "PAYCODE", "Formula in AT. P→A uses 'Payment Terms' lookup instead."),
    rec("CUSTOMERS - Credit Limit", "A>P", "Credit Limit", "fld5MrUHgUTws2LEn", "MAX_CREDIT", "Transform: to_float."),
    rec("CUSTOMERS - Payment Method (1)", "A>P", "Payment Method (1)", "fldpdc7DvNi4yXmrf", "SPEC1", ""),
    rec("CUSTOMERS - Are you part of Hilton? (2)", "A>P", "Are you part of Hilton? (2)", "fld84ysxqcV3cUvSe", "SPEC2", ""),
    rec("CUSTOMERS - Hilton Inncode (3)", "A>P", "Hilton Inncode (3)", "fldBKs6npT71SjCCz", "SPEC3", ""),
    rec("CUSTOMERS - Rekki Output (4)", "A>P", "Rekki Output (4)", "flda9oG5ZrfDy0hrQ", "SPEC4", "P→A skipped (no need to sync)."),
    rec("CUSTOMERS - Carrier Account Number (5)", "A>P", "Carrier Account Number (5)", "fld8A7QdwG2Wo1T1f", "SPEC5", ""),
    rec("CUSTOMERS - Curr", "A>P", "Curr", "fldoGuJdTrWu2SApq", "CODE", "P→A skipped (no need to sync)."),
    rec("CUSTOMERS - Country", "A>P", "Country", "fldWuHkO0YY00zgit", "COUNTRYNAME", ""),
    rec("CUSTOMERS - Tax Code", "A>P", "Tax Code", "fldl7A0ajRvutueWk", "TAXCODE", "P→A skipped (no need to sync)."),
    rec("CUSTOMERS - Dist. Route Code", "A>P", "Dist. Route Code", "fld07nuAN3KRg69PK", "DISTRLINECODE", "P→A deferred (harder)."),
    rec("CUSTOMERS - Approval_Before_Charging (6)", "A>P", "Approval_Before_Charging (6)", "fld3frlgCUVQG6Tg7", "SPEC6", ""),
    rec("CUSTOMERS - Shipment Code", "A>P", "Shipment Code", "fldjamRfwSrNJIKMw", "STCODE", "P→A deferred (harder)."),
    rec("CUSTOMERS - Priority Customer Status", "A>P", "Priority Customer Status", "fldLhntR45Memi1na", "STATDES", ""),
    rec("CUSTOMERS - Billing_Legal Name Output", "A>P", "Billing_Legal Name Output", "fldV0OhCRmkBTM0sq", "CUSTDESLONG", "Formula/aiText in AT. P→A uses 'Billing_Legal Name Input' instead."),
]

# ═════════════════════════════════════════════════════════════════════════════
# CUSTOMERS P→A (13 fields: 7 always-overwrite + 3 linked-table lookups + 3 write-if-empty)
# ═════════════════════════════════════════════════════════════════════════════

CUSTOMERS_P2A = [
    # Always-overwrite
    rec("CUSTOMERS - Priority Customer Status", "P>A", "Priority Customer Status", "fldLhntR45Memi1na", "STATDES", "Always-overwrite."),
    rec("CUSTOMERS - Credit Limit", "P>A", "Credit Limit", "fld5MrUHgUTws2LEn", "MAX_CREDIT", "Transform: to_float. Always-overwrite."),
    rec("CUSTOMERS - Payment Method (1)", "P>A", "Payment Method (1)", "fldpdc7DvNi4yXmrf", "SPEC1", "Always-overwrite."),
    rec("CUSTOMERS - Are you part of Hilton? (2)", "P>A", "Are you part of Hilton? (2)", "fld84ysxqcV3cUvSe", "SPEC2", "Always-overwrite."),
    rec("CUSTOMERS - Hilton Inncode (3)", "P>A", "Hilton Inncode (3)", "fldBKs6npT71SjCCz", "SPEC3", "Always-overwrite."),
    rec("CUSTOMERS - Carrier Account Number (5)", "P>A", "Carrier Account Number (5)", "fld8A7QdwG2Wo1T1f", "SPEC5", "Always-overwrite."),
    rec("CUSTOMERS - Approval_Before_Charging (6)", "P>A", "Approval_Before_Charging (6)", "fld3frlgCUVQG6Tg7", "SPEC6", "Always-overwrite."),
    # Linked-table lookups (code → description)
    rec("CUSTOMERS - Account Executive", "P>A", "Account Executive", "fldNq5gGxwvcwgiWr", "AGENTCODE", "Linked-table lookup: AGENTS (code→description). Writes to writable singleSelect."),
    rec("CUSTOMERS - Business Type", "P>A", "Business Type", "fldlwyGJn56BZjU3r", "CTYPECODE", "Linked-table lookup: CTYPE (code→description). Writes to writable singleSelect."),
    rec("CUSTOMERS - Payment Terms", "P>A", "Payment Terms", "fldtJbcuxychfBkSs", "PAYCODE", "Linked-table lookup: PAY (code→description). Writes to writable singleSelect."),
    # Write-if-empty
    rec("CUSTOMERS - Business Name Input", "P>A", "Business Name Input", "fldd61qbZI7L3KCg5", "CUSTDES", "Write-if-empty."),
    rec("CUSTOMERS - Website Input", "P>A", "Website Input", "fldNoV5ycIyHJVIfP", "HOSTNAME", "Write-if-empty."),
    rec("CUSTOMERS - Billing_Legal Name Input", "P>A", "Billing_Legal Name Input", "fldZaL0gBirbtwsB7", "CUSTDESLONG", "Write-if-empty."),
]

# ═════════════════════════════════════════════════════════════════════════════
# FNCCUST A→P (13 fields, includes primary key)
# ═════════════════════════════════════════════════════════════════════════════

FNCCUST_A2P = [
    rec("FNCCUST - Priority Cust. ID", "A>P", "Priority Cust. ID", "fldsnRoMbOuU26Ezc", "CUSTNAME", "Primary key. Links to CUSTOMERS."),
    rec("FNCCUST - Billing Priority Cust. ID (from Billing Cust. Number)", "A>P", "Billing Priority Cust. ID (from Billing Cust. Number)", "fldXpYyrOi3aLQr4a", "PCUSTNAME", "Formula in AT."),
    rec("FNCCUST - Cust Group Code", "A>P", "Cust Group Code", "fldA2clRU9QmVdXwy", "CTYPECODE", "Formula in AT."),
    rec("FNCCUST - Priority Customer Status", "A>P", "Priority Customer Status", "fldLhntR45Memi1na", "STATDES", ""),
    rec("FNCCUST - Financial Parameters Assigned", "A>P", "Financial Parameters Assigned", "fldBOeGZ4A33NNM1o", "OWNERLOGIN", "Formula in AT."),
    rec("FNCCUST - Financial Account No", "A>P", "Financial Account No", "fldAGfiflEytwOPlI", "ACCNAME", "Formula in AT."),
    rec("FNCCUST - Tax Code", "A>P", "Tax Code", "fldl7A0ajRvutueWk", "TAXCODE", "Formula in AT."),
    rec("FNCCUST - Invoice Type", "A>P", "Invoice Type", "fldOZsMtsorhrXu0I", "IVTYPE", "Formula in AT."),
    rec("FNCCUST - Payment Terms Code", "A>P", "Payment Terms Code", "fldaSfFLCnVHKAWti", "PAYCODE", "Formula in AT."),
    rec("FNCCUST - Finance Charge Code", "A>P", "Finance Charge Code", "fldFaoGchkTHlStjK", "INTERESTNAME", "Formula in AT."),
    rec("FNCCUST - Curr", "A>P", "Curr", "fldoGuJdTrWu2SApq", "CODE", "Formula in AT."),
    rec("FNCCUST - EDI_Y", "A>P", "EDI_Y", "fld1YPOQs19ILedr6", "PAYMENTLINK", "Formula in AT."),
    rec("FNCCUST - Pay to Bank Account", "A>P", "Pay to Bank Account", "fldTXArpiuPDDBqyI", "CASHNAME", "Formula in AT."),
]

# ═════════════════════════════════════════════════════════════════════════════
# FNCCUST P→A (1 field — most AT fields are formulas)
# ═════════════════════════════════════════════════════════════════════════════

FNCCUST_P2A = [
    rec("FNCCUST - Priority Customer Status", "P>A", "Priority Customer Status", "fldLhntR45Memi1na", "STATDES", "Only writable FNCCUST field in AT. All others are formulas."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Customer Contacts A→P (19 fields — CUSTPERSONNEL_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

CONTACTS_A2P = [
    rec("Customer Contacts - Clean First Name", "A>P", "Clean First Name", "fldP8R2YkLs59r0wc", "FIRSTNAME", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Clean Last Name", "A>P", "Clean Last Name", "fldUXuN2tPvLznoCn", "LASTNAME", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Clean Phone Number", "A>P", "Clean Phone Number", "fldAzbY4BolcbPQJu", "PHONENUM", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Clean Cell Phone", "A>P", "Clean Cell Phone", "fldszV8PW8VtGf8xR", "CELLPHONE", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Email_ID", "A>P", "Email_ID", "fldxHFcsanzzWf8fn", "EMAIL", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Consent to Receive Emails Output", "A>P", "Consent to Receive Emails Output", "fldM1eDjhTQbGqRp5", "MAILAPP", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Clean Full Name", "A>P", "Clean Full Name", "fldJgSoTTx2eRcgEg", "NAME", "CUSTPERSONNEL_SUBFORM. Match field for upsert."),
    rec("Customer Contacts - Clean Position", "A>P", "Clean Position", "fldti46pIk4p7cIXG", "POSITIONDES", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Status", "A>P", "Status", "fld9kBeWqtHTczqLj", "STATDES", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Linkedin", "A>P", "Linkedin", "fldamkilBbLhalu0X", "EXTFILENAME", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Sign Up Priority", "A>P", "Sign Up Priority", "fldy25icq78YSMcUk", "GWREGISTERED", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Main Contact Output", "A>P", "Main Contact Output", "fldlSY1amLBqTeFlr", "MAINPHONE", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Marketing Output", "A>P", "Marketing Output", "fldZlEv78f9iajYA3", "DEALFLAG", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Price Quote Output", "A>P", "Price Quote Output", "fldijSOdUr8HwqcCn", "CPROFFLAG", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Sales Order Output", "A>P", "Sales Order Output", "fldvQDpD5IFeZ7kaT", "ORDFLAG", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Shipment Output", "A>P", "Shipment Output", "fldkU2Li0iLA2fUUw", "DOCFLAG", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Invoice Output", "A>P", "Invoice Output", "fldDrc3EDhQwMr6v1", "CIVFLAG", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Cust. Statement Output", "A>P", "Cust. Statement Output", "fld9lL0as0ia7t9EK", "FNCFLAG", "CUSTPERSONNEL_SUBFORM."),
    rec("Customer Contacts - Outgoing Voucher Output", "A>P", "Outgoing Voucher Output", "fldxiahOYOBseWdbU", "CONSINGEEFLAG", "CUSTPERSONNEL_SUBFORM."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Customer Contacts P→A (15 fields — all write-if-empty)
# ═════════════════════════════════════════════════════════════════════════════

CONTACTS_P2A = [
    rec("Customer Contacts - First Name Input", "P>A", "First Name Input", "fldP8R2YkLs59r0wc", "FIRSTNAME", "CUSTPERSONNEL_SUBFORM. Write-if-empty."),
    rec("Customer Contacts - Last Name Input", "P>A", "Last Name Input", "fldUXuN2tPvLznoCn", "LASTNAME", "CUSTPERSONNEL_SUBFORM. Write-if-empty."),
    rec("Customer Contacts - Phone Number Input", "P>A", "Phone Number Input", "fldAzbY4BolcbPQJu", "PHONENUM", "CUSTPERSONNEL_SUBFORM. Write-if-empty."),
    rec("Customer Contacts - Cell Phone", "P>A", "Cell Phone", "fldszV8PW8VtGf8xR", "CELLPHONE", "CUSTPERSONNEL_SUBFORM. Write-if-empty."),
    rec("Customer Contacts - Email Input", "P>A", "Email Input", "fldxHFcsanzzWf8fn", "EMAIL", "CUSTPERSONNEL_SUBFORM. Write-if-empty."),
    rec("Customer Contacts - Position Input", "P>A", "Position Input", "fldti46pIk4p7cIXG", "POSITIONDES", "CUSTPERSONNEL_SUBFORM. Write-if-empty."),
    rec("Customer Contacts - Status", "P>A", "Status", "fld9kBeWqtHTczqLj", "STATDES", "CUSTPERSONNEL_SUBFORM. Write-if-empty."),
    rec("Customer Contacts - Main Contact", "P>A", "Main Contact", "fldlSY1amLBqTeFlr", "MAINPHONE", "CUSTPERSONNEL_SUBFORM. Transform: priority_yn. Write-if-empty."),
    rec("Customer Contacts - Marketing", "P>A", "Marketing", "fldZlEv78f9iajYA3", "DEALFLAG", "CUSTPERSONNEL_SUBFORM. Transform: priority_yn. Write-if-empty."),
    rec("Customer Contacts - Price Quote", "P>A", "Price Quote", "fldijSOdUr8HwqcCn", "CPROFFLAG", "CUSTPERSONNEL_SUBFORM. Transform: priority_yn. Write-if-empty."),
    rec("Customer Contacts - Sales Order", "P>A", "Sales Order", "fldvQDpD5IFeZ7kaT", "ORDFLAG", "CUSTPERSONNEL_SUBFORM. Transform: priority_yn. Write-if-empty."),
    rec("Customer Contacts - Shipment", "P>A", "Shipment", "fldkU2Li0iLA2fUUw", "DOCFLAG", "CUSTPERSONNEL_SUBFORM. Transform: priority_yn. Write-if-empty."),
    rec("Customer Contacts - Invoice", "P>A", "Invoice", "fldDrc3EDhQwMr6v1", "CIVFLAG", "CUSTPERSONNEL_SUBFORM. Transform: priority_yn. Write-if-empty."),
    rec("Customer Contacts - Cust. Statement", "P>A", "Cust. Statement", "fld9lL0as0ia7t9EK", "FNCFLAG", "CUSTPERSONNEL_SUBFORM. Transform: priority_yn. Write-if-empty."),
    rec("Customer Contacts - Outgoing Voucher", "P>A", "Outgoing Voucher", "fldxiahOYOBseWdbU", "CONSINGEEFLAG", "CUSTPERSONNEL_SUBFORM. Transform: priority_yn. Write-if-empty."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Customer Sites A→P (16 fields — CUSTDESTS_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

SITES_A2P = [
    rec("Customer Sites - Site Id", "A>P", "Site Id", "fld9bYfmERdt0T3Az", "CODE", "CUSTDESTS_SUBFORM. Match field for upsert."),
    rec("Customer Sites - Ship To Name Output", "A>P", "Ship To Name Output", "fldjOXa5Kg7lqGS46", "CODEDES", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Main Output", "A>P", "Main Output", "fldM2QoftKTXiTlVM", "MAINFLAG", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Address Line 1 Output", "A>P", "Address Line 1 Output", "fldmg9e952h8XyPFE", "ADDRESS", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Address Line 2 Output", "A>P", "Address Line 2 Output", "fldg3vOpR8GKGb1ca", "ADDRESS2", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Address Remarks Output", "A>P", "Address Remarks Output", "fldYNN7GhBxP7IIZV", "ADDRESS3", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - City Output", "A>P", "City Output", "fldc0TVQC5G7kd9oE", "STATE", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - State", "A>P", "State", "fldsLpLJ0H4mR4gmo", "STATECODE", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Zip", "A>P", "Zip", "fld25oyjjBA2C9i6x", "ZIP", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - EDI_USA", "A>P", "EDI_USA", "fldOWRPJ249yT1bjn", "COUNTRYNAME", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Primary receiver (from Customers)", "A>P", "Primary receiver (from Customers)", "fldQq0M0icXcpYU0L", "NAME", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Phone Output", "A>P", "Phone Output", "fldSUThO49r0D8hWf", "PHONE", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Zone Code", "A>P", "Zone Code", "fld47Y12aa9P4VGul", "ZONECODE", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Dist. Route Code", "A>P", "Dist. Route Code", "fldWy8cTBTkGnKp2L", "DISTRLINECODE", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - EDI_Carrier Code", "A>P", "EDI_Carrier Code", "fldsKJxxcpSnBgNnb", "SHIPPERNAME", "CUSTDESTS_SUBFORM."),
    rec("Customer Sites - Carrier Account Number (5) (from Customers)", "A>P", "Carrier Account Number (5) (from Customers)", "fldl2Tg6PyAyM4MYm", "SHIPACCOUNTNUM", "CUSTDESTS_SUBFORM."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Customer Sites P→A (6 fields — CUSTDESTS_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

SITES_P2A = [
    rec("Customer Sites - Ship To Name Input", "P>A", "Ship To Name Input", "fldjOXa5Kg7lqGS46", "CODEDES", "CUSTDESTS_SUBFORM. Write-if-empty."),
    rec("Customer Sites - Main", "P>A", "Main", "fldM2QoftKTXiTlVM", "MAINFLAG", "CUSTDESTS_SUBFORM. Transform: priority_yn. Always-overwrite."),
    rec("Customer Sites - Address Remarks Input", "P>A", "Address Remarks Input", "fldYNN7GhBxP7IIZV", "ADDRESS3", "CUSTDESTS_SUBFORM. Transform: strip_html. Write-if-empty."),
    rec("Customer Sites - Phone Input", "P>A", "Phone Input", "fldSUThO49r0D8hWf", "PHONE", "CUSTDESTS_SUBFORM. Write-if-empty."),
    rec("Customer Sites - Shipping Zone", "P>A", "Shipping Zone", "fld47Y12aa9P4VGul", "ZONECODE", "CUSTDESTS_SUBFORM. Transform: zone_lookup. Write-if-empty."),
    rec("Customer Sites - Main Delivery Method", "P>A", "Main Delivery Method", "fldsKJxxcpSnBgNnb", "SHIPPERNAME", "CUSTDESTS_SUBFORM. Transform: shipper_lookup. Write-if-empty."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Special Cust. Prices A→P (4 fields — CUSTPARTPRICE_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

SPECIAL_PRICES_A2P = [
    rec("Special Prices - SKU", "A>P", "SKU", "fld50GU0eYjvT2CC2", "PARTNAME", "CUSTPARTPRICE_SUBFORM. Match field for upsert. A→P only."),
    rec("Special Prices - Formatted Start Date", "A>P", "Formatted Start Date", "fldnogjeffyFmGV5y", "FROMDATE", "CUSTPARTPRICE_SUBFORM. A→P only."),
    rec("Special Prices - Expiration Date", "A>P", "Expiration Date", "fldzbrfYiaWW8eaV9", "EXPIRYDATE", "CUSTPARTPRICE_SUBFORM. A→P only."),
    rec("Special Prices - Special Price", "A>P", "Special Price", "fldOg7NcP6Vckpxak", "PRICE", "CUSTPARTPRICE_SUBFORM. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Price List A→P (1 field — CUSTPLIST_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

PRICE_LIST_A2P = [
    rec("Price List - Price List Code", "A>P", "Price List Code", "fldxB83teEXqu6aMq", "PLNAME", "CUSTPLIST_SUBFORM. Match field for upsert. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Delivery Days A→P (3 fields — CUSTWEEKDAY_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

DELIVERY_DAYS_A2P = [
    rec("Delivery Days - Days of Business", "A>P", "Days of Business", "fldFJJ3w9sPLZlLZi", "WEEKDAY", "CUSTWEEKDAY_SUBFORM. Comma-separated → one row per day. Transform: day_to_priority_int. A→P only."),
    rec("Delivery Days - Deliver After", "A>P", "Deliver After", "fldlUAvkUo7v9E4PG", "STARTTIME", "CUSTWEEKDAY_SUBFORM. Transform: format_time_24h. A→P only."),
    rec("Delivery Days - Deliver Before", "A>P", "Deliver Before", "fldaHJIvSdQMcva5S", "ENDTIME", "CUSTWEEKDAY_SUBFORM. Transform: format_time_24h. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Credit Application A→P (1 field — CUSTEXTFILE_SUBFORM)
# ═════════════════════════════════════════════════════════════════════════════

CREDIT_APP_A2P = [
    rec("Credit Application - Credit Application", "A>P", "Credit Application", "", "CUSTEXTFILE_SUBFORM", "Attachment field. Download + base64 encode + upload. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Shipment Remarks A→P (1 field — CUSTSHIPTEXT_SUBFORM, Pattern A)
# ═════════════════════════════════════════════════════════════════════════════

SHIP_REMARKS_A2P = [
    rec("Shipment Remarks - Cleaned Delivery Instructions", "A>P", "Cleaned Delivery Instructions", "", "TEXT", "CUSTSHIPTEXT_SUBFORM. Pattern A (single entity). Transform: strip_html. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Internal Remarks A→P (1 field — CUSTOMERSTEXT_SUBFORM, Pattern A)
# ═════════════════════════════════════════════════════════════════════════════

INT_REMARKS_A2P = [
    rec("Internal Remarks - Billing_Instructions_Output", "A>P", "Billing_Instructions_Output", "", "TEXT", "CUSTOMERSTEXT_SUBFORM. Pattern A (single entity). Transform: strip_html. A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Customer Prices A→P (1 field — PRICELIST parent entity)
# ═════════════════════════════════════════════════════════════════════════════

CUSTOMER_PRICES_A2P = [
    rec("Customer Prices - Price List Code", "A>P", "Price List Code", "fldPympnf6DCHeZvo", "PLNAME", "PRICELIST parent entity. Each record produces up to 3 output rows (Base, Whole, Lvl 3). A→P only."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Combine all records
# ═════════════════════════════════════════════════════════════════════════════

ALL_RECORDS = (
    CUSTOMERS_A2P + CUSTOMERS_P2A
    + FNCCUST_A2P + FNCCUST_P2A
    + CONTACTS_A2P + CONTACTS_P2A
    + SITES_A2P + SITES_P2A
    + SPECIAL_PRICES_A2P
    + PRICE_LIST_A2P
    + DELIVERY_DAYS_A2P
    + CREDIT_APP_A2P
    + SHIP_REMARKS_A2P
    + INT_REMARKS_A2P
    + CUSTOMER_PRICES_A2P
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
    print(f"  CUSTOMERS A→P: {len(CUSTOMERS_A2P)}")
    print(f"  CUSTOMERS P→A: {len(CUSTOMERS_P2A)}")
    print(f"  FNCCUST A→P: {len(FNCCUST_A2P)}")
    print(f"  FNCCUST P→A: {len(FNCCUST_P2A)}")
    print(f"  Customer Contacts A→P: {len(CONTACTS_A2P)}")
    print(f"  Customer Contacts P→A: {len(CONTACTS_P2A)}")
    print(f"  Customer Sites A→P: {len(SITES_A2P)}")
    print(f"  Customer Sites P→A: {len(SITES_P2A)}")
    print(f"  Special Prices A→P: {len(SPECIAL_PRICES_A2P)}")
    print(f"  Price List A→P: {len(PRICE_LIST_A2P)}")
    print(f"  Delivery Days A→P: {len(DELIVERY_DAYS_A2P)}")
    print(f"  Credit Application A→P: {len(CREDIT_APP_A2P)}")
    print(f"  Shipment Remarks A→P: {len(SHIP_REMARKS_A2P)}")
    print(f"  Internal Remarks A→P: {len(INT_REMARKS_A2P)}")
    print(f"  Customer Prices A→P: {len(CUSTOMER_PRICES_A2P)}")
    print()

    total = batch_create(ALL_RECORDS)
    print(f"\nDone! Created {total} records in API Matching table.")
