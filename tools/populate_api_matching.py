"""
Populate the API Matching table in Airtable with all Product workflow field mappings.
Uses batch creates (10 records per request) for efficiency.
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
# So production and test field IDs/names are the same for all synced fields.


def rec(name, direction, at_field, at_id, priority_field, comment=""):
    """Build a single record dict."""
    return {
        "fields": {
            "Name": name,
            "Direction": direction,
            "Category": "Products",
            "Production Airtable Field Name": at_field,
            "API Test Airtable Field Name": at_field,  # Same (duplicated base)
            "Production Airtable Field Id": at_id,
            "API Test Airtable Field Id": at_id,  # Same (duplicated base)
            "Priority Field Name": priority_field,
            "Claude Comments": comment,
            "Status": "Pending Approval",
        }
    }


# ═════════════════════════════════════════════════════════════════════════════
# LOGPART A→P (29 fields)
# ═════════════════════════════════════════════════════════════════════════════

LOGPART_A2P = [
    rec("LOGPART - SKU Trim (EDI)", "A>P", "SKU Trim (EDI)", "fldiGjELA26tbBi8f", "PARTNAME", "Primary key. Required. Formula field (read-only in AT)."),
    rec("LOGPART - Brand + Product Title + Net Weight + Case Pack", "A>P", "Brand + Product Title + Net Weight + Case Pack", "fldCh4dhezc1ollsM", "PARTDES", "Concatenated description."),
    rec("LOGPART - Case Pack", "A>P", "Case Pack", "fld1zWWJwiwAnbq6X", "SPEC1", "Transform: clean (sent as string to Priority)."),
    rec("LOGPART - Product Net Weight Input", "A>P", "Product Net Weight Input", "fldRK7RwBeu0vCBCO", "SPEC2", "Transform: clean (sent as string to Priority)."),
    rec("LOGPART - Product Net Weight Unit Input", "A>P", "Product Net Weight Unit Input", "fld7UhpTbJlgwuZH6", "SPEC3", ""),
    rec("LOGPART - Brand", "A>P", "Brand", "flderGjy8obS1Qpqk", "SPEC4", ""),
    rec("LOGPART - Kelsey_Categories", "A>P", "Kelsey_Categories", "fldurKZqu5KJYH2ri", "SPEC5", ""),
    rec("LOGPART - Kelsey_Subcategories", "A>P", "Kelsey_Subcategories", "fldbQDZAdqSu9wg8E", "SPEC6", ""),
    rec("LOGPART - Catalog Status", "A>P", "Catalog Status", "fldDb9LcQpdhNmKYU", "SPEC7", "Also in status-only mode."),
    rec("LOGPART - Inventory Status", "A>P", "Inventory Status", "fldATOgqP5Y8i7nT7", "SPEC8", "Also in status-only mode."),
    rec("LOGPART - Perishable", "A>P", "Perishable", "fldhSuR2LjoyaCXWu", "SPEC9", ""),
    rec("LOGPART - Retail", "A>P", "Retail", "fldMm970r8SxD6ojA", "SPEC10", ""),
    rec("LOGPART - Feature_Individual Portions", "A>P", "Feature_Individual Portions", "fldlA4nRFKiaCBXLW", "SPEC11", ""),
    rec("LOGPART - Staff Pick", "A>P", "Staff Pick", "fldXVC9aFsJ1bxWcC", "SPEC12", ""),
    rec("LOGPART - Storage", "A>P", "Storage", "fldsFJrCXXKur6P1F", "SPEC13", ""),
    rec("LOGPART - Availability Priority Output", "A>P", "Availability Priority Output", "fldouLxtxbUhY0Caq", "SPEC14", "Formula field in AT."),
    rec("LOGPART - Direct Import", "A>P", "Direct Import", "fldlqTQO2U5V2d46l", "SPEC15", ""),
    rec("LOGPART - Vendor SKU Trim", "A>P", "Vendor SKU Trim", "fldOE3NlqM1kIPMdk", "SPEC16", ""),
    rec("LOGPART - Buy_Sell Unit (Priority)", "A>P", "Buy_Sell Unit (Priority)", "fldRgdNphJxTXxivY", "PUNITNAME", "create_only=True. Cannot be changed via PATCH."),
    rec("LOGPART - Base Price", "A>P", "Base Price", "fldPwojUSmVpiHA2C", "BASEPLPRICE", "Transform: format_price. Formula field in AT."),
    rec("LOGPART - Base Price Currency", "A>P", "Base Price Currency", "fldpYviH2hsfvEEZQ", "BASEPLCODE", "Formula field in AT."),
    rec("LOGPART - Priority Status", "A>P", "Priority Status", "fldqt4AnlaxYgYhXw", "STATDES", "Also in status-only mode."),
    rec("LOGPART - V-Vendor ID (from Preferred Vendor)", "A>P", "V-Vendor ID (from Preferred Vendor)", "fld1EMYPIfS1lW9GC", "SUPNAME", "Linked record lookup in AT."),
    rec("LOGPART - Standard Cost", "A>P", "Standard Cost", "fldjS688T5OawrcjM", "PRICE", "Transform: format_price. Lookup field in AT."),
    rec("LOGPART - LVL 2 SALE PRICE", "A>P", "LVL 2 SALE PRICE (from Price Import)", "fld9Ku04QU2bJiBYX", "WSPLPRICE", "Transform: format_price. Lookup field in AT."),
    rec("LOGPART - Conversion Ratio", "A>P", "Conversion Ratio", "fldKhMa20lTbBmULi", "CONV", "Transform: to_int. Formula field in AT."),
    rec("LOGPART - Family (Number from Product Type)", "A>P", "Family (Number from Product Type)", "fldeg7E2g2l8xe6NZ", "FAMILYNAME", "Formula field in AT (computes code from Product Type)."),
    rec("LOGPART - Type (P/R/O)", "A>P", "Type (P/R/O)", "fld5uQwSdX7lEn0PO", "TYPE", ""),
    rec("LOGPART - Allocate Inventory", "A>P", "Allocate Inventory", "fldjX6kGZJUmzrf48", "RESERVFLAG", "Formula field in AT."),
]

# ═════════════════════════════════════════════════════════════════════════════
# LOGPART P→A (19 fields)
# ═════════════════════════════════════════════════════════════════════════════

LOGPART_P2A = [
    rec("LOGPART - Product Title Priority Input", "P>A", "Product Title Priority Input", "fldDipTGNBEwgqz4e", "PARTDES", "create_only=True. Only set when creating new AT records."),
    rec("LOGPART - Case Pack", "P>A", "Case Pack", "fld1zWWJwiwAnbq6X", "SPEC1", "Transform: to_float (AT number field)."),
    rec("LOGPART - Product Net Weight Input", "P>A", "Product Net Weight Input", "fldRK7RwBeu0vCBCO", "SPEC2", "Transform: to_float (AT number field)."),
    rec("LOGPART - Product Net Weight Unit Input", "P>A", "Product Net Weight Unit Input", "fld7UhpTbJlgwuZH6", "SPEC3", ""),
    rec("LOGPART - Brand", "P>A", "Brand", "flderGjy8obS1Qpqk", "SPEC4", ""),
    rec("LOGPART - Kelsey_Categories", "P>A", "Kelsey_Categories", "fldurKZqu5KJYH2ri", "SPEC5", ""),
    rec("LOGPART - Kelsey_Subcategories", "P>A", "Kelsey_Subcategories", "fldbQDZAdqSu9wg8E", "SPEC6", ""),
    rec("LOGPART - Catalog Status", "P>A", "Catalog Status", "fldDb9LcQpdhNmKYU", "SPEC7", "Also in status-only mode."),
    rec("LOGPART - Inventory Status", "P>A", "Inventory Status", "fldATOgqP5Y8i7nT7", "SPEC8", "Also in status-only mode."),
    rec("LOGPART - Perishable", "P>A", "Perishable", "fldhSuR2LjoyaCXWu", "SPEC9", ""),
    rec("LOGPART - Retail", "P>A", "Retail", "fldMm970r8SxD6ojA", "SPEC10", ""),
    rec("LOGPART - Feature_Individual Portions", "P>A", "Feature_Individual Portions", "fldlA4nRFKiaCBXLW", "SPEC11", ""),
    rec("LOGPART - Staff Pick", "P>A", "Staff Pick", "fldXVC9aFsJ1bxWcC", "SPEC12", ""),
    rec("LOGPART - Storage", "P>A", "Storage", "fldsFJrCXXKur6P1F", "SPEC13", ""),
    rec("LOGPART - Direct Import", "P>A", "Direct Import", "fldlqTQO2U5V2d46l", "SPEC15", ""),
    rec("LOGPART - Priority Status", "P>A", "Priority Status", "fldqt4AnlaxYgYhXw", "STATDES", "Also in status-only mode."),
    rec("LOGPART - Type (P/R/O)", "P>A", "Type (P/R/O)", "fld5uQwSdX7lEn0PO", "TYPE", ""),
    rec("LOGPART - Product Type", "P>A", "Product Type", "fldxgGB8vwPpxOrYL", "FAMILYNAME", "Linked-table lookup: FAMILY_LOG (code→description). Writes to writable singleSelect."),
    rec("LOGPART - Preferred Vendor", "P>A", "Preferred Vendor", "fld35rCJHCF81Weaa", "SUPNAME", "Linked record to Vendors table. Auto-creates stub vendor if missing."),
]

# ═════════════════════════════════════════════════════════════════════════════
# FNCPART A→P (10 fields, includes primary key)
# ═════════════════════════════════════════════════════════════════════════════

FNCPART_A2P = [
    rec("FNCPART - SKU Trim (EDI)", "A>P", "SKU Trim (EDI)", "fldiGjELA26tbBi8f", "PARTNAME", "Primary key. Links to LOGPART."),
    rec("FNCPART - Accounting Family", "A>P", "Accounting Family", "fldlGOIIFAYsfACLV", "ACCFAMILYNAME", "Formula field in AT (computes code)."),
    rec("FNCPART - Filemaker Weight", "A>P", "Filemaker Weight", "fldmWMrWRPRf80YvB", "WEIGHT", "Transform: to_float."),
    rec("FNCPART - HTS Code", "A>P", "HTS Code", "fldLqUOHkGk3S6oqO", "CUSTOMSITEMNAME", ""),
    rec("FNCPART - Shipping Cost Type", "A>P", "Shipping Cost Type", "fldw7Z2NVdmLuASMS", "DUTYPERCENTTYPE", ""),
    rec("FNCPART - Tax_Group_Code", "A>P", "Tax_Group_Code", "fldKDZwbsBUocZxPO", "TAXGROUPCODE", ""),
    rec("FNCPART - Country of Origin", "A>P", "Country of Origin", "fldc5ymwTg4cIFHLF", "COUNTRYNAME", ""),
    rec("FNCPART - Pallet TI", "A>P", "Pallet TI", "flduKMzFab3at76IE", "SAVR_PALLETTI", "Transform: to_float."),
    rec("FNCPART - Pallet HI", "A>P", "Pallet HI", "fld5zkZQyoS8YopbD", "SAVR_PALLETHI", "Transform: to_float."),
    rec("FNCPART - Case / Pallet", "A>P", "Case / Pallet", "fld5zvA4EmDUlXlRp", "SAVR_CASEPERPALLET", "Transform: to_float."),
]

# ═════════════════════════════════════════════════════════════════════════════
# FNCPART P→A (8 fields)
# ═════════════════════════════════════════════════════════════════════════════

FNCPART_P2A = [
    rec("FNCPART - Kelsey_Types/Accounting Family", "P>A", "Kelsey_Types/Accounting Family", "fldA9ti7pRdS5VpuT", "ACCFAMILYNAME", "Linked-table lookup: FAMILY_FNC (code→description). Writes to writable singleSelect."),
    rec("FNCPART - HTS Code", "P>A", "HTS Code", "fldLqUOHkGk3S6oqO", "CUSTOMSITEMNAME", ""),
    rec("FNCPART - Shipping Cost Type", "P>A", "Shipping Cost Type", "fldw7Z2NVdmLuASMS", "DUTYPERCENTTYPE", ""),
    rec("FNCPART - Tax_Group_Code", "P>A", "Tax_Group_Code", "fldKDZwbsBUocZxPO", "TAXGROUPCODE", ""),
    rec("FNCPART - Country of Origin", "P>A", "Country of Origin", "fldc5ymwTg4cIFHLF", "COUNTRYNAME", ""),
    rec("FNCPART - Pallet TI", "P>A", "Pallet TI", "flduKMzFab3at76IE", "SAVR_PALLETTI", "Transform: to_float."),
    rec("FNCPART - Pallet HI", "P>A", "Pallet HI", "fld5zkZQyoS8YopbD", "SAVR_PALLETHI", "Transform: to_float."),
    rec("FNCPART - Case / Pallet", "P>A", "Case / Pallet", "fld5zvA4EmDUlXlRp", "SAVR_CASEPERPALLET", "Transform: to_float."),
]

# ═════════════════════════════════════════════════════════════════════════════
# PRDPART A→P (8 fields, includes primary key)
# ═════════════════════════════════════════════════════════════════════════════

PRDPART_A2P = [
    rec("PRDPART - SKU Trim (EDI)", "A>P", "SKU Trim (EDI)", "fldiGjELA26tbBi8f", "PARTNAME", "Primary key. Links to LOGPART."),
    rec("PRDPART - Purchase Lead Time", "A>P", "Purchase Lead Time", "fldoSaWmwYoObgF3X", "PLEADTIME", "Transform: to_int."),
    rec("PRDPART - Shipping Days", "A>P", "Shipping Days", "fldUvBExbJD7L07lX", "SHIPMDAYS", "Transform: to_int."),
    rec("PRDPART - Safety Stock", "A>P", "Safety Stock", "fldnchhLM2lTsdEmS", "INVLEVEL", "Transform: to_int."),
    rec("PRDPART - Main Buyer Priority Code", "A>P", "Main Buyer Priority Code", "fld3x0njsSny4q6hx", "USERLOGIN", "Formula field in AT (read-only)."),
    rec("PRDPART - V-Vendor ID (from Preferred Vendor)", "A>P", "V-Vendor ID (from Preferred Vendor)", "fld1EMYPIfS1lW9GC", "SUPNAME", "Lookup field in AT (read-only)."),
    rec("PRDPART - Min for Order", "A>P", "Min for Order", "fldNCOEZH1OF0TCGk", "MINPURQUANT", "Transform: to_int."),
    rec("PRDPART - Increment for Order", "A>P", "Increment for Order", "fldAOemtBN449wYPU", "INCREMENT", "Transform: to_int."),
]

# ═════════════════════════════════════════════════════════════════════════════
# PRDPART P→A (5 fields)
# ═════════════════════════════════════════════════════════════════════════════

PRDPART_P2A = [
    rec("PRDPART - Purchase Lead Time", "P>A", "Purchase Lead Time", "fldoSaWmwYoObgF3X", "PLEADTIME", "Transform: to_int."),
    rec("PRDPART - Shipping Days", "P>A", "Shipping Days", "fldUvBExbJD7L07lX", "SHIPMDAYS", "Transform: to_int."),
    rec("PRDPART - Safety Stock", "P>A", "Safety Stock", "fldnchhLM2lTsdEmS", "INVLEVEL", "Transform: to_int."),
    rec("PRDPART - Min for Order", "P>A", "Min for Order", "fldNCOEZH1OF0TCGk", "MINPURQUANT", "Transform: to_int."),
    rec("PRDPART - Increment for Order", "P>A", "Increment for Order", "fldAOemtBN449wYPU", "INCREMENT", "Transform: to_int."),
]

# ═════════════════════════════════════════════════════════════════════════════
# ALLERGENS A→P (23 fields)
# ═════════════════════════════════════════════════════════════════════════════

ALLERGENS_A2P = [
    rec("Allergens - Allergen Present", "A>P", "Allergen_Allergen Present", "fldme5BPKgpTRCisB", "HASALLERGEN", "SAVR_ALLERGENS_SUBFORM. Pattern A (single entity)."),
    rec("Allergens - Eggs", "A>P", "Allergen_Eggs", "fldOCGoSAYkf3HQca", "EGGS", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Dairy", "A>P", "Allergen_Dairy", "fldVP6zqqj7IFTavX", "DAIRY", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Fish", "A>P", "Allergen_Fish", "fldsZno9KAk98vv5F", "FISH", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Peanut", "A>P", "Allergen_Peanut", "fldgGUfEJjB7x5w0O", "PEANUT", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Sesame", "A>P", "Allergen_Sesame", "fldWlixvEtuu5E9HO", "SESAME", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Shellfish", "A>P", "Allergen_Shellfish", "fldFeMlvvDzR4R880", "SHELLFISH", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Soybean", "A>P", "Allergen_Soybean", "fldBv39ryUOCtaJFs", "SOYBEAN", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Tree Nuts", "A>P", "Allergen_Tree Nuts", "fldkGD69JIqtTGOWD", "NUTS", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Wheat", "A>P", "Allergen_Wheat", "fldOTslxRXLdDMe4w", "WHEAT", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Feature Present", "A>P", "Feature_Feature Present", "fldWYusMhacIoxyGA", "HASFEATURE", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Gluten Free", "A>P", "Feature_Gluten Free", "fldwhzExKrWiqhWPd", "GLUTENFREE", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Organic", "A>P", "Feature_Organic", "fldekjulU3D3eA1Vp", "ORGANIC", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Kosher", "A>P", "Feature_Kosher", "fldD7xyvmBU8NDaYH", "KOSHER", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Vegan", "A>P", "Feature_Vegan", "fld6VtJocYdD4Z0AK", "VEGAN", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Halal", "A>P", "Feature_Halal", "fldF4XPKMHq33hRc5", "HALAL", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Non GMO", "A>P", "Feature_Non GMO", "fldQVpyXmShI2tLiK", "NONGMO", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Identity Protected", "A>P", "Feature_Identity Protected", "fldFOCHgsuVFkf0I4", "IDPROTECT", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - GFSI Certified", "A>P", "GFSI Certified", "fldVvu86eNEVZXofS", "GFSI", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Glass Packaging", "A>P", "Glass Packaging", "fld9LpRFM1iFuxEBH", "PACKAGING", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Prop. 65 Warning", "A>P", "Prop. 65 Warning", "fldQHNowsLlMC8NMv", "PROP65", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Calif. Ass. Bill 418", "A>P", "Calif. Ass. Bill 418", "fldXwUO50begWvnan", "CABILL418", "SAVR_ALLERGENS_SUBFORM."),
    rec("Allergens - Traceability Type Output", "A>P", "Traceability Type Output", "fldAJpbCG0JAB7oHO", "TRACEABILITY", "SAVR_ALLERGENS_SUBFORM."),
]

# ═════════════════════════════════════════════════════════════════════════════
# ALLERGENS P→A (23 fields — same fields, reverse direction)
# ═════════════════════════════════════════════════════════════════════════════

ALLERGENS_P2A = [
    rec("Allergens - Allergen Present", "P>A", "Allergen_Allergen Present", "fldme5BPKgpTRCisB", "HASALLERGEN", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn (Y→Yes, N→No)."),
    rec("Allergens - Eggs", "P>A", "Allergen_Eggs", "fldOCGoSAYkf3HQca", "EGGS", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Dairy", "P>A", "Allergen_Dairy", "fldVP6zqqj7IFTavX", "DAIRY", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Fish", "P>A", "Allergen_Fish", "fldsZno9KAk98vv5F", "FISH", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Peanut", "P>A", "Allergen_Peanut", "fldgGUfEJjB7x5w0O", "PEANUT", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Sesame", "P>A", "Allergen_Sesame", "fldWlixvEtuu5E9HO", "SESAME", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Shellfish", "P>A", "Allergen_Shellfish", "fldFeMlvvDzR4R880", "SHELLFISH", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Soybean", "P>A", "Allergen_Soybean", "fldBv39ryUOCtaJFs", "SOYBEAN", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Tree Nuts", "P>A", "Allergen_Tree Nuts", "fldkGD69JIqtTGOWD", "NUTS", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Wheat", "P>A", "Allergen_Wheat", "fldOTslxRXLdDMe4w", "WHEAT", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Feature Present", "P>A", "Feature_Feature Present", "fldWYusMhacIoxyGA", "HASFEATURE", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Gluten Free", "P>A", "Feature_Gluten Free", "fldwhzExKrWiqhWPd", "GLUTENFREE", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Organic", "P>A", "Feature_Organic", "fldekjulU3D3eA1Vp", "ORGANIC", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Kosher", "P>A", "Feature_Kosher", "fldD7xyvmBU8NDaYH", "KOSHER", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Vegan", "P>A", "Feature_Vegan", "fld6VtJocYdD4Z0AK", "VEGAN", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Halal", "P>A", "Feature_Halal", "fldF4XPKMHq33hRc5", "HALAL", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Non GMO", "P>A", "Feature_Non GMO", "fldQVpyXmShI2tLiK", "NONGMO", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Identity Protected", "P>A", "Feature_Identity Protected", "fldFOCHgsuVFkf0I4", "IDPROTECT", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - GFSI Certified", "P>A", "GFSI Certified", "fldVvu86eNEVZXofS", "GFSI", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Glass Packaging", "P>A", "Glass Packaging", "fld9LpRFM1iFuxEBH", "PACKAGING", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Prop. 65 Warning", "P>A", "Prop. 65 Warning", "fldQHNowsLlMC8NMv", "PROP65", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Calif. Ass. Bill 418", "P>A", "Calif. Ass. Bill 418", "fldXwUO50begWvnan", "CABILL418", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
    rec("Allergens - Traceability Type Output", "P>A", "Traceability Type Output", "fldAJpbCG0JAB7oHO", "TRACEABILITY", "SAVR_ALLERGENS_SUBFORM. P→A transform: priority_yn."),
]

# ═════════════════════════════════════════════════════════════════════════════
# SHELF LIVES A→P (3 fields — from Shelf Lives table, not Products)
# ═════════════════════════════════════════════════════════════════════════════

SHELF_A2P = [
    rec("Shelf Lives - Type Label (Custom)", "A>P", "Type Label (Custom)", "fldm7AVt5zm0H9Pt9", "TYPE", "SAVR_PARTSHELF_SUBFORM. Pattern B (multi + URL key). Source: Shelf Lives table."),
    rec("Shelf Lives - Shelf Life Input", "A>P", "Shelf Life Input", "fldCahhZnYomym7KM", "NUMBER", "SAVR_PARTSHELF_SUBFORM. Transform: to_int. Source: Shelf Lives table."),
    rec("Shelf Lives - Shelf Life Unit Input", "A>P", "Shelf Life Unit Input", "fldJAMWuzPKE7rByk", "TIMEUNIT", "SAVR_PARTSHELF_SUBFORM. Source: Shelf Lives table."),
]

# ═════════════════════════════════════════════════════════════════════════════
# SHELF LIVES P→A (3 fields — writes back to Shelf Lives table)
# ═════════════════════════════════════════════════════════════════════════════

SHELF_P2A = [
    rec("Shelf Lives - Type", "P>A", "Type", "fldPayhs9G6nGH5Ox", "TYPE", "SAVR_PARTSHELF_SUBFORM. Writes to 'Type' singleSelect in Shelf Lives table."),
    rec("Shelf Lives - Shelf Life Input", "P>A", "Shelf Life Input", "fldCahhZnYomym7KM", "NUMBER", "SAVR_PARTSHELF_SUBFORM. Transform: to_int. Writes to Shelf Lives table."),
    rec("Shelf Lives - Shelf Life Unit Input", "P>A", "Shelf Life Unit Input", "fldJAMWuzPKE7rByk", "TIMEUNIT", "SAVR_PARTSHELF_SUBFORM. Writes to Shelf Lives table."),
]

# ═════════════════════════════════════════════════════════════════════════════
# PRICE LISTS A→P (9 fields — 3 levels × 3 per-level fields + 3 shared)
# ═════════════════════════════════════════════════════════════════════════════

PRICE_LISTS_A2P = [
    rec("Price Lists - Lvl 1 Price List Code", "A>P", "Lvl 1 Price List Code", "fldrTfEeH1ADgfMlk", "PLNAME", "PARTINCUSTPLISTS_SUBFORM. Pattern C (deep PATCH). Default: 'Base'."),
    rec("Price Lists - LVL 1 SALE PRICE", "A>P", "LVL 1 SALE PRICE (from Price Import)", "fld30xcHt2DgOAtkR", "PRICE", "PARTINCUSTPLISTS_SUBFORM. Transform: format_price. Lookup field in AT."),
    rec("Price Lists - Lvl 2 Price List Code", "A>P", "Lvl 2 Price List Code", "fldBvFfxqXmOJO5lF", "PLNAME", "PARTINCUSTPLISTS_SUBFORM. Default: 'Whole'."),
    rec("Price Lists - LVL 2 SALE PRICE", "A>P", "LVL 2 SALE PRICE (from Price Import)", "fld9Ku04QU2bJiBYX", "PRICE", "PARTINCUSTPLISTS_SUBFORM. Transform: format_price. Lookup field in AT."),
    rec("Price Lists - Lvl 3 Price List Code", "A>P", "Lvl 3 Price List Code", "fldjxbX4zPxqc4TBt", "PLNAME", "PARTINCUSTPLISTS_SUBFORM. Default: 'Lvl 3'."),
    rec("Price Lists - LVL 3 SALE PRICE", "A>P", "LVL 3 SALE PRICE (from Price Import)", "fld8xjGcFsoLKefQN", "PRICE", "PARTINCUSTPLISTS_SUBFORM. Transform: format_price. Lookup field in AT."),
    rec("Price Lists - EDI $", "A>P", "EDI $", "fldalxbwNuJkfe3hY", "CODE", "PARTINCUSTPLISTS_SUBFORM. Shared across all 3 levels. Default: '$'."),
    rec("Price Lists - EDI Price Quantity", "A>P", "EDI Price Quantity", "fld0qRVk6cgwPrFjL", "QUANT", "PARTINCUSTPLISTS_SUBFORM. Transform: to_int. Shared. Default: 1."),
    rec("Price Lists - Buy_Sell Unit (Priority)", "A>P", "Buy_Sell Unit (Priority)", "fldRgdNphJxTXxivY", "UNITNAME", "PARTINCUSTPLISTS_SUBFORM. Shared. Also in main LOGPART mapping."),
]

# ═════════════════════════════════════════════════════════════════════════════
# BIN LOCATIONS A→P (2 fields)
# ═════════════════════════════════════════════════════════════════════════════

BINS_A2P = [
    rec("Bins - EDI Main", "A>P", "EDI Main", "fldVWfuOMYZumA02w", "WARHSNAME", "PARTLOCATIONS_SUBFORM. Pattern C (deep PATCH). Default: 'Main'."),
    rec("Bins - Simplified Bin Location", "A>P", "Simplified Bin Location (from Bin # Priority)", "fldtSNAJWDOy9k0oP", "LOCNAME", "PARTLOCATIONS_SUBFORM. Lookup field in AT."),
]

# ═════════════════════════════════════════════════════════════════════════════
# Combine all records
# ═════════════════════════════════════════════════════════════════════════════

ALL_RECORDS = (
    LOGPART_A2P + LOGPART_P2A
    + FNCPART_A2P + FNCPART_P2A
    + PRDPART_A2P + PRDPART_P2A
    + ALLERGENS_A2P + ALLERGENS_P2A
    + SHELF_A2P + SHELF_P2A
    + PRICE_LISTS_A2P
    + BINS_A2P
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
        # Rate limit: 5 req/sec
        time.sleep(0.25)
    return created


if __name__ == "__main__":
    print(f"Total records to create: {len(ALL_RECORDS)}")
    print(f"  LOGPART A→P: {len(LOGPART_A2P)}")
    print(f"  LOGPART P→A: {len(LOGPART_P2A)}")
    print(f"  FNCPART A→P: {len(FNCPART_A2P)}")
    print(f"  FNCPART P→A: {len(FNCPART_P2A)}")
    print(f"  PRDPART A→P: {len(PRDPART_A2P)}")
    print(f"  PRDPART P→A: {len(PRDPART_P2A)}")
    print(f"  Allergens A→P: {len(ALLERGENS_A2P)}")
    print(f"  Allergens P→A: {len(ALLERGENS_P2A)}")
    print(f"  Shelf Lives A→P: {len(SHELF_A2P)}")
    print(f"  Shelf Lives P→A: {len(SHELF_P2A)}")
    print(f"  Price Lists A→P: {len(PRICE_LISTS_A2P)}")
    print(f"  Bins A→P: {len(BINS_A2P)}")
    print()

    total = batch_create(ALL_RECORDS)
    print(f"\nDone! Created {total} records in API Matching table.")
