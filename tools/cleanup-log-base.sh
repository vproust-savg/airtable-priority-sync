#!/bin/bash
# Deletes all tables in the Clean Data API TEST base EXCEPT Sync Runs and Sync Errors.
# Run this once, then delete this script.
#
# Usage: bash tools/cleanup_log_base.sh

# Load credentials from .env
set -a
source "$(dirname "$0")/../.env"
set +a

TOKEN="${AIRTABLE_SYNC_LOG_TOKEN:?Missing AIRTABLE_SYNC_LOG_TOKEN in .env}"
BASE_ID="${AIRTABLE_SYNC_LOG_BASE_ID:?Missing AIRTABLE_SYNC_LOG_BASE_ID in .env}"

# Tables to DELETE (48 tables — everything except Sync Runs and Sync Errors)
TABLES=(
  "tbljnPqn35V3raJ1c"  # Customers
  "tblzqEyjb13MneKrU"  # Inventory_Pallet Type
  "tblJb4wJxSucTBa2E"  # Customer Contacts 2025
  "tbl3Xmj7sc0lPfSzB"  # Customer Price List
  "tblSObjhLCOiaKTN6"  # Customer Price List v2
  "tblDyi7KFCeZxYBfX"  # Customer Delivery Days
  "tblmsNME6yT76k3HO"  # Old_Customer Contacts
  "tblXaK8CFm1ZKhQb0"  # New_Customer Contacts
  "tbl6xsKAS7MArVQse"  # Special Cust. Prices
  "tblvenpZXbcgGz8Ry"  # Vendors
  "tblhSVfliCzSvNEuj"  # Vendor Contacts
  "tblue2O91whogLk5o"  # Team
  "tblnEyDO3fEy7Jsfq"  # Vendor Attachment
  "tblFU4YGTbTrHhbow"  # Vendor Products
  "tbleL8PK918UIukFv"  # Vendor Price List
  "tblP7PYoKx72VIpVi"  # Activity
  "tbl3CbW2LT5XuZb3x"  # BPM Rules
  "tblHx4XKBJqkWFxMw"  # Vendor Imports
  "tblvriHX4AR41vJRs"  # SOW Sections
  "tblyHFlDlXDHfxXuQ"  # User IDs
  "tblcGUDtSKuYlulBN"  # Part Availability
  "tblVvKk3AXVJXnMhk"  # Tax Groups
  "tblCOhiPTZwWAOIoq"  # Master Database Sync
  "tblsdrrr4TGDxh6aN"  # Clearance
  "tblBXiiy3A4QLvsFb"  # Website Collection
  "tbldyWLi1VYPbloW9"  # Metafields
  "tbl9LPFZV54kTucId"  # Best For
  "tblbj6sgqQm4zKwBg"  # Web Pages
  "tblyBaFOX1dfVQz7E"  # Warehouse Bins
  "tblpO1xKitjFxCOQm"  # Mailing
  "tbl7mCXcNB5HX3ytZ"  # Customer Groups
  "tblq5SO2lKAuboTFI"  # Weight Import
  "tblvfoH6WGsRhxNiH"  # Returns
  "tblEJpSCHgBS3Zk0d"  # Return_Line Items
  "tblQmZQdaRrQU6TYZ"  # Purchase Orders
  "tblPMUm9oy2mnqqnU"  # Sales Order
  "tbllOaj2ZpsCPF7w5"  # GRVs
  "tbl927wrE2wBmfykM"  # General Ledger
  "tblKClyhZQ7X9iUpQ"  # Journal Entries
  "tblBgTcepN8Ovhlqm"  # Kelsey Import
  "tblP8YDXtDkuoU6Ul"  # Customer Leads
  "tblUC98rzU6pyX6xT"  # Supplier Application
  "tblgWcqQp6peGboze"  # Product Variants
  "tblBstRHx1MQQ9Bub"  # Lot Numbers
  "tblaQcz049K489mJn"  # Sparklayer_Price_Lists
  "tbli4QlE64rfUP3X7"  # Fields in Base
  "tblv9XfF2bFT8Mofs"  # Fields in Base copy
  "tblbGAlFk6RYnjeAz"  # Lot Numbers Fixing Data
)

echo "Deleting ${#TABLES[@]} tables from Clean Data API TEST base..."
echo "Keeping: Sync Runs (tblSN1oQUP18mDq0K) and Sync Errors (tbljeM0YaEIWJRs63)"
echo ""

for TABLE_ID in "${TABLES[@]}"; do
  echo -n "Deleting $TABLE_ID... "
  RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X DELETE \
    "https://api.airtable.com/v0/meta/bases/$BASE_ID/tables/$TABLE_ID" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json")

  if [ "$RESPONSE" = "200" ]; then
    echo "✅ Deleted"
  else
    echo "❌ HTTP $RESPONSE"
  fi

  # Small delay to avoid rate limits
  sleep 0.3
done

echo ""
echo "Done! Only Sync Runs and Sync Errors should remain."
