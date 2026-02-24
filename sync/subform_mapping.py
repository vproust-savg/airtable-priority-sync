"""
Sub-form field mappings for Priority ERP sub-entities.
Maps Airtable fields to Priority sub-form fields for:
  - SAVR_ALLERGENS_SUBFORM  (allergens & features, single-record per product)
  - SAVR_PARTSHELF_SUBFORM  (shelf lives, multi-record per product)
  - PARTINCUSTPLISTS_SUBFORM (price lists, multi-record per product)
  - PARTLOCATIONS_SUBFORM   (bin locations, single-record per product)

Source of truth: .tmp/convert_to_python.py lines 166-221,
cross-referenced with tools/3. SG Part All Priority Interface.txt.
"""

from __future__ import annotations

from typing import Any

from sync.models import FieldMapping
from sync.utils import clean, format_price, to_int

# ── Transform function registry (reuse from field_mapping) ────────────────────

_TRANSFORMS = {
    "clean": clean,
    "format_price": format_price,
    "to_int": to_int,
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. ALLERGENS & FEATURES (23 fields → SAVR_ALLERGENS_SUBFORM)
#    Pattern: single-record sub-form (1 allergen record per product)
# ─────────────────────────────────────────────────────────────────────────────

ALLERGEN_SUBFORM_NAME = "SAVR_ALLERGENS_SUBFORM"

ALLERGEN_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(airtable_field="Allergen_Allergen Present", priority_field="HASALLERGEN", transform="clean"),
    FieldMapping(airtable_field="Allergen_Eggs", priority_field="EGGS", transform="clean"),
    FieldMapping(airtable_field="Allergen_Dairy", priority_field="DAIRY", transform="clean"),
    FieldMapping(airtable_field="Allergen_Fish", priority_field="FISH", transform="clean"),
    FieldMapping(airtable_field="Allergen_Peanut", priority_field="PEANUT", transform="clean"),
    FieldMapping(airtable_field="Allergen_Sesame", priority_field="SESAME", transform="clean"),
    FieldMapping(airtable_field="Allergen_Shellfish", priority_field="SHELLFISH", transform="clean"),
    FieldMapping(airtable_field="Allergen_Soybean", priority_field="SOYBEAN", transform="clean"),
    FieldMapping(airtable_field="Allergen_Tree Nuts", priority_field="NUTS", transform="clean"),
    FieldMapping(airtable_field="Allergen_Wheat", priority_field="WHEAT", transform="clean"),
    FieldMapping(airtable_field="Feature_Feature Present", priority_field="HASFEATURE", transform="clean"),
    FieldMapping(airtable_field="Feature_Gluten Free", priority_field="GLUTENFREE", transform="clean"),
    FieldMapping(airtable_field="Feature_Organic", priority_field="ORGANIC", transform="clean"),
    FieldMapping(airtable_field="Feature_Kosher", priority_field="KOSHER", transform="clean"),
    FieldMapping(airtable_field="Feature_Vegan", priority_field="VEGAN", transform="clean"),
    FieldMapping(airtable_field="Feature_Halal", priority_field="HALAL", transform="clean"),
    FieldMapping(airtable_field="Feature_Non GMO", priority_field="NONGMO", transform="clean"),
    FieldMapping(airtable_field="Feature_Identity Protected", priority_field="IDPROTECT", transform="clean"),
    FieldMapping(airtable_field="GFSI Certified", priority_field="GFSI", transform="clean"),
    FieldMapping(airtable_field="Glass Packaging", priority_field="PACKAGING", transform="clean"),
    FieldMapping(airtable_field="Prop. 65 Warning", priority_field="PROP65", transform="clean"),
    FieldMapping(airtable_field="Calif. Ass. Bill 418", priority_field="CABILL418", transform="clean"),
    FieldMapping(airtable_field="Traceability Type Output", priority_field="TRACEABILITY", transform="clean"),
]

# Airtable field names for allergens/features (for fields[] API parameter)
ALLERGEN_AIRTABLE_FIELDS: list[str] = [m.airtable_field for m in ALLERGEN_FIELD_MAP]


# ─────────────────────────────────────────────────────────────────────────────
# 2. SHELF LIVES (SAVR_PARTSHELF_SUBFORM)
#    Pattern: multi-record sub-form (multiple shelf life types per product)
#    Source: Airtable Shelf Lives table, NOT the Products table
# ─────────────────────────────────────────────────────────────────────────────

SHELF_LIFE_SUBFORM_NAME = "SAVR_PARTSHELF_SUBFORM"

# Shelf Lives table fields (fetched separately)
SHELF_LIFE_AIRTABLE_FIELDS: list[str] = [
    "SKU Trim (EDI) (from Products)",
    "Type Label (Custom)",
    "Shelf Life Input",
    "Shelf Life Unit Input",
]


# ─────────────────────────────────────────────────────────────────────────────
# 3. PRICE LISTS (PARTINCUSTPLISTS_SUBFORM)
#    Pattern: multi-record sub-form (up to 3 price levels per product)
#    Source: Products table, 3 levels with shared fields
# ─────────────────────────────────────────────────────────────────────────────

PRICE_LIST_SUBFORM_NAME = "PARTINCUSTPLISTS_SUBFORM"

# Config for each price level
PRICE_LEVEL_CONFIG: list[dict[str, str]] = [
    {
        "plname_field": "Lvl 1 Price List Code",
        "price_field": "LVL 1 SALE PRICE (from Price Import)",
        "default_plname": "Base",
    },
    {
        "plname_field": "Lvl 2 Price List Code",
        "price_field": "LVL 2 SALE PRICE (from Price Import)",
        "default_plname": "Whole",
    },
    {
        "plname_field": "Lvl 3 Price List Code",
        "price_field": "LVL 3 SALE PRICE (from Price Import)",
        "default_plname": "Lvl 3",
    },
]

# Shared price list fields
PRICE_LIST_SHARED_FIELDS = {
    "code_field": "EDI $",
    "quant_field": "EDI Price Quantity",
    "unit_field": "Buy_Sell Unit (Priority)",
}

# All Airtable fields needed for price lists
PRICE_LIST_AIRTABLE_FIELDS: list[str] = [
    "Lvl 1 Price List Code",
    "LVL 1 SALE PRICE (from Price Import)",
    "Lvl 2 Price List Code",
    "LVL 2 SALE PRICE (from Price Import)",
    "Lvl 3 Price List Code",
    "LVL 3 SALE PRICE (from Price Import)",
    "EDI $",
    "EDI Price Quantity",
    # "Buy_Sell Unit (Priority)" already in main field map
]


# ─────────────────────────────────────────────────────────────────────────────
# 4. BIN LOCATIONS (PARTLOCATIONS_SUBFORM)
#    Pattern: single-record sub-form (1 bin record per product)
# ─────────────────────────────────────────────────────────────────────────────

BIN_SUBFORM_NAME = "PARTLOCATIONS_SUBFORM"

BIN_FIELD_MAP: list[FieldMapping] = [
    FieldMapping(airtable_field="EDI Main", priority_field="WARHSNAME", transform="clean"),
    FieldMapping(airtable_field="Simplified Bin Location (from Bin # Priority)", priority_field="LOCNAME", transform="clean"),
]

BIN_AIRTABLE_FIELDS: list[str] = [m.airtable_field for m in BIN_FIELD_MAP]


# ─────────────────────────────────────────────────────────────────────────────
# Mapping functions
# ─────────────────────────────────────────────────────────────────────────────

def map_allergens(airtable_fields: dict[str, Any]) -> dict[str, Any]:
    """Map Airtable allergen/feature fields → Priority SAVR_ALLERGENS payload."""
    payload: dict[str, Any] = {}
    for mapping in ALLERGEN_FIELD_MAP:
        raw = airtable_fields.get(mapping.airtable_field)
        transform_fn = _TRANSFORMS[mapping.transform]
        value = transform_fn(raw)
        if value is not None:
            payload[mapping.priority_field] = value
    return payload


def map_shelf_lives(shelf_life_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Map Airtable Shelf Lives records → list of Priority SAVR_PARTSHELF payloads.

    Args:
        shelf_life_records: List of Airtable records from the Shelf Lives table,
                           pre-filtered to a single SKU.

    Returns:
        List of dicts, each with keys TYPE, NUMBER, TIMEUNIT.
    """
    payloads: list[dict[str, Any]] = []
    for record in shelf_life_records:
        fields = record.get("fields", {})
        type_label = clean(fields.get("Type Label (Custom)"))
        number = to_int(fields.get("Shelf Life Input"))
        time_unit = clean(fields.get("Shelf Life Unit Input"))

        if type_label and number is not None:
            payload: dict[str, Any] = {"TYPE": type_label, "NUMBER": number}
            if time_unit:
                payload["TIMEUNIT"] = time_unit
            payloads.append(payload)

    return payloads


def map_price_lists(airtable_fields: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Map Airtable price list fields → list of Priority PARTINCUSTPLISTS payloads.
    Up to 3 price levels. Skips levels where both PLNAME and PRICE are empty.

    Returns:
        List of dicts, each with keys PLNAME, PRICE, CODE, QUANT, UNITNAME.
    """
    shared = PRICE_LIST_SHARED_FIELDS
    code_val = clean(airtable_fields.get(shared["code_field"])) or "$"
    quant_val = clean(airtable_fields.get(shared["quant_field"])) or "1"
    unit_val = clean(airtable_fields.get(shared["unit_field"]))

    payloads: list[dict[str, Any]] = []

    for level in PRICE_LEVEL_CONFIG:
        plname = clean(airtable_fields.get(level["plname_field"]))
        price = format_price(airtable_fields.get(level["price_field"]))

        # Skip if no price list code AND no price
        if not plname and price is None:
            continue

        # Use defaults if one is present but not the other
        if not plname:
            plname = level["default_plname"]

        payload: dict[str, Any] = {
            "PLNAME": plname,
            "CODE": code_val,
            "QUANT": quant_val,
        }
        if price is not None:
            payload["PRICE"] = price
        if unit_val:
            payload["UNITNAME"] = unit_val

        payloads.append(payload)

    return payloads


def map_bins(airtable_fields: dict[str, Any]) -> dict[str, Any]:
    """Map Airtable bin fields → Priority PARTLOCATIONS payload."""
    payload: dict[str, Any] = {}
    for mapping in BIN_FIELD_MAP:
        raw = airtable_fields.get(mapping.airtable_field)
        transform_fn = _TRANSFORMS[mapping.transform]
        value = transform_fn(raw)
        if value is not None:
            payload[mapping.priority_field] = value
    # Use 'Main' as default warehouse if not specified
    if "WARHSNAME" not in payload:
        payload["WARHSNAME"] = "Main"
    return payload
