"""
Bidirectional field mapping between Airtable and Priority ERP.
Source of truth: .tmp/convert_to_python.py lines 128-163,
cross-referenced with tools/3. SG Part All Priority Interface.txt
and tools/10. Script for Product All v8.py.

Designed for two-way sync: the same mapping table works in both directions.
"""

from __future__ import annotations

from typing import Any, Callable

from sync.models import FieldMapping
from sync.utils import clean, format_price, to_int


# ── Transform function registry ──────────────────────────────────────────────

TRANSFORMS: dict[str, Callable[[Any], Any]] = {
    "clean": clean,
    "format_price": format_price,
    "to_int": to_int,
}


# ── Complete field mapping table ─────────────────────────────────────────────
# Verified from .tmp/convert_to_python.py lines 128-163.
# Priority interface: tools/3. SG Part All Priority Interface.txt
# Airtable fields: tools/10. Script for Product All v8.py lines 292-360

PRODUCT_FIELD_MAP: list[FieldMapping] = [
    # ── Primary key ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="SKU Trim (EDI)",
        priority_field="PARTNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
    # ── Description ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Brand + Product Title + Net Weight + Case Pack",
        priority_field="PARTDES",
        transform="clean",
        field_type="str",
    ),
    # ── SPEC fields (custom specs mapped via EDI interface) ──────────────
    FieldMapping(
        airtable_field="Case Pack",
        priority_field="SPEC1",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Product Net Weight Input",
        priority_field="SPEC2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Product Net Weight Unit Input",
        priority_field="SPEC3",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Brand",
        priority_field="SPEC4",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Kelsey_Categories",
        priority_field="SPEC5",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Kelsey_Subcategories",
        priority_field="SPEC6",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Catalog Status",
        priority_field="SPEC7",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Inventory Status",
        priority_field="SPEC8",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Perishable",
        priority_field="SPEC9",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Retail",
        priority_field="SPEC10",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Feature_Individual Portions",
        priority_field="SPEC11",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Staff Pick",
        priority_field="SPEC12",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Storage",
        priority_field="SPEC13",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Availability Priority Output",
        priority_field="SPEC14",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Direct Import",
        priority_field="SPEC15",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Vendor SKU Trim",
        priority_field="SPEC16",
        transform="clean",
        field_type="str",
    ),
    # ── Standard LOGPART fields ──────────────────────────────────────────
    FieldMapping(
        airtable_field="Buy_Sell Unit (Priority)",
        priority_field="PUNITNAME",
        transform="clean",
        field_type="str",
        create_only=True,  # Priority requires "Change Part Unit" program to modify
    ),
    FieldMapping(
        airtable_field="Base Price",
        priority_field="BASEPLPRICE",
        transform="format_price",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Base Price Currency",
        priority_field="BASEPLCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Priority Status",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="V-Vendor ID (from Preferred Vendor)",
        priority_field="SUPNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Standard Cost",
        priority_field="PRICE",
        transform="format_price",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="LVL 2 SALE PRICE (from Price Import)",
        priority_field="WSPLPRICE",
        transform="format_price",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Conversion Ratio",
        priority_field="CONV",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Family (Number from Product Type)",
        priority_field="FAMILYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Type (P/R/O)",
        priority_field="TYPE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Allocate Inventory",
        priority_field="RESERVFLAG",
        transform="clean",
        field_type="str",
    ),
]


# ── Status-only mode (light sync) ────────────────────────────────────────────
# Only these fields are fetched/mapped when running in STATUS mode.

STATUS_PRIORITY_FIELDS = {"PARTNAME", "SPEC7", "SPEC8", "STATDES"}

STATUS_FIELD_MAP: list[FieldMapping] = [
    m for m in PRODUCT_FIELD_MAP if m.priority_field in STATUS_PRIORITY_FIELDS
]

STATUS_FIELDS_TO_FETCH: list[str] = [
    "SKU Trim (EDI)",
    "Catalog Status",
    "Inventory Status",
    "Priority Status",
    "Last Airtable Modified",
    "Last Synced to Priority",
    "Priority UDATE",
]


# ── Lookup helpers ───────────────────────────────────────────────────────────

# Quick lookup: Airtable field name → FieldMapping
_BY_AIRTABLE = {m.airtable_field: m for m in PRODUCT_FIELD_MAP}

# Quick lookup: Priority field name → FieldMapping
_BY_PRIORITY = {m.priority_field: m for m in PRODUCT_FIELD_MAP}

# All Airtable field names we need to fetch from the PRODUCTS table
# (for the fields[] API parameter)
# Includes: main LOGPART fields + sub-form fields + timestamps
AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in PRODUCT_FIELD_MAP]
    + [
        "Last Airtable Modified",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
    + [
        # ── Allergens & Features (23 fields) ──────────────────────────────
        "Allergen_Allergen Present",
        "Allergen_Eggs",
        "Allergen_Dairy",
        "Allergen_Fish",
        "Allergen_Peanut",
        "Allergen_Sesame",
        "Allergen_Shellfish",
        "Allergen_Soybean",
        "Allergen_Tree Nuts",
        "Allergen_Wheat",
        "Feature_Feature Present",
        "Feature_Gluten Free",
        "Feature_Organic",
        "Feature_Kosher",
        "Feature_Vegan",
        "Feature_Halal",
        "Feature_Non GMO",
        "Feature_Identity Protected",
        "GFSI Certified",
        "Glass Packaging",
        "Prop. 65 Warning",
        "Calif. Ass. Bill 418",
        "Traceability Type Output",
        # ── Price Lists (8 fields — Buy_Sell Unit already in main map) ────
        "Lvl 1 Price List Code",
        "LVL 1 SALE PRICE (from Price Import)",
        "Lvl 2 Price List Code",
        "LVL 2 SALE PRICE (from Price Import)",
        "Lvl 3 Price List Code",
        "LVL 3 SALE PRICE (from Price Import)",
        "EDI $",
        "EDI Price Quantity",
        # ── Bin Locations (2 fields) ──────────────────────────────────────
        "EDI Main",
        "Simplified Bin Location (from Bin # Priority)",
    ]
)


def get_mapping_by_airtable(field_name: str) -> FieldMapping | None:
    """Look up a mapping by Airtable field name."""
    return _BY_AIRTABLE.get(field_name)


def get_mapping_by_priority(field_name: str) -> FieldMapping | None:
    """Look up a mapping by Priority field name."""
    return _BY_PRIORITY.get(field_name)


# ── Mapping functions ────────────────────────────────────────────────────────

def map_airtable_to_priority(
    airtable_fields: dict[str, Any],
    field_map: list[FieldMapping] | None = None,
) -> dict[str, Any]:
    """
    Transform an Airtable record's fields dict into a Priority-ready payload.
    Skips fields with empty/None values (they won't be sent to Priority).

    Args:
        airtable_fields: Raw Airtable record fields.
        field_map: Optional custom field map (e.g. STATUS_FIELD_MAP for light sync).
                   Defaults to PRODUCT_FIELD_MAP (full sync).

    Returns:
        dict mapping Priority field names to cleaned values.
    """
    payload: dict[str, Any] = {}

    for mapping in (field_map or PRODUCT_FIELD_MAP):
        raw_value = airtable_fields.get(mapping.airtable_field)
        transform_fn = TRANSFORMS[mapping.transform]
        cleaned = transform_fn(raw_value)

        if cleaned is None:
            continue

        # Validate max_length if set
        if mapping.max_length and isinstance(cleaned, str) and len(cleaned) > mapping.max_length:
            cleaned = cleaned[: mapping.max_length]

        payload[mapping.priority_field] = cleaned

    return payload


def build_patch_body(
    airtable_mapped: dict[str, Any],
    priority_current: dict[str, Any],
) -> dict[str, Any]:
    """
    Compare mapped Airtable values with current Priority values.
    Returns only the fields that actually changed (for PATCH).

    Rules:
    - Empty Airtable value does NOT overwrite a populated Priority field.
    - Numeric fields use float comparison with epsilon tolerance.
    - Returns empty dict if nothing changed.
    """
    patch: dict[str, Any] = {}
    EPSILON = 0.001

    for priority_field, airtable_value in airtable_mapped.items():
        # Skip PARTNAME — it's the key, never patched
        if priority_field == "PARTNAME":
            continue

        priority_value = priority_current.get(priority_field)

        # Don't overwrite populated Priority field with empty Airtable value
        if airtable_value is None or airtable_value == "":
            continue

        # Get the mapping to know the field type
        mapping = _BY_PRIORITY.get(priority_field)
        if not mapping:
            continue

        # Skip create_only fields (e.g. PUNITNAME can't be changed via PATCH)
        if mapping.create_only:
            continue

        # Compare based on type
        if mapping.field_type == "float":
            try:
                a_float = float(airtable_value) if airtable_value is not None else 0.0
                p_float = float(priority_value) if priority_value is not None else 0.0
                if abs(a_float - p_float) > EPSILON:
                    patch[priority_field] = airtable_value
            except (ValueError, TypeError):
                # If conversion fails, compare as strings
                if str(airtable_value) != str(priority_value or ""):
                    patch[priority_field] = airtable_value

        elif mapping.field_type == "int":
            try:
                a_int = int(float(airtable_value)) if airtable_value is not None else 0
                p_int = int(float(priority_value)) if priority_value is not None else 0
                if a_int != p_int:
                    patch[priority_field] = airtable_value
            except (ValueError, TypeError):
                if str(airtable_value) != str(priority_value or ""):
                    patch[priority_field] = airtable_value

        else:
            # String comparison
            a_str = str(airtable_value).strip()
            p_str = str(priority_value or "").strip()
            if a_str != p_str:
                patch[priority_field] = airtable_value

    return patch
