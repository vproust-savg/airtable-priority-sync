"""
Bidirectional field mapping between Airtable and Priority ERP.
Source of truth: .tmp/convert_to_python.py lines 128-163,
cross-referenced with tools/3. SG Part All Priority Interface.txt
and tools/10. Script for Product All v8.py.

Designed for two-way sync: the same mapping table works in both directions.
"""

from __future__ import annotations

from typing import Any, Callable

from sync.core.models import FieldMapping
from sync.core.utils import clean, format_price, priority_yn, to_float, to_int

# Import secondary entity field mappings (merged from fncpart/prdpart)
from sync.workflows.fncpart.field_mapping import (
    A2P_FIELD_MAP as FNCPART_A2P_FIELD_MAP,
    P2A_FIELD_MAP as FNCPART_P2A_FIELD_MAP,
)
from sync.workflows.prdpart.field_mapping import (
    A2P_FIELD_MAP as PRDPART_A2P_FIELD_MAP,
    P2A_FIELD_MAP as PRDPART_P2A_FIELD_MAP,
)


# ── Transform function registry ──────────────────────────────────────────────

TRANSFORMS: dict[str, Callable[[Any], Any]] = {
    "clean": clean,
    "format_price": format_price,
    "to_int": to_int,
    "to_float": to_float,
    "priority_yn": priority_yn,
}


# ── Complete field mapping table ─────────────────────────────────────────────
# Verified from .tmp/convert_to_python.py lines 128-163.
# Priority interface: tools/3. SG Part All Priority Interface.txt
# Airtable fields: tools/10. Script for Product All v8.py lines 292-360

PRODUCT_FIELD_MAP: list[FieldMapping] = [
    # ── Primary key ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="SKU Trim (EDI)",
        airtable_field_id="fldiGjELA26tbBi8f",
        priority_field="PARTNAME",
        transform="clean",
        required=True,
        field_type="str",
    ),
    # ── Description ──────────────────────────────────────────────────────
    FieldMapping(
        airtable_field="Brand + Product Title + Net Weight + Case Pack",
        airtable_field_id="fldCh4dhezc1ollsM",
        priority_field="PARTDES",
        transform="clean",
        field_type="str",
    ),
    # ── SPEC fields (custom specs mapped via EDI interface) ──────────────
    FieldMapping(
        airtable_field="Case Pack",
        airtable_field_id="fld1zWWJwiwAnbq6X",
        priority_field="SPEC1",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Product Net Weight Input",
        airtable_field_id="fldRK7RwBeu0vCBCO",
        priority_field="SPEC2",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Product Net Weight Unit Input",
        airtable_field_id="fld7UhpTbJlgwuZH6",
        priority_field="SPEC3",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Brand",
        airtable_field_id="flderGjy8obS1Qpqk",
        priority_field="SPEC4",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Kelsey_Categories",
        airtable_field_id="fldurKZqu5KJYH2ri",
        priority_field="SPEC5",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Kelsey_Subcategories",
        airtable_field_id="fldbQDZAdqSu9wg8E",
        priority_field="SPEC6",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Catalog Status",
        airtable_field_id="fldDb9LcQpdhNmKYU",
        priority_field="SPEC7",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Inventory Status",
        airtable_field_id="fldATOgqP5Y8i7nT7",
        priority_field="SPEC8",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Perishable",
        airtable_field_id="fldhSuR2LjoyaCXWu",
        priority_field="SPEC9",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Retail",
        airtable_field_id="fldMm970r8SxD6ojA",
        priority_field="SPEC10",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Feature_Individual Portions",
        airtable_field_id="fldlA4nRFKiaCBXLW",
        priority_field="SPEC11",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Staff Pick",
        airtable_field_id="fldXVC9aFsJ1bxWcC",
        priority_field="SPEC12",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Storage",
        airtable_field_id="fldsFJrCXXKur6P1F",
        priority_field="SPEC13",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Availability Priority Output",
        airtable_field_id="fldouLxtxbUhY0Caq",
        priority_field="SPEC14",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Direct Import",
        airtable_field_id="fldlqTQO2U5V2d46l",
        priority_field="SPEC15",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Vendor SKU Trim",
        airtable_field_id="fldOE3NlqM1kIPMdk",
        priority_field="SPEC16",
        transform="clean",
        field_type="str",
    ),
    # ── Standard LOGPART fields ──────────────────────────────────────────
    FieldMapping(
        airtable_field="Buy_Sell Unit (Priority)",
        airtable_field_id="fldRgdNphJxTXxivY",
        priority_field="PUNITNAME",
        transform="clean",
        field_type="str",
        create_only=True,  # Priority requires "Change Part Unit" program to modify
    ),
    FieldMapping(
        airtable_field="Base Price",
        airtable_field_id="fldPwojUSmVpiHA2C",
        priority_field="BASEPLPRICE",
        transform="format_price",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Base Price Currency",
        airtable_field_id="fldpYviH2hsfvEEZQ",
        priority_field="BASEPLCODE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Priority Status",
        airtable_field_id="fldqt4AnlaxYgYhXw",
        priority_field="STATDES",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="V-Vendor ID (from Preferred Vendor)",
        airtable_field_id="fld1EMYPIfS1lW9GC",
        priority_field="SUPNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Standard Cost",
        airtable_field_id="fldjS688T5OawrcjM",
        priority_field="PRICE",
        transform="format_price",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="LVL 2 SALE PRICE (from Price Import)",
        airtable_field_id="fld9Ku04QU2bJiBYX",
        priority_field="WSPLPRICE",
        transform="format_price",
        field_type="float",
    ),
    FieldMapping(
        airtable_field="Conversion Ratio",
        airtable_field_id="fldKhMa20lTbBmULi",
        priority_field="CONV",
        transform="to_int",
        field_type="int",
    ),
    FieldMapping(
        airtable_field="Family (Number from Product Type)",
        airtable_field_id="fldeg7E2g2l8xe6NZ",
        priority_field="FAMILYNAME",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Type (P/R/O)",
        airtable_field_id="fld5uQwSdX7lEn0PO",
        priority_field="TYPE",
        transform="clean",
        field_type="str",
    ),
    FieldMapping(
        airtable_field="Allocate Inventory",
        airtable_field_id="fldjX6kGZJUmzrf48",
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
        "Last Synced from Priority",  # For loop prevention (P→A check)
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
    # ── FNCPART fields (Accounting Family, Weight, HTS, etc.) ────────
    + [m.airtable_field for m in FNCPART_A2P_FIELD_MAP
       if m.airtable_field not in {"SKU Trim (EDI)", "Last Synced to Priority",
                                    "Last Synced from Priority", "Priority UDATE"}]
    # ── PRDPART fields (Lead Time, Safety Stock, etc.) ───────────────
    + [m.airtable_field for m in PRDPART_A2P_FIELD_MAP
       if m.airtable_field not in {"SKU Trim (EDI)", "Last Synced to Priority",
                                    "Last Synced from Priority", "Priority UDATE"}]
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


# ═════════════════════════════════════════════════════════════════════════════
# P→A (Priority → Airtable) field mapping — REVERSE DIRECTION
# ═════════════════════════════════════════════════════════════════════════════
# Separate from the A→P mapping above. Both can be fine-tuned independently.
# To add/remove a field synced from Priority → Airtable, edit this list.

P2A_FIELD_MAP: list[FieldMapping] = [
    # ── Product description (CREATE only — not updated on existing records) ──
    FieldMapping(
        airtable_field="Product Title Priority Input",
        airtable_field_id="fldDipTGNBEwgqz4e",
        priority_field="PARTDES",
        transform="clean",
        create_only=True,
    ),
    # ── SPEC fields ─────────────────────────────────────────────────────────
    # Case Pack & Net Weight are NUMBER fields in Airtable → use to_float
    FieldMapping(airtable_field="Case Pack", airtable_field_id="fld1zWWJwiwAnbq6X", priority_field="SPEC1", transform="to_float", field_type="float"),
    FieldMapping(airtable_field="Product Net Weight Input", airtable_field_id="fldRK7RwBeu0vCBCO", priority_field="SPEC2", transform="to_float", field_type="float"),
    FieldMapping(airtable_field="Product Net Weight Unit Input", airtable_field_id="fld7UhpTbJlgwuZH6", priority_field="SPEC3", transform="clean"),
    FieldMapping(airtable_field="Brand", airtable_field_id="flderGjy8obS1Qpqk", priority_field="SPEC4", transform="clean"),
    FieldMapping(airtable_field="Kelsey_Categories", airtable_field_id="fldurKZqu5KJYH2ri", priority_field="SPEC5", transform="clean"),
    FieldMapping(airtable_field="Kelsey_Subcategories", airtable_field_id="fldbQDZAdqSu9wg8E", priority_field="SPEC6", transform="clean"),
    FieldMapping(airtable_field="Catalog Status", airtable_field_id="fldDb9LcQpdhNmKYU", priority_field="SPEC7", transform="clean"),
    FieldMapping(airtable_field="Inventory Status", airtable_field_id="fldATOgqP5Y8i7nT7", priority_field="SPEC8", transform="clean"),
    FieldMapping(airtable_field="Perishable", airtable_field_id="fldhSuR2LjoyaCXWu", priority_field="SPEC9", transform="clean"),
    FieldMapping(airtable_field="Retail", airtable_field_id="fldMm970r8SxD6ojA", priority_field="SPEC10", transform="clean"),
    FieldMapping(airtable_field="Feature_Individual Portions", airtable_field_id="fldlA4nRFKiaCBXLW", priority_field="SPEC11", transform="clean"),
    FieldMapping(airtable_field="Staff Pick", airtable_field_id="fldXVC9aFsJ1bxWcC", priority_field="SPEC12", transform="clean"),
    FieldMapping(airtable_field="Storage", airtable_field_id="fldsFJrCXXKur6P1F", priority_field="SPEC13", transform="clean"),
    # NOTE: Availability Priority Output (SPEC14) is a FORMULA in Airtable — can't write
    FieldMapping(airtable_field="Direct Import", airtable_field_id="fldlqTQO2U5V2d46l", priority_field="SPEC15", transform="clean"),
    # ── Standard LOGPART fields ─────────────────────────────────────────────
    # NOTE: Base Price, Base Price Currency, Standard Cost, Conversion Ratio,
    #       Allocate Inventory are FORMULAS or LOOKUPS in Airtable — can't write.
    FieldMapping(airtable_field="Priority Status", airtable_field_id="fldqt4AnlaxYgYhXw", priority_field="STATDES", transform="clean"),
    FieldMapping(airtable_field="Type (P/R/O)", airtable_field_id="fld5uQwSdX7lEn0PO", priority_field="TYPE", transform="clean"),
]


# ── P→A status-only mode (light sync) ──────────────────────────────────────
# Same 3 status fields as A→P status mode.

P2A_STATUS_PRIORITY_FIELDS = {"SPEC7", "SPEC8", "STATDES"}

P2A_STATUS_FIELD_MAP: list[FieldMapping] = [
    m for m in P2A_FIELD_MAP if m.priority_field in P2A_STATUS_PRIORITY_FIELDS
]


# ── P→A Airtable fields to fetch (for comparing current values) ────────────

P2A_AIRTABLE_FIELDS_TO_FETCH: list[str] = (
    [m.airtable_field for m in P2A_FIELD_MAP]
    + [m.airtable_field for m in FNCPART_P2A_FIELD_MAP]
    + [m.airtable_field for m in PRDPART_P2A_FIELD_MAP]
    + [
        "SKU Trim (EDI)",
        "Last Synced from Priority",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
)

P2A_STATUS_AIRTABLE_FIELDS: list[str] = (
    [m.airtable_field for m in P2A_STATUS_FIELD_MAP]
    + [
        "SKU Trim (EDI)",
        "Last Synced from Priority",
        "Last Synced to Priority",
        "Priority UDATE",
    ]
)


# ── P→A Priority $select fields (for API call — only fetch mapped fields) ──

P2A_PRIORITY_SELECT: list[str] = (
    ["PARTNAME", "UDATE"]
    + [m.priority_field for m in P2A_FIELD_MAP if m.priority_field != "PARTNAME"]
)

P2A_STATUS_PRIORITY_SELECT: list[str] = (
    ["PARTNAME", "UDATE"]
    + [m.priority_field for m in P2A_STATUS_FIELD_MAP]
)


# ── P→A lookup helpers ─────────────────────────────────────────────────────

_P2A_BY_AIRTABLE = {m.airtable_field: m for m in P2A_FIELD_MAP}
_P2A_BY_PRIORITY = {m.priority_field: m for m in P2A_FIELD_MAP}


# ── P→A Mapping functions ──────────────────────────────────────────────────

def map_priority_to_airtable(
    priority_fields: dict[str, Any],
    field_map: list[FieldMapping] | None = None,
    is_create: bool = False,
) -> dict[str, Any]:
    """
    Transform a Priority product's fields into an Airtable-ready payload.
    Skips fields with empty/None values.

    Args:
        priority_fields: Raw Priority product fields dict.
        field_map: Optional custom field map (e.g. P2A_STATUS_FIELD_MAP for light sync).
                   Defaults to P2A_FIELD_MAP (full sync).
        is_create: If True, include create_only fields (e.g. PARTDES).
                   If False, skip create_only fields (update mode).

    Returns:
        dict mapping Airtable field names to transformed values.
    """
    payload: dict[str, Any] = {}

    for mapping in (field_map or P2A_FIELD_MAP):
        # Skip create_only fields on updates
        if mapping.create_only and not is_create:
            continue

        raw_value = priority_fields.get(mapping.priority_field)
        transform_fn = TRANSFORMS[mapping.transform]
        cleaned = transform_fn(raw_value)

        if cleaned is None:
            continue

        payload[mapping.airtable_field] = cleaned

    return payload


def build_airtable_patch(
    mapped_from_priority: dict[str, Any],
    current_airtable: dict[str, Any],
) -> dict[str, Any]:
    """
    Compare mapped Priority values with current Airtable values.
    Returns only the Airtable fields that actually changed (for PATCH).

    Rules:
    - Empty Priority value does NOT overwrite a populated Airtable field.
    - Numeric fields use float comparison with epsilon tolerance.
    - Returns empty dict if nothing changed.
    """
    patch: dict[str, Any] = {}
    EPSILON = 0.001

    for airtable_field, priority_value in mapped_from_priority.items():
        current_value = current_airtable.get(airtable_field)

        # Don't overwrite populated Airtable field with empty Priority value
        if priority_value is None or priority_value == "":
            continue

        # Get the mapping to know the field type
        mapping = _P2A_BY_AIRTABLE.get(airtable_field)
        if not mapping:
            continue

        # Compare based on type
        if mapping.field_type == "float":
            try:
                p_float = float(priority_value) if priority_value is not None else 0.0
                a_float = float(current_value) if current_value is not None else 0.0
                if abs(p_float - a_float) > EPSILON:
                    patch[airtable_field] = priority_value
            except (ValueError, TypeError):
                if str(priority_value) != str(current_value or ""):
                    patch[airtable_field] = priority_value

        elif mapping.field_type == "int":
            try:
                p_int = int(float(priority_value)) if priority_value is not None else 0
                a_int = int(float(current_value)) if current_value is not None else 0
                if p_int != a_int:
                    patch[airtable_field] = priority_value
            except (ValueError, TypeError):
                if str(priority_value) != str(current_value or ""):
                    patch[airtable_field] = priority_value

        else:
            # String comparison
            p_str = str(priority_value).strip()
            a_str = str(current_value or "").strip()
            if p_str != a_str:
                patch[airtable_field] = priority_value

    return patch
