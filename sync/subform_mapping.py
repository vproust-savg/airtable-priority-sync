"""
Backward-compatible re-export shim.
New code should import from sync.workflows.products.subform_mapping.
"""
from sync.workflows.products.subform_mapping import (  # noqa: F401
    ALLERGEN_SUBFORM_NAME,
    BIN_SUBFORM_NAME,
    PRICE_LIST_SUBFORM_NAME,
    SHELF_LIFE_AIRTABLE_FIELDS,
    SHELF_LIFE_SUBFORM_NAME,
    map_allergens,
    map_allergens_to_airtable,
    map_bins,
    map_price_lists,
    map_shelf_lives,
)
