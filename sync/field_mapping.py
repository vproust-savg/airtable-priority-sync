"""
Backward-compatible re-export shim.
New code should import from sync.workflows.products.field_mapping
and sync.core.base_engine.
"""
# Product-specific field maps
from sync.workflows.products.field_mapping import (  # noqa: F401
    AIRTABLE_FIELDS_TO_FETCH,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
    P2A_STATUS_AIRTABLE_FIELDS,
    P2A_STATUS_FIELD_MAP,
    P2A_STATUS_PRIORITY_SELECT,
    PRODUCT_FIELD_MAP,
    STATUS_FIELD_MAP,
    STATUS_FIELDS_TO_FETCH,
    get_mapping_by_airtable,
    get_mapping_by_priority,
)

# Generic mapping functions (moved to base_engine)
from sync.core.base_engine import (  # noqa: F401
    TRANSFORMS,
    build_airtable_patch,
    build_patch_body,
    map_airtable_to_priority,
    map_priority_to_airtable,
)
