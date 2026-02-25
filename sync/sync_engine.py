"""
Backward-compatible re-export shim.
New code should import from:
  - sync.core.base_engine (BaseSyncEngine, generic mapping functions)
  - sync.workflows.products.engine (ProductSyncEngine)
"""
from sync.core.base_engine import BaseSyncEngine  # noqa: F401
from sync.workflows.products.engine import ProductSyncEngine  # noqa: F401
