"""
Backward-compatible re-export shim.
New code should import from sync.core.sync_log_client.
"""
from sync.core.sync_log_client import SyncLogClient  # noqa: F401
