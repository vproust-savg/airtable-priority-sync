"""
Backward-compatible re-export shim.
New code should import from sync.core.priority_client.
"""
from sync.core.priority_client import (  # noqa: F401
    PriorityClient,
    extract_priority_error,
)
