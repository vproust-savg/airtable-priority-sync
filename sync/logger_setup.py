"""
Backward-compatible re-export shim.
New code should import from sync.core.logger_setup.
"""
from sync.core.logger_setup import (  # noqa: F401
    print_banner,
    print_detail,
    print_record_line,
    print_section,
    print_summary,
    setup_logging,
)
