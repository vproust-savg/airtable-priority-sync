#!/usr/bin/env python3
"""
Entry point for Airtable ↔ Priority ERP Product Sync.

Usage:
    python -m sync.run_sync                          # Normal sync
    python -m sync.run_sync --dry-run                # Preview without writing
    python -m sync.run_sync --sku 14860              # Sync single product
    python -m sync.run_sync --dry-run --sku 14860    # Preview single product
"""

from __future__ import annotations

import argparse
import sys

from sync.logger_setup import print_detail, setup_logging
from sync.models import SyncDirection
from sync.sync_engine import ProductSyncEngine


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync product data between Airtable and Priority ERP.",
    )
    parser.add_argument(
        "--direction",
        choices=["airtable-to-priority", "priority-to-airtable", "both"],
        default="airtable-to-priority",
        help="Sync direction (default: airtable-to-priority)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to Priority or updating timestamps.",
    )
    parser.add_argument(
        "--sku",
        type=str,
        default=None,
        help="Sync a single product by SKU (for testing).",
    )

    args = parser.parse_args()

    # Set up logging
    log_file = setup_logging()

    # Run the sync
    try:
        engine = ProductSyncEngine(
            direction=SyncDirection(args.direction),
            dry_run=args.dry_run,
            single_sku=args.sku,
        )
        stats = engine.run()

        print_detail(f"Log file: {log_file}")
        print()

        # Exit code: 0 if no errors, 1 if any errors
        return 1 if stats.errors > 0 else 0

    except KeyboardInterrupt:
        print("\n  Sync interrupted by user.")
        return 130
    except Exception as e:
        print(f"\n  FATAL: {e}")
        import traceback
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
