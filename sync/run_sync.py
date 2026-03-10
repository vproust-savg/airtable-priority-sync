#!/usr/bin/env python3
"""
Entry point for Airtable ↔ Priority ERP Sync.

Supports 7 workflows via --workflow flag:
  products        (LOGPART + FNCPART + PRDPART) — Products (merged)
  vendors         (SUPPLIERS + FNCSUP)          — Vendors (merged)
  vendor-prices   (PRICELIST)                   — Vendor Price Lists
  customers       (CUSTOMERS + FNCCUST)         — Customers (merged)
  customer-prices (PRICELIST)                   — Customer Price Lists
  images          (LOGPART)                     — Product Images (A→P only)
  techsheets      (LOGPART)                     — Tech Sheet PDFs (A→P only)

Usage:
    python -m sync.run_sync --workflow products                   # Full product sync
    python -m sync.run_sync --workflow products --mode status     # Status-only product sync
    python -m sync.run_sync --workflow vendors --dry-run          # Preview vendors sync
    python -m sync.run_sync --workflow customers --sku C00001     # Single customer
    python -m sync.run_sync --server                              # Start webhook server
"""

from __future__ import annotations

import argparse
import sys

from sync.core.logger_setup import print_detail, setup_logging
from sync.core.models import ConflictStrategy, SyncDirection, SyncMode


# ── Workflow engine factories ────────────────────────────────────────────────

def _get_engine_class(workflow: str):
    """Lazy-import the engine class for a workflow."""
    if workflow == "products":
        from sync.workflows.products.engine import ProductSyncEngine
        return ProductSyncEngine
    elif workflow == "vendors":
        from sync.workflows.vendors.engine import VendorSyncEngine
        return VendorSyncEngine
    elif workflow == "vendor-prices":
        from sync.workflows.vendor_prices.engine import VendorPriceSyncEngine
        return VendorPriceSyncEngine
    elif workflow == "customers":
        from sync.workflows.customers.engine import CustomerSyncEngine
        return CustomerSyncEngine
    elif workflow == "customer-prices":
        from sync.workflows.customer_prices.engine import CustomerPriceSyncEngine
        return CustomerPriceSyncEngine
    elif workflow == "images":
        from sync.workflows.images.engine import ImageSyncEngine
        return ImageSyncEngine
    elif workflow == "techsheets":
        from sync.workflows.techsheets.engine import TechSheetSyncEngine
        return TechSheetSyncEngine
    else:
        raise ValueError(f"Unknown workflow: {workflow}")


def main() -> int:
    from sync.core.config import init_sentry
    init_sentry()

    parser = argparse.ArgumentParser(
        description="Sync data between Airtable and Priority ERP.",
    )
    parser.add_argument(
        "--workflow",
        choices=[
            "products",
            "vendors", "vendor-prices",
            "customers", "customer-prices",
            "images", "techsheets",
        ],
        default="products",
        help="Sync workflow to run (default: products)",
    )
    parser.add_argument(
        "--direction",
        choices=["airtable-to-priority", "priority-to-airtable", "both"],
        default="airtable-to-priority",
        help="Sync direction (default: airtable-to-priority)",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "status"],
        default="full",
        help="Sync mode: 'full' (all fields) or 'status' (status fields only). Not all workflows support status mode.",
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
        help="Sync a single entity by key (e.g., SKU for products, PARTNAME for fncpart).",
    )
    parser.add_argument(
        "--server",
        action="store_true",
        help="Start the FastAPI webhook server (for Railway deployment).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for the webhook server (default: 8000).",
    )
    parser.add_argument(
        "--test-base",
        action="store_true",
        help="Use the test Airtable base (AIRTABLE_TEST_BASE_ID) instead of production.",
    )
    parser.add_argument(
        "--priority-env",
        choices=["sandbox", "uat", "production"],
        default=None,
        help="Priority environment to target (default: uses PRIORITY_API_URL from .env).",
    )
    parser.add_argument(
        "--conflict-strategy",
        choices=["source_wins", "log_only", "skip_record"],
        default="source_wins",
        help=(
            "How to handle field-level conflicts when both systems changed a record. "
            "'source_wins' (default): sync source overwrites. "
            "'log_only': skip conflicting fields, log for manual resolution. "
            "'skip_record': skip entire record if any conflict."
        ),
    )

    args = parser.parse_args()

    # ── Server mode ───────────────────────────────────────────────────────
    if args.server:
        setup_logging()
        import os

        import uvicorn

        from sync.server import app

        # Railway injects PORT env var; fall back to --port arg (default 8000)
        port = int(os.environ.get("PORT", args.port))
        print(f"  Starting webhook server on port {port}...")
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
        return 0

    # ── CLI sync mode ─────────────────────────────────────────────────────
    log_file = setup_logging()

    # Resolve test base override
    base_id_override = None
    token_override = None
    if getattr(args, "test_base", False):
        from sync.core.config import AIRTABLE_TEST_BASE_ID, AIRTABLE_TEST_TOKEN
        if not AIRTABLE_TEST_BASE_ID:
            print("\n  ERROR: --test-base requires AIRTABLE_TEST_BASE_ID env var to be set.")
            return 2
        base_id_override = AIRTABLE_TEST_BASE_ID
        token_override = AIRTABLE_TEST_TOKEN  # Use test token if available
        print(f"  Using TEST Airtable base: {AIRTABLE_TEST_BASE_ID}")

    # Resolve Priority environment override
    priority_url_override = None
    if getattr(args, "priority_env", None):
        from sync.core.config import get_priority_url
        try:
            priority_url_override = get_priority_url(args.priority_env)
            print(f"  Using Priority environment: {args.priority_env} → {priority_url_override}")
        except RuntimeError as e:
            print(f"\n  ERROR: {e}")
            return 2

    try:
        conflict_strategy = ConflictStrategy(args.conflict_strategy)

        engine_class = _get_engine_class(args.workflow)

        # Build kwargs — standalone engines (images, techsheets) don't accept
        # conflict_strategy, so only pass it to engines that support it.
        engine_kwargs: dict = dict(
            direction=SyncDirection(args.direction),
            dry_run=args.dry_run,
            single_key=args.sku,
            trigger="manual",
            mode=SyncMode(args.mode),
            workflow_name=args.workflow,
            base_id_override=base_id_override,
            token_override=token_override,
            priority_url_override=priority_url_override,
        )
        if args.workflow not in ("images", "techsheets"):
            engine_kwargs["conflict_strategy"] = conflict_strategy

        engine = engine_class(**engine_kwargs)
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

        import sentry_sdk
        with sentry_sdk.new_scope() as scope:
            scope.set_extra("workflow", args.workflow)
            scope.set_extra("direction", args.direction)
            scope.set_extra("dry_run", args.dry_run)
            scope.set_extra("mode", args.mode)
            scope.set_extra("priority_env", args.priority_env)
            scope.set_extra("sku", args.sku)
            sentry_sdk.capture_exception(e)

        return 2


if __name__ == "__main__":
    sys.exit(main())
