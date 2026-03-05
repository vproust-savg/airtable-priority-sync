"""
Smoke tests: verify all engines can be imported and instantiated.

These tests do NOT call external APIs — they only check that the
engine classes can be created without errors.
"""

from __future__ import annotations

import pytest

from sync.core.models import ConflictStrategy, SyncDirection, SyncMode


# ── BaseSyncEngine subclasses ────────────────────────────────────────────────

class TestEngineImports:
    """Verify each engine class can be imported successfully."""

    def test_import_products(self):
        from sync.workflows.products.engine import ProductSyncEngine
        assert ProductSyncEngine is not None

    def test_import_vendors(self):
        from sync.workflows.vendors.engine import VendorSyncEngine
        assert VendorSyncEngine is not None

    def test_import_vendor_prices(self):
        from sync.workflows.vendor_prices.engine import VendorPriceSyncEngine
        assert VendorPriceSyncEngine is not None

    def test_import_customers(self):
        from sync.workflows.customers.engine import CustomerSyncEngine
        assert CustomerSyncEngine is not None

    def test_import_customer_prices(self):
        from sync.workflows.customer_prices.engine import CustomerPriceSyncEngine
        assert CustomerPriceSyncEngine is not None

    def test_import_images(self):
        from sync.workflows.images.engine import ImageSyncEngine
        assert ImageSyncEngine is not None

    def test_import_techsheets(self):
        from sync.workflows.techsheets.engine import TechSheetSyncEngine
        assert TechSheetSyncEngine is not None


class TestConflictStrategyParam:
    """Verify BaseSyncEngine subclasses accept conflict_strategy param."""

    def test_products_accepts_conflict_strategy(self):
        from sync.workflows.products.engine import ProductSyncEngine
        engine = ProductSyncEngine(
            conflict_strategy=ConflictStrategy.LOG_ONLY,
            dry_run=True,
        )
        assert engine.conflict_strategy == ConflictStrategy.LOG_ONLY

    def test_vendors_accepts_conflict_strategy(self):
        from sync.workflows.vendors.engine import VendorSyncEngine
        engine = VendorSyncEngine(
            conflict_strategy=ConflictStrategy.SKIP_RECORD,
            dry_run=True,
        )
        assert engine.conflict_strategy == ConflictStrategy.SKIP_RECORD

    def test_default_conflict_strategy(self):
        from sync.workflows.products.engine import ProductSyncEngine
        engine = ProductSyncEngine(dry_run=True)
        assert engine.conflict_strategy == ConflictStrategy.SOURCE_WINS


class TestCLIEntrypoint:
    """Verify CLI entry points import cleanly."""

    def test_import_run_sync(self):
        from sync.run_sync import main, _get_engine_class
        assert main is not None
        assert _get_engine_class is not None

    def test_import_server(self):
        from sync.server import app
        assert app is not None

    def test_get_engine_class_all_workflows(self):
        from sync.run_sync import _get_engine_class

        for workflow in [
            "products", "vendors", "vendor-prices",
            "customers", "customer-prices",
            "images", "techsheets",
        ]:
            cls = _get_engine_class(workflow)
            assert cls is not None, f"No engine class for {workflow}"
