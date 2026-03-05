"""Validation tests for all field_mapping.py files across workflows."""

from __future__ import annotations

import pytest

from sync.core.models import FieldMapping


def _validate_mappings(
    mappings: list[FieldMapping],
    label: str,
    require_field_ids: bool = True,
) -> None:
    """
    Validate a list of FieldMapping objects:
    - Each has a non-empty airtable_field
    - Each has a non-empty priority_field
    - Each has a valid transform
    - Each has a non-None airtable_field_id (if require_field_ids)
    - No duplicate priority fields
    - No duplicate airtable field IDs
    """
    valid_transforms = {"clean", "format_price", "to_int", "to_float", "priority_yn", "priority_lookup", "linked_record"}

    seen_priority: set[str] = set()
    seen_field_ids: set[str] = set()

    for i, m in enumerate(mappings):
        prefix = f"{label}[{i}] ({m.airtable_field})"

        assert m.airtable_field, f"{prefix}: empty airtable_field"
        assert m.priority_field, f"{prefix}: empty priority_field"
        assert m.transform in valid_transforms, (
            f"{prefix}: invalid transform '{m.transform}'"
        )

        if require_field_ids:
            assert m.airtable_field_id is not None, (
                f"{prefix}: missing airtable_field_id"
            )

        # Check for duplicate priority fields
        if m.priority_field in seen_priority:
            pytest.fail(f"{prefix}: duplicate priority_field '{m.priority_field}'")
        seen_priority.add(m.priority_field)

        # Check for duplicate field IDs
        if m.airtable_field_id:
            if m.airtable_field_id in seen_field_ids:
                pytest.fail(f"{prefix}: duplicate airtable_field_id '{m.airtable_field_id}'")
            seen_field_ids.add(m.airtable_field_id)


class TestProductsFieldMapping:
    def test_a2p_full(self):
        from sync.workflows.products.field_mapping import PRODUCT_FIELD_MAP
        _validate_mappings(PRODUCT_FIELD_MAP, "products/A2P_FULL")

    def test_a2p_status(self):
        from sync.workflows.products.field_mapping import STATUS_FIELD_MAP
        _validate_mappings(STATUS_FIELD_MAP, "products/A2P_STATUS")

    def test_p2a_full(self):
        from sync.workflows.products.field_mapping import P2A_FIELD_MAP
        _validate_mappings(P2A_FIELD_MAP, "products/P2A_FULL")

    def test_p2a_status(self):
        from sync.workflows.products.field_mapping import P2A_STATUS_FIELD_MAP
        _validate_mappings(P2A_STATUS_FIELD_MAP, "products/P2A_STATUS")


class TestFncpartFieldMapping:
    def test_a2p(self):
        from sync.workflows.fncpart.field_mapping import A2P_FIELD_MAP
        _validate_mappings(A2P_FIELD_MAP, "fncpart/A2P")

    def test_p2a(self):
        from sync.workflows.fncpart.field_mapping import P2A_FIELD_MAP
        _validate_mappings(P2A_FIELD_MAP, "fncpart/P2A")


class TestPrdpartFieldMapping:
    def test_a2p(self):
        from sync.workflows.prdpart.field_mapping import A2P_FIELD_MAP
        _validate_mappings(A2P_FIELD_MAP, "prdpart/A2P")

    def test_p2a(self):
        from sync.workflows.prdpart.field_mapping import P2A_FIELD_MAP
        _validate_mappings(P2A_FIELD_MAP, "prdpart/P2A")


class TestVendorsFieldMapping:
    def test_a2p(self):
        from sync.workflows.vendors.field_mapping import A2P_FIELD_MAP
        _validate_mappings(A2P_FIELD_MAP, "vendors/A2P")

    def test_p2a(self):
        from sync.workflows.vendors.field_mapping import P2A_FIELD_MAP
        _validate_mappings(P2A_FIELD_MAP, "vendors/P2A")


class TestFncsupFieldMapping:
    def test_a2p(self):
        from sync.workflows.fncsup.field_mapping import A2P_FIELD_MAP
        _validate_mappings(A2P_FIELD_MAP, "fncsup/A2P")

    def test_p2a(self):
        from sync.workflows.fncsup.field_mapping import P2A_FIELD_MAP
        _validate_mappings(P2A_FIELD_MAP, "fncsup/P2A")


class TestVendorPricesFieldMapping:
    def test_a2p(self):
        from sync.workflows.vendor_prices.field_mapping import A2P_FIELD_MAP
        _validate_mappings(A2P_FIELD_MAP, "vendor_prices/A2P")

    def test_p2a(self):
        from sync.workflows.vendor_prices.field_mapping import P2A_FIELD_MAP
        _validate_mappings(P2A_FIELD_MAP, "vendor_prices/P2A")


class TestCustomersFieldMapping:
    def test_a2p(self):
        from sync.workflows.customers.field_mapping import A2P_FIELD_MAP
        _validate_mappings(A2P_FIELD_MAP, "customers/A2P")

    def test_p2a(self):
        from sync.workflows.customers.field_mapping import P2A_FIELD_MAP
        _validate_mappings(P2A_FIELD_MAP, "customers/P2A")


class TestFnccustFieldMapping:
    def test_a2p(self):
        from sync.workflows.fnccust.field_mapping import A2P_FIELD_MAP
        _validate_mappings(A2P_FIELD_MAP, "fnccust/A2P")

    def test_p2a(self):
        from sync.workflows.fnccust.field_mapping import P2A_FIELD_MAP
        _validate_mappings(P2A_FIELD_MAP, "fnccust/P2A")


class TestCustomerPricesFieldMapping:
    def test_a2p(self):
        from sync.workflows.customer_prices.field_mapping import A2P_FIELD_MAP
        _validate_mappings(A2P_FIELD_MAP, "customer_prices/A2P")

    def test_p2a(self):
        from sync.workflows.customer_prices.field_mapping import P2A_FIELD_MAP
        _validate_mappings(P2A_FIELD_MAP, "customer_prices/P2A")
