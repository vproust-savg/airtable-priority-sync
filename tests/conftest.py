"""Shared fixtures for the sync engine test suite."""

from __future__ import annotations

import pytest


@pytest.fixture
def sample_airtable_record() -> dict:
    """A minimal Airtable record dict for testing."""
    return {
        "id": "recABC123",
        "fields": {
            "SKU Trim (EDI)": "P00001",
            "Brand": "Savory",
            "Storage": "Frozen",
            "Last Synced to Priority": "2026-01-01T00:00:00Z",
            "Last Synced from Priority": "2025-12-15T00:00:00Z",
            "Priority UDATE": "2026-01-01 12:00",
        },
    }


@pytest.fixture
def sample_priority_record() -> dict:
    """A minimal Priority record dict for testing."""
    return {
        "PARTNAME": "P00001",
        "SPEC4": "Savory",
        "SPEC12": "Frozen",
        "UDATE": "2026-01-01 12:00",
    }
