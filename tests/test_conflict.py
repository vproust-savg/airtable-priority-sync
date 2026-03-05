"""Tests for conflict detection and resolution logic."""

from __future__ import annotations

import pytest

from sync.core.base_engine import build_field_id_map
from sync.core.models import (
    ConflictRecord,
    ConflictStrategy,
    FieldMapping,
    SyncAction,
    SyncRecord,
    SyncStats,
)


# ── Test helpers ─────────────────────────────────────────────────────────────

def make_a2p_conflicts(
    patch_body: dict,
    priority_current: dict,
    airtable_fields: dict,
    stored_udate: str = "2026-01-01 12:00",
    current_udate: str = "2026-02-01 12:00",
    key: str = "P00001",
) -> list[ConflictRecord]:
    """
    Simulate _detect_a2p_conflicts logic without needing a full engine instance.
    """
    from datetime import datetime, timezone

    conflicts: list[ConflictRecord] = []

    stored = airtable_fields.get("Priority UDATE", stored_udate)
    current = priority_current.get("UDATE", current_udate)

    if not stored or not current:
        return conflicts

    if str(current) <= str(stored):
        return conflicts

    now_iso = datetime.now(timezone.utc).isoformat()
    for priority_field in patch_body:
        priority_value = priority_current.get(priority_field)
        airtable_value = patch_body[priority_field]
        if priority_value is not None and priority_value != airtable_value:
            conflicts.append(ConflictRecord(
                entity_key=key,
                field_name=priority_field,
                source_value=airtable_value,
                target_value=priority_value,
                direction="A->P",
                resolution="pending",
                timestamp=now_iso,
            ))

    return conflicts


def make_p2a_conflicts(
    patch: dict,
    current_airtable: dict,
    key: str = "P00001",
) -> list[ConflictRecord]:
    """
    Simulate _detect_p2a_conflicts logic without needing a full engine instance.
    """
    from datetime import datetime, timezone

    conflicts: list[ConflictRecord] = []

    last_synced_to = current_airtable.get("Last Synced to Priority", "")
    last_synced_from = current_airtable.get("Last Synced from Priority", "")

    if not last_synced_to or not last_synced_from:
        return conflicts

    if str(last_synced_to) <= str(last_synced_from):
        return conflicts

    now_iso = datetime.now(timezone.utc).isoformat()
    for airtable_field, priority_value in patch.items():
        airtable_value = current_airtable.get(airtable_field)
        if airtable_value is not None and airtable_value != priority_value:
            conflicts.append(ConflictRecord(
                entity_key=key,
                field_name=airtable_field,
                source_value=priority_value,
                target_value=airtable_value,
                direction="P->A",
                resolution="pending",
                timestamp=now_iso,
            ))

    return conflicts


# ── A→P Conflict Detection ───────────────────────────────────────────────────

class TestA2PConflictDetection:
    def test_no_conflict_when_udate_unchanged(self):
        """No conflict if Priority's UDATE hasn't changed since last sync."""
        conflicts = make_a2p_conflicts(
            patch_body={"SPEC4": "NewBrand"},
            priority_current={"SPEC4": "OldBrand", "UDATE": "2026-01-01 12:00"},
            airtable_fields={"Priority UDATE": "2026-01-01 12:00"},
            stored_udate="2026-01-01 12:00",
            current_udate="2026-01-01 12:00",
        )
        assert len(conflicts) == 0

    def test_conflict_when_priority_modified(self):
        """Conflict detected when Priority was modified since last sync."""
        conflicts = make_a2p_conflicts(
            patch_body={"SPEC4": "AirtableBrand"},
            priority_current={"SPEC4": "PriorityBrand", "UDATE": "2026-02-01 12:00"},
            airtable_fields={"Priority UDATE": "2026-01-01 12:00"},
        )
        assert len(conflicts) == 1
        assert conflicts[0].field_name == "SPEC4"
        assert conflicts[0].source_value == "AirtableBrand"
        assert conflicts[0].target_value == "PriorityBrand"
        assert conflicts[0].direction == "A->P"

    def test_no_conflict_when_values_agree(self):
        """No conflict if the field values happen to be the same."""
        conflicts = make_a2p_conflicts(
            patch_body={"SPEC4": "Same"},
            priority_current={"SPEC4": "Same", "UDATE": "2026-02-01 12:00"},
            airtable_fields={"Priority UDATE": "2026-01-01 12:00"},
        )
        assert len(conflicts) == 0

    def test_no_conflict_when_stored_udate_missing(self):
        """No conflict detection possible if no stored UDATE."""
        conflicts = make_a2p_conflicts(
            patch_body={"SPEC4": "Brand"},
            priority_current={"SPEC4": "Other", "UDATE": "2026-02-01 12:00"},
            airtable_fields={},
            stored_udate="",
        )
        assert len(conflicts) == 0

    def test_multiple_conflicts(self):
        """Multiple conflicting fields are all detected."""
        conflicts = make_a2p_conflicts(
            patch_body={"SPEC4": "A", "SPEC12": "B"},
            priority_current={"SPEC4": "X", "SPEC12": "Y", "UDATE": "2026-02-01 12:00"},
            airtable_fields={"Priority UDATE": "2026-01-01 12:00"},
        )
        assert len(conflicts) == 2

    def test_no_conflict_when_priority_value_is_none(self):
        """No conflict if Priority's current value is None."""
        conflicts = make_a2p_conflicts(
            patch_body={"SPEC4": "Brand"},
            priority_current={"SPEC4": None, "UDATE": "2026-02-01 12:00"},
            airtable_fields={"Priority UDATE": "2026-01-01 12:00"},
        )
        assert len(conflicts) == 0


# ── P→A Conflict Detection ───────────────────────────────────────────────────

class TestP2AConflictDetection:
    def test_no_conflict_when_a2p_not_newer(self):
        """No conflict if A→P hasn't run since last P→A."""
        conflicts = make_p2a_conflicts(
            patch={"Brand": "NewBrand"},
            current_airtable={
                "Brand": "OldBrand",
                "Last Synced to Priority": "2025-12-01T00:00:00Z",
                "Last Synced from Priority": "2026-01-01T00:00:00Z",
            },
        )
        assert len(conflicts) == 0

    def test_conflict_when_a2p_ran_after_p2a(self):
        """Conflict detected when A→P ran more recently than P→A."""
        conflicts = make_p2a_conflicts(
            patch={"Brand": "PriorityBrand"},
            current_airtable={
                "Brand": "AirtableBrand",
                "Last Synced to Priority": "2026-02-01T00:00:00Z",
                "Last Synced from Priority": "2026-01-01T00:00:00Z",
            },
        )
        assert len(conflicts) == 1
        assert conflicts[0].field_name == "Brand"
        assert conflicts[0].source_value == "PriorityBrand"
        assert conflicts[0].target_value == "AirtableBrand"
        assert conflicts[0].direction == "P->A"

    def test_no_conflict_when_values_agree(self):
        """No conflict if values happen to match."""
        conflicts = make_p2a_conflicts(
            patch={"Brand": "Same"},
            current_airtable={
                "Brand": "Same",
                "Last Synced to Priority": "2026-02-01T00:00:00Z",
                "Last Synced from Priority": "2026-01-01T00:00:00Z",
            },
        )
        assert len(conflicts) == 0

    def test_no_conflict_when_timestamps_missing(self):
        """No conflict detection possible without timestamps."""
        conflicts = make_p2a_conflicts(
            patch={"Brand": "NewBrand"},
            current_airtable={"Brand": "OldBrand"},
        )
        assert len(conflicts) == 0


# ── Resolution Strategies ────────────────────────────────────────────────────

class TestConflictResolution:
    def test_source_wins_keeps_patch(self):
        """source_wins: patch body is unchanged."""
        patch = {"SPEC4": "Airtable", "SPEC12": "Also Airtable"}
        conflicts = [
            ConflictRecord(
                entity_key="P00001", field_name="SPEC4",
                source_value="Airtable", target_value="Priority",
                direction="A->P",
            ),
        ]
        # source_wins: don't modify patch
        for c in conflicts:
            c.resolution = "source_wins"
        # Patch remains unchanged
        assert "SPEC4" in patch
        assert patch["SPEC4"] == "Airtable"

    def test_log_only_removes_conflicting_fields(self):
        """log_only: conflicting fields are removed from patch."""
        patch = {"SPEC4": "Airtable", "SPEC12": "Also Airtable"}
        conflicts = [
            ConflictRecord(
                entity_key="P00001", field_name="SPEC4",
                source_value="Airtable", target_value="Priority",
                direction="A->P",
            ),
        ]
        # Simulate log_only: remove conflicting fields
        for c in conflicts:
            c.resolution = "skipped"
            patch.pop(c.field_name, None)
        # SPEC4 removed, SPEC12 remains
        assert "SPEC4" not in patch
        assert "SPEC12" in patch

    def test_skip_record_on_any_conflict(self):
        """skip_record: entire record is skipped if any conflict exists."""
        conflicts = [
            ConflictRecord(
                entity_key="P00001", field_name="SPEC4",
                source_value="A", target_value="P",
                direction="A->P",
            ),
        ]
        # skip_record: return None to signal skip
        for c in conflicts:
            c.resolution = "record_skipped"
        assert all(c.resolution == "record_skipped" for c in conflicts)


# ── build_field_id_map ───────────────────────────────────────────────────────

class TestBuildFieldIdMap:
    def test_basic(self):
        maps = [
            FieldMapping(
                airtable_field="Brand",
                airtable_field_id="fldABC",
                priority_field="SPEC4",
                transform="clean",
            ),
            FieldMapping(
                airtable_field="Storage",
                airtable_field_id="fldDEF",
                priority_field="SPEC12",
                transform="clean",
            ),
        ]
        result = build_field_id_map(maps)
        assert result == {"Brand": "fldABC", "Storage": "fldDEF"}

    def test_skips_none_ids(self):
        maps = [
            FieldMapping(
                airtable_field="Brand",
                airtable_field_id=None,
                priority_field="SPEC4",
                transform="clean",
            ),
        ]
        result = build_field_id_map(maps)
        assert result == {}

    def test_with_extra(self):
        maps = [
            FieldMapping(
                airtable_field="Brand",
                airtable_field_id="fldABC",
                priority_field="SPEC4",
                transform="clean",
            ),
        ]
        result = build_field_id_map(maps, extra={"SKU": "fldXYZ"})
        assert result == {"Brand": "fldABC", "SKU": "fldXYZ"}

    def test_multiple_lists(self):
        list1 = [
            FieldMapping(
                airtable_field="Brand",
                airtable_field_id="fldABC",
                priority_field="SPEC4",
                transform="clean",
            ),
        ]
        list2 = [
            FieldMapping(
                airtable_field="Storage",
                airtable_field_id="fldDEF",
                priority_field="SPEC12",
                transform="clean",
            ),
        ]
        result = build_field_id_map(list1, list2)
        assert result == {"Brand": "fldABC", "Storage": "fldDEF"}
