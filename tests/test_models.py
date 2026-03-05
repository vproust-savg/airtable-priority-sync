"""Tests for sync/core/models.py Pydantic models."""

from __future__ import annotations

from sync.core.models import (
    ConflictRecord,
    ConflictStrategy,
    FieldMapping,
    SubformResult,
    SyncAction,
    SyncDirection,
    SyncError,
    SyncMode,
    SyncRecord,
    SyncStats,
)


class TestEnums:
    def test_sync_direction_values(self):
        assert SyncDirection.AIRTABLE_TO_PRIORITY.value == "airtable-to-priority"
        assert SyncDirection.PRIORITY_TO_AIRTABLE.value == "priority-to-airtable"
        assert SyncDirection.BIDIRECTIONAL.value == "both"

    def test_sync_mode_values(self):
        assert SyncMode.FULL.value == "full"
        assert SyncMode.STATUS.value == "status"

    def test_sync_action_values(self):
        assert SyncAction.CREATE.value == "CREATE"
        assert SyncAction.UPDATE.value == "UPDATE"
        assert SyncAction.SKIP.value == "SKIP"
        assert SyncAction.ERROR.value == "ERROR"

    def test_conflict_strategy_values(self):
        assert ConflictStrategy.SOURCE_WINS.value == "source_wins"
        assert ConflictStrategy.LOG_ONLY.value == "log_only"
        assert ConflictStrategy.SKIP_RECORD.value == "skip_record"


class TestFieldMapping:
    def test_basic(self):
        m = FieldMapping(
            airtable_field="Brand",
            priority_field="SPEC4",
            transform="clean",
        )
        assert m.airtable_field == "Brand"
        assert m.priority_field == "SPEC4"
        assert m.transform == "clean"
        assert m.airtable_field_id is None
        assert m.required is False
        assert m.field_type == "str"
        assert m.max_length is None
        assert m.create_only is False

    def test_with_field_id(self):
        m = FieldMapping(
            airtable_field="Brand",
            airtable_field_id="fldESkOOA4I6VqgEH",
            priority_field="SPEC4",
            transform="clean",
        )
        assert m.airtable_field_id == "fldESkOOA4I6VqgEH"

    def test_all_fields(self):
        m = FieldMapping(
            airtable_field="Price",
            airtable_field_id="fldXYZ",
            priority_field="PRICE",
            transform="format_price",
            required=True,
            field_type="float",
            max_length=10,
            create_only=True,
        )
        assert m.required is True
        assert m.field_type == "float"
        assert m.max_length == 10
        assert m.create_only is True


class TestConflictRecord:
    def test_basic(self):
        c = ConflictRecord(
            entity_key="P00001",
            field_name="SPEC4",
            source_value="Savory",
            target_value="Gourmet",
            direction="A->P",
            resolution="source_wins",
            timestamp="2026-03-05T12:00:00Z",
        )
        assert c.entity_key == "P00001"
        assert c.field_name == "SPEC4"
        assert c.source_value == "Savory"
        assert c.target_value == "Gourmet"
        assert c.direction == "A->P"
        assert c.resolution == "source_wins"

    def test_defaults(self):
        c = ConflictRecord(
            entity_key="P00001",
            field_name="SPEC4",
            direction="A->P",
        )
        assert c.source_value is None
        assert c.target_value is None
        assert c.resolution == "pending"
        assert c.timestamp == ""


class TestSyncStats:
    def test_defaults(self):
        stats = SyncStats()
        assert stats.total_fetched == 0
        assert stats.created == 0
        assert stats.updated == 0
        assert stats.skipped == 0
        assert stats.errors == 0
        assert stats.conflicts == []
        assert stats.max_priority_udate is None

    def test_conflicts_list(self):
        stats = SyncStats()
        c = ConflictRecord(
            entity_key="P00001",
            field_name="SPEC4",
            direction="A->P",
        )
        stats.conflicts.append(c)
        assert len(stats.conflicts) == 1


class TestSyncRecord:
    def test_defaults(self):
        r = SyncRecord(airtable_record_id="recXYZ", entity_key="P00001")
        assert r.action == SyncAction.SKIP
        assert r.fields_changed == []
        assert r.subform_results == []
        assert r.error_message == ""
        assert r.priority_udate is None
