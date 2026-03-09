"""
Generic base sync engine: orchestrates bidirectional sync between
Airtable and Priority ERP for any entity type.

Subclasses provide entity-specific configuration (field maps, clients,
sub-form logic) while this class handles the orchestration:
  - A->P (Airtable -> Priority): fetch, map, create/update, sub-forms, timestamps
  - P->A (Priority -> Airtable): fetch, map, compare, batch create/update, timestamps

Designed to be subclassed by workflow-specific engines (products, fncpart, etc.).
"""

from __future__ import annotations

import abc
import logging
from datetime import datetime, timezone
from typing import Any, Callable

import requests

from sync.core.airtable_client import AirtableClient
from sync.core.logger_setup import (
    print_banner,
    print_conflict_line,
    print_detail,
    print_record_line,
    print_section,
    print_summary,
)
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
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.core.utils import clean, format_price, priority_yn, to_float, to_int

logger = logging.getLogger(__name__)


# ── Transform function registry ──────────────────────────────────────────────
# Maps transform name strings (from FieldMapping.transform) to actual functions.

TRANSFORMS: dict[str, Callable[[Any], Any]] = {
    "clean": clean,
    "format_price": format_price,
    "to_int": to_int,
    "to_float": to_float,
    "priority_yn": priority_yn,
}


# ═════════════════════════════════════════════════════════════════════════════
# Field ID map builder
# ═════════════════════════════════════════════════════════════════════════════


def build_field_id_map(
    *field_maps: list[FieldMapping],
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build a field name → field ID mapping from FieldMapping lists + extras.

    Used to construct the ``field_id_map`` parameter for AirtableClient so that
    API calls reference stable field IDs instead of mutable field names.

    Args:
        *field_maps: One or more lists of FieldMapping objects.  Each mapping
            with a non-None ``airtable_field_id`` is included.
        extra: Additional name→ID pairs (e.g. config key fields, timestamp
            field IDs, subform field ID dicts).

    Returns:
        dict mapping Airtable field names to their stable field IDs.
    """
    result: dict[str, str] = {}
    for fm in field_maps:
        for m in fm:
            if m.airtable_field_id:
                result[m.airtable_field] = m.airtable_field_id
    if extra:
        result.update(extra)
    return result


# ═════════════════════════════════════════════════════════════════════════════
# Generic field mapping functions
# ═════════════════════════════════════════════════════════════════════════════
# These work with any list of FieldMapping objects and any entity type.


def map_airtable_to_priority(
    airtable_fields: dict[str, Any],
    field_map: list[FieldMapping],
) -> dict[str, Any]:
    """
    Transform an Airtable record's fields dict into a Priority-ready payload.
    Skips fields with empty/None values (they won't be sent to Priority).

    Args:
        airtable_fields: Raw Airtable record fields.
        field_map: The field mapping list to use.

    Returns:
        dict mapping Priority field names to cleaned values.
    """
    payload: dict[str, Any] = {}

    for mapping in field_map:
        raw_value = airtable_fields.get(mapping.airtable_field)
        transform_fn = TRANSFORMS[mapping.transform]
        cleaned = transform_fn(raw_value)

        if cleaned is None:
            continue

        # Validate max_length if set
        if mapping.max_length and isinstance(cleaned, str) and len(cleaned) > mapping.max_length:
            cleaned = cleaned[: mapping.max_length]

        payload[mapping.priority_field] = cleaned

    return payload


def build_patch_body(
    mapped_payload: dict[str, Any],
    priority_current: dict[str, Any],
    field_map: list[FieldMapping],
    key_field: str,
) -> dict[str, Any]:
    """
    Compare mapped Airtable values with current Priority values.
    Returns only the fields that actually changed (for PATCH).

    Rules:
    - Empty Airtable value does NOT overwrite a populated Priority field.
    - Numeric fields use float comparison with epsilon tolerance.
    - create_only fields are excluded (can't be changed via PATCH).
    - Returns empty dict if nothing changed.

    Args:
        mapped_payload: Priority field name -> Airtable-mapped value.
        priority_current: Current Priority record fields.
        field_map: The field mapping list to use for type info.
        key_field: The Priority key field name (e.g. "PARTNAME") -- never patched.
    """
    patch: dict[str, Any] = {}
    EPSILON = 0.001

    # Build lookup: priority_field -> FieldMapping
    by_priority = {m.priority_field: m for m in field_map}

    for priority_field, airtable_value in mapped_payload.items():
        # Skip key field -- it's the key, never patched
        if priority_field == key_field:
            continue

        priority_value = priority_current.get(priority_field)

        # Don't overwrite populated Priority field with empty Airtable value
        if airtable_value is None or airtable_value == "":
            continue

        # Get the mapping to know the field type
        mapping = by_priority.get(priority_field)
        if not mapping:
            continue

        # Skip create_only fields (e.g. PUNITNAME can't be changed via PATCH)
        if mapping.create_only:
            continue

        # Compare based on type
        if mapping.field_type == "float":
            try:
                a_float = float(airtable_value) if airtable_value is not None else 0.0
                p_float = float(priority_value) if priority_value is not None else 0.0
                if abs(a_float - p_float) > EPSILON:
                    patch[priority_field] = airtable_value
            except (ValueError, TypeError):
                # If conversion fails, compare as strings
                if str(airtable_value) != str(priority_value or ""):
                    patch[priority_field] = airtable_value

        elif mapping.field_type == "int":
            try:
                a_int = int(float(airtable_value)) if airtable_value is not None else 0
                p_int = int(float(priority_value)) if priority_value is not None else 0
                if a_int != p_int:
                    patch[priority_field] = airtable_value
            except (ValueError, TypeError):
                if str(airtable_value) != str(priority_value or ""):
                    patch[priority_field] = airtable_value

        else:
            # String comparison
            a_str = str(airtable_value).strip()
            p_str = str(priority_value or "").strip()
            if a_str != p_str:
                patch[priority_field] = airtable_value

    return patch


def map_priority_to_airtable(
    priority_fields: dict[str, Any],
    field_map: list[FieldMapping],
    is_create: bool = False,
    lookups: dict[str, dict[str, str]] | None = None,
    linked_records: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    """
    Transform a Priority record's fields into an Airtable-ready payload.
    Skips fields with empty/None values.

    Args:
        priority_fields: Raw Priority record fields dict.
        field_map: The field mapping list to use.
        is_create: If True, include create_only fields (e.g. PARTDES).
                   If False, skip create_only fields (update mode).
        lookups: Dict of {entity_name: {code: description}} for linked-table
                 reverse lookups.  Fetched once per sync run.
        linked_records: Dict of {table_id: {match_value: record_id}} for
                        Airtable linked record resolution.  Fetched once per run.

    Returns:
        dict mapping Airtable field names to transformed values.
    """
    payload: dict[str, Any] = {}

    for mapping in field_map:
        # Skip create_only fields on updates
        if mapping.create_only and not is_create:
            continue

        raw_value = priority_fields.get(mapping.priority_field)

        # Handle priority_lookup transform (linked-table code → description)
        if mapping.transform == "priority_lookup" and mapping.lookup and lookups is not None:
            cleaned = clean(raw_value)
            if cleaned:
                lookup_dict = lookups.get(mapping.lookup.entity, {})
                cleaned = lookup_dict.get(cleaned, cleaned)  # fallback to raw code
                # Wrap in array for multipleSelects fields
                if mapping.field_type == "multiple_select":
                    cleaned = [cleaned]

        # Handle linked_record transform (Priority code → Airtable record ID array)
        elif mapping.transform == "linked_record" and mapping.linked_record and linked_records is not None:
            cleaned = clean(raw_value)
            if cleaned:
                lr_map = linked_records.get(mapping.linked_record.table_id, {})
                rec_id = lr_map.get(cleaned)
                if rec_id:
                    cleaned = [rec_id]  # Airtable linked records = array of record IDs
                else:
                    logger.warning(
                        "No linked record found for %s=%s in table %s",
                        mapping.priority_field, cleaned, mapping.linked_record.table_id,
                    )
                    continue  # skip — don't write an invalid value
            else:
                continue  # empty value — skip

        else:
            transform_fn = TRANSFORMS[mapping.transform]
            cleaned = transform_fn(raw_value)

        if cleaned is None:
            continue

        payload[mapping.airtable_field] = cleaned

    return payload


def build_airtable_patch(
    mapped_from_priority: dict[str, Any],
    current_airtable: dict[str, Any],
    field_map: list[FieldMapping],
) -> dict[str, Any]:
    """
    Compare mapped Priority values with current Airtable values.
    Returns only the Airtable fields that actually changed (for PATCH).

    Rules:
    - Empty Priority value does NOT overwrite a populated Airtable field.
    - Numeric fields use float comparison with epsilon tolerance.
    - Returns empty dict if nothing changed.

    Args:
        mapped_from_priority: Airtable field name -> mapped value from Priority.
        current_airtable: Current Airtable record fields.
        field_map: The field mapping list for type info.
    """
    patch: dict[str, Any] = {}
    EPSILON = 0.001

    # Build lookup: airtable_field -> FieldMapping
    by_airtable = {m.airtable_field: m for m in field_map}

    for airtable_field, priority_value in mapped_from_priority.items():
        current_value = current_airtable.get(airtable_field)

        # Don't overwrite populated Airtable field with empty Priority value
        if priority_value is None or priority_value == "":
            continue

        # Get the mapping to know the field type
        mapping = by_airtable.get(airtable_field)
        if not mapping:
            continue

        # p2a_write_if_empty: skip if Airtable field already has a value
        if mapping.p2a_write_if_empty:
            if current_value is not None and str(current_value).strip() != "":
                continue

        # Compare based on type
        if mapping.field_type == "float":
            try:
                p_float = float(priority_value) if priority_value is not None else 0.0
                a_float = float(current_value) if current_value is not None else 0.0
                if abs(p_float - a_float) > EPSILON:
                    patch[airtable_field] = priority_value
            except (ValueError, TypeError):
                if str(priority_value) != str(current_value or ""):
                    patch[airtable_field] = priority_value

        elif mapping.field_type == "int":
            try:
                p_int = int(float(priority_value)) if priority_value is not None else 0
                a_int = int(float(current_value)) if current_value is not None else 0
                if p_int != a_int:
                    patch[airtable_field] = priority_value
            except (ValueError, TypeError):
                if str(priority_value) != str(current_value or ""):
                    patch[airtable_field] = priority_value

        elif mapping.field_type in ("linked_record", "multiple_select"):
            # Array comparison — both linked records and multipleSelects are arrays
            p_list = priority_value if isinstance(priority_value, list) else []
            a_list = current_value if isinstance(current_value, list) else []
            if sorted(p_list) != sorted(a_list):
                patch[airtable_field] = priority_value

        else:
            # String comparison
            p_str = str(priority_value).strip()
            a_str = str(current_value or "").strip()
            if p_str != a_str:
                patch[airtable_field] = priority_value

    return patch


# ═════════════════════════════════════════════════════════════════════════════
# BaseSyncEngine
# ═════════════════════════════════════════════════════════════════════════════


class BaseSyncEngine(abc.ABC):
    """
    Generic sync engine that orchestrates bidirectional sync between
    Airtable and Priority ERP for any entity type.

    Subclasses override abstract/hook methods to provide entity-specific
    configuration (field maps, clients, sub-form logic).

    Directions:
      A->P: Airtable -> Priority
      P->A: Priority -> Airtable
    """

    def __init__(
        self,
        direction: SyncDirection = SyncDirection.AIRTABLE_TO_PRIORITY,
        dry_run: bool = False,
        single_key: str | None = None,
        trigger: str = "manual",
        mode: SyncMode = SyncMode.FULL,
        workflow_name: str = "products",
        base_id_override: str | None = None,
        token_override: str | None = None,
        priority_url_override: str | None = None,
        conflict_strategy: ConflictStrategy = ConflictStrategy.SOURCE_WINS,
    ) -> None:
        self.direction = direction
        self.dry_run = dry_run
        self.single_key = single_key
        self.trigger = trigger
        self.mode = mode
        self.workflow_name = workflow_name
        self.priority_url_override = priority_url_override
        self.conflict_strategy = conflict_strategy

        # Create clients via subclass hooks
        self.airtable: AirtableClient = self._create_airtable_client(
            base_id_override=base_id_override,
            token_override=token_override,
        )
        self.priority: PriorityClient = self._create_priority_client(
            api_url_override=priority_url_override,
        )
        self.sync_log: SyncLogClient = self._create_sync_log_client()
        self.stats = SyncStats()

        # Set Sentry tags so all events during this sync are labelled
        try:
            import sentry_sdk
            sentry_sdk.set_tag("workflow", self.workflow_name)
            sentry_sdk.set_tag("direction", self.direction.value)
            sentry_sdk.set_tag("mode", self.mode.value)
            sentry_sdk.set_tag("trigger", self.trigger)
            sentry_sdk.set_tag("dry_run", str(self.dry_run))
            # Derive Priority environment from URL override
            if self.priority_url_override:
                from sync.core.config import PRIORITY_UAT_COMPANY, PRIORITY_PROD_COMPANY
                if PRIORITY_UAT_COMPANY and PRIORITY_UAT_COMPANY in self.priority_url_override:
                    sentry_sdk.set_tag("priority_env", "uat")
                elif PRIORITY_PROD_COMPANY and PRIORITY_PROD_COMPANY in self.priority_url_override:
                    sentry_sdk.set_tag("priority_env", "production")
                else:
                    sentry_sdk.set_tag("priority_env", "sandbox")
            else:
                sentry_sdk.set_tag("priority_env", "default")
        except Exception:
            pass  # Sentry not available or not initialized

    # ── Sentry helpers ────────────────────────────────────────────────────

    def _capture_sentry_error(
        self,
        exc: Exception,
        *,
        entity_key: str,
        subform: str | None = None,
        label: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Send a caught exception to Sentry with structured context.

        Tags (indexed, filterable): entity_key, subform.
        Extras (detailed context): label, plus anything in *extra*.
        Global tags (workflow, direction, priority_env) are already set in __init__.
        """
        try:
            import sentry_sdk
            with sentry_sdk.new_scope() as scope:
                scope.set_tag("entity_key", entity_key)
                if subform:
                    scope.set_tag("subform", subform)
                if label:
                    scope.set_extra("label", label)
                if extra:
                    for k, v in extra.items():
                        scope.set_extra(k, v)
                sentry_sdk.capture_exception(exc)
        except Exception:
            pass

    # ── Abstract / hook methods (subclasses override) ─────────────────────

    @abc.abstractmethod
    def _create_airtable_client(
        self,
        base_id_override: str | None,
        token_override: str | None,
    ) -> AirtableClient:
        """
        Create and return an AirtableClient configured for this workflow.
        Subclass must provide table_name, key_field, sync_view, etc.
        """

    @abc.abstractmethod
    def _create_priority_client(
        self, api_url_override: str | None = None,
    ) -> PriorityClient:
        """
        Create and return a PriorityClient configured for this workflow.
        Subclass must provide entity name, key_field, etc.

        Args:
            api_url_override: If set, use this Priority API URL instead of
                the default from PRIORITY_API_URL env var. Used for
                environment switching (sandbox/uat/production).
        """

    def _create_sync_log_client(self) -> SyncLogClient:
        """
        Create and return a SyncLogClient. Override if you need a custom
        entity_label (default uses workflow_name).
        """
        return SyncLogClient(entity_label="SKU")

    @abc.abstractmethod
    def _get_a2p_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        """Return the A->P field mapping list for the given mode."""

    @abc.abstractmethod
    def _get_a2p_fields_to_fetch(self, mode: SyncMode) -> list[str]:
        """Return the list of Airtable fields to fetch for A->P sync."""

    @abc.abstractmethod
    def _get_p2a_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        """Return the P->A field mapping list for the given mode."""

    @abc.abstractmethod
    def _get_p2a_airtable_fields(self, mode: SyncMode) -> list[str]:
        """Return the Airtable fields to fetch for P->A comparison."""

    @abc.abstractmethod
    def _get_p2a_priority_select(self, mode: SyncMode) -> list[str]:
        """Return the Priority $select fields for P->A fetch."""

    def _fetch_priority_lookups(
        self, field_map: list[FieldMapping],
    ) -> dict[str, dict[str, str]]:
        """
        Fetch Priority lookup tables for fields that use priority_lookup transform.

        Scans the field map for fields with a LookupConfig, fetches each unique
        entity once, and returns a dict of {entity: {code: description}}.
        """
        lookups: dict[str, dict[str, str]] = {}
        seen_entities: set[str] = set()

        for mapping in field_map:
            if mapping.lookup and mapping.lookup.entity not in seen_entities:
                seen_entities.add(mapping.lookup.entity)
                try:
                    lookup_dict = self.priority.fetch_lookup_table(
                        mapping.lookup.entity,
                        mapping.lookup.code_field,
                        mapping.lookup.desc_field,
                    )
                    lookups[mapping.lookup.entity] = lookup_dict
                    print_detail(
                        f"Loaded {len(lookup_dict)} entries from "
                        f"lookup table {mapping.lookup.entity}"
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to fetch lookup table %s: %s",
                        mapping.lookup.entity,
                        e,
                    )

        return lookups

    def _fetch_linked_record_maps(
        self, field_map: list[FieldMapping],
    ) -> dict[str, dict[str, str]]:
        """
        Fetch Airtable linked record tables for fields that use linked_record transform.

        Scans the field map for fields with a LinkedRecordConfig, fetches each
        unique target table once, and returns {table_id: {match_value: record_id}}.
        """
        lr_maps: dict[str, dict[str, str]] = {}
        seen_tables: set[str] = set()

        for mapping in field_map:
            if mapping.linked_record and mapping.linked_record.table_id not in seen_tables:
                seen_tables.add(mapping.linked_record.table_id)
                try:
                    lr_map = self.airtable.fetch_linked_record_map(
                        mapping.linked_record.table_id,
                        mapping.linked_record.match_field_id,
                    )
                    lr_maps[mapping.linked_record.table_id] = lr_map
                except Exception as e:
                    logger.warning(
                        "Failed to fetch linked record table %s: %s",
                        mapping.linked_record.table_id,
                        e,
                    )

        return lr_maps

    def _auto_create_missing_linked_records(
        self,
        field_map: list[FieldMapping],
        priority_records: list[dict[str, Any]],
        linked_records: dict[str, dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        """
        Auto-create missing linked records in Airtable for fields with auto_create config.

        For each linked_record field with auto_create:
        1. Scan priority_records for codes not present in the linked_records map
        2. Fetch extra data (e.g., company name) from Priority entity
        3. Create stub records in the target Airtable table
        4. Update the linked_records map with new record IDs

        Returns the updated linked_records dict.
        """
        for mapping in field_map:
            if not (mapping.linked_record and mapping.linked_record.auto_create):
                continue

            lr_config = mapping.linked_record
            auto = lr_config.auto_create
            table_id = lr_config.table_id
            existing_map = linked_records.get(table_id, {})

            # Collect unique codes from Priority that are missing in Airtable
            missing_codes: set[str] = set()
            for record in priority_records:
                raw = record.get(mapping.priority_field)
                if raw is not None:
                    code = str(raw).strip()
                    if code and code not in existing_map:
                        missing_codes.add(code)

            if not missing_codes:
                continue

            logger.info(
                "Found %d missing linked records for %s in table %s",
                len(missing_codes), mapping.airtable_field, table_id,
            )

            # Fetch extra field data from Priority entity if configured
            extra_data: dict[str, str] = {}
            if auto.priority_entity and auto.priority_key_field and auto.extra_fields:
                # Get the first extra field's Priority field name for the lookup
                priority_desc_fields = list(auto.extra_fields.values())
                if priority_desc_fields:
                    extra_data = self.priority.fetch_lookup_table(
                        entity=auto.priority_entity,
                        code_field=auto.priority_key_field,
                        desc_field=priority_desc_fields[0],
                    )

            # Build Airtable records to create (sorted for deterministic order)
            sorted_codes = sorted(missing_codes)
            to_create: list[dict[str, Any]] = []
            for code in sorted_codes:
                fields: dict[str, Any] = {
                    auto.writable_key_field_id: code,
                }
                # Add extra fields (e.g., Company Name from SUPDES)
                for at_field_id, _priority_field in auto.extra_fields.items():
                    desc = extra_data.get(code, "")
                    if desc:
                        fields[at_field_id] = desc
                to_create.append({"fields": fields})

            # Create records in Airtable — pass codes for 1:1 mapping
            created_map = self.airtable.create_linked_records(
                table_id=table_id,
                records=to_create,
                codes=sorted_codes,
            )

            if created_map:
                # Merge newly created records into the linked_records map
                if table_id not in linked_records:
                    linked_records[table_id] = {}
                linked_records[table_id].update(created_map)
                logger.info(
                    "Auto-created %d vendor records in Airtable", len(created_map),
                )

        return linked_records

    def _get_p2a_extra_field_map(self) -> list[FieldMapping]:
        """
        Return additional field maps used by _get_p2a_extra_fields() for comparison.

        Override in workflows that merge secondary entities (e.g., products merges
        FNCPART + PRDPART).  The returned mappings are appended to the main P→A
        field map so that build_airtable_patch() can compare them correctly.

        Default: empty (no extra field maps).
        """
        return []

    def _pre_a2p_batch(
        self, records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Called before processing A->P records. Use for batch pre-fetching
        (e.g., fetch shelf lives from a separate table).

        Args:
            records: The Airtable records about to be processed.

        Returns:
            A context dict that gets passed to _sync_subforms.
            Default: empty dict.
        """
        return {}

    def _sync_subforms(
        self,
        key: str,
        airtable_fields: dict[str, Any],
        context: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Override to sync sub-forms after the main entity sync (A->P direction).

        Args:
            key: The entity key (e.g. SKU).
            airtable_fields: The raw Airtable fields dict for this record.
            context: The dict returned by _pre_a2p_batch.
            result: The SyncRecord to append SubformResults to.
            dry_run: Whether this is a dry run.

        Default: no-op (no sub-forms).
        """

    def _get_p2a_extra_fields(
        self,
        key: str,
        priority_record: dict[str, Any],
        is_status: bool,
    ) -> dict[str, Any]:
        """
        Override to return extra Airtable fields from sub-forms in P->A direction.
        (e.g., allergens mapped to Airtable fields).

        Args:
            key: The entity key.
            priority_record: The full Priority record.
            is_status: Whether this is a status-only sync.

        Returns:
            Dict of extra Airtable field name -> value to merge into the mapped fields.
            Default: empty dict.
        """
        return {}

    def _post_p2a_sync(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """
        Hook called after the main P→A batch writes (creates + updates) complete,
        before timestamps are updated.

        Override to run additional sync steps that depend on both Priority data
        and existing Airtable records — e.g., syncing sub-form data into separate
        Airtable linked tables (shelf lives, etc.).

        Args:
            priority_records: The full list of Priority records fetched for this run.
            airtable_by_key: Map of ``{key: {"record_id": str, "fields": dict}}``
                             for all existing Airtable records.

        Default: no-op.
        """

    @abc.abstractmethod
    def _get_key_field_name(self) -> str:
        """Return the Priority key field name (e.g. 'PARTNAME')."""

    @abc.abstractmethod
    def _get_airtable_key_field(self) -> str:
        """Return the Airtable key field name for reading (e.g. 'SKU Trim (EDI)')."""

    @abc.abstractmethod
    def _get_airtable_key_field_writable(self) -> str:
        """Return the Airtable key field name for writing/creates (e.g. 'SKU')."""

    # ── Main entry point ─────────────────────────────────────────────────

    def run(self) -> SyncStats:
        """Execute the sync and return stats."""
        self.stats.start_time = datetime.now(timezone.utc)

        if self.direction == SyncDirection.AIRTABLE_TO_PRIORITY:
            self._sync_airtable_to_priority()
        elif self.direction == SyncDirection.PRIORITY_TO_AIRTABLE:
            self._sync_priority_to_airtable()
        else:
            raise NotImplementedError(
                f"Direction {self.direction.value} not yet implemented."
            )

        self.stats.end_time = datetime.now(timezone.utc)

        # Log run to Airtable (non-blocking -- failures don't affect sync result)
        if not self.dry_run:
            direction_label = (
                "A→P"
                if self.direction == SyncDirection.AIRTABLE_TO_PRIORITY
                else "P→A"
            )
            self.sync_log.log_run(
                self.stats,
                direction=direction_label,
                trigger=self.trigger,
                workflow=self.workflow_name,
            )

        return self.stats

    # ═════════════════════════════════════════════════════════════════════
    # Airtable -> Priority
    # ═════════════════════════════════════════════════════════════════════

    def _sync_airtable_to_priority(self) -> None:
        """Full sync flow: Airtable -> Priority."""

        is_status = self.mode == SyncMode.STATUS
        mode_suffix = " (STATUS ONLY)" if is_status else ""
        label = (
            f"DRY RUN -- Airtable -> Priority ({self.workflow_name}){mode_suffix}"
            if self.dry_run
            else f"Airtable -> Priority ({self.workflow_name}){mode_suffix}"
        )
        print_banner(label)

        # Determine fields to fetch based on mode
        fields_to_fetch = self._get_a2p_fields_to_fetch(self.mode)

        # Step 1: Fetch records from Airtable
        airtable_key_field = self._get_airtable_key_field()

        if self.single_key:
            # When testing a specific key, fetch it directly (bypass sync view)
            print_section(f"Fetching {self.single_key} from Airtable...")
            airtable_records = self.airtable.fetch_record_by_key(
                self.single_key, fields_to_fetch=fields_to_fetch,
            )
            if not airtable_records:
                # Fallback: try the sync view in case the key is flagged
                print_detail(
                    f"{self.single_key} not found by direct lookup, trying sync view..."
                )
                airtable_records = self.airtable.fetch_changed_records(
                    fields_to_fetch=fields_to_fetch,
                )
                airtable_records = [
                    r for r in airtable_records
                    if clean(r.get("fields", {}).get(airtable_key_field)) == self.single_key
                ]
        else:
            print_section("Fetching changed records from Airtable...")
            airtable_records = self.airtable.fetch_changed_records(
                fields_to_fetch=fields_to_fetch,
            )

        self.stats.total_fetched = len(airtable_records)

        if not airtable_records:
            print_detail("No changes detected. All records are up to date.")
            logger.info("No changed records found in Airtable.")
            return

        print_detail(f"Found {len(airtable_records)} records to sync.")

        # ── Detect duplicate Airtable keys — flag as error, skip dupes ──
        seen_keys: dict[str, str] = {}   # key → first Airtable record ID
        duplicates: list[tuple[str, str]] = []  # (key, record_id) to skip
        for record in airtable_records:
            key = clean(record.get("fields", {}).get(airtable_key_field))
            rec_id = record.get("id", "")
            if key:
                if key in seen_keys:
                    duplicates.append((key, rec_id))
                    logger.error(
                        "DUPLICATE: Airtable has multiple records for %s — "
                        "skipping duplicate (record %s). Fix in Airtable.",
                        key, rec_id,
                    )
                else:
                    seen_keys[key] = rec_id

        if duplicates:
            dupe_keys = {k for k, _ in duplicates}
            dupe_ids = {rid for _, rid in duplicates}
            airtable_records = [
                r for r in airtable_records
                if r.get("id") not in dupe_ids
            ]
            print_detail(
                f"⚠ {len(duplicates)} duplicate record(s) skipped "
                f"(keys: {', '.join(sorted(dupe_keys))}). "
                f"Fix in Airtable."
            )

        print()

        # Step 2: Fetch all existing keys from Priority
        print_section("Loading existing records from Priority...")
        existing_keys = self.priority.fetch_all_keys()
        logger.info(
            "Loaded %d existing keys from Priority", len(existing_keys)
        )

        # Classify records
        new_count = 0
        existing_count = 0
        for record in airtable_records:
            key = clean(record.get("fields", {}).get(airtable_key_field))
            if key and key not in existing_keys:
                new_count += 1
            else:
                existing_count += 1

        self.stats.existing_in_priority = existing_count
        self.stats.new_to_priority = new_count
        print_detail(
            f"Loaded {len(existing_keys):,} records from Priority. "
            f"{new_count} new, {existing_count} existing."
        )
        print()

        # Step 2b: Pre-batch hook (e.g. fetch shelf lives from separate table)
        if is_status:
            context: dict[str, Any] = {}
            print_section("Skipping pre-batch fetch (status-only mode).")
        else:
            context = self._pre_a2p_batch(airtable_records)
        print()

        # Step 3: Process each record
        print_section("Syncing:")
        sync_results: list[SyncRecord] = []
        timestamp_updates: list[dict[str, Any]] = []

        # Build reverse mapping: Priority field → Airtable field (for comments)
        _field_map = self._get_a2p_field_map(self.mode)
        _p2a_names = {m.priority_field: m.airtable_field for m in _field_map}

        for idx, record in enumerate(airtable_records, 1):
            result = self._process_record(
                record=record,
                existing_keys=existing_keys,
                context=context,
                index=idx,
                total=len(airtable_records),
            )
            sync_results.append(result)

            # Queue timestamp update for successful syncs and skips
            now_utc = datetime.now(timezone.utc).isoformat()
            now_short = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
            comment = self._compose_sync_comment(
                result, "A→P", now_short, field_name_map=_p2a_names,
            )

            if result.action in (SyncAction.CREATE, SyncAction.UPDATE, SyncAction.SKIP):
                # Post comment if main entity changed OR any sub-form changed
                has_subform_activity = any(
                    sf.action in ("created", "updated", "synced", "error")
                    for sf in result.subform_results
                )
                timestamp_updates.append({
                    "record_id": result.airtable_record_id,
                    "synced_at": now_utc,
                    "priority_udate": result.priority_udate,
                    "sync_comment": comment,
                    "_post_comment": result.action in (
                        SyncAction.CREATE, SyncAction.UPDATE,
                    ) or has_subform_activity,
                })
            elif result.action == SyncAction.ERROR and result.airtable_record_id:
                # Write error to Sync Comments but don't update the sync timestamp
                timestamp_updates.append({
                    "record_id": result.airtable_record_id,
                    "synced_at": None,
                    "priority_udate": None,
                    "sync_comment": comment,
                    "_post_comment": True,
                })

        # Step 4: Update Airtable timestamps
        if timestamp_updates and not self.dry_run:
            print()
            total_batches = (len(timestamp_updates) + 9) // 10
            print_section("Updating Airtable timestamps...")
            updated = self.airtable.batch_update_timestamps(timestamp_updates)
            print_detail(f"{updated} records stamped across {total_batches} batches.")

            # Post record comments (only for CREATE, UPDATE, ERROR)
            pending_comments = [
                {"record_id": u["record_id"], "text": u["sync_comment"]}
                for u in timestamp_updates
                if u.get("sync_comment") and u.get("_post_comment")
            ]
            if pending_comments:
                print_section("Posting record comments...")
                comment_count = self.airtable.post_record_comments(
                    pending_comments,
                )
                print_detail(f"{comment_count} comments posted.")
        elif self.dry_run:
            print()
            print_section("DRY RUN -- Skipping Airtable timestamp updates.")

        # Step 5: Print summary
        self._print_final_summary()

    # ── Sync Comment Composer ───────────────────────────────────────────

    @staticmethod
    def _compose_sync_comment(
        result: SyncRecord,
        direction: str,
        timestamp: str,
        field_name_map: dict[str, str] | None = None,
    ) -> str:
        """Compose a human-readable sync comment for the Airtable record."""
        # Build sub-form summary (only non-skipped results)
        subform_parts: list[str] = []
        for sf in result.subform_results:
            if sf.action in ("created", "updated", "synced"):
                subform_parts.append(f"{sf.subform}: {sf.action}")
            elif sf.action == "error":
                subform_parts.append(f"{sf.subform}: ERROR")
        subform_line = ""
        if subform_parts:
            subform_line = "\nSub-forms: " + ", ".join(subform_parts)

        if result.action == SyncAction.CREATE:
            return f"{direction}: Created in Priority ({timestamp}){subform_line}"
        elif result.action == SyncAction.UPDATE:
            changed = result.fields_changed
            if field_name_map:
                changed = [field_name_map.get(f, f) for f in changed]
            short = ", ".join(changed[:5])
            suffix = f" +{len(changed) - 5} more" if len(changed) > 5 else ""
            return f"{direction}: Updated {short}{suffix} ({timestamp}){subform_line}"
        elif result.action == SyncAction.SKIP:
            if result.error_message:
                return f"{direction}: Skipped — {result.error_message} ({timestamp}){subform_line}"
            if subform_line:
                return f"{direction}: No field changes ({timestamp}){subform_line}"
            return f"{direction}: No changes ({timestamp})"
        elif result.action == SyncAction.ERROR:
            msg = result.error_message[:80] if result.error_message else "unknown"
            return f"{direction}: ERROR — {msg} ({timestamp}){subform_line}"
        return f"{direction}: {result.action.value} ({timestamp}){subform_line}"

    # ── Single record processing (A->P) ──────────────────────────────────

    def _process_record(
        self,
        record: dict[str, Any],
        existing_keys: set[str],
        context: dict[str, Any],
        index: int,
        total: int,
    ) -> SyncRecord:
        """Process a single Airtable record: map, compare, and sync to Priority."""

        fields = record.get("fields", {})
        record_id = record.get("id", "")
        airtable_key_field = self._get_airtable_key_field()
        priority_key_field = self._get_key_field_name()
        key = clean(fields.get(airtable_key_field))

        result = SyncRecord(airtable_record_id=record_id, entity_key=key or "")

        # Skip records with no key
        if not key:
            result.action = SyncAction.ERROR
            result.error_message = "Empty key"
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                entity_key="(empty)",
                action="EXTRACT",
                message="Empty key -- skipped",
                timestamp=datetime.now(timezone.utc),
            ))
            print_record_line(index, total, "(empty)", "ERROR", "empty key -- skipped")
            logger.warning("Record %s has empty key, skipping", record_id)
            return result

        # ── Loop prevention: skip records recently synced FROM Priority ──
        # If "Last Synced from Priority" > "Last Synced to Priority", this record
        # was recently written by a P->A sync. Skip the API call to avoid a loop.
        last_from_p = fields.get(self.airtable.ts.get("last_synced_from", "Last Synced from Priority"))
        last_to_p = fields.get(self.airtable.ts.get("last_synced_to", "Last Synced to Priority"))
        if last_from_p and (not last_to_p or str(last_from_p) > str(last_to_p)):
            result.action = SyncAction.SKIP
            self.stats.skipped += 1
            print_record_line(
                index, total, key, "SKIP",
                "recently synced from Priority (loop prevention)",
            )
            logger.debug(
                "Skipping %s: Last Synced from Priority (%s) > Last Synced to Priority (%s)",
                key, last_from_p, last_to_p,
            )
            return result

        # Map Airtable fields -> Priority fields
        field_map = self._get_a2p_field_map(self.mode)
        try:
            priority_payload = map_airtable_to_priority(fields, field_map=field_map)
        except Exception as e:
            result.action = SyncAction.ERROR
            result.error_message = f"Mapping error: {e}"
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                entity_key=key,
                action="MAP",
                message=str(e),
                timestamp=datetime.now(timezone.utc),
            ))
            print_record_line(index, total, key, "ERROR", f"mapping error: {e}")
            logger.error("Mapping error for %s: %s", key, e)
            return result

        # Route: CREATE or UPDATE (main entity fields)
        if key not in existing_keys:
            result = self._create_entity(
                key=key,
                payload=priority_payload,
                result=result,
                index=index,
                total=total,
            )
        else:
            result = self._update_entity(
                key=key,
                payload=priority_payload,
                result=result,
                index=index,
                total=total,
                airtable_fields=fields,
            )

        # Sub-form sync -- only if main entity sync succeeded
        # (skipped entirely in STATUS mode -- no sub-form fields involved)
        if self.mode != SyncMode.STATUS and result.action in (
            SyncAction.CREATE, SyncAction.UPDATE, SyncAction.SKIP
        ):
            self._sync_subforms(key, fields, context, result, self.dry_run)

        return result

    # ── Entity create/update (A->P) ──────────────────────────────────────

    def _create_entity(
        self,
        key: str,
        payload: dict[str, Any],
        result: SyncRecord,
        index: int,
        total: int,
    ) -> SyncRecord:
        """Create a new entity in Priority."""

        if self.dry_run:
            result.action = SyncAction.CREATE
            result.fields_changed = list(payload.keys())
            self.stats.created += 1
            print_record_line(
                index, total, key, "CREATE",
                f"[DRY RUN] {len(payload)} fields",
            )
            return result

        try:
            response_data = self.priority.create_record(payload)
            result.action = SyncAction.CREATE
            result.fields_changed = list(payload.keys())
            result.priority_udate = response_data.get("UDATE")
            self.stats.created += 1

            print_record_line(
                index, total, key, "CREATE",
                f"new record ({len(payload)} fields)",
            )
            logger.info("Created %s with %d fields", key, len(payload))
            return result

        except (requests.HTTPError, requests.RequestException) as e:
            status_code = None
            message = str(e)
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                try:
                    err_body = e.response.json()
                    message = err_body.get("error", {}).get("message", str(e))
                except Exception:
                    message = e.response.text[:200] if e.response.text else str(e)

            # ── 409 fallback: record already exists → retry as UPDATE ──
            if status_code == 409:
                logger.warning(
                    "%s: POST returned 409 (already exists) — "
                    "falling back to UPDATE",
                    key,
                )
                print_record_line(
                    index, total, key, "RETRY", "exists → updating",
                )
                return self._update_entity(
                    key=key, payload=payload, result=result,
                    index=index, total=total,
                )

            result.action = SyncAction.ERROR
            result.error_message = message
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                entity_key=key,
                action="CREATE",
                status_code=status_code,
                message=message,
                timestamp=datetime.now(timezone.utc),
            ))

            status_str = f"HTTP {status_code}" if status_code else "Error"
            print_record_line(index, total, key, "ERROR", f"CREATE failed ({status_str})")
            logger.error("Failed to create %s: %s", key, message)

            self._capture_sentry_error(e, entity_key=key, extra={
                "http_status": status_code,
                "priority_message": message,
                "payload_fields": list(payload.keys()),
            })

            return result

    # ── Conflict detection (A->P) ─────────────────────────────────────

    def _detect_a2p_conflicts(
        self,
        key: str,
        patch_body: dict[str, Any],
        priority_current: dict[str, Any],
        airtable_fields: dict[str, Any],
    ) -> list[ConflictRecord]:
        """
        Check if Priority was independently modified since the last A→P sync.

        Compares Priority's current UDATE with the stored 'Priority UDATE'
        in the Airtable record.  If Priority's UDATE is newer, any fields
        in the patch that differ from Priority's current value are conflicts.
        """
        conflicts: list[ConflictRecord] = []

        # Get stored UDATE from Airtable record (set during last sync)
        stored_udate = airtable_fields.get(
            self.airtable.ts.get("priority_udate", "Priority UDATE"), ""
        )
        # Get current UDATE from Priority
        current_udate = priority_current.get("UDATE", "")

        if not stored_udate or not current_udate:
            return conflicts  # Can't compare — no conflict detection possible

        if str(current_udate) <= str(stored_udate):
            return conflicts  # Priority hasn't changed since last sync

        # Priority was modified — check which fields we're about to overwrite
        now_iso = datetime.now(timezone.utc).isoformat()
        for priority_field in patch_body:
            priority_value = priority_current.get(priority_field)
            airtable_value = patch_body[priority_field]
            # Only flag as conflict if Priority has a non-None value that differs
            if priority_value is not None and priority_value != airtable_value:
                conflicts.append(ConflictRecord(
                    entity_key=key,
                    field_name=priority_field,
                    source_value=airtable_value,
                    target_value=priority_value,
                    direction="A→P",
                    resolution="pending",
                    timestamp=now_iso,
                ))

        return conflicts

    def _apply_a2p_conflict_resolution(
        self,
        conflicts: list[ConflictRecord],
        patch_body: dict[str, Any],
        key: str,
        result: SyncRecord,
        index: int,
        total: int,
    ) -> dict[str, Any] | None:
        """
        Apply the conflict resolution strategy and return the (possibly modified)
        patch body, or None if the entire record should be skipped.

        Logs each conflict and appends to self.stats.conflicts.
        """
        for c in conflicts:
            if self.conflict_strategy == ConflictStrategy.LOG_ONLY:
                c.resolution = "skipped"
            elif self.conflict_strategy == ConflictStrategy.SKIP_RECORD:
                c.resolution = "record_skipped"
            else:
                c.resolution = "source_wins"

            self.stats.conflicts.append(c)
            print_conflict_line(key, c.field_name, c.source_value, c.target_value, c.resolution)
            logger.warning(
                "Conflict on %s.%s: source=%r target=%r → %s",
                key, c.field_name, c.source_value, c.target_value, c.resolution,
            )

        if self.conflict_strategy == ConflictStrategy.SKIP_RECORD:
            result.action = SyncAction.SKIP
            result.error_message = f"Conflict on {len(conflicts)} field(s) — record skipped"
            self.stats.skipped += 1
            print_record_line(
                index, total, key, "SKIP",
                f"conflict on {len(conflicts)} field(s) — record skipped",
            )
            return None

        if self.conflict_strategy == ConflictStrategy.LOG_ONLY:
            # Remove conflicting fields from the patch — don't overwrite
            for c in conflicts:
                patch_body.pop(c.field_name, None)

        # SOURCE_WINS: patch_body is unchanged
        return patch_body

    def _update_entity(
        self,
        key: str,
        payload: dict[str, Any],
        result: SyncRecord,
        index: int,
        total: int,
        airtable_fields: dict[str, Any] | None = None,
    ) -> SyncRecord:
        """Compare and update an existing entity in Priority."""

        # Fetch current record from Priority for comparison
        try:
            priority_current = self.priority.get_record(key)
        except (requests.HTTPError, requests.RequestException) as e:
            result.action = SyncAction.ERROR
            result.error_message = f"GET failed: {e}"
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                entity_key=key,
                action="GET",
                message=str(e),
                timestamp=datetime.now(timezone.utc),
            ))
            print_record_line(index, total, key, "ERROR", f"GET failed: {e}")
            logger.error("Failed to GET %s from Priority: %s", key, e)

            self._capture_sentry_error(e, entity_key=key)

            return result

        if priority_current is None:
            # Key was in the set but GET returned 404 -- treat as create
            logger.warning(
                "%s was in keys set but GET returned 404. Creating.", key
            )
            return self._create_entity(
                key=key,
                payload=payload,
                result=result,
                index=index,
                total=total,
            )

        # Build PATCH body (only changed fields)
        field_map = self._get_a2p_field_map(self.mode)
        priority_key_field = self._get_key_field_name()
        patch_body = build_patch_body(
            payload, priority_current,
            field_map=field_map,
            key_field=priority_key_field,
        )

        if not patch_body:
            # No changes detected
            result.action = SyncAction.SKIP
            result.priority_udate = priority_current.get("UDATE")
            self.stats.skipped += 1
            print_record_line(index, total, key, "SKIP", "no changes")
            logger.debug("No changes for %s", key)
            return result

        # ── Conflict detection ──────────────────────────────────────────
        if airtable_fields is not None:
            conflicts = self._detect_a2p_conflicts(
                key, patch_body, priority_current, airtable_fields,
            )
            if conflicts:
                resolved = self._apply_a2p_conflict_resolution(
                    conflicts, patch_body, key, result, index, total,
                )
                if resolved is None:
                    # Record was skipped by conflict strategy
                    return result
                patch_body = resolved
                if not patch_body:
                    # All fields were removed by log_only strategy
                    result.action = SyncAction.SKIP
                    result.priority_udate = priority_current.get("UDATE")
                    result.error_message = f"All {len(conflicts)} changed field(s) had conflicts — skipped"
                    self.stats.skipped += 1
                    print_record_line(index, total, key, "SKIP", "all fields had conflicts")
                    return result

        if self.dry_run:
            result.action = SyncAction.UPDATE
            result.fields_changed = list(patch_body.keys())
            self.stats.updated += 1
            field_names = ", ".join(patch_body.keys())
            print_record_line(
                index, total, key, "UPDATE",
                f"[DRY RUN] {len(patch_body)} fields ({field_names})",
            )
            return result

        # Execute PATCH
        try:
            response_data = self.priority.update_record(key, patch_body)
            result.action = SyncAction.UPDATE
            result.fields_changed = list(patch_body.keys())
            result.priority_udate = response_data.get("UDATE")
            self.stats.updated += 1

            field_names = ", ".join(patch_body.keys())
            print_record_line(
                index, total, key, "UPDATE",
                f"{len(patch_body)} fields ({field_names})",
            )
            logger.info(
                "Updated %s: %d fields (%s)", key, len(patch_body), field_names
            )
            return result

        except (requests.HTTPError, requests.RequestException) as e:
            status_code = None
            message = str(e)
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                try:
                    err_body = e.response.json()
                    message = err_body.get("error", {}).get("message", str(e))
                except Exception:
                    message = e.response.text[:200] if e.response.text else str(e)

            result.action = SyncAction.ERROR
            result.error_message = message
            self.stats.errors += 1
            self.stats.error_details.append(SyncError(
                entity_key=key,
                action="UPDATE",
                status_code=status_code,
                message=message,
                timestamp=datetime.now(timezone.utc),
            ))

            status_str = f"HTTP {status_code}" if status_code else "Error"
            print_record_line(index, total, key, "ERROR", f"UPDATE failed ({status_str})")
            logger.error("Failed to update %s: %s", key, message)

            self._capture_sentry_error(e, entity_key=key, extra={
                "http_status": status_code,
                "priority_message": message,
                "patch_fields": list(patch_body.keys()),
            })

            return result

    # ── Conflict detection (P->A) ─────────────────────────────────────

    def _detect_p2a_conflicts(
        self,
        key: str,
        patch: dict[str, Any],
        current_airtable: dict[str, Any],
    ) -> list[ConflictRecord]:
        """
        Check if Airtable was independently modified since the last P→A sync.

        Compares 'Last Synced to Priority' with 'Last Synced from Priority'.
        If A→P ran more recently than the last P→A, the record was modified
        in Airtable — any fields in the patch that differ are conflicts.
        """
        conflicts: list[ConflictRecord] = []

        last_synced_to = current_airtable.get(
            self.airtable.ts.get("last_synced_to", "Last Synced to Priority"), ""
        )
        last_synced_from = current_airtable.get(
            self.airtable.ts.get("last_synced_from", "Last Synced from Priority"), ""
        )

        if not last_synced_to or not last_synced_from:
            return conflicts

        if str(last_synced_to) <= str(last_synced_from):
            return conflicts  # A→P hasn't run since last P→A — no conflict

        # Airtable was modified via A→P — check which fields we're about to overwrite
        now_iso = datetime.now(timezone.utc).isoformat()
        for airtable_field, priority_value in patch.items():
            airtable_value = current_airtable.get(airtable_field)
            if airtable_value is not None and airtable_value != priority_value:
                conflicts.append(ConflictRecord(
                    entity_key=key,
                    field_name=airtable_field,
                    source_value=priority_value,
                    target_value=airtable_value,
                    direction="P→A",
                    resolution="pending",
                    timestamp=now_iso,
                ))

        return conflicts

    def _apply_p2a_conflict_resolution(
        self,
        conflicts: list[ConflictRecord],
        patch: dict[str, Any],
        key: str,
        idx: int,
        total: int,
    ) -> dict[str, Any] | None:
        """
        Apply the conflict resolution strategy for P→A and return the
        (possibly modified) patch, or None if the record should be skipped.
        """
        for c in conflicts:
            if self.conflict_strategy == ConflictStrategy.LOG_ONLY:
                c.resolution = "skipped"
            elif self.conflict_strategy == ConflictStrategy.SKIP_RECORD:
                c.resolution = "record_skipped"
            else:
                c.resolution = "source_wins"

            self.stats.conflicts.append(c)
            print_conflict_line(key, c.field_name, c.source_value, c.target_value, c.resolution)
            logger.warning(
                "Conflict on %s.%s: source=%r target=%r → %s",
                key, c.field_name, c.source_value, c.target_value, c.resolution,
            )

        if self.conflict_strategy == ConflictStrategy.SKIP_RECORD:
            return None

        if self.conflict_strategy == ConflictStrategy.LOG_ONLY:
            for c in conflicts:
                patch.pop(c.field_name, None)

        return patch

    # ═════════════════════════════════════════════════════════════════════
    # Priority -> Airtable
    # ═════════════════════════════════════════════════════════════════════

    def _sync_priority_to_airtable(self) -> None:
        """
        Full sync flow: Priority -> Airtable.

        Steps:
        1. Read last Max UDATE from sync log (change detection)
        2. Fetch changed records from Priority
        3. Fetch all Airtable keys + current field values
        4. For each Priority record: queue CREATE or PATCH
        5. Batch-write to Airtable
        6. Update timestamps
        """
        is_status = self.mode == SyncMode.STATUS
        mode_suffix = " (STATUS ONLY)" if is_status else ""
        label = (
            f"DRY RUN -- Priority -> Airtable ({self.workflow_name}){mode_suffix}"
            if self.dry_run
            else f"Priority -> Airtable ({self.workflow_name}){mode_suffix}"
        )
        print_banner(label)

        priority_key_field = self._get_key_field_name()
        airtable_key_field_writable = self._get_airtable_key_field_writable()

        # ── Step 1: Get last UDATE for change detection ──────────────────
        since_udate: str | None = None
        if not self.single_key:
            print_section("Reading last P->A sync timestamp...")
            since_udate = self.sync_log.get_last_p2a_max_udate(
                workflow=self.workflow_name,
            )
            if since_udate:
                print_detail(f"Fetching records updated since: {since_udate}")
            else:
                print_detail("No previous P->A run found -- fetching ALL records.")
            print()

        # ── Step 2: Fetch changed records from Priority ──────────────────
        select_fields = self._get_p2a_priority_select(self.mode)

        if self.single_key:
            print_section(f"Fetching {self.single_key} from Priority...")
            record = self.priority.get_record(self.single_key)
            priority_records = [record] if record else []
        else:
            print_section("Fetching changed records from Priority...")
            priority_records = self.priority.fetch_changed_records(
                since_udate=since_udate,
                select_fields=select_fields,
            )

        self.stats.total_fetched = len(priority_records)

        if not priority_records:
            print_detail("No changes detected. All records are up to date.")
            logger.info("No changed records found in Priority.")
            return

        print_detail(f"Found {len(priority_records)} records to sync.")
        print()

        # ── Step 3: Fetch all Airtable keys + current values ─────────────
        airtable_fields_to_fetch = self._get_p2a_airtable_fields(self.mode)

        print_section("Loading existing records from Airtable...")
        airtable_by_key = self.airtable.fetch_all_keys(
            fields_to_fetch=airtable_fields_to_fetch,
        )
        print_detail(f"Loaded {len(airtable_by_key):,} records from Airtable.")
        print()

        # ── Step 4: Process each Priority record ─────────────────────────
        print_section("Syncing:")

        # Choose field map based on mode
        field_map = self._get_p2a_field_map(self.mode)

        # Fetch Priority lookup tables for linked-table fields (once per run)
        # Stored on self so _get_p2a_extra_fields() can also use them
        lookups = self._fetch_priority_lookups(field_map)
        self._p2a_lookups = lookups

        # Fetch Airtable linked record maps (once per run)
        linked_records = self._fetch_linked_record_maps(field_map)

        # Auto-create missing linked records if configured
        linked_records = self._auto_create_missing_linked_records(
            field_map, priority_records, linked_records,
        )
        self._p2a_linked_records = linked_records

        # Queues for batch operations
        creates: list[dict[str, Any]] = []  # {"fields": {...}}
        updates: list[dict[str, Any]] = []  # {"id": record_id, "fields": {...}}
        timestamp_updates: list[dict[str, Any]] = []  # For P->A timestamp

        max_udate: str | None = None

        for idx, priority_record in enumerate(priority_records, 1):
            key = clean(priority_record.get(priority_key_field))
            if not key:
                print_record_line(
                    idx, len(priority_records), "(empty)", "ERROR",
                    f"empty {priority_key_field}",
                )
                self.stats.errors += 1
                continue

            # Track highest UDATE
            record_udate = priority_record.get("UDATE")
            if record_udate:
                if max_udate is None or str(record_udate) > str(max_udate):
                    max_udate = record_udate

            # Map Priority fields -> Airtable fields
            is_new = key not in airtable_by_key

            try:
                mapped_fields = map_priority_to_airtable(
                    priority_record,
                    field_map=field_map,
                    is_create=is_new,
                    lookups=lookups,
                    linked_records=linked_records,
                )
            except Exception as e:
                print_record_line(
                    idx, len(priority_records), key, "ERROR", f"mapping: {e}"
                )
                self.stats.errors += 1
                self.stats.error_details.append(SyncError(
                    entity_key=key,
                    action="MAP",
                    message=str(e),
                    timestamp=datetime.now(timezone.utc),
                ))
                continue

            # Fetch extra fields from sub-forms (e.g. allergens)
            extra_fields = self._get_p2a_extra_fields(key, priority_record, is_status)

            if is_new:
                # ── CREATE: new record in Airtable ───────────────────────
                create_fields: dict[str, Any] = {
                    airtable_key_field_writable: key,
                }
                create_fields.update(mapped_fields)
                if extra_fields:
                    create_fields.update(extra_fields)

                creates.append({"fields": create_fields})
                self.stats.created += 1

                field_names = ", ".join(mapped_fields.keys())
                print_record_line(
                    idx, len(priority_records), key,
                    "CREATE" if not self.dry_run else "CREATE [DRY]",
                    f"{len(mapped_fields)} fields ({field_names})",
                )
            else:
                # ── UPDATE: compare and patch changed fields ─────────────
                airtable_record = airtable_by_key[key]
                airtable_record_id = airtable_record["record_id"]
                current_fields = airtable_record["fields"]

                # Merge extra fields into mapped for comparison
                all_mapped = dict(mapped_fields)
                if extra_fields:
                    all_mapped.update(extra_fields)

                # Include extra field maps (e.g., FNCPART, PRDPART) for comparison
                comparison_map = field_map + self._get_p2a_extra_field_map()

                patch = build_airtable_patch(
                    all_mapped, current_fields, field_map=comparison_map,
                )

                now_short = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

                if not patch:
                    self.stats.skipped += 1
                    print_record_line(
                        idx, len(priority_records), key, "SKIP", "no changes"
                    )
                    comment = f"P→A: No changes ({now_short})"
                else:
                    # ── Conflict detection (P→A) ────────────────────────
                    p2a_conflicts = self._detect_p2a_conflicts(
                        key, patch, current_fields,
                    )
                    if p2a_conflicts:
                        resolved = self._apply_p2a_conflict_resolution(
                            p2a_conflicts, patch, key, idx, len(priority_records),
                        )
                        if resolved is None:
                            # Record skipped by conflict strategy
                            self.stats.skipped += 1
                            print_record_line(
                                idx, len(priority_records), key, "SKIP",
                                f"conflict on {len(p2a_conflicts)} field(s) — record skipped",
                            )
                            comment = (
                                f"P→A: Skipped — conflict on "
                                f"{len(p2a_conflicts)} field(s) ({now_short})"
                            )
                            timestamp_updates.append({
                                "record_id": airtable_record_id,
                                "synced_at": datetime.now(timezone.utc).isoformat(),
                                "priority_udate": record_udate,
                                "sync_comment": comment,
                                "_post_comment": True,
                            })
                            continue
                        patch = resolved
                        if not patch:
                            # All fields removed by log_only strategy
                            self.stats.skipped += 1
                            print_record_line(
                                idx, len(priority_records), key, "SKIP",
                                "all fields had conflicts",
                            )
                            comment = (
                                f"P→A: Skipped — all {len(p2a_conflicts)} "
                                f"changed field(s) had conflicts ({now_short})"
                            )
                            timestamp_updates.append({
                                "record_id": airtable_record_id,
                                "synced_at": datetime.now(timezone.utc).isoformat(),
                                "priority_udate": record_udate,
                                "sync_comment": comment,
                                "_post_comment": True,
                            })
                            continue

                    updates.append({"id": airtable_record_id, "fields": patch})
                    self.stats.updated += 1

                    field_names = ", ".join(patch.keys())
                    print_record_line(
                        idx, len(priority_records), key,
                        "UPDATE" if not self.dry_run else "UPDATE [DRY]",
                        f"{len(patch)} fields ({field_names})",
                    )
                    short_fields = ", ".join(list(patch.keys())[:5])
                    suffix = f" +{len(patch) - 5} more" if len(patch) > 5 else ""
                    comment = f"P→A: Updated {short_fields}{suffix} ({now_short})"

                # Queue timestamp for existing records (both updated and skipped)
                timestamp_updates.append({
                    "record_id": airtable_record_id,
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                    "priority_udate": record_udate,
                    "sync_comment": comment,
                    "_post_comment": bool(patch),  # True for UPDATE, False for SKIP
                })

        # Store max UDATE for sync log
        self.stats.max_priority_udate = max_udate

        # ── Step 5: Batch-write to Airtable ──────────────────────────────
        if self.dry_run:
            print()
            print_section("DRY RUN -- Skipping all Airtable writes.")
            if creates:
                print_detail(f"Would CREATE {len(creates)} new records.")
            if updates:
                print_detail(f"Would UPDATE {len(updates)} existing records.")
        else:
            print()

            # Batch CREATE new records
            if creates:
                print_section(f"Creating {len(creates)} new Airtable records...")
                created_count = self.airtable.batch_create_records(creates)
                print_detail(f"Created {created_count} records.")

            # Batch PATCH existing records
            if updates:
                print_section(f"Updating {len(updates)} Airtable records...")
                updated_count = self.airtable.batch_update_records(updates)
                print_detail(f"Updated {updated_count} records.")

            # ── Step 5b: Post-sync hook (sub-form → linked table) ────────
            self._post_p2a_sync(priority_records, airtable_by_key)

            # ── Step 6: Update P->A timestamps ───────────────────────────
            if timestamp_updates:
                print_section("Updating P->A timestamps...")
                ts_count = self.airtable.batch_update_p2a_timestamps(
                    timestamp_updates,
                )
                print_detail(f"Timestamped {ts_count} records.")

                # Post record comments (only for UPDATE, not SKIP)
                pending_comments = [
                    {"record_id": u["record_id"], "text": u["sync_comment"]}
                    for u in timestamp_updates
                    if u.get("sync_comment") and u.get("_post_comment")
                ]
                if pending_comments:
                    print_section("Posting record comments...")
                    comment_count = self.airtable.post_record_comments(
                        pending_comments,
                    )
                    print_detail(f"{comment_count} comments posted.")

        # ── Summary ──────────────────────────────────────────────────────
        self._print_final_summary()

    # ── Shared helpers ───────────────────────────────────────────────────

    def _print_final_summary(self) -> None:
        """Print the final sync summary and error details."""
        print_summary(
            created=self.stats.created,
            updated=self.stats.updated,
            skipped=self.stats.skipped,
            errors=self.stats.errors,
            duration=self.stats.duration_display,
            conflicts=len(self.stats.conflicts),
        )

        if self.stats.error_details:
            print("Errors:")
            for err in self.stats.error_details:
                status = f"HTTP {err.status_code}" if err.status_code else "Error"
                print(
                    f"  - {err.entity_key}: {err.action} failed "
                    f"({status}: {err.message})"
                )
            print()
