"""
FNCCUST sync engine: Financial Parameters for Customers.

Subclasses BaseSyncEngine with FNCCUST-specific configuration.
No sub-forms — all fields live on the main FNCCUST entity.
No STATUS mode.

NOTE: P→A is very limited — only Priority Customer Status is writable
in Airtable (all other financial parameter fields are formulas).
"""

from __future__ import annotations

from sync.core.airtable_client import AirtableClient
from sync.core.base_engine import BaseSyncEngine
from sync.core.models import FieldMapping, SyncMode
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.workflows.fnccust.config import (
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    PRIORITY_ENTITY,
    PRIORITY_KEY_FIELD,
    TIMESTAMP_FIELDS,
)
from sync.workflows.fnccust.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELDS_TO_FETCH,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
)


class FnccustSyncEngine(BaseSyncEngine):
    """
    Sync engine for FNCCUST (Financial Parameters for Customers).

    Simple engine with no sub-forms. Uses standard string keys
    (FNCCUST('C00001') works directly).
    """

    # ── Client factories ─────────────────────────────────────────────────

    def _create_airtable_client(
        self,
        base_id_override: str | None,
        token_override: str | None,
    ) -> AirtableClient:
        return AirtableClient(
            table_name=AIRTABLE_TABLE_NAME,
            key_field=AIRTABLE_KEY_FIELD,
            key_field_writable=AIRTABLE_KEY_FIELD_WRITABLE,
            sync_view=AIRTABLE_SYNC_VIEW,
            timestamp_fields=TIMESTAMP_FIELDS,
            base_id_override=base_id_override,
            token_override=token_override,
        )

    def _create_priority_client(self) -> PriorityClient:
        return PriorityClient(
            entity=PRIORITY_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            # FNCCUST uses string keys — standard URL access
        )

    def _create_sync_log_client(self) -> SyncLogClient:
        return SyncLogClient(entity_label="Customer ID")

    # ── Field mapping accessors ──────────────────────────────────────────

    def _get_a2p_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        return A2P_FIELD_MAP

    def _get_a2p_fields_to_fetch(self, mode: SyncMode) -> list[str]:
        return AIRTABLE_FIELDS_TO_FETCH

    def _get_p2a_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        return P2A_FIELD_MAP

    def _get_p2a_airtable_fields(self, mode: SyncMode) -> list[str]:
        return P2A_AIRTABLE_FIELDS_TO_FETCH

    def _get_p2a_priority_select(self, mode: SyncMode) -> list[str]:
        return P2A_PRIORITY_SELECT

    # ── Key field accessors ──────────────────────────────────────────────

    def _get_key_field_name(self) -> str:
        return PRIORITY_KEY_FIELD

    def _get_airtable_key_field(self) -> str:
        return AIRTABLE_KEY_FIELD

    def _get_airtable_key_field_writable(self) -> str:
        return AIRTABLE_KEY_FIELD_WRITABLE
