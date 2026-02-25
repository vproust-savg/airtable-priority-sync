"""
FNCPART sync engine: Financial Parameters for Parts.

Subclasses BaseSyncEngine with FNCPART-specific configuration.
No sub-forms -- all fields live on the main FNCPART entity.
No STATUS mode -- only 10 fields, always sync all.
"""

from __future__ import annotations

from sync.core.airtable_client import AirtableClient
from sync.core.base_engine import BaseSyncEngine
from sync.core.models import FieldMapping, SyncMode
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.workflows.fncpart.config import (
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    PRIORITY_ENTITY,
    PRIORITY_KEY_FIELD,
    TIMESTAMP_FIELDS,
)
from sync.workflows.fncpart.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELDS_TO_FETCH,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
)


class FncpartSyncEngine(BaseSyncEngine):
    """
    Sync engine for FNCPART (Financial Parameters for Parts).

    Simple engine with no sub-forms.  The _sync_subforms and
    _get_p2a_extra_fields hooks use the default no-op from BaseSyncEngine.
    No _pre_a2p_batch override needed either.
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

    def _create_priority_client(self, api_url_override: str | None = None) -> PriorityClient:
        return PriorityClient(
            entity=PRIORITY_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            use_filter_lookup=True,  # FNCPART uses integer PART as URL key, not PARTNAME
            api_url_override=api_url_override,
        )

    def _create_sync_log_client(self) -> SyncLogClient:
        return SyncLogClient(entity_label="SKU")

    # ── Field mapping accessors ──────────────────────────────────────────

    def _get_a2p_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        """Always return the full A2P map (no status mode for FNCPART)."""
        return A2P_FIELD_MAP

    def _get_a2p_fields_to_fetch(self, mode: SyncMode) -> list[str]:
        """Always return all fields (no status mode for FNCPART)."""
        return AIRTABLE_FIELDS_TO_FETCH

    def _get_p2a_field_map(self, mode: SyncMode) -> list[FieldMapping]:
        """Always return the full P2A map (no status mode for FNCPART)."""
        return P2A_FIELD_MAP

    def _get_p2a_airtable_fields(self, mode: SyncMode) -> list[str]:
        """Always return all P2A Airtable fields (no status mode)."""
        return P2A_AIRTABLE_FIELDS_TO_FETCH

    def _get_p2a_priority_select(self, mode: SyncMode) -> list[str]:
        """Always return all P2A Priority select fields (no status mode)."""
        return P2A_PRIORITY_SELECT

    # ── Key field accessors ──────────────────────────────────────────────

    def _get_key_field_name(self) -> str:
        return PRIORITY_KEY_FIELD

    def _get_airtable_key_field(self) -> str:
        return AIRTABLE_KEY_FIELD

    def _get_airtable_key_field_writable(self) -> str:
        return AIRTABLE_KEY_FIELD_WRITABLE
