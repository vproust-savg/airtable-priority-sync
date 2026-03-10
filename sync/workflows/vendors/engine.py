"""
Vendors sync engine: SUPPLIERS + FNCSUP + sub-forms.

Subclasses BaseSyncEngine with SUPPLIERS-specific configuration.
Merged workflow syncs both SUPPLIERS (main entity) and FNCSUP (secondary entity).

Handles 3 sub-forms on SUPPLIERS + 1 on FNCSUP:
  - Vendor Contacts  → SUPPERSONNEL_SUBFORM
  - Vendor Products  → SUPPART_SUBFORM
  - Vendor Sites     → SUPDESTCODES_SUBFORM
  - Bank Accounts    → ACCOUNTBANK_SUBFORM (on FNCSUP)
  - Vendor Remarks   → SUPPLIERSTEXT_SUBFORM (NOT accessible via API — 404)

Multi-table architecture:
  - Main vendor data comes from the "Vendors" table via the sync view.
  - Sub-form data comes from 3 separate Airtable tables fetched in _pre_a2p_batch.
  - Bank account data comes from a secondary view on the same Vendors table.
  - P→A is limited — many Airtable fields are formulas or AI-generated (aiText).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

from sync.core.airtable_client import AirtableClient
from sync.core.base_engine import BaseSyncEngine, build_field_id_map, map_airtable_to_priority
from sync.core.config import (
    AIRTABLE_API_BASE,
    AIRTABLE_MAX_RETRIES,
    AIRTABLE_REQUEST_TIMEOUT,
)
from sync.core.models import FieldMapping, SubformResult, SyncError, SyncMode, SyncRecord
from sync.core.priority_client import PriorityClient
from sync.core.sync_log_client import SyncLogClient
from sync.core.utils import clean, priority_yn, reverse_yn, to_int, values_equal
from sync.workflows.vendors.config import (
    ACCOUNTBANK_SUBFORM_NAME,
    AIRTABLE_BANK_VIEW,
    AIRTABLE_CONTACTS_TABLE_ID,
    AIRTABLE_CONTACTS_VIEW,
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_ID,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_KEY_FIELD_WRITABLE_ID,
    AIRTABLE_PRODUCTS_TABLE_ID,
    AIRTABLE_PRODUCTS_VIEW,
    AIRTABLE_SITES_TABLE_ID,
    AIRTABLE_SITES_VIEW,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    CONTACTS_SUBFORM_NAME,
    PARTS_SUBFORM_NAME,
    PRIORITY_ENTITY,
    PRIORITY_FNCSUP_ENTITY,
    PRIORITY_KEY_FIELD,
    SITES_SUBFORM_NAME,
    TIMESTAMP_FIELDS,
    TIMESTAMP_FIELD_IDS,
)
from sync.workflows.vendors.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELDS_TO_FETCH,
    FNCSUP_A2P_FIELD_MAP,
    FNCSUP_BANK_A2P_FIELD_MAP,
    FNCSUP_BANK_P2A_FIELD_MAP,
    FNCSUP_P2A_FIELD_MAP,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
)
from sync.workflows.vendors.subform_mapping import (
    CONTACTS_AIRTABLE_FIELDS,
    CONTACTS_FIELD_MAP,
    CONTACTS_MATCH_FIELD,
    P2A_CONTACTS_AIRTABLE_FIELDS,
    P2A_CONTACTS_AIRTABLE_MATCH_FIELD,
    P2A_CONTACTS_FIELD_MAP,
    P2A_CONTACTS_LINK_FIELD,
    P2A_CONTACTS_MATCH_FIELD,
    P2A_PRODUCTS_AIRTABLE_FIELDS,
    P2A_PRODUCTS_AIRTABLE_MATCH_FIELD,
    P2A_PRODUCTS_FIELD_MAP,
    P2A_PRODUCTS_MATCH_FIELD,
    P2A_SITES_ADDRESS_TARGET,
    P2A_SITES_AIRTABLE_FIELDS,
    P2A_SITES_AIRTABLE_MATCH_FIELD,
    P2A_SITES_FIELD_MAP,
    P2A_SITES_LINK_FIELD,
    P2A_SITES_MATCH_FIELD,
    PRODUCTS_AIRTABLE_FIELDS,
    PRODUCTS_FIELD_MAP,
    PRODUCTS_MATCH_FIELD,
    SITES_AIRTABLE_FIELDS,
    SITES_FIELD_MAP,
    SITES_MATCH_FIELD,
)

logger = logging.getLogger(__name__)


class VendorSyncEngine(BaseSyncEngine):
    """
    Sync engine for Vendors (SUPPLIERS + FNCSUP).

    Merged workflow syncs:
      - SUPPLIERS (main entity) with 3 sub-forms
      - FNCSUP (secondary entity — financial parameters) with bank sub-form

    Multi-table architecture:
      - Main fields from Vendors table
      - Contacts from Vendor Contacts table → SUPPERSONNEL_SUBFORM
      - Products from Vendor Products table → SUPPART_SUBFORM
      - Sites from Vendor Sites table → SUPDESTCODES_SUBFORM
      - Bank details from Vendors table (secondary view) → ACCOUNTBANK_SUBFORM

    Uses standard string keys (SUPPLIERS('2993') works directly).
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Secondary Priority client: FNCSUP (Financial Parameters for Vendors)
        self.priority_fncsup = PriorityClient(
            entity=PRIORITY_FNCSUP_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            api_url_override=self.priority_url_override,
            # FNCSUP uses string keys — standard URL access
        )

    # ── Client factories ─────────────────────────────────────────────────

    def _create_airtable_client(
        self,
        base_id_override: str | None,
        token_override: str | None,
    ) -> AirtableClient:
        # Skip ALL field IDs for test base — production IDs don't exist
        # in the duplicated base.  _to_id() falls back to field names.
        if base_id_override:
            field_id_map = None
        else:
            field_id_map = build_field_id_map(
                A2P_FIELD_MAP, P2A_FIELD_MAP,
                FNCSUP_A2P_FIELD_MAP, FNCSUP_P2A_FIELD_MAP,
                FNCSUP_BANK_A2P_FIELD_MAP, FNCSUP_BANK_P2A_FIELD_MAP,
                extra={
                    AIRTABLE_KEY_FIELD: AIRTABLE_KEY_FIELD_ID,
                    AIRTABLE_KEY_FIELD_WRITABLE: AIRTABLE_KEY_FIELD_WRITABLE_ID,
                    **{v: TIMESTAMP_FIELD_IDS[k] for k, v in TIMESTAMP_FIELDS.items()},
                },
            )
        return AirtableClient(
            table_name=AIRTABLE_TABLE_NAME,
            key_field=AIRTABLE_KEY_FIELD,
            key_field_writable=AIRTABLE_KEY_FIELD_WRITABLE,
            sync_view=AIRTABLE_SYNC_VIEW,
            timestamp_fields=TIMESTAMP_FIELDS,
            field_id_map=field_id_map,
            base_id_override=base_id_override,
            token_override=token_override,
        )

    def _create_priority_client(self, api_url_override: str | None = None) -> PriorityClient:
        return PriorityClient(
            entity=PRIORITY_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            api_url_override=api_url_override,
            # SUPPLIERS uses string keys — standard URL access
        )

    def _create_sync_log_client(self) -> SyncLogClient:
        return SyncLogClient(entity_label="Vendor ID")

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

    # ── Sub-form: Pre-fetch from related Airtable tables ──────────────────

    def _pre_a2p_batch(
        self, records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Pre-fetch sub-form data from 3 separate Airtable tables + bank view:
          - Vendor Contacts
          - Vendor Products
          - Vendor Sites
          - Bank Account Details (same Vendors table, different view)

        Returns a context dict with each indexed by vendor ID.
        """
        context: dict[str, Any] = {}

        # Contacts
        logger.info("Pre-fetching vendor contacts from '%s'...", AIRTABLE_CONTACTS_VIEW)
        contacts_raw = self._fetch_from_table(
            AIRTABLE_CONTACTS_TABLE_ID,
            AIRTABLE_CONTACTS_VIEW,
            CONTACTS_AIRTABLE_FIELDS,
        )
        contacts_by_vendor = self._group_by_vendor_key(
            contacts_raw,
            key_field="Priority Vendor ID (from Vendor_ID)",
        )
        logger.info("Loaded contacts for %d vendors.", len(contacts_by_vendor))
        context["contacts_by_vendor"] = contacts_by_vendor

        # Products
        logger.info("Pre-fetching vendor products from '%s'...", AIRTABLE_PRODUCTS_VIEW)
        products_raw = self._fetch_from_table(
            AIRTABLE_PRODUCTS_TABLE_ID,
            AIRTABLE_PRODUCTS_VIEW,
            PRODUCTS_AIRTABLE_FIELDS,
        )
        products_by_vendor = self._group_by_vendor_key(
            products_raw,
            key_field="V-Vendor ID (from Preferred Vendor) (from Savory SKU)",
        )
        logger.info("Loaded products for %d vendors.", len(products_by_vendor))
        context["products_by_vendor"] = products_by_vendor

        # Sites
        logger.info("Pre-fetching vendor sites from '%s'...", AIRTABLE_SITES_VIEW)
        sites_raw = self._fetch_from_table(
            AIRTABLE_SITES_TABLE_ID,
            AIRTABLE_SITES_VIEW,
            SITES_AIRTABLE_FIELDS,
        )
        sites_by_vendor = self._group_by_vendor_key(
            sites_raw,
            key_field="Priority Vendor ID (from Vendor)",
        )
        logger.info("Loaded sites for %d vendors.", len(sites_by_vendor))
        context["sites_by_vendor"] = sites_by_vendor

        # Bank account details (same Vendors table, different view — for FNCSUP sub-form)
        logger.info("Pre-fetching bank account details from '%s'...", AIRTABLE_BANK_VIEW)
        from sync.workflows.fncsup.field_mapping import BANK_AIRTABLE_FIELDS
        bank_records = self.airtable.fetch_records_from_view(
            view_name=AIRTABLE_BANK_VIEW,
            fields=BANK_AIRTABLE_FIELDS,
        )
        bank_by_vendor: dict[str, dict[str, Any]] = {}
        for rec in bank_records:
            fields = rec.get("fields", {})
            vendor_id = clean(fields.get("Priority Vendor ID"))
            if vendor_id:
                bank_by_vendor[vendor_id] = fields
        logger.info("Found bank details for %d vendors.", len(bank_by_vendor))
        context["bank_by_vendor"] = bank_by_vendor

        return context

    def _sync_subforms(
        self,
        key: str,
        airtable_fields: dict[str, Any],
        context: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Sync sub-forms + secondary entity for a vendor after main SUPPLIERS sync.

        1. Contacts → SUPPERSONNEL_SUBFORM (deep PATCH)
        2. Products → SUPPART_SUBFORM (deep PATCH)
        3. Sites → SUPDESTCODES_SUBFORM (deep PATCH)
        4. FNCSUP (Financial Parameters — secondary entity)
        5. Bank Accounts → ACCOUNTBANK_SUBFORM on FNCSUP (deep PATCH)
        """
        # 1. Contacts
        contacts = context.get("contacts_by_vendor", {}).get(key, [])

        # Single-flag enforcement — Priority allows only one "Y" per flag per vendor
        _VENDOR_FLAG_FIELDS = [
            "Main Contact",       # MAINPHONE
            "Quotes",             # PDPROFFLAG
            "Purchase Orders",    # PORDFLAG
            "Invoice",            # PIVFLAG
            "Shipping Voucher",   # IMPORTFLAG
        ]
        if contacts:
            # If only one contact, auto-set as Main
            if len(contacts) == 1:
                if clean(contacts[0].get("Main Contact")) not in ("Yes", "Y"):
                    contacts = [dict(contacts[0])]
                    contacts[0]["Main Contact"] = "Yes"
                    logger.info("%s: single contact — auto-setting as Main", key)
            # For each flag, ensure at most one contact has "Yes"
            for flag_field in _VENDOR_FLAG_FIELDS:
                flagged = [
                    i for i, c in enumerate(contacts)
                    if clean(c.get(flag_field)) in ("Yes", "Y")
                ]
                if len(flagged) > 1:
                    logger.warning(
                        "%s: %d contacts have %s=Yes — keeping first, clearing others",
                        key, len(flagged), flag_field,
                    )
                    contacts = [dict(c) for c in contacts]
                    for idx in flagged[1:]:
                        contacts[idx][flag_field] = "No"

        self._sync_one_subform(
            key=key,
            subform_name=CONTACTS_SUBFORM_NAME,
            airtable_records=contacts,
            field_map=CONTACTS_FIELD_MAP,
            match_field=CONTACTS_MATCH_FIELD,
            result=result,
            dry_run=dry_run,
            label="contacts",
            transforms={
                "MAINPHONE": reverse_yn,
                "PDPROFFLAG": reverse_yn,
                "PORDFLAG": reverse_yn,
                "PIVFLAG": reverse_yn,
                "IMPORTFLAG": reverse_yn,
            },
        )

        # 2. Products — DEACTIVATED (Victor 2026-03-10: not needed for now)
        # products = context.get("products_by_vendor", {}).get(key, [])
        # self._sync_vendor_products(
        #     key=key,
        #     airtable_records=products,
        #     result=result,
        #     dry_run=dry_run,
        # )
        logger.info("Vendor Products sub-form sync is currently deactivated.")

        # 3. Sites
        sites = context.get("sites_by_vendor", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=SITES_SUBFORM_NAME,
            airtable_records=sites,
            field_map=SITES_FIELD_MAP,
            match_field=SITES_MATCH_FIELD,
            result=result,
            dry_run=dry_run,
            label="sites",
        )

        # 4. FNCSUP (Financial Parameters — secondary entity)
        if not dry_run:
            self._sync_secondary_entity(
                self.priority_fncsup, key, airtable_fields, result,
                FNCSUP_A2P_FIELD_MAP, "fncsup",
            )

        # 5. Bank Accounts → ACCOUNTBANK_SUBFORM on FNCSUP
        self._sync_bank_subform(key, context, result, dry_run)

    # ── Helpers ──────────────────────────────────────────────────────────

    def _sync_one_subform(
        self,
        key: str,
        subform_name: str,
        airtable_records: list[dict[str, Any]],
        field_map: dict[str, str],
        match_field: str,
        result: SyncRecord,
        dry_run: bool,
        label: str,
        transforms: dict[str, Any] | None = None,
    ) -> None:
        """
        Build payloads from Airtable records, compare with existing Priority
        data, and only write changed records via deep PATCH.
        """
        if not airtable_records:
            return

        # Build sub-form payloads
        payloads: list[dict[str, Any]] = []
        for at_record in airtable_records:
            payload: dict[str, Any] = {}
            for at_field, p_field in field_map.items():
                raw = at_record.get(at_field)
                if transforms and p_field in transforms:
                    value = transforms[p_field](raw)
                else:
                    value = self._clean_lookup(raw)
                if value is not None:
                    payload[p_field] = value
            if payload:
                payloads.append(payload)

        if not payloads:
            return

        if dry_run:
            result.subform_results.append(
                SubformResult(
                    subform=subform_name,
                    action="DRY_RUN",
                    detail=f"Would sync {len(payloads)} {label}",
                )
            )
            return

        # ── GET existing sub-form records and compare ──
        try:
            existing = self.priority.get_subform(key, subform_name)
        except Exception as e:
            logger.warning(
                "%s: failed to GET existing %s, will write all: %s",
                key, label, e,
            )
            existing = []

        existing_by_key: dict[str, dict[str, Any]] = {}
        for rec in existing:
            mk = str(rec.get(match_field, "")).strip()
            if mk:
                existing_by_key[mk] = rec

        records_to_push: list[dict[str, Any]] = []
        created, updated, skipped = 0, 0, 0

        for desired in payloads:
            match_val = str(desired.get(match_field, "")).strip()
            if not match_val:
                records_to_push.append(desired)
                created += 1
                continue

            current = existing_by_key.get(match_val)
            if not current:
                records_to_push.append(desired)
                created += 1
                logger.debug(
                    "%s: %s %s '%s' → CREATE (not in Priority)",
                    key, label, match_field, match_val,
                )
            else:
                changed_fields = [
                    (k, v, current.get(k))
                    for k, v in desired.items()
                    if k != match_field and not values_equal(v, current.get(k))
                ]
                if changed_fields:
                    # Only send match field + changed fields (not full payload)
                    # to avoid triggering Priority validations on unchanged fields
                    update_payload = {match_field: match_val}
                    for fld, new_val, _old_val in changed_fields:
                        update_payload[fld] = new_val
                    records_to_push.append(update_payload)
                    updated += 1
                    for fld, new_val, old_val in changed_fields:
                        logger.debug(
                            "%s: %s '%s' field %s: %r → %r",
                            key, label, match_val, fld, old_val, new_val,
                        )
                else:
                    skipped += 1

        if not records_to_push:
            if skipped:
                result.subform_results.append(
                    SubformResult(
                        subform=subform_name,
                        action="SKIPPED",
                        detail=f"All {skipped} {label} unchanged",
                    )
                )
            return

        # ── Deep PATCH only changed records (with per-record retry) ──
        detail = f"c:{created} u:{updated} s:{skipped}"
        try:
            self.priority.deep_patch_subform(
                key_value=key,
                subform_name=subform_name,
                records=records_to_push,
            )
            result.subform_results.append(
                SubformResult(
                    subform=subform_name,
                    action="UPDATED",
                    detail=f"Synced {len(records_to_push)} {label} ({detail})",
                )
            )
        except Exception as batch_err:
            logger.warning(
                "%s: batch %s failed (%s) — retrying %d records individually",
                key, label, batch_err, len(records_to_push),
            )
            ok, failed = 0, 0
            for payload in records_to_push:
                try:
                    self.priority.deep_patch_subform(
                        key_value=key,
                        subform_name=subform_name,
                        records=[payload],
                    )
                    ok += 1
                except Exception as e:
                    failed += 1
                    record_name = payload.get(match_field, "unknown")
                    logger.error(
                        "%s: failed to sync %s '%s': %s",
                        key, label, record_name, e,
                    )
            if ok:
                result.subform_results.append(
                    SubformResult(
                        subform=subform_name,
                        action="PARTIAL",
                        detail=f"Synced {ok}/{ok + failed} {label} ({failed} failed)",
                    )
                )
            if failed:
                result.subform_results.append(
                    SubformResult(
                        subform=subform_name,
                        action="ERROR",
                        detail=f"{failed}/{ok + failed} {label} failed",
                    )
                )

    def _sync_vendor_products(
        self,
        key: str,
        airtable_records: list[dict[str, Any]],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Sync vendor products to SUPPART_SUBFORM using KLINE-based individual PATCHes.

        Deep PATCH creates duplicates for SUPPART_SUBFORM, so we use Pattern B:
        GET existing → match by PARTNAME → PATCH individual records by KLINE.
        """
        if not airtable_records:
            return

        # Build payloads from Airtable, keyed by PARTNAME
        payloads: dict[str, dict[str, Any]] = {}
        for at_record in airtable_records:
            partname = self._clean_lookup(at_record.get("SKU Trim (EDI) (from Savory SKU)"))
            if not partname:
                continue
            payload: dict[str, Any] = {"PARTNAME": partname}
            vpn = self._clean_lookup(at_record.get("Vendor Part Number"))
            if vpn:
                payload["SUPPARTNAME"] = vpn
            suptime = to_int(at_record.get("Purchase Lead Time (from Savory SKU)"))
            if suptime is not None:
                payload["SUPTIME"] = suptime
            minquant = to_int(at_record.get("Min for Order (from Savory SKU)"))
            if minquant is not None:
                payload["MINQUANT"] = minquant
            payloads[partname] = payload

        if not payloads:
            return

        if dry_run:
            result.subform_results.append(
                SubformResult(
                    subform=PARTS_SUBFORM_NAME,
                    action="DRY_RUN",
                    detail=f"Would sync {len(payloads)} parts",
                )
            )
            return

        # GET existing sub-form records
        try:
            existing = self.priority.get_subform(key, PARTS_SUBFORM_NAME)
        except Exception as e:
            logger.warning("%s: failed to GET existing parts: %s", key, e)
            return

        # Index by (PARTNAME, SUPPARTNAME) composite key → record
        # A vendor can have the same SKU with different VPNs
        existing_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for rec in existing:
            sku = str(rec.get("PARTNAME", "")).strip()
            vpn = str(rec.get("SUPPARTNAME", "")).strip()
            if sku:
                existing_by_key[(sku, vpn)] = rec

        updated, skipped, errors = 0, 0, 0
        for partname, desired in payloads.items():
            vpn = str(desired.get("SUPPARTNAME", "")).strip()
            current = existing_by_key.get((partname, vpn))
            if not current:
                logger.debug("%s: parts '%s' VPN='%s' not in Priority, skipping", key, partname, vpn)
                skipped += 1
                continue

            # Compare only updatable fields (SUPTIME, MINQUANT)
            # SUPPARTNAME cannot be changed via PATCH — skip it
            changed: dict[str, Any] = {}
            for field, new_val in desired.items():
                if field in ("PARTNAME", "SUPPARTNAME"):
                    continue
                old_val = current.get(field)
                if not values_equal(new_val, old_val):
                    changed[field] = new_val
                    logger.debug(
                        "%s: parts '%s' field %s: %r → %r",
                        key, partname, field, old_val, new_val,
                    )

            if not changed:
                skipped += 1
                continue

            # PATCH individual record by KLINE
            kline = current.get("KLINE")
            if kline is None:
                logger.warning("%s: parts '%s' has no KLINE, skipping", key, partname)
                skipped += 1
                continue

            url = (
                f"{self.priority.api_url}{self.priority.entity}('{key}')/"
                f"{PARTS_SUBFORM_NAME}(KLINE={kline})"
            )
            try:
                resp = self.priority._request("PATCH", url, json_body=changed)
                if resp is not None:
                    updated += 1
                else:
                    errors += 1
                    logger.error("%s: parts '%s' PATCH returned no response", key, partname)
            except Exception as e:
                errors += 1
                logger.error("%s: parts '%s' PATCH failed: %s", key, partname, e)

        detail = f"u:{updated} s:{skipped} e:{errors}"
        if updated:
            result.subform_results.append(
                SubformResult(
                    subform=PARTS_SUBFORM_NAME,
                    action="UPDATED",
                    detail=f"Updated {updated} parts ({detail})",
                )
            )
        elif skipped and not errors:
            result.subform_results.append(
                SubformResult(
                    subform=PARTS_SUBFORM_NAME,
                    action="SKIPPED",
                    detail=f"All {skipped} parts unchanged",
                )
            )
        if errors:
            result.subform_results.append(
                SubformResult(
                    subform=PARTS_SUBFORM_NAME,
                    action="ERROR",
                    detail=f"{errors} parts failed ({detail})",
                )
            )

    def _fetch_from_table(
        self,
        table_id: str,
        view_name: str,
        fields: list[str],
    ) -> list[dict[str, Any]]:
        """
        Fetch all records from a separate Airtable table (not the main Vendors table).
        Uses the same auth session as the main AirtableClient.
        """
        url = f"{AIRTABLE_API_BASE}/{self.airtable._base_id}/{table_id}"
        field_params = [("fields[]", f) for f in fields]
        records: list[dict[str, Any]] = []
        offset: str | None = None

        for attempt in range(AIRTABLE_MAX_RETRIES):
            try:
                while True:
                    params = [("view", view_name)] + field_params
                    if offset:
                        params.append(("offset", offset))

                    response = self.airtable.session.get(
                        url, params=params, timeout=AIRTABLE_REQUEST_TIMEOUT,
                    )

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 30))
                        logger.warning("Rate limited. Waiting %ds...", retry_after)
                        time.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    data = response.json()
                    new_records = data.get("records", [])
                    records.extend(new_records)

                    offset = data.get("offset")
                    if not offset:
                        break
                    time.sleep(0.2)

                break  # Success

            except requests.exceptions.RequestException as e:
                if attempt == AIRTABLE_MAX_RETRIES - 1:
                    logger.error(
                        "Failed to fetch from table %s after %d attempts: %s",
                        table_id, AIRTABLE_MAX_RETRIES, e,
                    )
                    raise
                wait_time = 2 ** attempt
                logger.warning("Attempt %d failed, retrying in %ds: %s", attempt + 1, wait_time, e)
                time.sleep(wait_time)

        return records

    def _group_by_vendor_key(
        self,
        raw_records: list[dict[str, Any]],
        key_field: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Group fetched Airtable records by vendor ID.
        The key_field is often a lookup (list type), so we handle both
        plain strings and lists.
        """
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in raw_records:
            fields = record.get("fields", {})
            raw_key = fields.get(key_field)

            # Handle lookup fields that return a list
            if isinstance(raw_key, list):
                vendor_id = clean(raw_key[0]) if raw_key else None
            else:
                vendor_id = clean(raw_key)

            if vendor_id:
                grouped.setdefault(vendor_id, []).append(fields)

        return grouped

    @staticmethod
    def _clean_lookup(value: Any) -> str | None:
        """
        Clean a value that might be a lookup (list).
        Returns the first item if it's a list, otherwise clean the value.
        """
        if isinstance(value, list):
            if not value:
                return None
            value = value[0]
        return clean(value)

    # ── Secondary entity sync (FNCSUP) ────────────────────────────────────

    def _sync_secondary_entity(
        self,
        client: PriorityClient,
        key: str,
        airtable_fields: dict[str, Any],
        result: SyncRecord,
        field_map: list[FieldMapping],
        label: str,
    ) -> None:
        """
        Sync fields to a secondary Priority entity (FNCSUP).

        Secondary entities are auto-created by Priority when the parent entity
        is created — so this only does UPDATE (PATCH), never CREATE.
        Errors are isolated and don't fail the main sync.
        """
        try:
            payload = map_airtable_to_priority(airtable_fields, field_map=field_map)
            # Remove the key field — it's for lookup only, not for patching
            payload.pop(client.key_field, None)
            if not payload:
                return

            existing = client.get_record(key)
            if not existing:
                result.subform_results.append(SubformResult(
                    subform=label,
                    action="skipped",
                    detail="not found in Priority",
                ))
                return

            # Build diff — only changed fields
            patch: dict[str, Any] = {}
            for k, v in payload.items():
                current = existing.get(k)
                if not values_equal(current, v):
                    patch[k] = v

            if not patch:
                return  # No changes — silently skip

            client.update_record(key, patch)
            result.subform_results.append(SubformResult(
                subform=label,
                action="updated",
                detail=f"{len(patch)} fields",
            ))
            logger.debug("%s for %s: updated %d fields", label.upper(), key, len(patch))

        except Exception as e:
            logger.error("%s sync error for %s: %s", label.upper(), key, e)
            result.subform_results.append(SubformResult(
                subform=label,
                action="error",
                detail=str(e)[:100],
            ))
            self.stats.error_details.append(SyncError(
                entity_key=key,
                action="SECONDARY",
                message=f"{label}: {e}",
                timestamp=datetime.now(timezone.utc),
            ))

    # ── Bank account sub-form (on FNCSUP) ─────────────────────────────────

    def _sync_bank_subform(
        self,
        key: str,
        context: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Sync bank account details to ACCOUNTBANK_SUBFORM on FNCSUP.
        Bank data is pre-fetched from a secondary Airtable view in _pre_a2p_batch.
        """
        bank_by_vendor = context.get("bank_by_vendor", {})
        bank_fields = bank_by_vendor.get(key)

        if not bank_fields:
            logger.debug("No bank details found for vendor %s, skipping ACCOUNTBANK.", key)
            return

        # Build the bank payload from Airtable fields
        bank_payload: dict[str, Any] = {}
        for mapping in FNCSUP_BANK_A2P_FIELD_MAP:
            raw_value = bank_fields.get(mapping.airtable_field)
            cleaned = clean(raw_value)
            if cleaned:
                bank_payload[mapping.priority_field] = cleaned

        if not bank_payload:
            logger.debug("No bank data to sync for vendor %s.", key)
            return

        if dry_run:
            result.subform_results.append(
                SubformResult(
                    subform=ACCOUNTBANK_SUBFORM_NAME,
                    action="DRY_RUN",
                    detail=f"Would sync bank: {bank_payload}",
                )
            )
            return

        # ── GET existing bank records and compare ──
        try:
            existing = self.priority_fncsup.get_subform(key, ACCOUNTBANK_SUBFORM_NAME)
        except Exception as e:
            logger.warning(
                "%s: failed to GET existing bank records, will write: %s", key, e,
            )
            existing = []

        if existing:
            current = existing[0]  # typically one bank record
            has_changes = any(
                not values_equal(v, current.get(k))
                for k, v in bank_payload.items()
            )
            if not has_changes:
                result.subform_results.append(
                    SubformResult(
                        subform=ACCOUNTBANK_SUBFORM_NAME,
                        action="SKIPPED",
                        detail="Bank details unchanged",
                    )
                )
                return

        try:
            self.priority_fncsup.deep_patch_subform(
                key_value=key,
                subform_name=ACCOUNTBANK_SUBFORM_NAME,
                records=[bank_payload],
            )
            result.subform_results.append(
                SubformResult(
                    subform=ACCOUNTBANK_SUBFORM_NAME,
                    action="updated",
                    detail=f"Bank details synced: {list(bank_payload.keys())}",
                )
            )
        except Exception as e:
            logger.error("Failed to sync bank for vendor %s: %s", key, e)
            result.subform_results.append(
                SubformResult(
                    subform=ACCOUNTBANK_SUBFORM_NAME,
                    action="error",
                    detail=str(e)[:100],
                )
            )

    # ── P→A extra fields (FNCSUP + bank) ──────────────────────────────────

    def _get_p2a_extra_field_map(self) -> list[FieldMapping]:
        """Include FNCSUP + bank field maps for P→A comparison."""
        return list(FNCSUP_P2A_FIELD_MAP) + list(FNCSUP_BANK_P2A_FIELD_MAP)

    def _get_p2a_extra_fields(
        self,
        key: str,
        priority_record: dict[str, Any],
        is_status: bool,
    ) -> dict[str, Any]:
        """
        Fetch extra fields from Priority for P→A direction (full mode only).

        Sources:
        1. FNCSUP (Financial Parameters) — secondary entity
        2. Bank account details (ACCOUNTBANK_SUBFORM on FNCSUP)
        """
        if is_status:
            return {}

        extra: dict[str, Any] = {}

        # 1. FNCSUP fields → Airtable
        try:
            fncsup_data = self.priority_fncsup.get_record(key)
            if fncsup_data:
                from sync.core.base_engine import map_priority_to_airtable
                fncsup_mapped = map_priority_to_airtable(
                    fncsup_data, FNCSUP_P2A_FIELD_MAP, is_create=False,
                )
                extra.update(fncsup_mapped)
        except Exception as e:
            logger.warning("Failed to fetch FNCSUP for %s: %s", key, e)

        # 2. Bank account details from ACCOUNTBANK_SUBFORM
        try:
            bank_records = self.priority_fncsup.get_subform(key, ACCOUNTBANK_SUBFORM_NAME)
            if bank_records:
                # Use first bank record (vendors typically have one bank account)
                bank_record = bank_records[0]
                for mapping in FNCSUP_BANK_P2A_FIELD_MAP:
                    value = bank_record.get(mapping.priority_field)
                    if value:
                        cleaned = clean(value)
                        if cleaned:
                            extra[mapping.airtable_field] = cleaned
        except Exception as e:
            logger.warning("Failed to fetch bank details for %s: %s", key, e)

        return extra

    # ── P→A sub-form sync ────────────────────────────────────────────────

    def _post_p2a_sync(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """After main P→A vendor sync, sync contacts, products, and sites."""
        try:
            self._sync_p2a_contacts(priority_records, airtable_by_key)
        except Exception as e:
            logger.error("P→A vendor contacts sync failed: %s", e)
        # DEACTIVATED (Victor 2026-03-10: not needed for now)
        # try:
        #     self._sync_p2a_products(priority_records, airtable_by_key)
        # except Exception as e:
        #     logger.error("P→A vendor products sync failed: %s", e)
        logger.info("P→A Vendor Products sync is currently deactivated.")
        try:
            self._sync_p2a_sites(priority_records, airtable_by_key)
        except Exception as e:
            logger.error("P→A vendor sites sync failed: %s", e)

    # ── P→A Contacts ─────────────────────────────────────────────────────

    def _sync_p2a_contacts(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """
        Sync contacts from Priority SUPPERSONNEL_SUBFORM into the
        Airtable Vendor Contacts table.

        All fields are write-if-empty. New contacts are created.
        """
        logger.info("Syncing vendor contacts (P→A)...")

        # Step 1: Pre-fetch existing Airtable contacts
        airtable_contacts = self._fetch_airtable_contacts_for_p2a()
        logger.info(
            "Existing Airtable vendor contacts: %d records across %d vendors",
            sum(len(v) for v in airtable_contacts.values()),
            len(airtable_contacts),
        )

        # Step 2: Collect vendor keys to process
        priority_key = self._get_key_field_name()
        keys_to_process = []
        for rec in priority_records:
            key = str(rec.get(priority_key, "")).strip()
            if key:
                keys_to_process.append(key)

        creates: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        unchanged = 0

        # Step 3: For each vendor, fetch Priority contacts and compare
        for vendor_key in keys_to_process:
            vendor_record = airtable_by_key.get(vendor_key)
            if not vendor_record:
                continue
            vendor_record_id = vendor_record["record_id"]

            try:
                priority_contacts = self.priority.get_subform(
                    vendor_key, CONTACTS_SUBFORM_NAME,
                )
            except Exception as e:
                logger.warning(
                    "Failed to fetch contacts from Priority for %s: %s",
                    vendor_key, e,
                )
                continue

            if not priority_contacts:
                continue

            existing_by_name = airtable_contacts.get(vendor_key, {})

            for p_contact in priority_contacts:
                p_name = str(p_contact.get(P2A_CONTACTS_MATCH_FIELD, "")).strip()
                if not p_name:
                    continue

                existing = existing_by_name.get(p_name.lower())
                fields = self._build_contact_fields(p_contact)
                if not fields:
                    continue

                if existing is None:
                    create_fields = dict(fields)
                    create_fields[P2A_CONTACTS_LINK_FIELD] = [vendor_record_id]
                    creates.append({"fields": create_fields})
                else:
                    existing_fields = existing["fields"]
                    patch_fields: dict[str, Any] = {}

                    for at_field, new_value in fields.items():
                        current = existing_fields.get(at_field)
                        if current is not None and str(current).strip() != "":
                            continue
                        if new_value is not None and str(new_value).strip() != "":
                            patch_fields[at_field] = new_value

                    if patch_fields:
                        updates.append({
                            "id": existing["record_id"],
                            "fields": patch_fields,
                        })
                    else:
                        unchanged += 1

        # Step 4: Batch write to Airtable Vendor Contacts table
        created_ids: list[str] = []
        updated_count = 0

        if creates and not self.dry_run:
            created_ids = self.airtable.batch_create_to_table(
                AIRTABLE_CONTACTS_TABLE_ID, creates,
            )

        if updates and not self.dry_run:
            updated_count = self.airtable.batch_update_to_table(
                AIRTABLE_CONTACTS_TABLE_ID, updates,
            )

        # Post comments on created and updated contact records
        if not self.dry_run:
            contact_comments = []
            for rec_id in created_ids:
                contact_comments.append({
                    "record_id": rec_id,
                    "text": "P→A: Contact created from Priority.",
                })
            for upd in updates:
                field_names = ", ".join(upd["fields"].keys())
                contact_comments.append({
                    "record_id": upd["id"],
                    "text": f"P→A: Updated {field_names} from Priority.",
                })
            if contact_comments:
                posted = self.airtable.post_comments_to_table(
                    AIRTABLE_CONTACTS_TABLE_ID, contact_comments,
                )
                logger.info(
                    "Posted %d comments on vendor contacts.", posted,
                )

        summary = (
            f"Vendor Contacts P→A: "
            f"{len(created_ids) or len(creates)} created, "
            f"{updated_count or len(updates)} updated, "
            f"{unchanged} unchanged"
        )
        logger.info(summary)

    def _build_contact_fields(
        self,
        p_contact: dict[str, Any],
    ) -> dict[str, Any]:
        """Build Airtable field values from a single Priority vendor contact."""
        fields: dict[str, Any] = {}

        for priority_field, (at_field, transform) in P2A_CONTACTS_FIELD_MAP.items():
            raw_value = p_contact.get(priority_field)
            if transform == "priority_yn":
                value = priority_yn(raw_value)
            else:
                value = clean(raw_value)
            if value is not None and str(value).strip() != "":
                fields[at_field] = value

        # Name splitting fallback: use NAME if FIRSTNAME/LASTNAME are empty
        full_name = clean(p_contact.get("NAME")) or ""
        if full_name:
            if "First Name" not in fields:
                first, _ = self._split_full_name(full_name)
                if first:
                    fields["First Name"] = first
            if "Last Name" not in fields:
                _, last = self._split_full_name(full_name)
                if last:
                    fields["Last Name"] = last

        return fields

    @staticmethod
    def _split_full_name(full_name: str) -> tuple[str, str]:
        """Split 'John Smith' into ('John', 'Smith'). Single word → (word, '')."""
        parts = full_name.strip().split(None, 1)
        first = parts[0] if parts else ""
        last = parts[1] if len(parts) > 1 else ""
        return first, last

    def _fetch_airtable_contacts_for_p2a(
        self,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """
        Fetch ALL existing contacts from the Airtable Vendor Contacts table.

        Returns nested dict: {vendor_id: {clean_name_lower: {"record_id": str, "fields": dict}}}
        """
        contacts_url = (
            f"{AIRTABLE_API_BASE}/{self.airtable._base_id}/{AIRTABLE_CONTACTS_TABLE_ID}"
        )
        field_params = [("fields[]", f) for f in P2A_CONTACTS_AIRTABLE_FIELDS]
        records: list[dict[str, Any]] = []
        offset: str | None = None

        while True:
            params = list(field_params)
            if offset:
                params.append(("offset", offset))

            response = self.airtable.session.get(
                contacts_url,
                params=params,
                timeout=AIRTABLE_REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 30))
                logger.warning("Rate limited fetching vendor contacts. Waiting %ds...", retry_after)
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            time.sleep(0.2)

        # Group by vendor ID → clean full name (lowercase)
        by_vendor: dict[str, dict[str, dict[str, Any]]] = {}
        for record in records:
            fields = record.get("fields", {})
            record_id = record.get("id", "")

            # Vendor ID comes from a lookup field — it's a list
            vendor_id_raw = fields.get("Priority Vendor ID (from Vendor_ID)")
            if isinstance(vendor_id_raw, list):
                vendor_ids = [clean(c) for c in vendor_id_raw if clean(c)]
            else:
                vid = clean(vendor_id_raw)
                vendor_ids = [vid] if vid else []

            full_name = clean(fields.get(P2A_CONTACTS_AIRTABLE_MATCH_FIELD))
            if not full_name:
                continue

            name_key = full_name.lower()

            for vendor_id in vendor_ids:
                by_vendor.setdefault(vendor_id, {})[name_key] = {
                    "record_id": record_id,
                    "fields": fields,
                }

        return by_vendor

    # ── P→A Products ─────────────────────────────────────────────────────

    def _sync_p2a_products(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """
        Sync vendor products from Priority SUPPART_SUBFORM into the
        Airtable Vendor Products table. UPDATE-only, write-if-empty.
        """
        logger.info("Syncing vendor products (P→A)...")

        airtable_products = self._fetch_airtable_products_for_p2a()
        logger.info(
            "Existing Airtable vendor products: %d records across %d vendors",
            sum(len(v) for v in airtable_products.values()),
            len(airtable_products),
        )

        priority_key = self._get_key_field_name()
        updates: list[dict[str, Any]] = []
        unchanged = 0
        skipped_no_match = 0

        for rec in priority_records:
            vendor_key = str(rec.get(priority_key, "")).strip()
            if not vendor_key:
                continue

            try:
                priority_products = self.priority.get_subform(
                    vendor_key, PARTS_SUBFORM_NAME,
                )
            except Exception as e:
                logger.warning(
                    "Failed to fetch products from Priority for %s: %s",
                    vendor_key, e,
                )
                continue

            if not priority_products:
                continue

            existing_by_sku = airtable_products.get(vendor_key, {})

            for p_product in priority_products:
                p_sku = str(p_product.get(P2A_PRODUCTS_MATCH_FIELD, "")).strip()
                if not p_sku:
                    continue

                existing = existing_by_sku.get(p_sku.lower())
                if existing is None:
                    skipped_no_match += 1
                    continue

                existing_fields = existing["fields"]
                patch_fields: dict[str, Any] = {}

                for priority_field, (at_field, _) in P2A_PRODUCTS_FIELD_MAP.items():
                    new_value = clean(p_product.get(priority_field))
                    current = existing_fields.get(at_field)
                    if current is not None and str(current).strip() != "":
                        continue
                    if new_value is not None and str(new_value).strip() != "":
                        patch_fields[at_field] = new_value

                if patch_fields:
                    updates.append({
                        "id": existing["record_id"],
                        "fields": patch_fields,
                    })
                else:
                    unchanged += 1

        updated_count = 0
        if updates and not self.dry_run:
            updated_count = self.airtable.batch_update_to_table(
                AIRTABLE_PRODUCTS_TABLE_ID, updates,
            )

        summary = (
            f"Vendor Products P→A: "
            f"{updated_count or len(updates)} updated, "
            f"{unchanged} unchanged, "
            f"{skipped_no_match} skipped (no Airtable match)"
        )
        logger.info(summary)

    def _fetch_airtable_products_for_p2a(
        self,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """
        Fetch ALL existing products from the Airtable Vendor Products table.

        Returns nested dict: {vendor_id: {sku_lower: {"record_id": str, "fields": dict}}}
        """
        products_url = (
            f"{AIRTABLE_API_BASE}/{self.airtable._base_id}/{AIRTABLE_PRODUCTS_TABLE_ID}"
        )
        field_params = [("fields[]", f) for f in P2A_PRODUCTS_AIRTABLE_FIELDS]
        records: list[dict[str, Any]] = []
        offset: str | None = None

        while True:
            params = list(field_params)
            if offset:
                params.append(("offset", offset))

            response = self.airtable.session.get(
                products_url,
                params=params,
                timeout=AIRTABLE_REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 30))
                logger.warning("Rate limited fetching vendor products. Waiting %ds...", retry_after)
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            time.sleep(0.2)

        # Group by vendor ID → SKU (lowercase)
        by_vendor: dict[str, dict[str, dict[str, Any]]] = {}
        for record in records:
            fields = record.get("fields", {})
            record_id = record.get("id", "")

            vendor_id_raw = fields.get("V-Vendor ID (from Vendor)")
            if isinstance(vendor_id_raw, list):
                vendor_ids = [clean(c) for c in vendor_id_raw if clean(c)]
            else:
                vid = clean(vendor_id_raw)
                vendor_ids = [vid] if vid else []

            sku = clean(fields.get(P2A_PRODUCTS_AIRTABLE_MATCH_FIELD))
            if not sku:
                continue

            sku_key = sku.lower()

            for vendor_id in vendor_ids:
                by_vendor.setdefault(vendor_id, {})[sku_key] = {
                    "record_id": record_id,
                    "fields": fields,
                }

        return by_vendor

    # ── P→A Sites ────────────────────────────────────────────────────────

    def _sync_p2a_sites(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """
        Sync vendor sites from Priority SUPDESTCODES_SUBFORM into the
        Airtable Vendor Sites table. Write-if-empty for updates, creates
        new records when no Airtable match exists.

        Address fields are consolidated into a single "Address Input" field.
        """
        logger.info("Syncing vendor sites (P→A)...")

        airtable_sites = self._fetch_airtable_sites_for_p2a()
        logger.info(
            "Existing Airtable vendor sites: %d records across %d vendors",
            sum(len(v) for v in airtable_sites.values()),
            len(airtable_sites),
        )

        priority_key = self._get_key_field_name()
        updates: list[dict[str, Any]] = []
        creates: list[dict[str, Any]] = []
        unchanged = 0

        for rec in priority_records:
            vendor_key = str(rec.get(priority_key, "")).strip()
            if not vendor_key:
                continue

            # Get vendor's Airtable record ID for linking new sites
            vendor_airtable = airtable_by_key.get(vendor_key)
            vendor_record_id = (
                vendor_airtable["record_id"] if vendor_airtable else None
            )

            try:
                priority_sites = self.priority.get_subform(
                    vendor_key, SITES_SUBFORM_NAME,
                )
            except Exception as e:
                logger.warning(
                    "Failed to fetch sites from Priority for %s: %s",
                    vendor_key, e,
                )
                continue

            if not priority_sites:
                continue

            existing_by_code = airtable_sites.get(vendor_key, {})

            for p_site in priority_sites:
                p_code = str(p_site.get(P2A_SITES_MATCH_FIELD, "")).strip()
                if not p_code:
                    continue

                existing = existing_by_code.get(p_code.lower())
                if existing is None:
                    # ── CREATE: new site record ────────────────────────
                    if not vendor_record_id:
                        logger.warning(
                            "Cannot create site %s for %s — vendor not in Airtable",
                            p_code, vendor_key,
                        )
                        continue

                    create_fields: dict[str, Any] = {
                        P2A_SITES_AIRTABLE_MATCH_FIELD: p_code,
                        P2A_SITES_LINK_FIELD: [vendor_record_id],
                    }
                    # Populate mapped fields
                    for pf, (af, _) in P2A_SITES_FIELD_MAP.items():
                        val = clean(p_site.get(pf))
                        if val:
                            create_fields[af] = val
                    # Consolidated address
                    consolidated = self._consolidate_site_address(p_site)
                    if consolidated:
                        create_fields[P2A_SITES_ADDRESS_TARGET] = consolidated

                    creates.append({"fields": create_fields})
                    continue

                # ── UPDATE: write-if-empty ─────────────────────────
                existing_fields = existing["fields"]
                patch_fields: dict[str, Any] = {}

                # Standard fields (write-if-empty)
                for priority_field, (at_field, _) in P2A_SITES_FIELD_MAP.items():
                    new_value = clean(p_site.get(priority_field))
                    current = existing_fields.get(at_field)
                    if current is not None and str(current).strip() != "":
                        continue
                    if new_value is not None and str(new_value).strip() != "":
                        patch_fields[at_field] = new_value

                # Address consolidation (write-if-empty)
                current_addr = existing_fields.get(P2A_SITES_ADDRESS_TARGET)
                if not current_addr or str(current_addr).strip() == "":
                    consolidated = self._consolidate_site_address(p_site)
                    if consolidated:
                        patch_fields[P2A_SITES_ADDRESS_TARGET] = consolidated

                if patch_fields:
                    updates.append({
                        "id": existing["record_id"],
                        "fields": patch_fields,
                    })
                else:
                    unchanged += 1

        # ── Batch writes ──────────────────────────────────────────────
        created_count = 0
        if creates and not self.dry_run:
            created_ids = self.airtable.batch_create_to_table(
                AIRTABLE_SITES_TABLE_ID, creates,
            )
            created_count = len(created_ids)

        updated_count = 0
        if updates and not self.dry_run:
            updated_count = self.airtable.batch_update_to_table(
                AIRTABLE_SITES_TABLE_ID, updates,
            )

        summary = (
            f"Vendor Sites P→A: "
            f"{created_count or len(creates)} created, "
            f"{updated_count or len(updates)} updated, "
            f"{unchanged} unchanged"
        )
        logger.info(summary)

    @staticmethod
    def _consolidate_site_address(p_site: dict[str, Any]) -> str | None:
        """Combine Priority address fields into a single address string."""
        parts = []

        addr1 = clean(p_site.get("ADDRESS"))
        if addr1:
            parts.append(addr1)

        addr2 = clean(p_site.get("ADDRESS2"))
        if addr2:
            parts.append(addr2)

        city = clean(p_site.get("STATE"))
        state = clean(p_site.get("STATECODE"))
        zip_code = clean(p_site.get("ZIP"))

        city_state_zip = ""
        if city:
            city_state_zip = city
        if state:
            city_state_zip = f"{city_state_zip}, {state}" if city_state_zip else state
        if zip_code:
            city_state_zip = f"{city_state_zip} {zip_code}" if city_state_zip else zip_code
        if city_state_zip:
            parts.append(city_state_zip)

        country = clean(p_site.get("COUNTRYNAME"))
        if country:
            parts.append(country)

        return ", ".join(parts) if parts else None

    def _fetch_airtable_sites_for_p2a(
        self,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """
        Fetch ALL existing sites from the Airtable Vendor Sites table.

        Returns nested dict: {vendor_id: {site_code_lower: {"record_id": str, "fields": dict}}}
        """
        sites_url = (
            f"{AIRTABLE_API_BASE}/{self.airtable._base_id}/{AIRTABLE_SITES_TABLE_ID}"
        )
        field_params = [("fields[]", f) for f in P2A_SITES_AIRTABLE_FIELDS]
        records: list[dict[str, Any]] = []
        offset: str | None = None

        while True:
            params = list(field_params)
            if offset:
                params.append(("offset", offset))

            response = self.airtable.session.get(
                sites_url,
                params=params,
                timeout=AIRTABLE_REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 30))
                logger.warning("Rate limited fetching vendor sites. Waiting %ds...", retry_after)
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            time.sleep(0.2)

        # Group by vendor ID → site code (lowercase)
        by_vendor: dict[str, dict[str, dict[str, Any]]] = {}
        for record in records:
            fields = record.get("fields", {})
            record_id = record.get("id", "")

            vendor_id_raw = fields.get("Priority Vendor ID (from Vendor)")
            if isinstance(vendor_id_raw, list):
                vendor_ids = [clean(c) for c in vendor_id_raw if clean(c)]
            else:
                vid = clean(vendor_id_raw)
                vendor_ids = [vid] if vid else []

            site_code = clean(fields.get(P2A_SITES_AIRTABLE_MATCH_FIELD))
            if not site_code:
                continue

            code_key = site_code.lower()

            for vendor_id in vendor_ids:
                by_vendor.setdefault(vendor_id, {})[code_key] = {
                    "record_id": record_id,
                    "fields": fields,
                }

        return by_vendor
