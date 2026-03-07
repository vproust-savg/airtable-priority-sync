"""
Customers sync engine: CUSTOMERS + FNCCUST + sub-forms.

Subclasses BaseSyncEngine with CUSTOMERS-specific configuration.
Merged workflow syncs both CUSTOMERS (main entity) and FNCCUST (secondary entity).

Handles 6 accessible sub-forms from 4 Airtable tables:
  - Customer Contacts  → CUSTPERSONNEL_SUBFORM (separate table)
  - Customer Sites     → CUSTDESTS_SUBFORM (separate table)
  - Special Prices     → CUSTPARTPRICE_SUBFORM (separate table)
  - Price List         → CUSTPLIST_SUBFORM (Customers table, different view)
  - Delivery Days      → CUSTWEEKDAY_SUBFORM (Customers table, different view)
                         Requires row explosion + day→integer conversion + time conversion
  - Credit Application → CUSTEXTFILE_SUBFORM (attachment download + base64 upload)

NOT accessible via API (return 404):
  - CUSTOMERSTEXT_SUBFORM (internal remarks)
  - CUSTSHIPTEXT_SUBFORM (shipment remarks)
"""

from __future__ import annotations

import base64
import logging
import mimetypes
import time
from datetime import datetime, timezone
from typing import Any

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
from sync.core.utils import clean, day_to_priority_int, format_price, format_time_24h, to_priority_date, values_equal
from sync.workflows.customers.config import (
    AIRTABLE_CONTACTS_TABLE_ID,
    AIRTABLE_CONTACTS_VIEW,
    AIRTABLE_DELIVERY_DAYS_VIEW,
    AIRTABLE_KEY_FIELD,
    AIRTABLE_KEY_FIELD_ID,
    AIRTABLE_KEY_FIELD_WRITABLE,
    AIRTABLE_KEY_FIELD_WRITABLE_ID,
    AIRTABLE_PRICE_LIST_VIEW,
    AIRTABLE_SITES_TABLE_ID,
    AIRTABLE_SITES_VIEW,
    AIRTABLE_SPECIAL_PRICES_TABLE_ID,
    AIRTABLE_SPECIAL_PRICES_VIEW,
    AIRTABLE_SYNC_VIEW,
    AIRTABLE_TABLE_NAME,
    CONTACTS_SUBFORM_NAME,
    CREDIT_APP_AIRTABLE_FIELD,
    CREDIT_APP_EXTFILEDES_PREFIX,
    CREDIT_APP_SUBFORM_NAME,
    PRICE_LIST_SUBFORM_NAME,
    PRIORITY_ENTITY,
    PRIORITY_FNCCUST_ENTITY,
    PRIORITY_KEY_FIELD,
    SITES_SUBFORM_NAME,
    SPECIAL_PRICES_SUBFORM_NAME,
    TIMESTAMP_FIELDS,
    TIMESTAMP_FIELD_IDS,
    WEEKDAY_SUBFORM_NAME,
)
from sync.workflows.customers.field_mapping import (
    A2P_FIELD_MAP,
    AIRTABLE_FIELDS_TO_FETCH,
    FNCCUST_A2P_FIELD_MAP,
    FNCCUST_P2A_FIELD_MAP,
    P2A_AIRTABLE_FIELDS_TO_FETCH,
    P2A_FIELD_MAP,
    P2A_PRIORITY_SELECT,
)
from sync.workflows.customers.subform_mapping import (
    CONTACTS_AIRTABLE_FIELDS,
    CONTACTS_FIELD_MAP,
    CONTACTS_MATCH_FIELD,
    DELIVERY_DAYS_AIRTABLE_FIELDS,
    PRICE_LIST_AIRTABLE_FIELDS,
    PRICE_LIST_FIELD_MAP,
    PRICE_LIST_MATCH_FIELD,
    SITES_AIRTABLE_FIELDS,
    SITES_FIELD_MAP,
    SITES_MATCH_FIELD,
    SPECIAL_PRICES_AIRTABLE_FIELDS,
    SPECIAL_PRICES_FIELD_MAP,
    SPECIAL_PRICES_MATCH_FIELD,
)

logger = logging.getLogger(__name__)


class CustomerSyncEngine(BaseSyncEngine):
    """
    Sync engine for Customers (CUSTOMERS + FNCCUST).

    Merged workflow syncs:
      - CUSTOMERS (main entity) with 5 sub-forms
      - FNCCUST (secondary entity — financial parameters)

    Multi-table architecture:
      - Main fields from Customers table
      - Contacts from Customer Contacts 2025 table → CUSTPERSONNEL_SUBFORM
      - Sites from Customer Sites table → CUSTDESTS_SUBFORM
      - Special Prices from Special Cust. Prices table → CUSTPARTPRICE_SUBFORM
      - Price Lists from Customers table (different view) → CUSTPLIST_SUBFORM
      - Delivery Days from Customers table (different view) → CUSTWEEKDAY_SUBFORM

    Uses standard string keys (CUSTOMERS('C00001') works directly).
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Secondary Priority client: FNCCUST (Financial Parameters for Customers)
        self.priority_fnccust = PriorityClient(
            entity=PRIORITY_FNCCUST_ENTITY,
            key_field=PRIORITY_KEY_FIELD,
            api_url_override=self.priority_url_override,
            # FNCCUST uses string keys — standard URL access
        )

    # ── Client factories ─────────────────────────────────────────────────

    def _create_airtable_client(
        self,
        base_id_override: str | None,
        token_override: str | None,
    ) -> AirtableClient:
        # Skip timestamp field IDs for test base — production IDs may not
        # exist in the duplicated base.  _to_id() falls back to field names.
        ts_ids = (
            {}
            if base_id_override
            else {v: TIMESTAMP_FIELD_IDS[k] for k, v in TIMESTAMP_FIELDS.items()}
        )
        field_id_map = build_field_id_map(
            A2P_FIELD_MAP, P2A_FIELD_MAP,
            FNCCUST_A2P_FIELD_MAP, FNCCUST_P2A_FIELD_MAP,
            extra={
                AIRTABLE_KEY_FIELD: AIRTABLE_KEY_FIELD_ID,
                AIRTABLE_KEY_FIELD_WRITABLE: AIRTABLE_KEY_FIELD_WRITABLE_ID,
                **ts_ids,
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

    # ── Sub-form: Pre-fetch from related Airtable tables ──────────────────

    def _pre_a2p_batch(
        self, records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Pre-fetch sub-form data from separate tables and additional views:
          - Customer Contacts (separate table)
          - Customer Sites (separate table)
          - Special Cust. Prices (separate table)
          - Price Lists (Customers table, different view)
          - Delivery Days (Customers table, different view)
        """
        context: dict[str, Any] = {}

        # 1. Contacts (separate table)
        logger.info("Pre-fetching customer contacts...")
        contacts_raw = self._fetch_from_table(
            AIRTABLE_CONTACTS_TABLE_ID,
            AIRTABLE_CONTACTS_VIEW,
            CONTACTS_AIRTABLE_FIELDS,
        )
        contacts_by_cust = self._group_by_key(
            contacts_raw,
            key_field="Priority Cust. ID (from Customers)",
        )
        logger.info("Loaded contacts for %d customers.", len(contacts_by_cust))
        context["contacts_by_cust"] = contacts_by_cust

        # 2. Sites (separate table)
        logger.info("Pre-fetching customer sites...")
        sites_raw = self._fetch_from_table(
            AIRTABLE_SITES_TABLE_ID,
            AIRTABLE_SITES_VIEW,
            SITES_AIRTABLE_FIELDS,
        )
        sites_by_cust = self._group_by_key(
            sites_raw,
            key_field="Priority Cust. ID",
        )
        logger.info("Loaded sites for %d customers.", len(sites_by_cust))
        context["sites_by_cust"] = sites_by_cust

        # 3. Special Prices (separate table)
        logger.info("Pre-fetching special customer prices...")
        prices_raw = self._fetch_from_table(
            AIRTABLE_SPECIAL_PRICES_TABLE_ID,
            AIRTABLE_SPECIAL_PRICES_VIEW,
            SPECIAL_PRICES_AIRTABLE_FIELDS,
        )
        prices_by_cust = self._group_by_key(
            prices_raw,
            key_field="Cust. IDs",
        )
        logger.info("Loaded special prices for %d customers.", len(prices_by_cust))
        context["prices_by_cust"] = prices_by_cust

        # 4. Price Lists (same Customers table, different view)
        logger.info("Pre-fetching price list assignments...")
        plist_raw = self.airtable.fetch_records_from_view(
            view_name=AIRTABLE_PRICE_LIST_VIEW,
            fields=PRICE_LIST_AIRTABLE_FIELDS,
        )
        plist_by_cust: dict[str, list[dict[str, Any]]] = {}
        for rec in plist_raw:
            fields = rec.get("fields", {})
            cust_id = clean(fields.get("Priority Cust. ID"))
            if cust_id:
                plist_by_cust.setdefault(cust_id, []).append(fields)
        logger.info("Loaded price lists for %d customers.", len(plist_by_cust))
        context["plist_by_cust"] = plist_by_cust

        # 5. Delivery Days (same Customers table, different view)
        logger.info("Pre-fetching delivery days...")
        days_raw = self.airtable.fetch_records_from_view(
            view_name=AIRTABLE_DELIVERY_DAYS_VIEW,
            fields=DELIVERY_DAYS_AIRTABLE_FIELDS,
        )
        days_by_cust: dict[str, list[dict[str, Any]]] = {}
        for rec in days_raw:
            fields = rec.get("fields", {})
            cust_id = clean(fields.get("Priority Cust. ID"))
            if cust_id:
                days_by_cust.setdefault(cust_id, []).append(fields)
        logger.info("Loaded delivery days for %d customers.", len(days_by_cust))
        context["days_by_cust"] = days_by_cust

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
        Sync sub-forms + secondary entity for a customer after main CUSTOMERS sync.

        1. Contacts → CUSTPERSONNEL_SUBFORM
        2. Sites → CUSTDESTS_SUBFORM
        3. Special Prices → CUSTPARTPRICE_SUBFORM
        4. Price Lists → CUSTPLIST_SUBFORM
        5. Delivery Days → CUSTWEEKDAY_SUBFORM (with row explosion)
        6. Credit Application → CUSTEXTFILE_SUBFORM (file upload)
        7. FNCCUST (Financial Parameters — secondary entity)
        """
        # 1. Contacts
        contacts = context.get("contacts_by_cust", {}).get(key, [])
        # Filter Email_ID to match this customer's account ID
        # (contacts linked to multiple customers have multi-line emails)
        account_id = key.lstrip("C")  # "C7021" → "7021"
        filtered_contacts = []
        for contact in contacts:
            contact_copy = dict(contact)
            email_raw = contact_copy.get("Email_ID")
            if email_raw:
                email_str = clean(email_raw) or ""
                parts = [p.strip() for p in email_str.split() if "@" in p]
                match = next((p for p in parts if f"+{account_id}@" in p), None)
                if match:
                    contact_copy["Email_ID"] = match
                elif parts:
                    contact_copy["Email_ID"] = parts[0]
            filtered_contacts.append(contact_copy)
        # Single-flag enforcement — Priority allows only one "Y" per flag per customer
        _SINGLE_FLAG_FIELDS = [
            "Main Contact Output",
            "Marketing Output",
            "Price Quote Output",
            "Sales Order Output",
            "Shipment Output",
            "Invoice Output",
            "Cust. Statement Output",
            "Outgoing Voucher Output",
        ]
        # If only one contact, auto-set as Main
        if len(filtered_contacts) == 1:
            if clean(filtered_contacts[0].get("Main Contact Output")) != "Y":
                filtered_contacts[0]["Main Contact Output"] = "Y"
                logger.info("%s: single contact — auto-setting as Main", key)
        # For each flag, ensure at most one contact has "Y"
        for flag_field in _SINGLE_FLAG_FIELDS:
            flagged = [
                i for i, c in enumerate(filtered_contacts)
                if clean(c.get(flag_field)) == "Y"
            ]
            if len(flagged) > 1:
                logger.warning(
                    "%s: %d contacts have %s=Y — keeping first, clearing others",
                    key, len(flagged), flag_field,
                )
                for idx in flagged[1:]:
                    filtered_contacts[idx][flag_field] = "N"
        self._sync_one_subform(
            key=key,
            subform_name=CONTACTS_SUBFORM_NAME,
            airtable_records=filtered_contacts,
            field_map=CONTACTS_FIELD_MAP,
            match_field=CONTACTS_MATCH_FIELD,
            result=result,
            dry_run=dry_run,
            label="contacts",
        )

        # 2. Sites
        sites = context.get("sites_by_cust", {}).get(key, [])
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

        # 3. Special Prices
        prices = context.get("prices_by_cust", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=SPECIAL_PRICES_SUBFORM_NAME,
            airtable_records=prices,
            field_map=SPECIAL_PRICES_FIELD_MAP,
            match_field=SPECIAL_PRICES_MATCH_FIELD,
            result=result,
            dry_run=dry_run,
            label="special prices",
            transforms={
                "FROMDATE": to_priority_date,
                "EXPIRYDATE": to_priority_date,
                "PRICE": format_price,
            },
        )

        # 4. Price Lists
        plists = context.get("plist_by_cust", {}).get(key, [])
        self._sync_one_subform(
            key=key,
            subform_name=PRICE_LIST_SUBFORM_NAME,
            airtable_records=plists,
            field_map=PRICE_LIST_FIELD_MAP,
            match_field=PRICE_LIST_MATCH_FIELD,
            result=result,
            dry_run=dry_run,
            label="price lists",
        )

        # 5. Delivery Days (with row explosion)
        days_records = context.get("days_by_cust", {}).get(key, [])
        self._sync_delivery_days(key, days_records, result, dry_run)

        # 6. Credit Application document → CUSTEXTFILE_SUBFORM
        self._sync_credit_application(key, airtable_fields, result, dry_run)

        # 7. FNCCUST (Financial Parameters — secondary entity)
        if not dry_run:
            self._sync_secondary_entity(
                self.priority_fnccust, key, airtable_fields, result,
                FNCCUST_A2P_FIELD_MAP, "fnccust",
            )

    # ── Delivery Days: special handling ──────────────────────────────────

    def _sync_delivery_days(
        self,
        key: str,
        days_records: list[dict[str, Any]],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Sync delivery days to CUSTWEEKDAY_SUBFORM.

        Special processing:
        - "Days of Business" is comma-separated → one sub-form row per day
        - Day names → Priority integer (Sun=1, Mon=2, …, Sat=7)
        - Times converted: 12h AM/PM → 24h
        """
        if not days_records:
            return

        payloads: list[dict[str, Any]] = []

        for record in days_records:
            days_str = clean(record.get("Days of Business"))
            deliver_after = clean(record.get("Deliver After"))
            deliver_before = clean(record.get("Deliver Before"))

            if not days_str:
                continue

            # Row explosion: split comma-separated days
            day_list = [d.strip() for d in days_str.split(",") if d.strip()]

            for day in day_list:
                payload: dict[str, Any] = {
                    "WEEKDAY": day_to_priority_int(day),
                }
                if deliver_after:
                    payload["FROMTIME"] = format_time_24h(deliver_after)
                if deliver_before:
                    payload["TOTIME"] = format_time_24h(deliver_before)

                payloads.append(payload)

        if not payloads:
            return

        if dry_run:
            result.subform_results.append(
                SubformResult(
                    subform=WEEKDAY_SUBFORM_NAME,
                    action="DRY_RUN",
                    detail=f"Would sync {len(payloads)} delivery day entries",
                )
            )
            return

        # ── GET existing delivery days and compare ──
        try:
            existing = self.priority.get_subform(key, WEEKDAY_SUBFORM_NAME)
        except Exception as e:
            logger.warning(
                "%s: failed to GET existing delivery days, will write all: %s",
                key, e,
            )
            existing = []

        existing_by_day: dict[str, dict[str, Any]] = {}
        for rec in existing:
            day = str(rec.get("WEEKDAY", "")).strip()
            if day:
                existing_by_day[day] = rec

        records_to_push: list[dict[str, Any]] = []
        created, updated, skipped = 0, 0, 0

        for desired in payloads:
            day = str(desired.get("WEEKDAY", "")).strip()
            current = existing_by_day.get(day)
            if not current:
                records_to_push.append(desired)
                created += 1
            else:
                has_changes = any(
                    not values_equal(v, current.get(k))
                    for k, v in desired.items() if k != "WEEKDAY"
                )
                if has_changes:
                    records_to_push.append(desired)
                    updated += 1
                else:
                    skipped += 1

        if not records_to_push:
            if skipped:
                result.subform_results.append(
                    SubformResult(
                        subform=WEEKDAY_SUBFORM_NAME,
                        action="SKIPPED",
                        detail=f"All {skipped} delivery days unchanged",
                    )
                )
            return

        detail = f"c:{created} u:{updated} s:{skipped}"
        try:
            self.priority.deep_patch_subform(
                key_value=key,
                subform_name=WEEKDAY_SUBFORM_NAME,
                records=records_to_push,
            )
            result.subform_results.append(
                SubformResult(
                    subform=WEEKDAY_SUBFORM_NAME,
                    action="UPDATED",
                    detail=f"Synced {len(records_to_push)} delivery days ({detail})",
                )
            )
        except Exception as e:
            logger.error(
                "Failed to sync delivery days for customer %s: %s", key, e,
            )
            result.subform_results.append(
                SubformResult(
                    subform=WEEKDAY_SUBFORM_NAME,
                    action="ERROR",
                    detail=str(e),
                )
            )

    # ── Credit Application upload ────────────────────────────────────────

    def _sync_credit_application(
        self,
        key: str,
        airtable_fields: dict[str, Any],
        result: SyncRecord,
        dry_run: bool,
    ) -> None:
        """
        Upload Credit Application attachment to CUSTEXTFILE_SUBFORM.

        Pattern: download from Airtable CDN → base64 encode → POST to sub-form.
        Deduplication: skip if EXTFILEDES label already exists in Priority.
        """
        attachments = airtable_fields.get(CREDIT_APP_AIRTABLE_FIELD, [])
        if not attachments or not isinstance(attachments, list):
            return

        attachment = attachments[0] if isinstance(attachments[0], dict) else {}
        file_url = attachment.get("url", "")
        filename = attachment.get("filename", "credit_application.pdf")
        mime_type = attachment.get("type", "")

        if not file_url:
            return

        if not mime_type:
            mime_type, _ = mimetypes.guess_type(filename)
            mime_type = mime_type or "application/pdf"

        doc_label = f"{CREDIT_APP_EXTFILEDES_PREFIX} - {key}"

        # Check for existing document (dedup by EXTFILEDES)
        if not dry_run:
            try:
                url = (
                    f"{self.priority.api_url}{PRIORITY_ENTITY}"
                    f"('{key}')/{CREDIT_APP_SUBFORM_NAME}"
                )
                resp = self.priority.session.get(url, timeout=30)
                resp.raise_for_status()
                existing = resp.json().get("value", [])
                if any(r.get("EXTFILEDES") == doc_label for r in existing):
                    logger.debug(
                        "%s: Credit Application already in Priority — skipping", key,
                    )
                    return
            except Exception as e:
                logger.warning(
                    "Could not check existing docs for %s: %s", key, e,
                )

        if dry_run:
            result.subform_results.append(
                SubformResult(
                    subform=CREDIT_APP_SUBFORM_NAME,
                    action="DRY_RUN",
                    detail=f"Would upload credit application ({filename})",
                )
            )
            return

        # Download file from Airtable CDN
        try:
            resp = requests.get(file_url, timeout=120)
            resp.raise_for_status()
            file_bytes = resp.content
        except Exception as e:
            logger.error("%s: Credit Application download failed: %s", key, e)
            result.subform_results.append(
                SubformResult(
                    subform=CREDIT_APP_SUBFORM_NAME,
                    action="ERROR",
                    detail=f"Download failed: {e}",
                )
            )
            return

        # Base64 encode and upload
        b64_data = base64.b64encode(file_bytes).decode("ascii")
        data_uri = f"data:{mime_type};base64,{b64_data}"

        try:
            url = (
                f"{self.priority.api_url}{PRIORITY_ENTITY}"
                f"('{key}')/{CREDIT_APP_SUBFORM_NAME}"
            )
            resp = self.priority.session.post(
                url,
                json={"EXTFILENAME": data_uri, "EXTFILEDES": doc_label},
                timeout=180,
            )
            resp.raise_for_status()
            size_kb = len(file_bytes) / 1024
            logger.info(
                "%s: Credit Application uploaded (%.0fKB, %s)", key, size_kb, filename,
            )
            result.subform_results.append(
                SubformResult(
                    subform=CREDIT_APP_SUBFORM_NAME,
                    action="UPDATED",
                    detail=f"Uploaded credit application ({size_kb:.0f}KB, {filename})",
                )
            )
        except Exception as e:
            logger.error("%s: Credit Application upload failed: %s", key, e)
            result.subform_results.append(
                SubformResult(
                    subform=CREDIT_APP_SUBFORM_NAME,
                    action="ERROR",
                    detail=f"Upload failed: {e}",
                )
            )

    # ── Generic sub-form sync ────────────────────────────────────────────

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

        transforms: optional dict of Priority field name → callable to apply
                    after extracting the value (e.g., date conversion).
        """
        if not airtable_records:
            return

        payloads: list[dict[str, Any]] = []
        for at_record in airtable_records:
            payload: dict[str, Any] = {}
            for at_field, p_field in field_map.items():
                raw = at_record.get(at_field)
                if transforms and p_field in transforms:
                    # Apply transform on raw value (e.g., format_price
                    # needs numeric input, to_priority_date needs string)
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
            else:
                has_changes = any(
                    not values_equal(v, current.get(k))
                    for k, v in desired.items() if k != match_field
                )
                if has_changes:
                    records_to_push.append(desired)
                    updated += 1
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
            # Batch failed — retry each record individually so good ones still sync
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

    # ── Helpers ──────────────────────────────────────────────────────────

    def _fetch_from_table(
        self,
        table_id: str,
        view_name: str,
        fields: list[str],
    ) -> list[dict[str, Any]]:
        """
        Fetch all records from a separate Airtable table (not the main Customers table).
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

                break

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

    def _group_by_key(
        self,
        raw_records: list[dict[str, Any]],
        key_field: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Group fetched Airtable records by customer ID.
        Handles both plain strings and lookup (list) fields.
        """
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in raw_records:
            fields = record.get("fields", {})
            raw_key = fields.get(key_field)

            if isinstance(raw_key, list):
                # Contact linked to multiple customers → group under ALL of them
                for item in raw_key:
                    cust_id = clean(item)
                    if cust_id:
                        grouped.setdefault(cust_id, []).append(fields)
                continue
            else:
                cust_id = clean(raw_key)

            if cust_id:
                grouped.setdefault(cust_id, []).append(fields)

        return grouped

    @staticmethod
    def _clean_lookup(value: Any) -> str | None:
        """Clean a value that might be a lookup (list)."""
        if isinstance(value, list):
            if not value:
                return None
            value = value[0]
        return clean(value)

    # ── Secondary entity sync (FNCCUST) ───────────────────────────────────

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
        Sync fields to a secondary Priority entity (FNCCUST).

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

    # ── P→A extra fields (FNCCUST) ───────────────────────────────────────

    def _get_p2a_extra_fields(
        self,
        key: str,
        priority_record: dict[str, Any],
        is_status: bool,
    ) -> dict[str, Any]:
        """
        Fetch extra fields from Priority for P→A direction (full mode only).

        Sources:
        1. FNCCUST (Financial Parameters) — secondary entity
        """
        if is_status:
            return {}

        extra: dict[str, Any] = {}

        # 1. FNCCUST fields → Airtable
        try:
            fnccust_data = self.priority_fnccust.get_record(key)
            if fnccust_data:
                from sync.core.base_engine import map_priority_to_airtable
                fnccust_mapped = map_priority_to_airtable(
                    fnccust_data, FNCCUST_P2A_FIELD_MAP, is_create=False,
                )
                extra.update(fnccust_mapped)
        except Exception as e:
            logger.warning("Failed to fetch FNCCUST for %s: %s", key, e)

        return extra
