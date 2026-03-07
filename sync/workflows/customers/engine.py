"""
Customers sync engine: CUSTOMERS + FNCCUST + sub-forms.

Subclasses BaseSyncEngine with CUSTOMERS-specific configuration.
Merged workflow syncs both CUSTOMERS (main entity) and FNCCUST (secondary entity).

Handles 8 accessible sub-forms from 4 Airtable tables:
  - Customer Contacts   → CUSTPERSONNEL_SUBFORM (separate table)
  - Customer Sites      → CUSTDESTS_SUBFORM (separate table)
  - Special Prices      → CUSTPARTPRICE_SUBFORM (separate table)
  - Price List          → CUSTPLIST_SUBFORM (Customers table, different view)
  - Delivery Days       → CUSTWEEKDAY_SUBFORM (Customers table, different view)
                          Requires row explosion + day→integer conversion + time conversion
  - Credit Application  → CUSTEXTFILE_SUBFORM (attachment download + base64 upload)
  - Shipment Remarks    → CUSTSHIPTEXT_SUBFORM (Pattern A — single entity, TEXT field)
  - Internal Remarks    → CUSTOMERSTEXT_SUBFORM (Pattern A — single entity, TEXT field)
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
from sync.core.utils import clean, day_to_priority_int, format_price, format_time_24h, priority_yn, strip_html, to_priority_date, values_equal
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
    CUSTTEXT_AIRTABLE_FIELD,
    CUSTTEXT_SUBFORM_NAME,
    PRICE_LIST_SUBFORM_NAME,
    SHIPTEXT_AIRTABLE_FIELD,
    SHIPTEXT_SUBFORM_NAME,
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
    P2A_CONTACTS_AIRTABLE_FIELDS,
    P2A_CONTACTS_AIRTABLE_MATCH_FIELD,
    P2A_CONTACTS_FIELD_MAP,
    P2A_CONTACTS_LINK_FIELD,
    P2A_CONTACTS_MATCH_FIELD,
    P2A_SITES_ADDRESS_TARGET,
    P2A_SITES_AIRTABLE_FIELDS,
    P2A_SITES_AIRTABLE_MATCH_FIELD,
    P2A_SITES_FIELD_MAP,
    P2A_SITES_MATCH_FIELD,
    P2A_SITES_OVERWRITE_FIELDS,
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
        # Skip ALL field IDs for test base — production IDs don't exist
        # in the duplicated base.  _to_id() falls back to field names.
        if base_id_override:
            field_id_map = None
        else:
            field_id_map = build_field_id_map(
                A2P_FIELD_MAP, P2A_FIELD_MAP,
                FNCCUST_A2P_FIELD_MAP, FNCCUST_P2A_FIELD_MAP,
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
        7. Shipment Remarks → CUSTSHIPTEXT_SUBFORM (Pattern A text)
        8. Internal Remarks → CUSTOMERSTEXT_SUBFORM (Pattern A text)
        9. FNCCUST (Financial Parameters — secondary entity)
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

        # 7. Shipment Remarks → CUSTSHIPTEXT_SUBFORM (Pattern A)
        self._sync_text_subform(
            key, airtable_fields,
            airtable_field_name=SHIPTEXT_AIRTABLE_FIELD,
            subform_name=SHIPTEXT_SUBFORM_NAME,
            result=result, dry_run=dry_run,
            label="shipment remarks",
        )

        # 8. Internal Remarks → CUSTOMERSTEXT_SUBFORM (Pattern A)
        self._sync_text_subform(
            key, airtable_fields,
            airtable_field_name=CUSTTEXT_AIRTABLE_FIELD,
            subform_name=CUSTTEXT_SUBFORM_NAME,
            result=result, dry_run=dry_run,
            label="internal remarks",
        )

        # 9. FNCCUST (Financial Parameters — secondary entity)
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
                    "WEEKDAY": str(day_to_priority_int(day)),
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
            self._capture_sentry_error(
                e, entity_key=key, subform=WEEKDAY_SUBFORM_NAME,
                label="delivery days",
            )

    # ── Text sub-forms (Pattern A — single entity) ──────────────────────

    def _sync_text_subform(
        self,
        key: str,
        airtable_fields: dict[str, Any],
        airtable_field_name: str,
        subform_name: str,
        result: SyncRecord,
        dry_run: bool,
        label: str,
    ) -> None:
        """
        Sync a single-entity text sub-form (Pattern A).

        Used for CUSTSHIPTEXT_SUBFORM (shipment remarks) and
        CUSTOMERSTEXT_SUBFORM (internal remarks). These sub-forms have
        a single TEXT field.

        Priority wraps TEXT in HTML styling after PATCH, so comparison
        uses strip_html() to avoid false positives (protecting write quota).
        """
        text = clean(airtable_fields.get(airtable_field_name))
        if not text:
            return

        payload = {"TEXT": text}

        if dry_run:
            result.subform_results.append(SubformResult(
                subform=subform_name, action="DRY_RUN",
                detail=f"Would sync {label}",
            ))
            return

        # GET existing and compare with HTML stripping
        try:
            existing = self.priority.get_subform(key, subform_name)
            if existing:
                current = existing[0] if isinstance(existing, list) else existing
                existing_text = strip_html(str(current.get("TEXT") or ""))
                if existing_text == text.strip():
                    result.subform_results.append(SubformResult(
                        subform=subform_name, action="SKIPPED",
                        detail=f"{label} unchanged",
                    ))
                    return
        except Exception as e:
            logger.warning(
                "%s: failed to GET %s, will write: %s", key, label, e,
            )

        # Write (POST if new, PATCH if changed)
        try:
            res = self.priority.upsert_single_subform(
                key, subform_name, payload,
            )
            result.subform_results.append(SubformResult(
                subform=subform_name, action=res["action"],
                detail=f"{label}: {res['fields_changed']} fields",
            ))
            logger.debug("%s %s: %s", key, label, res["action"])
        except Exception as e:
            logger.error("%s: %s error: %s", key, label, e)
            result.subform_results.append(SubformResult(
                subform=subform_name, action="ERROR",
                detail=str(e),
            ))
            self._capture_sentry_error(
                e, entity_key=key, subform=subform_name, label=label,
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
            self._capture_sentry_error(
                e, entity_key=key, subform=CREDIT_APP_SUBFORM_NAME,
                label="credit app download", extra={"file_url": file_url},
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
            self._capture_sentry_error(
                e, entity_key=key, subform=CREDIT_APP_SUBFORM_NAME,
                label="credit app upload", extra={"filename": filename},
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
                    self._capture_sentry_error(
                        e, entity_key=key, subform=subform_name,
                        label=label, extra={"record_name": record_name},
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
                    self._capture_sentry_error(
                        e, entity_key=table_id,
                        label="airtable table fetch",
                        extra={"table_id": table_id, "view": view},
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
            self._capture_sentry_error(
                e, entity_key=key, subform=label,
                label="secondary entity",
            )

    # ── P→A extra fields (FNCCUST + address consolidation) ──────────────

    # Virtual FieldMapping for address consolidation (write-if-empty).
    # Returned by _get_p2a_extra_field_map() so build_airtable_patch()
    # can compare and enforce the p2a_write_if_empty rule.
    _ADDRESS_FIELD_MAP = FieldMapping(
        airtable_field="Billing Address Input",
        airtable_field_id="fld8CIUzWEElBGJKz",
        priority_field="ADDRESS",  # nominal — actual value is consolidated
        transform="clean",
        field_type="str",
        p2a_write_if_empty=True,
    )

    def _get_p2a_extra_field_map(self) -> list[FieldMapping]:
        """Include FNCCUST P→A field map + address consolidation mapping."""
        return list(FNCCUST_P2A_FIELD_MAP) + [self._ADDRESS_FIELD_MAP]

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
        2. Address consolidation — 6 Priority fields → 1 Airtable field
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

        # 2. Address consolidation → Billing Address Input
        address = self._consolidate_address(priority_record)
        if address:
            extra["Billing Address Input"] = address

        return extra

    @staticmethod
    def _consolidate_address(priority_record: dict[str, Any]) -> str | None:
        """
        Build a comma-separated address from 6 Priority address fields.

        Format: "123 Main St, Suite 200, New York, NY 10001, United States"
        Skips empty parts.  Returns None if all parts are empty.
        """
        addr1 = clean(priority_record.get("ADDRESS")) or ""
        addr2 = clean(priority_record.get("ADDRESS2")) or ""
        city = clean(priority_record.get("STATEA")) or ""
        state = clean(priority_record.get("STATENAME")) or ""
        zip_code = clean(priority_record.get("ZIP")) or ""
        country = clean(priority_record.get("COUNTRYNAME")) or ""

        # Build city/state/zip segment: "City, State ZIP"
        city_state_zip_parts: list[str] = []
        if city:
            city_state_zip_parts.append(city)
        state_zip = f"{state} {zip_code}".strip() if state or zip_code else ""
        if state_zip:
            city_state_zip_parts.append(state_zip)
        city_state_zip = ", ".join(city_state_zip_parts)

        # Build full address: join non-empty segments with ", "
        parts = [p for p in [addr1, addr2, city_state_zip, country] if p]
        return ", ".join(parts) if parts else None

    # ── P→A contacts sync (CUSTPERSONNEL_SUBFORM → Customer Contacts 2025) ──

    def _post_p2a_sync(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """After main P→A customer sync, sync contacts and sites from Priority sub-forms."""
        try:
            self._sync_p2a_contacts(priority_records, airtable_by_key)
        except Exception as e:
            logger.error("P→A contacts sync failed: %s", e)
        try:
            self._sync_p2a_sites(priority_records, airtable_by_key)
        except Exception as e:
            logger.error("P→A sites sync failed: %s", e)

    def _sync_p2a_contacts(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """
        Sync contacts from Priority CUSTPERSONNEL_SUBFORM into the
        Airtable Customer Contacts 2025 table.

        All fields are write-if-empty: only populate empty Airtable fields.
        New Priority contacts (no match in Airtable) are created.
        """
        logger.info("Syncing contacts (P→A)...")

        # Step 1: Pre-fetch all existing Airtable contacts
        airtable_contacts = self._fetch_airtable_contacts_for_p2a()
        logger.info(
            "Existing Airtable contacts: %d records across %d customers",
            sum(len(v) for v in airtable_contacts.values()),
            len(airtable_contacts),
        )

        # Step 2: Collect customer keys to process
        priority_key = self._get_key_field_name()
        keys_to_process = []
        for rec in priority_records:
            key = str(rec.get(priority_key, "")).strip()
            if key:
                keys_to_process.append(key)

        creates: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        unchanged = 0

        # Step 3: For each customer, fetch Priority contacts and compare
        for cust_key in keys_to_process:
            cust_record = airtable_by_key.get(cust_key)
            if not cust_record:
                logger.debug("Customer %s not in Airtable — skipping contacts", cust_key)
                continue
            cust_record_id = cust_record["record_id"]

            # Fetch contacts from Priority
            try:
                priority_contacts = self.priority.get_subform(
                    cust_key, CONTACTS_SUBFORM_NAME,
                )
            except Exception as e:
                logger.warning("Failed to fetch contacts from Priority for %s: %s", cust_key, e)
                continue

            if not priority_contacts:
                continue

            # Existing Airtable contacts for this customer, indexed by clean name
            existing_by_name = airtable_contacts.get(cust_key, {})

            for p_contact in priority_contacts:
                p_name = str(p_contact.get(P2A_CONTACTS_MATCH_FIELD, "")).strip()
                if not p_name:
                    continue

                # Match by full name (case-insensitive)
                existing = existing_by_name.get(p_name.lower())

                # Build field values from Priority contact
                fields = self._build_contact_fields(p_contact)
                if not fields:
                    continue

                if existing is None:
                    # CREATE: new contact in Airtable
                    create_fields = dict(fields)
                    create_fields[P2A_CONTACTS_LINK_FIELD] = [cust_record_id]
                    creates.append({"fields": create_fields})
                else:
                    # UPDATE: write-if-empty only
                    existing_fields = existing["fields"]
                    patch_fields: dict[str, Any] = {}

                    for at_field, new_value in fields.items():
                        current = existing_fields.get(at_field)
                        # Write-if-empty: skip if Airtable field already has a value
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

        # Step 4: Batch write to Airtable Customer Contacts 2025 table
        created_count = 0
        updated_count = 0

        if creates and not self.dry_run:
            created_count = self.airtable.batch_create_to_table(
                AIRTABLE_CONTACTS_TABLE_ID, creates,
            )

        if updates and not self.dry_run:
            updated_count = self.airtable.batch_update_to_table(
                AIRTABLE_CONTACTS_TABLE_ID, updates,
            )

        summary = (
            f"Contacts P→A: "
            f"{created_count or len(creates)} created, "
            f"{updated_count or len(updates)} updated, "
            f"{unchanged} unchanged"
        )
        logger.info(summary)

    def _build_contact_fields(
        self,
        p_contact: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Build Airtable field values from a single Priority contact record.

        Handles:
        - clean transform for text fields
        - priority_yn transform for flag fields (Y→Yes, N→No)
        - Name splitting fallback: if FIRSTNAME/LASTNAME empty, split NAME
        """
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
            if "First Name Input" not in fields:
                first, _ = self._split_full_name(full_name)
                if first:
                    fields["First Name Input"] = first
            if "Last Name Input" not in fields:
                _, last = self._split_full_name(full_name)
                if last:
                    fields["Last Name Input"] = last

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
        Fetch ALL existing contacts from the Airtable Customer Contacts 2025 table.

        Returns:
            Nested dict: ``{cust_id: {clean_name_lower: {"record_id": str, "fields": dict}}}``
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
                logger.warning("Rate limited fetching contacts. Waiting %ds...", retry_after)
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            time.sleep(0.2)

        # Group by customer ID → clean full name (lowercase)
        by_cust: dict[str, dict[str, dict[str, Any]]] = {}
        for record in records:
            fields = record.get("fields", {})
            record_id = record.get("id", "")

            # Customer ID comes from a lookup field — it's a list
            cust_id_raw = fields.get("Priority Cust. ID (from Customers)")
            if isinstance(cust_id_raw, list):
                cust_ids = [clean(c) for c in cust_id_raw if clean(c)]
            else:
                cid = clean(cust_id_raw)
                cust_ids = [cid] if cid else []

            # Get the clean full name for matching
            full_name = clean(fields.get(P2A_CONTACTS_AIRTABLE_MATCH_FIELD))
            if not full_name:
                continue

            name_key = full_name.lower()

            for cust_id in cust_ids:
                by_cust.setdefault(cust_id, {})[name_key] = {
                    "record_id": record_id,
                    "fields": fields,
                }

        return by_cust

    # ── P→A sites sync (CUSTDESTS_SUBFORM → Customer Sites table) ────────

    def _sync_p2a_sites(
        self,
        priority_records: list[dict[str, Any]],
        airtable_by_key: dict[str, dict[str, Any]],
    ) -> None:
        """
        Sync sites from Priority CUSTDESTS_SUBFORM into the
        Airtable Customer Sites table.

        Update only — no new site creation (Site Id is auto-generated).
        Most fields are write-if-empty; MAINFLAG always-overwrites.
        """
        logger.info("Syncing sites (P→A)...")

        # Step 1: Pre-fetch all existing Airtable sites
        airtable_sites = self._fetch_airtable_sites_for_p2a()
        logger.info(
            "Existing Airtable sites: %d records across %d customers",
            sum(len(v) for v in airtable_sites.values()),
            len(airtable_sites),
        )

        # Step 2: Fetch lookup tables for zone and shipper
        zone_lookup = self._fetch_zone_lookup()
        shipper_lookup = self._fetch_shipper_lookup()

        # Step 3: Collect customer keys to process
        priority_key = self._get_key_field_name()
        keys_to_process = []
        for rec in priority_records:
            key = str(rec.get(priority_key, "")).strip()
            if key:
                keys_to_process.append(key)

        updates: list[dict[str, Any]] = []
        unchanged = 0
        skipped_no_match = 0

        # Step 4: For each customer, fetch Priority sites and compare
        for cust_key in keys_to_process:
            # Fetch sites from Priority
            try:
                priority_sites = self.priority.get_subform(
                    cust_key, SITES_SUBFORM_NAME,
                )
            except Exception as e:
                logger.warning("Failed to fetch sites from Priority for %s: %s", cust_key, e)
                continue

            if not priority_sites:
                continue

            # Existing Airtable sites for this customer, indexed by site code
            existing_by_code = airtable_sites.get(cust_key, {})

            for p_site in priority_sites:
                p_code = str(p_site.get(P2A_SITES_MATCH_FIELD, "")).strip()
                if not p_code:
                    continue

                # Match by site code (case-insensitive)
                existing = existing_by_code.get(p_code.lower())

                if existing is None:
                    # No match — skip (update only, no creates)
                    skipped_no_match += 1
                    continue

                # Build field values from Priority site
                fields = self._build_site_fields(p_site, zone_lookup, shipper_lookup)
                if not fields:
                    unchanged += 1
                    continue

                # Apply mixed write logic: write-if-empty for most, always-overwrite for MAINFLAG
                existing_fields = existing["fields"]
                patch_fields: dict[str, Any] = {}

                for at_field, new_value in fields.items():
                    current = existing_fields.get(at_field)
                    is_overwrite = at_field in P2A_SITES_OVERWRITE_FIELDS

                    if not is_overwrite:
                        # Write-if-empty: skip if Airtable field already has a value
                        if current is not None and str(current).strip() != "":
                            continue

                    # Only write if there's actually a new value and it differs
                    if new_value is not None and str(new_value).strip() != "":
                        if not values_equal(new_value, current):
                            patch_fields[at_field] = new_value

                if patch_fields:
                    updates.append({
                        "id": existing["record_id"],
                        "fields": patch_fields,
                    })
                else:
                    unchanged += 1

        # Step 5: Batch write to Airtable Customer Sites table
        updated_count = 0

        if updates and not self.dry_run:
            updated_count = self.airtable.batch_update_to_table(
                AIRTABLE_SITES_TABLE_ID, updates,
            )

        summary = (
            f"Sites P→A: "
            f"{updated_count or len(updates)} updated, "
            f"{unchanged} unchanged, "
            f"{skipped_no_match} skipped (no Airtable match)"
        )
        logger.info(summary)

    def _build_site_fields(
        self,
        p_site: dict[str, Any],
        zone_lookup: dict[str, str],
        shipper_lookup: dict[str, str],
    ) -> dict[str, Any]:
        """
        Build Airtable field values from a single Priority site record.

        Handles:
        - clean transform for text fields
        - priority_yn transform for MAINFLAG (Y→Yes, N→No)
        - strip_html transform for ADDRESS3 (remarks may contain HTML)
        - zone_lookup / shipper_lookup for code→name conversion
        - Address consolidation into Address Input
        """
        fields: dict[str, Any] = {}

        for priority_field, (at_field, transform) in P2A_SITES_FIELD_MAP.items():
            raw_value = p_site.get(priority_field)

            if transform == "priority_yn":
                value = priority_yn(raw_value)
            elif transform == "strip_html":
                cleaned = clean(raw_value)
                value = strip_html(cleaned) if cleaned else None
            elif transform in ("zone_lookup", "shipper_lookup"):
                code = clean(raw_value)
                lkp = zone_lookup if transform == "zone_lookup" else shipper_lookup
                if code and lkp:
                    # Try raw, zero-padded, and stripped (Priority codes may be "01" or "1")
                    value = (
                        lkp.get(str(code))
                        or lkp.get(str(code).zfill(2))
                        or lkp.get(str(code).lstrip("0") or "0")
                        or code
                    )
                else:
                    value = code
            else:
                value = clean(raw_value)

            if value is not None and str(value).strip() != "":
                fields[at_field] = value

        # Address consolidation
        address = self._consolidate_site_address(p_site)
        if address:
            fields[P2A_SITES_ADDRESS_TARGET] = address

        return fields

    @staticmethod
    def _consolidate_site_address(site_record: dict[str, Any]) -> str | None:
        """
        Build a comma-separated address from Priority site address fields.

        Format: "123 Main St, Suite 200, Los Angeles, CA 90012"
        From: ADDRESS, ADDRESS2, STATE (city), STATECODE, ZIP
        No COUNTRYNAME (not synced for sites).
        Skips empty parts. Returns None if all parts are empty.
        """
        addr1 = clean(site_record.get("ADDRESS")) or ""
        addr2 = clean(site_record.get("ADDRESS2")) or ""
        city = clean(site_record.get("STATE")) or ""
        state_code = clean(site_record.get("STATECODE")) or ""
        zip_code = clean(site_record.get("ZIP")) or ""

        # Build city/state/zip segment: "City, ST ZIP"
        city_state_zip_parts: list[str] = []
        if city:
            city_state_zip_parts.append(city)
        state_zip = f"{state_code} {zip_code}".strip() if state_code or zip_code else ""
        if state_zip:
            city_state_zip_parts.append(state_zip)
        city_state_zip = ", ".join(city_state_zip_parts)

        # Build full address: join non-empty segments with ", "
        parts = [p for p in [addr1, addr2, city_state_zip] if p]
        return ", ".join(parts) if parts else None

    def _fetch_zone_lookup(self) -> dict[str, str]:
        """Fetch ZONECODE → zone name lookup from Priority."""
        try:
            lookup = self.priority.fetch_lookup_table(
                "DISTRLINES", "DISTRLINECODE", "DISTRLINEDES",
            )
            if lookup:
                logger.info("Fetched %d zone lookup entries", len(lookup))
            return lookup
        except Exception as e:
            logger.warning("Zone lookup (DISTRLINES) not available: %s", e)
            return {}

    def _fetch_shipper_lookup(self) -> dict[str, str]:
        """Fetch SHIPPERNAME → shipper name lookup from Priority."""
        try:
            lookup = self.priority.fetch_lookup_table(
                "SHIPPERS", "SHIPPERNAME", "SHIPPERDES",
            )
            if lookup:
                logger.info("Fetched %d shipper lookup entries", len(lookup))
            return lookup
        except Exception as e:
            logger.warning("Shipper lookup (SHIPPERS) not available: %s", e)
            return {}

    def _fetch_airtable_sites_for_p2a(
        self,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """
        Fetch ALL existing sites from the Airtable Customer Sites table.

        Returns:
            Nested dict: ``{cust_id: {site_code_lower: {"record_id": str, "fields": dict}}}``
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
                logger.warning("Rate limited fetching sites. Waiting %ds...", retry_after)
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            time.sleep(0.2)

        # Group by customer ID → site code (lowercase)
        by_cust: dict[str, dict[str, dict[str, Any]]] = {}
        for record in records:
            fields = record.get("fields", {})
            record_id = record.get("id", "")

            # Customer ID from formula field
            cust_id = clean(fields.get("Priority Cust. ID"))
            if not cust_id:
                continue

            # Site code for matching
            site_code = clean(fields.get(P2A_SITES_AIRTABLE_MATCH_FIELD))
            if not site_code:
                continue

            code_key = site_code.lower()

            by_cust.setdefault(cust_id, {})[code_key] = {
                "record_id": record_id,
                "fields": fields,
            }

        return by_cust
