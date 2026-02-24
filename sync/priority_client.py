"""
Priority ERP REST API client for product sync.
Patterns adapted from tools/Airtable to Priority Customers - One Way v4 - Approved.json.
API docs: https://prioritysoftware.github.io/restapi/
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any

import requests

from sync.config import (
    PRIORITY_API_URL,
    PRIORITY_MAX_CALLS_PER_MINUTE,
    PRIORITY_MAX_RETRIES,
    PRIORITY_PAGE_SIZE,
    PRIORITY_PASS,
    PRIORITY_REQUEST_TIMEOUT,
    PRIORITY_USER,
)

logger = logging.getLogger(__name__)


def extract_priority_error(response: requests.Response) -> str:
    """
    Extract a human-readable error message from Priority's response.
    Priority returns errors in a JSON-wrapped XML format like:
    {"FORM": {"InterfaceErrors": {"text": "Line 1- error message here"}}}
    """
    try:
        data = response.json()
        # Standard OData error format
        if "error" in data:
            return data["error"].get("message", response.text[:200])
        # Priority's InterfaceErrors format
        if "FORM" in data:
            form = data["FORM"]
            if "InterfaceErrors" in form:
                return form["InterfaceErrors"].get("text", response.text[:200])
        return response.text[:200]
    except Exception:
        return response.text[:200] if response.text else f"HTTP {response.status_code}"


class PriorityClient:
    """Client for reading and writing Priority ERP product records (LOGPART)."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.auth = (PRIORITY_USER, PRIORITY_PASS)
        self.session.headers.update({
            "Content-Type": "application/json",
            "IEEE754Compatible": "true",
        })
        # Rolling window of request timestamps for rate limiting
        self._request_times: deque[float] = deque()

    # ── Rate Limiting ────────────────────────────────────────────────────

    def _throttle(self) -> None:
        """
        Ensure we stay under PRIORITY_MAX_CALLS_PER_MINUTE.
        Uses a rolling 60-second window of request timestamps.
        """
        now = time.monotonic()

        # Remove timestamps older than 60 seconds
        while self._request_times and (now - self._request_times[0]) > 60:
            self._request_times.popleft()

        # If at limit, sleep until the oldest request expires
        if len(self._request_times) >= PRIORITY_MAX_CALLS_PER_MINUTE:
            sleep_time = 60 - (now - self._request_times[0]) + 0.1
            if sleep_time > 0:
                logger.debug("Rate limit: sleeping %.1fs", sleep_time)
                time.sleep(sleep_time)

        self._request_times.append(time.monotonic())

    # ── HTTP helpers ─────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        url: str,
        json_body: dict[str, Any] | None = None,
        allow_404: bool = False,
    ) -> requests.Response | None:
        """
        Execute an HTTP request with retry logic and rate limiting.

        Args:
            method: GET, POST, or PATCH
            url: Full URL
            json_body: JSON payload for POST/PATCH
            allow_404: If True, return None for 404 instead of raising

        Returns:
            Response object, or None if 404 and allow_404=True
        """
        for attempt in range(PRIORITY_MAX_RETRIES):
            self._throttle()

            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    json=json_body,
                    timeout=PRIORITY_REQUEST_TIMEOUT,
                )

                # Handle 404
                if response.status_code == 404 and allow_404:
                    return None

                # Handle rate limiting
                if response.status_code == 429:
                    wait = min(60, 2 ** (attempt + 2))
                    logger.warning(
                        "Priority rate limited (429). Waiting %ds...", wait
                    )
                    time.sleep(wait)
                    continue

                # Handle server errors with retry
                if response.status_code >= 500:
                    if attempt < PRIORITY_MAX_RETRIES - 1:
                        wait = 2 ** (attempt + 1)
                        logger.warning(
                            "Priority server error (%d). Retrying in %ds...",
                            response.status_code,
                            wait,
                        )
                        time.sleep(wait)
                        continue

                # For client errors (400-499), raise with Priority's error message
                if 400 <= response.status_code < 500:
                    error_msg = extract_priority_error(response)
                    logger.error(
                        "Priority %d error for %s %s: %s",
                        response.status_code,
                        method,
                        url,
                        error_msg,
                    )
                    raise requests.HTTPError(
                        error_msg, response=response
                    )

                response.raise_for_status()
                return response

            except requests.exceptions.ConnectionError as e:
                if attempt < PRIORITY_MAX_RETRIES - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "Connection error, retrying in %ds: %s", wait, e
                    )
                    time.sleep(wait)
                else:
                    raise

            except requests.exceptions.Timeout as e:
                if attempt < PRIORITY_MAX_RETRIES - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "Timeout, retrying in %ds: %s", wait, e
                    )
                    time.sleep(wait)
                else:
                    raise

        return None

    # ── Read operations ──────────────────────────────────────────────────

    def fetch_all_partnames(self) -> set[str]:
        """
        Fetch all existing PARTNAMEs from Priority via paginated GET.
        Uses $select=PARTNAME for minimal payload.

        Returns:
            Set of all PARTNAME values (SKUs) in Priority.
        """
        partnames: set[str] = set()
        skip = 0

        while True:
            url = (
                f"{PRIORITY_API_URL}LOGPART"
                f"?$select=PARTNAME"
                f"&$top={PRIORITY_PAGE_SIZE}"
                f"&$skip={skip}"
            )

            logger.debug("Fetching PARTNAMEs (skip=%d)", skip)
            response = self._request("GET", url)
            if response is None:
                break

            data = response.json()
            records = data.get("value", [])

            if not records:
                break

            for record in records:
                pn = record.get("PARTNAME", "").strip()
                if pn:
                    partnames.add(pn)

            logger.debug(
                "Fetched %d PARTNAMEs (total so far: %d)",
                len(records),
                len(partnames),
            )

            # If we got fewer than page size, we're done
            if len(records) < PRIORITY_PAGE_SIZE:
                break

            skip += PRIORITY_PAGE_SIZE

        return partnames

    def get_product(self, partname: str) -> dict[str, Any] | None:
        """
        Fetch a single product by PARTNAME.

        Returns:
            Product dict, or None if not found.
        """
        url = f"{PRIORITY_API_URL}LOGPART('{partname}')"
        response = self._request("GET", url, allow_404=True)

        if response is None:
            return None

        return response.json()

    # ── Write operations ─────────────────────────────────────────────────

    def create_product(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Create a new product in Priority (POST to LOGPART).

        Args:
            payload: Dict of Priority field names → values.

        Returns:
            The created product record from Priority.

        Raises:
            requests.HTTPError on failure.
        """
        url = f"{PRIORITY_API_URL}LOGPART"
        logger.debug(
            "Creating product %s with %d fields",
            payload.get("PARTNAME", "?"),
            len(payload),
        )

        response = self._request("POST", url, json_body=payload)
        if response is None:
            raise requests.HTTPError("No response from Priority on POST")

        return response.json()

    def update_product(
        self, partname: str, patch_body: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Update an existing product in Priority (PATCH to LOGPART).

        Args:
            partname: The product's PARTNAME (SKU).
            patch_body: Dict of only the changed fields.

        Returns:
            The updated product record from Priority.

        Raises:
            requests.HTTPError on failure.
        """
        url = f"{PRIORITY_API_URL}LOGPART('{partname}')"
        logger.debug(
            "Updating product %s: %d fields (%s)",
            partname,
            len(patch_body),
            ", ".join(patch_body.keys()),
        )

        response = self._request("PATCH", url, json_body=patch_body)
        if response is None:
            raise requests.HTTPError("No response from Priority on PATCH")

        return response.json()
