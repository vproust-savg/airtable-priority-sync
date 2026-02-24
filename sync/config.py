"""
Configuration for Airtable <-> Priority ERP Product Sync.
All secrets loaded from environment variables (via .env file locally).
"""

from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Load .env file from project root (safe no-op if file doesn't exist)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _require_env(key: str) -> str:
    """Return env var or raise with a clear message."""
    value = os.environ.get(key, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value


# ── Airtable ─────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN = _require_env("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = _require_env("AIRTABLE_BASE_ID")
AIRTABLE_PRODUCTS_TABLE_ID = "tblAOeqWrVrBu16Tj"
AIRTABLE_PRODUCTS_TABLE_NAME = "Products"
AIRTABLE_SYNC_VIEW = "Airtable > Priority API Sync"

# Shelf Lives table (for sub-form sync)
AIRTABLE_SHELF_LIVES_TABLE_ID = "tbluWqVkrpLFh0D1G"
AIRTABLE_SHELF_LIVES_VIEW = "EDI Parts 2 - Shelf Lives"

# Airtable field names
AIRTABLE_FIELD_SKU = "SKU Trim (EDI)"
AIRTABLE_FIELD_LAST_MODIFIED = "Last Airtable Modified"
AIRTABLE_FIELD_LAST_SYNCED = "Last Synced to Priority"
AIRTABLE_FIELD_PRIORITY_UDATE = "Priority UDATE"

# Airtable API
AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_BATCH_SIZE = 10  # Max records per batch update
AIRTABLE_REQUEST_TIMEOUT = 30  # seconds
AIRTABLE_MAX_RETRIES = 3

# ── Airtable Sync Logs (optional — graceful degradation if not set) ──────────
AIRTABLE_LOG_BASE_ID = os.environ.get("AIRTABLE_LOG_BASE_ID", "").strip() or None

# ── Priority ERP ─────────────────────────────────────────────────────────────
PRIORITY_API_URL = _require_env("PRIORITY_API_URL")
PRIORITY_USER = _require_env("PRIORITY_USER")
PRIORITY_PASS = _require_env("PRIORITY_PASS")

# Priority API limits
PRIORITY_MAX_CALLS_PER_MINUTE = 100
PRIORITY_PAGE_SIZE = 500  # Records per paginated GET
PRIORITY_REQUEST_TIMEOUT = 60  # seconds (Priority can be slow)
PRIORITY_MAX_RETRIES = 3

# ── Webhook ──────────────────────────────────────────────────────────────────
WEBHOOK_API_KEY = os.environ.get("WEBHOOK_API_KEY", "").strip() or None

# ── Timezone ─────────────────────────────────────────────────────────────────
LA_TIMEZONE = ZoneInfo("America/Los_Angeles")

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_DIR = "logs"
