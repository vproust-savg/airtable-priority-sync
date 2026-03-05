"""
Shared configuration for all sync workflows.
All secrets loaded from environment variables (via .env file locally).
Workflow-specific config (table IDs, views, field names) lives in each workflow's config.py.
"""

from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Load .env file from project root (safe no-op if file doesn't exist)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _require_env(key: str) -> str:
    """Return env var or raise with a clear message."""
    value = os.environ.get(key, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value


# ── Airtable (shared) ────────────────────────────────────────────────────────
AIRTABLE_TOKEN = _require_env("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = _require_env("AIRTABLE_BASE_ID")
AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_BATCH_SIZE = 10  # Max records per batch update
AIRTABLE_REQUEST_TIMEOUT = 30  # seconds
AIRTABLE_MAX_RETRIES = 3
AIRTABLE_COMMENTS_ENABLED = os.environ.get(
    "AIRTABLE_COMMENTS_ENABLED", "true"
).strip().lower() in ("true", "1", "yes")

# ── Airtable Test Base (for safe testing) ─────────────────────────────────────
AIRTABLE_TEST_BASE_ID = os.environ.get("AIRTABLE_TEST_BASE_ID", "").strip() or None
AIRTABLE_TEST_TOKEN = os.environ.get("AIRTABLE_TEST_TOKEN", "").strip() or None

# ── Airtable Sync Logs (optional — graceful degradation if not set) ───────────
AIRTABLE_LOG_BASE_ID = os.environ.get("AIRTABLE_LOG_BASE_ID", "").strip() or None
# Separate token for log base (falls back to main AIRTABLE_TOKEN if not set)
AIRTABLE_LOG_TOKEN = os.environ.get("AIRTABLE_LOG_TOKEN", "").strip() or AIRTABLE_TOKEN

# ── Priority ERP ──────────────────────────────────────────────────────────────
PRIORITY_API_URL = _require_env("PRIORITY_API_URL")
PRIORITY_USER = _require_env("PRIORITY_USER")
PRIORITY_PASS = _require_env("PRIORITY_PASS")

# Priority environment company codes (same host + credentials, different company)
# URL pattern: https://us.priority-connect.online/odata/Priority/tabc8cae.ini/{company}/
PRIORITY_SANDBOX_COMPANY = os.environ.get("PRIORITY_SANDBOX_COMPANY", "a071024").strip()
PRIORITY_UAT_COMPANY = os.environ.get("PRIORITY_UAT_COMPANY", "a012226").strip()
PRIORITY_PROD_COMPANY = os.environ.get("PRIORITY_PROD_COMPANY", "").strip() or None

# Base URL without company code (derived from PRIORITY_API_URL)
# e.g., "https://us.priority-connect.online/odata/Priority/tabc8cae.ini/"
_url_parts = PRIORITY_API_URL.rstrip("/").rsplit("/", 1)
PRIORITY_API_BASE = _url_parts[0] + "/" if len(_url_parts) == 2 else PRIORITY_API_URL

PRIORITY_ENVS = {
    "sandbox": PRIORITY_SANDBOX_COMPANY,
    "uat": PRIORITY_UAT_COMPANY,
    "production": PRIORITY_PROD_COMPANY,
}


def get_priority_url(env: str) -> str:
    """Resolve the full Priority API URL for a given environment name."""
    code = PRIORITY_ENVS.get(env)
    if not code:
        raise RuntimeError(
            f"Priority company code not configured for environment '{env}'. "
            f"Set PRIORITY_{env.upper()}_COMPANY in .env"
        )
    return f"{PRIORITY_API_BASE}{code}/"


# Priority API limits
PRIORITY_MAX_CALLS_PER_MINUTE = 100
PRIORITY_PAGE_SIZE = 500  # Records per paginated GET
PRIORITY_REQUEST_TIMEOUT = 60  # seconds (Priority can be slow)
PRIORITY_MAX_RETRIES = 3

# ── Webhook ───────────────────────────────────────────────────────────────────
WEBHOOK_API_KEY = os.environ.get("WEBHOOK_API_KEY", "").strip() or None

# ── Timezone ──────────────────────────────────────────────────────────────────
LA_TIMEZONE = ZoneInfo("America/Los_Angeles")

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = "logs"
