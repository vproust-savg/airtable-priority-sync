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
PRIORITY_MAXAPILINES = 2000  # Server-side result cap per query context

# ── Webhook ───────────────────────────────────────────────────────────────────
WEBHOOK_API_KEY = os.environ.get("WEBHOOK_API_KEY", "").strip() or None

# ── Timezone ──────────────────────────────────────────────────────────────────
LA_TIMEZONE = ZoneInfo("America/Los_Angeles")

# ── Sentry (optional — error monitoring) ──────────────────────────────────────
SENTRY_DSN = os.environ.get("SENTRY_DSN", "").strip() or None
SENTRY_ENVIRONMENT = os.environ.get("SENTRY_ENVIRONMENT", "development").strip()
SENTRY_TRACES_SAMPLE_RATE = float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.1"))

_sentry_initialized = False


def init_sentry() -> None:
    """Initialize Sentry SDK if SENTRY_DSN is configured. Safe to call multiple times."""
    global _sentry_initialized
    if _sentry_initialized or not SENTRY_DSN:
        return

    import logging

    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration

        sentry_sdk.init(
            dsn=SENTRY_DSN,
            environment=SENTRY_ENVIRONMENT,
            traces_sample_rate=SENTRY_TRACES_SAMPLE_RATE,
            release=os.environ.get("COMMIT_SHA") or None,
            integrations=[
                StarletteIntegration(),
                FastApiIntegration(),
                LoggingIntegration(
                    level=logging.INFO,
                    event_level=logging.ERROR,
                ),
            ],
            send_default_pii=False,
        )

        _sentry_initialized = True
        logging.getLogger(__name__).info("Sentry initialized (env: %s)", SENTRY_ENVIRONMENT)
    except Exception:
        logging.getLogger(__name__).warning(
            "Sentry initialization failed — continuing without error monitoring",
            exc_info=True,
        )


# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = "logs"
