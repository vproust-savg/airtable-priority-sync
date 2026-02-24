"""
FastAPI webhook server for triggering product sync.

Endpoints:
  GET  /health         → Health check (Railway uses this)
  POST /webhook/sync   → Trigger sync via POST + Bearer header (automations)
  GET  /webhook/sync   → Trigger sync via clickable URL with ?key= param
  GET  /webhook/status → Last run result
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException

from sync.config import LA_TIMEZONE, WEBHOOK_API_KEY
from sync.models import SyncDirection
from sync.sync_engine import ProductSyncEngine

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Airtable ↔ Priority Product Sync",
    description="Webhook-triggered product sync between Airtable and Priority ERP.",
    version="1.0.0",
)

# ── Sync state ────────────────────────────────────────────────────────────────

_sync_lock = threading.Lock()
_sync_running = False
_last_run: dict[str, Any] = {}


def _verify_api_key(authorization: str | None) -> None:
    """Validate the Bearer token against WEBHOOK_API_KEY."""
    if not WEBHOOK_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="Webhook not configured. Set WEBHOOK_API_KEY env var.",
        )

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header.")

    token = authorization.removeprefix("Bearer ").strip()
    if token != WEBHOOK_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key.")


def _verify_query_key(key: str | None) -> None:
    """Validate the ?key= query parameter against WEBHOOK_API_KEY."""
    if not WEBHOOK_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="Webhook not configured. Set WEBHOOK_API_KEY env var.",
        )
    if not key or key != WEBHOOK_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key.")


def _run_sync_background() -> None:
    """Execute the sync in a background thread."""
    global _sync_running, _last_run  # noqa: PLW0603

    try:
        engine = ProductSyncEngine(
            direction=SyncDirection.AIRTABLE_TO_PRIORITY,
            dry_run=False,
            trigger="webhook",
        )
        stats = engine.run()

        now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
        _last_run = {
            "completed_at": now_la.isoformat(),
            "status": "success" if stats.errors == 0 else "partial",
            "fetched": stats.total_fetched,
            "created": stats.created,
            "updated": stats.updated,
            "skipped": stats.skipped,
            "errors": stats.errors,
            "duration": stats.duration_display,
        }
        logger.info("Webhook sync completed: %s", _last_run)

    except Exception as e:
        now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
        _last_run = {
            "completed_at": now_la.isoformat(),
            "status": "failed",
            "error": str(e),
        }
        logger.error("Webhook sync failed: %s", e)

    finally:
        with _sync_lock:
            _sync_running = False


# ── Endpoints ─────────────────────────────────────────────────────────────────


@app.get("/health")
def health_check() -> dict[str, str]:
    """Health check for Railway."""
    return {"status": "ok"}


@app.post("/webhook/sync", status_code=202)
def trigger_sync(
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    """
    Trigger a full Airtable → Priority sync.
    Returns 202 immediately; sync runs in background.
    """
    global _sync_running  # noqa: PLW0603

    _verify_api_key(authorization)

    with _sync_lock:
        if _sync_running:
            raise HTTPException(
                status_code=409,
                detail="Sync already in progress. Try again later.",
            )
        _sync_running = True

    background_tasks.add_task(_run_sync_background)

    now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
    return {
        "message": "Sync started",
        "started_at": now_la.isoformat(),
    }


@app.get("/webhook/sync", status_code=202)
def trigger_sync_get(
    background_tasks: BackgroundTasks,
    key: str | None = None,
) -> dict[str, str]:
    """
    Trigger sync via clickable GET URL with ?key= query param.
    Same behavior as POST but works as a simple link click.
    """
    global _sync_running  # noqa: PLW0603

    _verify_query_key(key)

    with _sync_lock:
        if _sync_running:
            raise HTTPException(
                status_code=409,
                detail="Sync already in progress. Try again later.",
            )
        _sync_running = True

    background_tasks.add_task(_run_sync_background)

    now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
    return {
        "message": "Sync started",
        "started_at": now_la.isoformat(),
    }


@app.get("/webhook/status")
def sync_status(
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Return the status of the last sync run."""
    _verify_api_key(authorization)

    with _sync_lock:
        running = _sync_running

    return {
        "running": running,
        "last_run": _last_run or None,
    }
