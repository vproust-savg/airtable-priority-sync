"""
FastAPI webhook server for triggering sync workflows.

Per-workflow routing with independent locks so different workflows
can run simultaneously.

Endpoints (per workflow):
  GET  /health                                        → Health check
  GET  /webhook/{workflow}/sync?key=...               → A→P full
  GET  /webhook/{workflow}/sync-status?key=...        → A→P status-only
  GET  /webhook/{workflow}/sync-from-priority?key=... → P→A full
  GET  /webhook/{workflow}/sync-from-priority-status?key=... → P→A status-only

Backward-compatible product endpoints:
  GET  /webhook/sync?key=...                → products A→P full
  GET  /webhook/sync-status?key=...         → products A→P status-only
  GET  /webhook/sync-from-priority?key=...  → products P→A full
  GET  /webhook/sync-from-priority-status?key=... → products P→A status-only
  POST /webhook/sync                        → products A→P full (POST + Bearer)
  GET  /webhook/status                      → last run status (all workflows)
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException

from sync.core.config import LA_TIMEZONE, WEBHOOK_API_KEY
from sync.core.models import SyncDirection, SyncMode

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Airtable ↔ Priority ERP Sync",
    description="Webhook-triggered sync between Airtable and Priority ERP.",
    version="2.0.0",
)

# ── Workflow registry ─────────────────────────────────────────────────────────

WORKFLOW_REGISTRY: dict[str, dict[str, Any]] = {}


def _register_workflow(name: str, engine_factory: Any, has_status_mode: bool = True) -> None:
    """Register a workflow with its engine factory."""
    WORKFLOW_REGISTRY[name] = {
        "engine_factory": engine_factory,
        "has_status_mode": has_status_mode,
        "lock": threading.Lock(),
        "running": False,
        "last_run": {},
    }


def _lazy_register_workflows() -> None:
    """Register all known workflows (lazy import to avoid circular deps)."""
    if WORKFLOW_REGISTRY:
        return  # Already registered

    from sync.workflows.products.engine import ProductSyncEngine
    from sync.workflows.fncpart.engine import FncpartSyncEngine
    from sync.workflows.prdpart.engine import PrdpartSyncEngine
    from sync.workflows.vendors.engine import VendorSyncEngine
    from sync.workflows.fncsup.engine import FncsupSyncEngine
    from sync.workflows.vendor_prices.engine import VendorPriceSyncEngine
    from sync.workflows.customers.engine import CustomerSyncEngine
    from sync.workflows.fnccust.engine import FnccustSyncEngine
    from sync.workflows.customer_prices.engine import CustomerPriceSyncEngine

    _register_workflow("products", ProductSyncEngine, has_status_mode=True)
    _register_workflow("fncpart", FncpartSyncEngine, has_status_mode=False)
    _register_workflow("prdpart", PrdpartSyncEngine, has_status_mode=False)
    _register_workflow("vendors", VendorSyncEngine, has_status_mode=False)
    _register_workflow("fncsup", FncsupSyncEngine, has_status_mode=False)
    _register_workflow("vendor-prices", VendorPriceSyncEngine, has_status_mode=False)
    _register_workflow("customers", CustomerSyncEngine, has_status_mode=False)
    _register_workflow("fnccust", FnccustSyncEngine, has_status_mode=False)
    _register_workflow("customer-prices", CustomerPriceSyncEngine, has_status_mode=False)


# ── Auth helpers ──────────────────────────────────────────────────────────────

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


# ── Background sync runner ────────────────────────────────────────────────────

def _run_sync_background(
    workflow_name: str,
    mode: SyncMode = SyncMode.FULL,
    direction: SyncDirection = SyncDirection.AIRTABLE_TO_PRIORITY,
) -> None:
    """Execute the sync in a background thread."""
    _lazy_register_workflows()
    workflow = WORKFLOW_REGISTRY[workflow_name]

    try:
        engine_class = workflow["engine_factory"]
        engine = engine_class(
            direction=direction,
            dry_run=False,
            trigger="webhook",
            mode=mode,
            workflow_name=workflow_name,
        )
        stats = engine.run()

        now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
        workflow["last_run"] = {
            "completed_at": now_la.isoformat(),
            "workflow": workflow_name,
            "direction": direction.value,
            "mode": mode.value,
            "status": "success" if stats.errors == 0 else "partial",
            "fetched": stats.total_fetched,
            "created": stats.created,
            "updated": stats.updated,
            "skipped": stats.skipped,
            "errors": stats.errors,
            "duration": stats.duration_display,
        }
        logger.info("Webhook sync completed (%s): %s", workflow_name, workflow["last_run"])

    except Exception as e:
        now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
        workflow["last_run"] = {
            "completed_at": now_la.isoformat(),
            "workflow": workflow_name,
            "direction": direction.value,
            "mode": mode.value,
            "status": "failed",
            "error": str(e),
        }
        logger.error("Webhook sync failed (%s): %s", workflow_name, e)

    finally:
        with workflow["lock"]:
            workflow["running"] = False


def _start_workflow(
    workflow_name: str,
    background_tasks: BackgroundTasks,
    mode: SyncMode = SyncMode.FULL,
    direction: SyncDirection = SyncDirection.AIRTABLE_TO_PRIORITY,
) -> dict[str, str]:
    """Start a workflow sync in background. Returns response dict."""
    _lazy_register_workflows()

    if workflow_name not in WORKFLOW_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown workflow: {workflow_name}")

    workflow = WORKFLOW_REGISTRY[workflow_name]

    with workflow["lock"]:
        if workflow["running"]:
            raise HTTPException(
                status_code=409,
                detail=f"{workflow_name} sync already in progress. Try again later.",
            )
        workflow["running"] = True

    background_tasks.add_task(
        _run_sync_background,
        workflow_name=workflow_name,
        mode=mode,
        direction=direction,
    )

    now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
    dir_label = "Airtable → Priority" if direction == SyncDirection.AIRTABLE_TO_PRIORITY else "Priority → Airtable"
    return {
        "message": f"{workflow_name} sync started ({dir_label}, {mode.value})",
        "workflow": workflow_name,
        "direction": direction.value,
        "mode": mode.value,
        "started_at": now_la.isoformat(),
    }


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
def health_check() -> dict[str, str]:
    """Health check for Railway."""
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════════════════════
# Per-workflow endpoints
# ═══════════════════════════════════════════════════════════════════════════════

# ── Products ──────────────────────────────────────────────────────────────────

@app.get("/webhook/products/sync", status_code=202)
def products_sync(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P full sync for products."""
    _verify_query_key(key)
    return _start_workflow("products", background_tasks)


@app.get("/webhook/products/sync-status", status_code=202)
def products_sync_status(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P status-only sync for products."""
    _verify_query_key(key)
    return _start_workflow("products", background_tasks, mode=SyncMode.STATUS)


@app.get("/webhook/products/sync-from-priority", status_code=202)
def products_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A full sync for products."""
    _verify_query_key(key)
    return _start_workflow("products", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


@app.get("/webhook/products/sync-from-priority-status", status_code=202)
def products_sync_from_priority_status(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A status-only sync for products."""
    _verify_query_key(key)
    return _start_workflow(
        "products", background_tasks,
        mode=SyncMode.STATUS, direction=SyncDirection.PRIORITY_TO_AIRTABLE,
    )


# ── FNCPART ───────────────────────────────────────────────────────────────────

@app.get("/webhook/fncpart/sync", status_code=202)
def fncpart_sync(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P full sync for financial parameters (parts)."""
    _verify_query_key(key)
    return _start_workflow("fncpart", background_tasks)


@app.get("/webhook/fncpart/sync-from-priority", status_code=202)
def fncpart_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A full sync for financial parameters (parts)."""
    _verify_query_key(key)
    return _start_workflow("fncpart", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


# ── PRDPART ──────────────────────────────────────────────────────────────────

@app.get("/webhook/prdpart/sync", status_code=202)
def prdpart_sync(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P full sync for MRP parameters (parts)."""
    _verify_query_key(key)
    return _start_workflow("prdpart", background_tasks)


@app.get("/webhook/prdpart/sync-from-priority", status_code=202)
def prdpart_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A full sync for MRP parameters (parts)."""
    _verify_query_key(key)
    return _start_workflow("prdpart", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


# ── Vendors ──────────────────────────────────────────────────────────────────

@app.get("/webhook/vendors/sync", status_code=202)
def vendors_sync(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P full sync for vendors (all)."""
    _verify_query_key(key)
    return _start_workflow("vendors", background_tasks)


@app.get("/webhook/vendors/sync-from-priority", status_code=202)
def vendors_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A full sync for vendors (all)."""
    _verify_query_key(key)
    return _start_workflow("vendors", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


# ── FNCSUP ───────────────────────────────────────────────────────────────────

@app.get("/webhook/fncsup/sync", status_code=202)
def fncsup_sync(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P full sync for financial parameters (vendors)."""
    _verify_query_key(key)
    return _start_workflow("fncsup", background_tasks)


@app.get("/webhook/fncsup/sync-from-priority", status_code=202)
def fncsup_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A full sync for financial parameters (vendors)."""
    _verify_query_key(key)
    return _start_workflow("fncsup", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


# ── Vendor Price Lists ───────────────────────────────────────────────────────

@app.get("/webhook/vendor-prices/sync", status_code=202)
def vendor_prices_sync(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P full sync for vendor price lists."""
    _verify_query_key(key)
    return _start_workflow("vendor-prices", background_tasks)


@app.get("/webhook/vendor-prices/sync-from-priority", status_code=202)
def vendor_prices_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A full sync for vendor price lists."""
    _verify_query_key(key)
    return _start_workflow("vendor-prices", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


# ── Customers ────────────────────────────────────────────────────────────────

@app.get("/webhook/customers/sync", status_code=202)
def customers_sync(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P full sync for customers (all)."""
    _verify_query_key(key)
    return _start_workflow("customers", background_tasks)


@app.get("/webhook/customers/sync-from-priority", status_code=202)
def customers_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A full sync for customers (all)."""
    _verify_query_key(key)
    return _start_workflow("customers", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


# ── FNCCUST ──────────────────────────────────────────────────────────────────

@app.get("/webhook/fnccust/sync", status_code=202)
def fnccust_sync(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P full sync for financial parameters (customers)."""
    _verify_query_key(key)
    return _start_workflow("fnccust", background_tasks)


@app.get("/webhook/fnccust/sync-from-priority", status_code=202)
def fnccust_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A full sync for financial parameters (customers)."""
    _verify_query_key(key)
    return _start_workflow("fnccust", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


# ── Customer Price Lists ─────────────────────────────────────────────────────

@app.get("/webhook/customer-prices/sync", status_code=202)
def customer_prices_sync(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """A→P full sync for customer price lists."""
    _verify_query_key(key)
    return _start_workflow("customer-prices", background_tasks)


@app.get("/webhook/customer-prices/sync-from-priority", status_code=202)
def customer_prices_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """P→A full sync for customer price lists."""
    _verify_query_key(key)
    return _start_workflow("customer-prices", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


# ═══════════════════════════════════════════════════════════════════════════════
# Backward-compatible product endpoints (original URLs still work)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/webhook/sync", status_code=202)
def trigger_sync_post(
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    """[Backward compat] Trigger full A→P product sync via POST + Bearer."""
    _verify_api_key(authorization)
    return _start_workflow("products", background_tasks)


@app.get("/webhook/sync", status_code=202)
def trigger_sync_get(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """[Backward compat] Trigger A→P product sync via clickable GET URL."""
    _verify_query_key(key)
    return _start_workflow("products", background_tasks)


@app.get("/webhook/sync-status", status_code=202)
def trigger_sync_status_get(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """[Backward compat] A→P status-only product sync."""
    _verify_query_key(key)
    return _start_workflow("products", background_tasks, mode=SyncMode.STATUS)


@app.get("/webhook/sync-from-priority", status_code=202)
def trigger_sync_from_priority_get(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """[Backward compat] P→A full product sync."""
    _verify_query_key(key)
    return _start_workflow("products", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE)


@app.get("/webhook/sync-from-priority-status", status_code=202)
def trigger_sync_from_priority_status_get(
    background_tasks: BackgroundTasks, key: str | None = None,
) -> dict[str, str]:
    """[Backward compat] P→A status-only product sync."""
    _verify_query_key(key)
    return _start_workflow(
        "products", background_tasks,
        mode=SyncMode.STATUS, direction=SyncDirection.PRIORITY_TO_AIRTABLE,
    )


@app.get("/webhook/status")
def sync_status(
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Return status of all workflows."""
    _verify_api_key(authorization)
    _lazy_register_workflows()

    statuses: dict[str, Any] = {}
    for name, workflow in WORKFLOW_REGISTRY.items():
        with workflow["lock"]:
            running = workflow["running"]
        statuses[name] = {
            "running": running,
            "last_run": workflow["last_run"] or None,
        }

    return statuses
