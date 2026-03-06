"""
FastAPI webhook server for triggering sync workflows.

Per-workflow routing with independent locks so different workflows
can run simultaneously.

7 workflows (merged from 11):
  products        (LOGPART + FNCPART + PRDPART)
  vendors         (SUPPLIERS + FNCSUP)
  vendor-prices   (PRICELIST)
  customers       (CUSTOMERS + FNCCUST)
  customer-prices (PRICELIST)
  images          (LOGPART, A→P only)
  techsheets      (LOGPART, A→P only)

Environment switching:
  Add ?env=sandbox or ?env=uat to any endpoint to target a specific
  Priority environment. Default (no env param) uses PRIORITY_API_URL from .env.
  Production is blocked from webhooks for safety — use CLI only.

Endpoints (per workflow):
  GET  /health                                        → Health check
  GET  /webhook/{workflow}/sync?key=...&env=...       → A→P full
  GET  /webhook/{workflow}/sync-status?key=...&env=...→ A→P status-only
  GET  /webhook/{workflow}/sync-from-priority?key=...&env=... → P→A full
  GET  /webhook/{workflow}/sync-from-priority-status?key=...&env=... → P→A status-only

Backward-compatible product endpoints:
  GET  /webhook/sync?key=...                → products A→P full
  GET  /webhook/sync-status?key=...         → products A→P status-only
  GET  /webhook/sync-from-priority?key=...  → products P→A full
  GET  /webhook/sync-from-priority-status?key=... → products P→A status-only
  POST /webhook/sync                        → products A→P full (POST + Bearer)
  GET  /webhook/status                      → last run status (all workflows)
"""

from __future__ import annotations

import hmac
import logging
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException

from sync.core.config import LA_TIMEZONE, WEBHOOK_API_KEY, get_priority_url, init_sentry
from sync.core.models import ConflictStrategy, SyncDirection, SyncMode

# Sentry must be initialized before FastAPI app creation so integrations
# can attach their middleware to the Starlette ASGI stack.
init_sentry()

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
    from sync.workflows.vendors.engine import VendorSyncEngine
    from sync.workflows.vendor_prices.engine import VendorPriceSyncEngine
    from sync.workflows.customers.engine import CustomerSyncEngine
    from sync.workflows.customer_prices.engine import CustomerPriceSyncEngine
    from sync.workflows.images.engine import ImageSyncEngine
    from sync.workflows.techsheets.engine import TechSheetSyncEngine

    _register_workflow("products", ProductSyncEngine, has_status_mode=True)
    _register_workflow("vendors", VendorSyncEngine, has_status_mode=False)
    _register_workflow("vendor-prices", VendorPriceSyncEngine, has_status_mode=False)
    _register_workflow("customers", CustomerSyncEngine, has_status_mode=False)
    _register_workflow("customer-prices", CustomerPriceSyncEngine, has_status_mode=False)
    _register_workflow("images", ImageSyncEngine, has_status_mode=False)
    _register_workflow("techsheets", TechSheetSyncEngine, has_status_mode=False)


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
    if not hmac.compare_digest(token, WEBHOOK_API_KEY):
        raise HTTPException(status_code=403, detail="Invalid API key.")


def _verify_query_key(key: str | None) -> None:
    """Validate the ?key= query parameter against WEBHOOK_API_KEY."""
    if not WEBHOOK_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="Webhook not configured. Set WEBHOOK_API_KEY env var.",
        )
    if not key or not hmac.compare_digest(key, WEBHOOK_API_KEY):
        raise HTTPException(status_code=403, detail="Invalid API key.")


# ── Environment helpers ───────────────────────────────────────────────────────

ALLOWED_WEBHOOK_ENVS = {"sandbox", "uat"}  # "production" blocked from webhooks for safety


def _resolve_priority_env(env: str | None) -> str | None:
    """
    Resolve the ?env= query param to a Priority API URL override.
    Returns None (use default) if env is not specified.
    Raises HTTPException for invalid or blocked environments.
    """
    if not env:
        return None

    env = env.strip().lower()
    if env == "production":
        raise HTTPException(
            status_code=403,
            detail="Production environment is blocked from webhook triggers for safety. Use CLI with --priority-env production instead.",
        )
    if env not in ALLOWED_WEBHOOK_ENVS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid environment: '{env}'. Allowed: {', '.join(sorted(ALLOWED_WEBHOOK_ENVS))}",
        )

    try:
        return get_priority_url(env)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Conflict strategy helpers ─────────────────────────────────────────────────

VALID_CONFLICT_STRATEGIES = {s.value for s in ConflictStrategy}


def _resolve_conflict_strategy(conflict: str | None) -> ConflictStrategy:
    """
    Resolve the ?conflict= query param to a ConflictStrategy enum.
    Returns SOURCE_WINS (default) if not specified.
    """
    if not conflict:
        return ConflictStrategy.SOURCE_WINS
    conflict = conflict.strip().lower()
    if conflict not in VALID_CONFLICT_STRATEGIES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid conflict strategy: '{conflict}'. "
                f"Allowed: {', '.join(sorted(VALID_CONFLICT_STRATEGIES))}"
            ),
        )
    return ConflictStrategy(conflict)


# Standalone workflows that don't support conflict_strategy
_STANDALONE_WORKFLOWS = {"images", "techsheets"}


# ── Background sync runner ────────────────────────────────────────────────────

def _run_sync_background(
    workflow_name: str,
    mode: SyncMode = SyncMode.FULL,
    direction: SyncDirection = SyncDirection.AIRTABLE_TO_PRIORITY,
    priority_url_override: str | None = None,
    conflict_strategy: ConflictStrategy = ConflictStrategy.SOURCE_WINS,
) -> None:
    """Execute the sync in a background thread."""
    _lazy_register_workflows()
    workflow = WORKFLOW_REGISTRY[workflow_name]

    try:
        engine_class = workflow["engine_factory"]

        engine_kwargs: dict[str, Any] = dict(
            direction=direction,
            dry_run=False,
            trigger="webhook",
            mode=mode,
            workflow_name=workflow_name,
            priority_url_override=priority_url_override,
        )
        if workflow_name not in _STANDALONE_WORKFLOWS:
            engine_kwargs["conflict_strategy"] = conflict_strategy

        engine = engine_class(**engine_kwargs)
        stats = engine.run()

        now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
        run_info: dict[str, Any] = {
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
        if stats.conflicts:
            run_info["conflicts"] = len(stats.conflicts)
        workflow["last_run"] = run_info
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

        import sentry_sdk
        with sentry_sdk.new_scope() as scope:
            scope.set_tag("workflow", workflow_name)
            scope.set_tag("direction", direction.value)
            scope.set_tag("mode", mode.value)
            sentry_sdk.capture_exception(e)

    finally:
        with workflow["lock"]:
            workflow["running"] = False


def _start_workflow(
    workflow_name: str,
    background_tasks: BackgroundTasks,
    mode: SyncMode = SyncMode.FULL,
    direction: SyncDirection = SyncDirection.AIRTABLE_TO_PRIORITY,
    priority_url_override: str | None = None,
    conflict_strategy: ConflictStrategy = ConflictStrategy.SOURCE_WINS,
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
        priority_url_override=priority_url_override,
        conflict_strategy=conflict_strategy,
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
def health_check() -> dict[str, Any]:
    """Health check for Railway. Pings Priority and Airtable APIs."""
    import requests as _requests

    results: dict[str, str] = {}

    # Test Priority API
    try:
        from sync.core.config import PRIORITY_API_URL, PRIORITY_USER, PRIORITY_PASS
        resp = _requests.get(
            f"{PRIORITY_API_URL}LOGPART?$top=1&$select=PARTNAME",
            auth=(PRIORITY_USER, PRIORITY_PASS),
            headers={"IEEE754Compatible": "true"},
            timeout=10,
        )
        results["priority"] = "ok" if resp.status_code == 200 else f"error ({resp.status_code})"
    except Exception as e:
        results["priority"] = f"error ({e.__class__.__name__})"

    # Test Airtable API
    try:
        from sync.core.config import AIRTABLE_TOKEN
        resp = _requests.get(
            "https://api.airtable.com/v0/meta/whoami",
            headers={"Authorization": f"Bearer {AIRTABLE_TOKEN}"},
            timeout=10,
        )
        results["airtable"] = "ok" if resp.status_code == 200 else f"error ({resp.status_code})"
    except Exception as e:
        results["airtable"] = f"error ({e.__class__.__name__})"

    all_ok = all(v == "ok" for v in results.values())
    return {"status": "ok" if all_ok else "degraded", **results}


@app.get("/sentry-debug")
async def trigger_error():
    """Temporary endpoint to verify Sentry is working. Remove after verification."""
    division_by_zero = 1 / 0


# ═══════════════════════════════════════════════════════════════════════════════
# Per-workflow endpoints
# ═══════════════════════════════════════════════════════════════════════════════

# ── Products ──────────────────────────────────────────────────────────────────

@app.get("/webhook/products/sync", status_code=202)
def products_sync(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """A→P full sync for products."""
    _verify_query_key(key)
    return _start_workflow("products", background_tasks, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


@app.get("/webhook/products/sync-status", status_code=202)
def products_sync_status(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """A→P status-only sync for products."""
    _verify_query_key(key)
    return _start_workflow("products", background_tasks, mode=SyncMode.STATUS, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


@app.get("/webhook/products/sync-from-priority", status_code=202)
def products_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """P→A full sync for products."""
    _verify_query_key(key)
    return _start_workflow("products", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


@app.get("/webhook/products/sync-from-priority-status", status_code=202)
def products_sync_from_priority_status(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """P→A status-only sync for products."""
    _verify_query_key(key)
    return _start_workflow(
        "products", background_tasks,
        mode=SyncMode.STATUS, direction=SyncDirection.PRIORITY_TO_AIRTABLE,
        priority_url_override=_resolve_priority_env(env),
        conflict_strategy=_resolve_conflict_strategy(conflict),
    )


# ── Vendors ──────────────────────────────────────────────────────────────────

@app.get("/webhook/vendors/sync", status_code=202)
def vendors_sync(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """A→P full sync for vendors (all)."""
    _verify_query_key(key)
    return _start_workflow("vendors", background_tasks, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


@app.get("/webhook/vendors/sync-from-priority", status_code=202)
def vendors_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """P→A full sync for vendors (all)."""
    _verify_query_key(key)
    return _start_workflow("vendors", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


# ── Vendor Price Lists ───────────────────────────────────────────────────────

@app.get("/webhook/vendor-prices/sync", status_code=202)
def vendor_prices_sync(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """A→P full sync for vendor price lists."""
    _verify_query_key(key)
    return _start_workflow("vendor-prices", background_tasks, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


@app.get("/webhook/vendor-prices/sync-from-priority", status_code=202)
def vendor_prices_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """P→A full sync for vendor price lists."""
    _verify_query_key(key)
    return _start_workflow("vendor-prices", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


# ── Customers ────────────────────────────────────────────────────────────────

@app.get("/webhook/customers/sync", status_code=202)
def customers_sync(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """A→P full sync for customers (all)."""
    _verify_query_key(key)
    return _start_workflow("customers", background_tasks, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


@app.get("/webhook/customers/sync-from-priority", status_code=202)
def customers_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """P→A full sync for customers (all)."""
    _verify_query_key(key)
    return _start_workflow("customers", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


# ── Customer Price Lists ─────────────────────────────────────────────────────

@app.get("/webhook/customer-prices/sync", status_code=202)
def customer_prices_sync(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """A→P full sync for customer price lists."""
    _verify_query_key(key)
    return _start_workflow("customer-prices", background_tasks, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


@app.get("/webhook/customer-prices/sync-from-priority", status_code=202)
def customer_prices_sync_from_priority(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
    conflict: str | None = None,
) -> dict[str, str]:
    """P→A full sync for customer price lists."""
    _verify_query_key(key)
    return _start_workflow("customer-prices", background_tasks, direction=SyncDirection.PRIORITY_TO_AIRTABLE, priority_url_override=_resolve_priority_env(env), conflict_strategy=_resolve_conflict_strategy(conflict))


# ── Images ──────────────────────────────────────────────────────────────────

@app.get("/webhook/images/sync", status_code=202)
def images_sync(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
) -> dict[str, str]:
    """A→P image sync (upload product images to Priority)."""
    _verify_query_key(key)
    return _start_workflow("images", background_tasks, priority_url_override=_resolve_priority_env(env))


# ── Tech Sheets ────────────────────────────────────────────────────────────

@app.get("/webhook/techsheets/sync", status_code=202)
def techsheets_sync(
    background_tasks: BackgroundTasks, key: str | None = None, env: str | None = None,
) -> dict[str, str]:
    """A→P tech sheet sync (upload tech sheet PDFs to Priority)."""
    _verify_query_key(key)
    return _start_workflow("techsheets", background_tasks, priority_url_override=_resolve_priority_env(env))


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
