# Airtable ↔ Priority ERP — Multi-Workflow 2-Way Sync

## Project Overview

**What we're building:** A bidirectional sync engine between Airtable and Priority ERP covering **7 workflows** across products, vendors, and customers.
**Tech stack:** Pure Python (no n8n). Hosted on Railway.
**User context:** The project owner is not a developer. Keep explanations clear, ask before making assumptions, and never leave the project in a broken state.

---

## Commands

| Command | Description |
|---------|-------------|
| `pip install -r requirements.txt` | Install dependencies |
| `python3 -m sync.run_sync --workflow products --dry-run` | Preview product sync (no writes) |
| `python3 -m sync.run_sync --workflow products` | Run product A→P sync |
| `python3 -m sync.run_sync --workflow products --direction priority-to-airtable --test-base` | P→A sync to test base |
| `python3 -m sync.run_sync --workflow vendors --priority-env uat` | Vendor sync against UAT |
| `python3 -m sync.run_sync --workflow products --sku P00001` | Sync single product |
| `python3 -m sync.run_sync --server` | Start webhook server (port 8000) |

**Key flags:** `--workflow` (7 options), `--direction` (airtable-to-priority / priority-to-airtable / both), `--mode` (full / status), `--dry-run`, `--sku`, `--test-base`, `--priority-env` (sandbox / uat / production)

**Requires:** Python 3.11+ (`pyproject` uses `.python-version`)

---

## Scope: 7 Sync Workflows (Merged)

| # | Workflow | Priority Entities | CLI Name | Airtable Table | Direction |
|---|---------|-------------------|----------|----------------|-----------|
| 10 | Products | LOGPART + FNCPART + PRDPART | `products` | Products | Bidirectional |
| 20 | Vendors | SUPPLIERS + FNCSUP | `vendors` | Vendors | Bidirectional |
| 21 | Vendor Price Lists | PRICELIST | `vendor-prices` | Vendor Price List | Bidirectional |
| 30 | Customers | CUSTOMERS + FNCCUST | `customers` | Customers | Bidirectional |
| 31 | Customer Price Lists | PRICELIST | `customer-prices` | Customer Price List | Bidirectional |
| 40 | Product Images | LOGPART | `images` | Products | A→P only |
| 41 | Tech Sheets | LOGPART | `techsheets` | Products | A→P only |

Workflows 10–31 support bidirectional sync (A→P and P→A). Products, Vendors, and Customers each sync their financial parameter entities (FNCPART/PRDPART, FNCSUP, FNCCUST) as secondary entities within the same workflow. Product Images (40) downloads from Airtable, compresses with Pillow (<150KB JPG), and uploads to Priority's `EXTFILENAME` field. Tech Sheets (41) uploads PDFs from Airtable to Priority's `PARTEXTFILE_SUBFORM` sub-form. Both are A→P only.

**Products also supports `--mode status`** for status-only sync (touches only LOGPART status fields, skips sub-forms and secondary entities).

---

## Systems & Credentials

All credentials are in `.env`. Never hardcode them in source files.

### Airtable
- **Base:** Savory Gourmet (`appjwOgR4HsXeGIda`)
- **Main table:** `Products` (accessed via multiple views for different field groups)
- **Secondary table:** `Shelf Lives` (table ID: `tbluWqVkrpLFh0D1G`, view: `EDI Parts 2 - Shelf Lives`)
- **Sync view:** `Airtable > Priority API Sync` (only returns records where Priority Sync Needed = "Yes")
- **Auth:** Personal Access Token (in `.env` as `AIRTABLE_TOKEN`)
- **MCP access:** Claude has direct Airtable MCP access for reads/writes
- **Sync Logs base:** `appr935iOTErWivM1` (tables: Sync Runs `tblSN1oQUP18mDq0K`, Sync Errors `tbljeM0YaEIWJRs63`)

### Priority ERP
- **API URL:** `https://us.priority-connect.online/odata/Priority/tabc8cae.ini/a071024/`
- **Auth:** HTTP Basic Auth (`PRIORITY_USER` / `PRIORITY_PASS` in `.env`)
- **Product entity:** `LOGPART` (key field: `PARTNAME` = SKU)
- **API docs:** https://prioritysoftware.github.io/restapi/
- **Rate limits:** 100 calls/minute, 15 queued max, 3-minute timeout per request
- **Pagination:** `$top` + `$skip` params

### Railway (Hosting)
- **URL:** `https://airtable-priority-sync-production.up.railway.app`
- **Health check:** `GET /health`
- **Status check:** `GET /webhook/status` (requires Bearer header)
- **Auto-deploys:** Pushes to `main` on GitHub trigger automatic redeploy
- **Env vars:** All credentials set in Railway Variables tab (never in code)

**Webhook endpoint pattern** (14 workflow-specific endpoints + 5 legacy):
```
GET /webhook/{workflow}/sync?key={KEY}                    # A→P full sync
GET /webhook/{workflow}/sync-from-priority?key={KEY}      # P→A full sync
GET /webhook/{workflow}/sync-status?key={KEY}             # A→P status-only (products only)
GET /webhook/{workflow}/sync-from-priority-status?key={KEY} # P→A status-only (products only)
```
Where `{workflow}` = `products`, `vendors`, `vendor-prices`, `customers`, `customer-prices`, `images`, `techsheets`.

**Note:** `images` and `techsheets` workflows only have the `/sync` endpoint (A→P only, no reverse direction or status mode).

Add `&env=uat` or `&env=sandbox` for environment switching. Production blocked from webhooks.

### Airtable API Sync Table
- **Table:** `API Sync` (`tblpwvHgbDzYx5Edm`) in the Savory Gourmet base
- **Purpose:** Stores all sync URLs (Railway + AirPower) with clickable Start Sync buttons
- **Automation:** Single "Start API Sync" automation reads URL from the Url field and fetches it
- **User preference:** All sync triggers should be **clickable GET URLs with auth in query params** (no POST + headers). This matches how AirPower works and keeps the Airtable automation simple.

---

## Environments

### Priority Environments (3 environments — same host, same credentials, different company codes)

| Environment | Company Code | URL | CLI Flag | Webhook Param |
|---|---|---|---|---|
| **Sandbox** | `a071024` | `.../tabc8cae.ini/a071024/` | `--priority-env sandbox` | `&env=sandbox` |
| **UAT** | `a012226` | `.../tabc8cae.ini/a012226/` | `--priority-env uat` | `&env=uat` |
| **Production** | TBD | `.../tabc8cae.ini/{TBD}/` | `--priority-env production` | **BLOCKED from webhooks** |

- All environments share the same host (`us.priority-connect.online`) and credentials (`SGAPI`)
- The only difference is the company code in the URL path
- **Default (no flag):** Uses `PRIORITY_API_URL` from `.env` (currently sandbox)
- **Production is blocked from webhook endpoints** for safety — use CLI with `--priority-env production` only
- Env vars: `PRIORITY_SANDBOX_COMPANY`, `PRIORITY_UAT_COMPANY`, `PRIORITY_PROD_COMPANY`

### Airtable Bases (2 bases)

| Base | ID | Purpose | CLI Flag |
|---|---|---|---|
| **Production** | `appjwOgR4HsXeGIda` | Main data — DO NOT overwrite via P→A testing | (default) |
| **Test** | `appqRALXnLSbi1hq3` | Duplicated base for safe P→A testing | `--test-base` |

- **A→P testing:** Reads from production Airtable (safe — only reads), writes to Priority sandbox/UAT
- **P→A testing:** Reads from Priority sandbox/UAT, writes to TEST Airtable base (safe — not production)
- Env vars: `AIRTABLE_TEST_BASE_ID`, `AIRTABLE_TEST_TOKEN`

### Testing Matrix

| Test Scenario | Airtable Base | Priority Target | CLI Flags |
|---|---|---|---|
| **A→P sandbox** | Production (reads) | Sandbox | `--priority-env sandbox` |
| **A→P UAT** | Production (reads) | UAT | `--priority-env uat` |
| **P→A sandbox** | Test (writes) | Sandbox | `--test-base --priority-env sandbox` |
| **P→A UAT** | Test (writes) | UAT | `--test-base --priority-env uat` |
| **A→P production** | Production | Production | `--priority-env production` (CLI only) |
| **P→A production** | Production | Production | `--priority-env production` (no --test-base) |

### Webhook Environment Switching

Add `&env=sandbox` or `&env=uat` to any webhook URL:
```
GET /webhook/products/sync?key={KEY}&env=uat
```
Default (no `env` param): uses `PRIORITY_API_URL` from `.env` (sandbox).

---

## Field Mappings & Priority Interfaces

Field-level details live in the code and reference files — not here. Consult these sources:

| Workflow | Priority Interface File | Code Mapping |
|----------|----------------------|--------------|
| Products (LOGPART) | `tools/products-interface.txt` | `sync/workflows/products/field_mapping.py` |
| Fin. Params Parts (FNCPART) | `tools/fncpart-interface.txt` | `sync/workflows/fncpart/field_mapping.py` |
| MRP Parts (PRDPART) | `tools/prdpart-interface.txt` | `sync/workflows/prdpart/field_mapping.py` |
| Vendors (SUPPLIERS) | `tools/vendors-interface.txt` | `sync/workflows/vendors/field_mapping.py` |
| Fin. Params Vendors (FNCSUP) | `tools/fncsup-interface.txt` | `sync/workflows/fncsup/field_mapping.py` |
| Vendor Prices (PRICELIST) | — | `sync/workflows/vendor_prices/field_mapping.py` |
| Customers (CUSTOMERS) | `tools/customers-interface.txt` | `sync/workflows/customers/field_mapping.py` |
| Fin. Params Customers (FNCCUST) | `tools/fnccust-interface.txt` | `sync/workflows/fnccust/field_mapping.py` |
| Customer Prices (PRICELIST) | — | `sync/workflows/customer_prices/field_mapping.py` |

**Airtable field names & views:** See `tools/*.py` scripts (one per workflow) — these are the original export scripts that define every Airtable field name and view used.

---

## Reference Files

| File | Purpose |
|------|---------|
| `tools/*.txt` | Priority interface definitions — source of truth for Priority field names per entity |
| `tools/*.py` | Airtable export scripts — source of truth for Airtable field names, views, and data transforms |
| `tools/n8n-customer-sync-reference.json` | n8n workflow reference for Priority API patterns (auth, upsert logic, `IEEE754Compatible: true` header) |
| `.env` / `.env.example` | API credentials. Never hardcode elsewhere. |

---

## Priority API Patterns (Tested & Proven)

### Authentication & Headers
- **Auth:** HTTP Basic Auth with username/password from `.env`
- **Required header:** `IEEE754Compatible: true` on all requests
- **Content-Type:** `application/json`

### Main Entity (LOGPART) Operations
- **GET one:** `{API_URL}LOGPART('{SKU}')` → 200 with entity, or 404
- **GET all (paginated):** `{API_URL}LOGPART?$select=PARTNAME&$top=500&$skip=0`
- **POST (create):** `{API_URL}LOGPART` with JSON body
- **PATCH (update):** `{API_URL}LOGPART('{SKU}')` with only changed fields
- **Upsert pattern:** GET first → 404 means POST, 200 means compare + PATCH
- **create_only fields:** `PUNITNAME` can only be set on POST, not PATCH (Priority rejects it)

### Sub-Form Operations — CRITICAL PATTERNS

Priority has **three distinct sub-form behaviors**. Each requires a different API strategy.
Getting this wrong causes 404s, 409s, or silent failures. Always match the pattern below.

#### Pattern A: Single-Entity Sub-Forms (Allergens)
**Applies to:** `SAVR_ALLERGENS_SUBFORM`

These sub-forms return a **single entity** (not an array). The GET response has
`$entity` in its `@odata.context` and fields are returned directly at the top level
(NOT wrapped in a `"value"` array).

```
GET  .../LOGPART('{SKU}')/SAVR_ALLERGENS_SUBFORM
→ {"@odata.context": "...$entity", "DAIRY": "Yes", "EGGS": "No", ...}
   (NOT {"value": [...]})

PATCH .../LOGPART('{SKU}')/SAVR_ALLERGENS_SUBFORM
→ Body: {"PEANUT": "Yes"}  (only changed fields)
→ 200 OK
```

**Key rules:**
- GET returns the entity directly — code must detect `$entity` in context and NOT look for `"value"` key
- PATCH directly on the sub-form URL (no key in parentheses needed)
- POST only for products that have NO allergen record yet (returns 409 if record already exists)
- Implemented in: `priority_client.py → upsert_single_subform()`

#### Pattern B: Multi-Record Sub-Forms with URL Keys (Shelf Lives)
**Applies to:** `SAVR_PARTSHELF_SUBFORM`

These sub-forms return a `"value"` array. Individual records are accessible by their
**internal integer key** (NOT by a human-readable field value).

```
GET  .../LOGPART('{SKU}')/SAVR_PARTSHELF_SUBFORM
→ {"value": [
     {"TYPE": "Frozen", "NUMBER": 18, "TIMEUNIT": "Months", "SHELFLIFE": 3},
     {"TYPE": "Aft. Op.", "NUMBER": 1, "TIMEUNIT": "Days", "SHELFLIFE": 4}
   ]}

GET  .../SAVR_PARTSHELF_SUBFORM(3)   → returns "Frozen" record (SHELFLIFE=3)
GET  .../SAVR_PARTSHELF_SUBFORM(4)   → returns "Aft. Op." record (SHELFLIFE=4)

PATCH .../SAVR_PARTSHELF_SUBFORM(3)  → update the "Frozen" record
POST  .../SAVR_PARTSHELF_SUBFORM     → create a new shelf life record
```

**Key rules:**
- Match records by human-readable field (`TYPE`) but PATCH using the **integer entity key** (`SHELFLIFE`)
- The entity key field name varies per sub-form — for shelf lives it's `SHELFLIFE`
- `SAVR_PARTSHELF_SUBFORM('Frozen')` → 404! String keys DO NOT work here
- Implemented in: `priority_client.py → sync_multi_subform(url_key_field="SHELFLIFE")`

#### Pattern C: Multi-Record Sub-Forms WITHOUT URL Keys (Price Lists, Bins)
**Applies to:** `PARTINCUSTPLISTS_SUBFORM`, `PARTLOCATIONS_SUBFORM`

These sub-forms return a `"value"` array but individual records **cannot be accessed
by any key**. All attempts to GET/PATCH/DELETE a specific record return 404.

```
GET  .../LOGPART('{SKU}')/PARTINCUSTPLISTS_SUBFORM
→ {"value": [{"PLNAME": "Base", "PRICE": 84.81, ...}, ...]}

GET  .../PARTINCUSTPLISTS_SUBFORM('Base')      → 404!
GET  .../PARTINCUSTPLISTS_SUBFORM(1)           → 404!
GET  .../PARTINCUSTPLISTS_SUBFORM(PLNAME='Base') → 404!
PATCH .../PARTINCUSTPLISTS_SUBFORM (collection) → 400!

POST .../PARTINCUSTPLISTS_SUBFORM → creates a NEW record (409 if already exists)
```

**Solution: Deep PATCH on the parent entity.** Include sub-form records as a nested
array in the parent LOGPART PATCH:

```
PATCH .../LOGPART('{SKU}')
Body: {
  "PARTINCUSTPLISTS_SUBFORM": [
    {"PLNAME": "Base", "PRICE": 84.81, "CODE": "$", "QUANT": 1, "UNITNAME": "cs"}
  ]
}
→ 200 OK (updates matching records by PLNAME)
```

**Key rules:**
- Cannot access individual records — no key works in the URL
- POST creates new records but fails with 409 if record already exists
- **Use deep PATCH** on `LOGPART('{SKU}')` with nested sub-form array
- Priority matches records internally (e.g., by PLNAME for price lists)
- Compare locally first (GET → diff) to avoid unnecessary API calls
- Implemented in: `priority_client.py → deep_patch_subform()`

### Sub-Form Summary Table

| Sub-Form | Pattern | GET Response | Update Method | Key |
|----------|---------|-------------|---------------|-----|
| `SAVR_ALLERGENS_SUBFORM` | A (single entity) | `$entity` (no value array) | PATCH on sub-form URL | None needed |
| `SAVR_PARTSHELF_SUBFORM` | B (multi + URL key) | `{"value": [...]}` | PATCH with integer key | `SHELFLIFE` (int) |
| `PARTINCUSTPLISTS_SUBFORM` | C (multi, no key) | `{"value": [...]}` | Deep PATCH on LOGPART | N/A |
| `PARTLOCATIONS_SUBFORM` | C (multi, no key) | `{"value": [...]}` | Deep PATCH on LOGPART | N/A |

### Error Messages to Know
- **409 "A record with the specified key already exists"** → tried POST when record exists; need PATCH
- **404 on sub-form** → either wrong entity key or the sub-form doesn't support individual access (use Pattern C)
- **400 "Quantity missing"** on price list POST → QUANT field is required
- **400 on PUNITNAME PATCH** → this field is create_only; can only be set on POST

---

## Airtable Field Types — CRITICAL for P→A Writes

When writing to Airtable (P→A direction), field types matter. Sending the wrong type causes 422 errors.

### Read-Only Fields (Formulas, Lookups, Rollups — CANNOT write)

These Airtable fields are computed — they **cannot be set via the API**. Attempting to write returns 422.

| Airtable Field | Type | Why Read-Only |
|---|---|---|
| `SKU Trim (EDI)` | formula | `TRIM({SKU})` — use writable `SKU` field instead |
| `Base Price` | formula | References Price Import lookup |
| `Base Price Currency` | formula | Computed |
| `Standard Cost` | multipleLookupValues | Lookup from Price Import |
| `Conversion Ratio` | formula | Hardcoded `1` |
| `Availability Priority Output` | formula | Computed from availability dates |
| `Allocate Inventory` | formula | Computed from Accounting Family |
| `Vendor SKU Trim` | formula | Computed from Vendor SKU |
| `Family (Number from Product Type)` | formula | Computed |

**Rule:** Before adding a field to P→A mapping, verify its Airtable type via `GET /meta/bases/{baseId}/tables`. Only `singleLineText`, `number`, `singleSelect`, `multipleSelects`, `dateTime`, `checkbox` etc. are writable.

### Number Fields (must send numeric values, not strings)

| Airtable Field | Airtable Type | Priority Source | Transform |
|---|---|---|---|
| `Case Pack` | number (precision: 1) | SPEC1 (string) | `to_float` |
| `Product Net Weight Input` | number (precision: 1) | SPEC2 (string) | `to_float` |

Priority SPEC fields are always strings (even when they hold numbers like "100"). These must be converted to numeric values before sending to Airtable number fields.

### SingleSelect Fields (needs `typecast: true`)

Most mapped fields (Brand, Storage, Kelsey_Categories, etc.) are `singleSelect` in Airtable. If the Priority value doesn't match an existing option, Airtable returns 422 unless `typecast: true` is included in the API payload. With `typecast: true`, Airtable auto-creates new select options.

**Rule:** Always include `"typecast": true` in batch create and batch update payloads for P→A sync.

### SKU Fields — Read vs. Write

| Purpose | Field | Type | Usage |
|---|---|---|---|
| **Reading/matching** SKUs | `SKU Trim (EDI)` | formula | Use for lookups and comparisons |
| **Writing** SKUs (on create) | `SKU` | singleLineText | Use when creating new Airtable records |

### Priority Y/N → Airtable Yes/No

Priority stores boolean-like fields as `"Y"` / `"N"`. Airtable singleSelect fields use `"Yes"` / `"No"`. The `priority_yn()` transform handles this conversion. Apply it to: RESERVFLAG, and any SPEC field that stores Yes/No (Perishable, Retail, Staff Pick, Direct Import, etc.). Currently the Priority data for SPEC fields already contains "Yes"/"No" strings, so `clean()` works. If Priority changes to "Y"/"N" for SPEC fields, switch their transform to `priority_yn`.

---

## P→A Sync — Key Patterns

- **Change detection:** `UDATE` field on LOGPART with `$filter=UDATE gt '{last_udate}'`. High-water mark stored in Sync Runs table. First run (no stored UDATE) fetches ALL.
- **New product creation:** Priority-only products → CREATE in Airtable. Uses writable `SKU` field (not formula `SKU Trim (EDI)`). `PARTDES` → `Product Title Priority Input` is **create_only** (skipped on updates).
- **Loop prevention:** P→A sets `Last Synced from Priority` = now → A→P checks if `Last Synced from Priority` > `Last Synced to Priority` → SKIP if so (no API call, just updates timestamp).

---

## Claude Workflow Rules

### 1. Plan Mode (Non-Negotiable)
- Enter plan mode for **ANY** task that touches this sync
- Start with: Data Flow → Field Mapping → Error Strategy → Conflict Rules
- If anything behaves unexpectedly: **STOP** and re-plan

### 2. Use Subagents Aggressively
This project has parallel workstreams. Use subagents for:
- Airtable API research / field discovery
- Priority ERP API research / metadata calls
- Data mapping & transformation logic
- Sync engine & conflict resolution
- Testing & validation
- Keep main context clean

### 3. Learn From Mistakes
- After **every** correction or bug: update `tasks/lessons.md`
- Review lessons at the start of every session
- Key areas: rate limits, field quirks, conflict resolution, auth refresh, idempotency

### 4. Verify Before Done
- Never mark complete until tested with real data
- Show before/after records in both systems
- Show API call logs
- Test conflict resolution scenarios
- Ask: "Would this survive in production with 10,000+ records?"

### 5. Production Mindset
- Handle: network failures, rate limits, partial failures, duplicates
- Idempotent operations everywhere
- Comprehensive logging
- Zero data loss tolerance

### 6. Task Management
1. Get explicit approval before coding major parts
2. Update `tasks/lessons.md` after every correction

---

## Implementation Phases

- **Phase 1:** ✅ DONE — Auth, connection, one-way sync (Airtable → Priority) for all LOGPART fields
- **Phase 2:** ✅ DONE — Sub-forms (allergens, shelf lives, price lists, bins), webhook server, sync logging, GitHub
- **Phase 3:** ✅ DONE — Railway deployment, clickable GET endpoint, Airtable button trigger via API Sync table
- **Phase 4:** ✅ DONE — P→A sync for products, both directions, test base support
- **Phase 5:** ✅ DONE — All workflows with bidirectional sync + environment switching
- **Phase 5.5:** ✅ DONE — Merged financial parameter workflows into parent workflows (11→7 workflows: products+fncpart+prdpart, vendors+fncsup, customers+fnccust)
- **Phase 6:** 🔄 NEXT — Testing all 7 workflows, then 2-way conflict detection & resolution

## Current Architecture

```
sync/
├── core/                  # Shared engine + clients (entity-agnostic)
│   ├── config.py          # Env vars, Priority environments, test base config
│   ├── models.py          # Pydantic: SyncStats, SyncRecord, SubformResult, FieldMapping
│   ├── base_engine.py     # Abstract sync engine: A→P + P→A orchestration
│   ├── airtable_client.py # Read + write for both directions
│   ├── priority_client.py # Entity CRUD + sub-form ops (environment-aware)
│   ├── sync_log_client.py # Writes run summaries to Airtable Sync Logs base
│   ├── logger_setup.py    # Logging config + console formatting
│   └── utils.py           # clean(), format_price(), to_int(), to_float(), priority_yn()
├── workflows/             # Per-entity workflow configs
│   ├── products/          # LOGPART + FNCPART + PRDPART — merged engine
│   ├── fncpart/           # FNCPART — field_mapping imported by products (engine unused)
│   ├── prdpart/           # PRDPART — field_mapping imported by products (engine unused)
│   ├── vendors/           # SUPPLIERS + FNCSUP — merged engine
│   ├── fncsup/            # FNCSUP — field_mapping imported by vendors (engine unused)
│   ├── vendor_prices/     # PRICELIST (vendor) — standalone engine
│   ├── customers/         # CUSTOMERS + FNCCUST — merged engine
│   ├── fnccust/           # FNCCUST — field_mapping imported by customers (engine unused)
│   └── customer_prices/   # PRICELIST (customer) — standalone engine
├── server.py              # FastAPI: /health, 14 webhook endpoints, env switching
└── run_sync.py            # CLI: --workflow (7), --direction, --priority-env, --test-base, --dry-run
```

---

## User Preferences

- **Clickable URLs:** All sync triggers should be simple GET URLs with `?key=` auth param. No POST + Bearer header patterns. This matches AirPower and keeps Airtable automations simple.
- **API Sync table:** All sync URLs (Railway, AirPower, future) stored in the API Sync table with Start Sync buttons. One automation handles all syncs.
- **No code for non-developers:** The project owner is not a developer. Prefer visual/clickable solutions over CLI commands or scripts.

---

## Core Principles

- **Defined scope** — 7 workflows (products, vendors, customers + price lists + images + tech sheets). Do not expand without explicit approval
- **Simplicity first** — but never at the expense of robustness
- **Production grade** — this is not a prototype
- **Idempotency & safety** — zero duplicates or data loss
- **Observability** — excellent logging, clear error messages
- **Minimal footprint** — change only what is necessary
- **Consistent patterns** — all workflows follow the same base engine architecture
