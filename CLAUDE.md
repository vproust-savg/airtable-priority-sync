# Airtable ↔ Priority ERP — Product 2-Way Sync

## Project Overview

**What we're building:** A true 2-way sync for **product data only** between Airtable and Priority ERP.
**What we're NOT building (yet):** No inventory, orders, invoices, customers, or any other entity.
**Tech stack:** Pure Python (no n8n). Hosted on Railway.
**User context:** The project owner is not a developer. Keep explanations clear, ask before making assumptions, and never leave the project in a broken state.

---

## Scope: Products Only

Changes in either system should flow to the other:
- Product created/updated in Airtable → pushed to Priority
- Product created/updated in Priority → pushed to Airtable
- Conflict resolution is needed when both sides change the same product

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
- **Webhook trigger:** `GET /webhook/sync?key={WEBHOOK_API_KEY}` (clickable URL, no headers needed)
- **Health check:** `GET /health`
- **Status check:** `GET /webhook/status` (requires Bearer header)
- **Auto-deploys:** Pushes to `main` on GitHub trigger automatic redeploy
- **Env vars:** All credentials set in Railway Variables tab (never in code)

### Airtable API Sync Table
- **Table:** `API Sync` (`tblpwvHgbDzYx5Edm`) in the Savory Gourmet base
- **Purpose:** Stores all sync URLs (Railway + AirPower) with clickable Start Sync buttons
- **Automation:** Single "Start API Sync" automation reads URL from the Url field and fetches it
- **User preference:** All sync triggers should be **clickable GET URLs with auth in query params** (no POST + headers). This matches how AirPower works and keeps the Airtable automation simple.

---

## Priority Interface: SGI_PART_ALL

The file `tools/3. SG Part All Priority Interface.txt` defines the exact Priority entities and fields used. This is the source of truth for what fields exist in Priority.

### Priority Entities & Fields

**LOGPART** (main product form):
| Field | Description |
|-------|-------------|
| `PARTNAME` | Part number / SKU (**primary key**) |
| `PARTDES` | Full product description |
| `SPEC1` | Case Pack |
| `SPEC2` | Product Net Weight Input |
| `SPEC3` | Product Net Weight Unit Input |
| `SPEC4` | Brand |
| `SPEC5` | Kelsey Categories |
| `SPEC6` | Kelsey Subcategories |
| `SPEC7` | Catalog Status |
| `SPEC8` | Inventory Status |
| `SPEC9` | Perishable |
| `SPEC10` | Retail |
| `SPEC11` | Feature Individual Portions |
| `SPEC12` | Staff Pick |
| `SPEC13` | Storage |
| `SPEC14` | Availability Priority Output |
| `SPEC15` | Direct Import |
| `SPEC16` | Vendor SKU Trim |
| `PUNITNAME` | Buy/Sell Unit |
| `BASEPLPRICE` | Base Price |
| `BASEPLCODE` | Base Price Currency |
| `STATDES` | Priority Status |
| `SUPNAME` | Vendor ID (Preferred Vendor) |
| `PRICE` | Standard Cost |
| `WSPLPRICE` | Wholesale Price List Price |
| `CONV` | Conversion Ratio |
| `FAMILYNAME` | Family (Number from Product Type) |
| `TYPE` | Part Type (P/R/O) |
| `RESERVFLAG` | Allocate Inventory |

**SAVR_PARTSHELF** (shelf life sub-form):
| Field | Description |
|-------|-------------|
| `TYPE` | Shelf life type label |
| `NUMBER` | Shelf life duration |
| `TIMEUNIT` | Shelf life time unit |

**SAVR_ALLERGENS** (allergens & features sub-form):
| Field | Description |
|-------|-------------|
| `HASALLERGEN` | Allergen present flag |
| `EGGS` | Eggs allergen |
| `DAIRY` | Dairy allergen |
| `FISH` | Fish allergen |
| `PEANUT` | Peanut allergen |
| `SESAME` | Sesame allergen |
| `SHELLFISH` | Shellfish allergen |
| `SOYBEAN` | Soybean allergen |
| `NUTS` | Tree Nuts allergen |
| `WHEAT` | Wheat allergen |
| `HASFEATURE` | Feature present flag |
| `GLUTENFREE` | Gluten Free |
| `ORGANIC` | Organic |
| `KOSHER` | Kosher |
| `VEGAN` | Vegan |
| `HALAL` | Halal |
| `NONGMO` | Non GMO |
| `IDPROTECT` | Identity Protected |
| `GFSI` | GFSI Certified |
| `PACKAGING` | Glass Packaging |
| `PROP65` | Prop. 65 Warning |
| `CABILL418` | California Assembly Bill 418 |
| `TRACEABILITY` | Traceability Type |

**PARTINCUSTPLISTS** (price lists sub-form):
| Field | Description |
|-------|-------------|
| `PLNAME` | Price list name/code |
| `PRICE` | Price in that list |
| `CODE` | Price list code |
| `QUANT` | Price quantity |
| `UNITNAME` | Unit name |

**PARTLOCATIONS** (bin locations sub-form):
| Field | Description |
|-------|-------------|
| `WARHSNAME` | Warehouse name |
| `LOCNAME` | Bin location name |

---

## Airtable Product Fields

The existing script (`tools/10. Script for Product All v8.py`) defines the Airtable field structure. Key Airtable views and their fields:

**EDI Parts 1 — Part Catalogue** (Products table):
`SKU Trim (EDI)`, `Brand`, `Brand + Product Title + Net Weight + Case Pack`, `Product Net Weight Input`, `Product Net Weight Unit Input`, `Case Pack`, `Buy_Sell Unit (Priority)`, `Base Price`, `Base Price Currency`, `Priority Status`, `Inventory Status`, `Catalog Status`, `V-Vendor ID (from Preferred Vendor)`, `Standard Cost`, `Kelsey_Categories`, `Kelsey_Subcategories`, `Perishable`, `Retail`, `Feature_Individual Portions`, `Staff Pick`, `Storage`, `Availability Priority Output`, `Direct Import`, `LVL 2 SALE PRICE (from Price Import)`, `Conversion Ratio`, `Family (Number from Product Type)`, `Type (P/R/O)`, `Vendor SKU Trim`, `Allocate Inventory`

**EDI Parts 2 — Shelf Lives** (Shelf Lives table):
`SKU Trim (EDI) (from Products)`, `Type Label (Custom)`, `Shelf Life Input`, `Shelf Life Unit Input`

**EDI Parts 3 — Allergens & Features** (Products table):
`Allergen_Allergen Present`, `Allergen_Eggs`, `Allergen_Dairy`, `Allergen_Fish`, `Allergen_Peanut`, `Allergen_Sesame`, `Allergen_Shellfish`, `Allergen_Soybean`, `Allergen_Tree Nuts`, `Allergen_Wheat`, `Feature_Feature Present`, `Feature_Gluten Free`, `Feature_Organic`, `Feature_Kosher`, `Feature_Vegan`, `Feature_Halal`, `Feature_Non GMO`, `Feature_Identity Protected`, `GFSI Certified`, `Glass Packaging`, `Prop. 65 Warning`, `Calif. Ass. Bill 418`, `Traceability Type Output`

**EDI Parts 5.1/5.2/5.3 — Price Lists** (Products table):
`Lvl 1/2/3 Price List Code`, `LVL 1/2/3 SALE PRICE (from Price Import)`, `EDI $`, `EDI Price Quantity`, `Buy_Sell Unit (Priority)`

**EDI Parts 6 — Bins** (Products table):
`EDI Main`, `Simplified Bin Location (from Bin # Priority)`

---

## Reference Files

| File | Purpose |
|------|---------|
| `tools/10. Script for Product All v8.py` | **Working** Python script that exports Airtable product data to EDI format. Use as reference for Airtable field names, views, and data transformations. |
| `tools/3. SG Part All Priority Interface.txt` | Priority interface definition — lists every Priority field that must be populated. Source of truth for Priority field names. |
| `tools/Airtable to Priority Customers - One Way v4 - Approved.json` | **Working** n8n workflow for customer sync. Reference for Priority API patterns: auth headers, upsert logic (GET then POST/PATCH), error handling, `IEEE754Compatible: true` header. |
| `.env` | All API credentials. Never hardcode these elsewhere. |

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

## P→A Sync — Phase 4 Design

### Two Modes (same as A→P)
- **Full sync:** 17 writable fields + allergens sub-form
- **Status-only:** 3 fields (Catalog Status, Inventory Status, Priority Status)

### Change Detection
- Uses `UDATE` field on LOGPART: `$filter=UDATE gt '{last_udate}'`
- High-water mark stored in Sync Runs table ("Max UDATE" field)
- First run (no stored UDATE) fetches ALL products

### New Product Creation
- Product in Priority but NOT in Airtable → CREATE new record
- Includes `PARTDES` → `Product Title Priority Input` (**create_only** — skipped on updates)
- Uses `SKU` (writable) field, not `SKU Trim (EDI)` (formula)

### Loop Prevention (A→P ↔ P→A)
1. P→A writes fields + sets `Last Synced from Priority` = now
2. `Last Airtable Modified` changes → `Priority Sync Needed = "Yes"`
3. A→P sync checks: if `Last Synced from Priority` > `Last Synced to Priority` → **SKIP** (no API call, just update timestamp)
4. Loop broken.

### Test Base
- **Base:** Savory Gourmet (API Test) — `appqRALXnLSbi1hq3`
- **CLI flag:** `--test-base` overrides base ID + token for safe testing
- **Env vars:** `AIRTABLE_TEST_BASE_ID`, `AIRTABLE_TEST_TOKEN`
- All P→A testing runs against the test base first, then production

### Webhook Endpoints (Phase 4)
- `GET /webhook/sync-from-priority?key=...` — Full P→A sync
- `GET /webhook/sync-from-priority-status?key=...` — Status-only P→A sync

---

## Claude Workflow Rules

### 1. Plan Mode (Non-Negotiable)
- Enter plan mode for **ANY** task that touches this sync
- Start with: Data Flow → Field Mapping → Error Strategy → Conflict Rules
- If anything behaves unexpectedly: **STOP**, re-plan, and update `tasks/todo.md`

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
1. Write/update `tasks/todo.md` with checkable items
2. Get explicit approval before coding major parts
3. Mark items complete as you go
4. Update `tasks/lessons.md` after every correction
5. Keep `docs/runbook.md` updated

---

## Implementation Phases

- **Phase 1:** ✅ DONE — Auth, connection, one-way sync (Airtable → Priority) for all LOGPART fields
- **Phase 2:** ✅ DONE — Sub-forms (allergens, shelf lives, price lists, bins), webhook server, sync logging, GitHub
- **Phase 3:** ✅ DONE — Railway deployment, clickable GET endpoint, Airtable button trigger via API Sync table
- **Phase 4:** 🔄 IN PROGRESS — One-way sync: Priority → Airtable (reverse direction). Core code complete, testing on test base.
- **Phase 5:** 2-way sync engine with conflict detection & resolution
- **Phase 6:** Change detection (polling with timestamps + Airtable webhooks)

## Current Architecture

```
sync/
├── config.py              # Env vars, constants, table IDs, test base config
├── models.py              # Pydantic: SyncStats, SyncRecord, SubformResult, FieldMapping
├── field_mapping.py       # A→P (28 fields) + P→A (17 fields) mappings, both directions
├── subform_mapping.py     # 4 sub-form mappings + reverse allergen mapper (P→A)
├── airtable_client.py     # Read + write for both directions (fetch, create, update, timestamps)
├── priority_client.py     # LOGPART CRUD + sub-form ops + fetch_changed_products (P→A)
├── sync_engine.py         # Orchestrator: A→P + P→A sync methods, loop prevention
├── sync_log_client.py     # Writes run summaries to Airtable Sync Logs base + UDATE tracking
├── server.py              # FastAPI: /health, A→P + P→A webhook endpoints (4 total)
├── run_sync.py            # CLI: --dry-run, --sku, --direction, --mode, --server, --test-base
├── logger_setup.py        # Logging config + console formatting
└── utils.py               # clean(), format_price(), to_int(), to_float(), priority_yn()
```

---

## User Preferences

- **Clickable URLs:** All sync triggers should be simple GET URLs with `?key=` auth param. No POST + Bearer header patterns. This matches AirPower and keeps Airtable automations simple.
- **API Sync table:** All sync URLs (Railway, AirPower, future) stored in the API Sync table with Start Sync buttons. One automation handles all syncs.
- **No code for non-developers:** The project owner is not a developer. Prefer visual/clickable solutions over CLI commands or scripts.

---

## Core Principles

- **Products only** — do not expand scope without explicit approval
- **Simplicity first** — but never at the expense of robustness
- **Production grade** — this is not a prototype
- **Idempotency & safety** — zero duplicates or data loss
- **Observability** — excellent logging, clear error messages
- **Minimal footprint** — change only what is necessary
- **Future-proof** — easy to add new entities (customers, orders) later
