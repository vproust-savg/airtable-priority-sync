# Lessons Learned

## Priority API Sub-Forms (2026-02-24)

### Three distinct sub-form patterns
Priority sub-forms do NOT all behave the same way. There are three patterns:

1. **Single-entity sub-forms** (e.g. `SAVR_ALLERGENS_SUBFORM`):
   - GET returns the entity directly (NOT in a `"value"` array)
   - Response has `$entity` in `@odata.context`
   - PATCH directly on the sub-form URL (no key needed)
   - POST fails with 409 if a record already exists

2. **Multi-record with URL keys** (e.g. `SAVR_PARTSHELF_SUBFORM`):
   - GET returns `{"value": [...]}`
   - Individual records accessible by integer key: `SUBFORM(3)`
   - The key is an internal integer field (e.g. `SHELFLIFE`), NOT the human-readable field (`TYPE`)
   - `SUBFORM('Frozen')` returns 404 -- string keys DO NOT work

3. **Multi-record without URL keys** (e.g. `PARTINCUSTPLISTS_SUBFORM`, `PARTLOCATIONS_SUBFORM`):
   - GET returns `{"value": [...]}`
   - NO individual record access works (all key patterns return 404)
   - POST creates new records (409 if already exists)
   - **Solution:** Deep PATCH on parent `LOGPART('{SKU}')` with nested sub-form array

### PUNITNAME is create-only
`PUNITNAME` (Buy/Sell Unit) can only be set on POST (create), not PATCH (update).
Priority returns HTTP 400 if you try to PATCH this field. Use `create_only=True` flag on `FieldMapping`.

### Airtable token permissions are per-base
When adding a new Airtable base (e.g. Sync Logs), the token must be explicitly
granted access to that base. Otherwise writes fail with 403 Forbidden.

## P→A Sync — Airtable Field Type Gotchas (2026-02-24)

### Many Airtable fields are read-only formulas/lookups
Before adding a field to the P→A mapping, ALWAYS check its Airtable type via the Metadata API
(`GET /meta/bases/{baseId}/tables`). Formula, lookup, rollup, and count fields CANNOT be written
to — the API returns 422 "INVALID_VALUE_FOR_COLUMN".

**Fields confirmed read-only (removed from P→A mapping):**
- `Base Price` (formula), `Base Price Currency` (formula), `Standard Cost` (lookup),
  `Conversion Ratio` (formula), `Availability Priority Output` (formula),
  `Allocate Inventory` (formula), `Vendor SKU Trim` (formula), `Family (Number from Product Type)` (formula)

### Priority SPEC fields are ALWAYS strings, even for numbers
SPEC1="100", SPEC2="1" etc. are returned as strings by Priority even when the values are numeric.
If the Airtable field is a `number` type (like Case Pack, Product Net Weight Input), you MUST
convert the string to a float/int. Use the `to_float` transform. Sending a string to a number
field returns 422 "INVALID_VALUE_FOR_COLUMN".

### singleSelect fields reject unknown options (without typecast)
If you send a value like `"TEST"` to a Brand singleSelect field and "TEST" isn't an existing
option, Airtable returns 422 "INVALID_MULTIPLE_CHOICE_OPTIONS" / "Insufficient permissions to
create new select option". **Fix:** Add `"typecast": true` to the API payload. This tells
Airtable to auto-create missing select options.

### SKU Trim (EDI) is a formula — can't write to it
`SKU Trim (EDI)` is `TRIM({SKU})`. For reading/matching: use `SKU Trim (EDI)`.
For creating new records: write to the `SKU` field (singleLineText, writable).

### Test base needs its own token
The duplicated test base ("Savory Gourmet API Test") has a separate Personal Access Token.
The main AIRTABLE_TOKEN doesn't have access to it. Use `AIRTABLE_TEST_TOKEN` env var
and pass `token_override` to AirtableClient.

## General Patterns

### Always test with --dry-run first
The `--dry-run` flag previews all changes without writing. Use it before every live sync test.

### --sku flag bypasses the sync view
When using `--sku 15412`, the code fetches the record directly by SKU (filterByFormula)
rather than from the sync view. This allows re-testing a product even after it's been synced.

### python3, not python
macOS does not have `python` in PATH by default. Always use `python3`.

## Priority Linked Tables — P→A Reverse Lookups (2026-03-05)

### Formula fields in Airtable CANNOT be written to
"Accounting Family" is a SWITCH formula that converts text names to numeric codes.
P→A sync tried to write to it → 422 error. Solution: identify the writable singleSelect
source field ("Kelsey_Types/Accounting Family") and write there instead.

### Priority lookup table entities need API access enabled
`FAMILY_FNC` initially returned 400 "API cannot be run for this form". The user had to
enable API access in Priority before we could query it. When encountering a new linked
table field, **ask the user directly** to enable API access for the corresponding form.

### Fetch lookup tables dynamically — never hardcode
The mapping between codes and descriptions can change over time (new categories added).
Always fetch the lookup table from Priority at sync time using `fetch_lookup_table()`.
Use `LookupConfig` on the `FieldMapping` to configure which entity/fields to fetch.

### Priority descriptions may differ from Airtable option names
Priority has "Jam & Honey" (ampersand) while Airtable may have "Jam and Honey".
With `typecast: true`, Airtable creates new options if names don't match exactly.
User should align Airtable singleSelect options with Priority descriptions to avoid duplicates.

## Linked Record Sync — P→A (2026-03-05)

### AirtableClient uses `_base_id`, not `base_id`
The Airtable client stores its base ID as `self._base_id` (underscore prefix). When adding
new methods, always use `self._base_id`. Using `self.base_id` raises AttributeError.

### Empty dict `{}` is falsy in Python — use `is not None`
When checking if a lookup/map was loaded, use `linked_records is not None` instead of just
`linked_records`. An empty dict `{}` is falsy, which causes the code to fall through to the
default `TRANSFORMS` lookup and fail with a KeyError on unregistered transform names.

### Linked record fields need array format
Airtable linked record fields require values written as arrays of record IDs: `["recXXXX"]`.
Plain text or single strings won't work. The `LinkedRecordConfig` pattern resolves
Priority codes → Airtable record IDs by fetching the target table and building a mapping.

### Two distinct P→A lookup patterns
1. **LookupConfig** (priority_lookup): Priority code → text description via Priority API.
   Used when Airtable field is singleSelect/text (e.g., Product Type, Accounting Family).
2. **LinkedRecordConfig** (linked_record): Priority code → Airtable record ID via Airtable API.
   Used when Airtable field is a linked record (e.g., Preferred Vendor → Vendors table).

### Auto-creating missing linked records
When a Priority code has no matching Airtable record, `LinkedRecordAutoCreate` config on
`LinkedRecordConfig` tells the engine to create a stub record. Key considerations:
- The **match field** (e.g., "Priority Vendor ID") may be a formula/read-only field.
  Write to a different **writable** field (e.g., "Vendor_ID") specified by `writable_key_field_id`.
- Extra data (e.g., Company Name from SUPDES) is fetched from a different Priority entity
  (e.g., SUPPLIERS) via `fetch_lookup_table()` — the same method used for LookupConfig.
- The `create_linked_records()` method returns `{code: record_id}` so the
  linked_records map is updated immediately for use in the same sync run.
- When creating in the Airtable target table, use field IDs (not names) since the
  AirtableClient's `_fields_to_ids()` isn't available for arbitrary tables.
- **Airtable create responses return field NAMES, not IDs.** Don't try to extract
  match values from the response using field IDs — instead pass the known codes
  directly and use 1:1 correspondence with the response records.

## Test Base — Field ID Mismatch (2026-03-05)

### Duplicated bases may have different field IDs for fields added after duplication
When a base is duplicated, existing fields keep their IDs. But fields added AFTER
duplication (e.g., "Sync Comments" with ID `fldi8arwqVkZmZzHs`) get new IDs in the
duplicate. Using production field IDs on the test base causes 422 "Unknown field name".

### Fix: skip timestamp field IDs for test base
In each workflow engine's `_create_airtable_client()`, when `base_id_override` is set
(test base), skip adding timestamp field IDs to the `extra` dict. The `_to_id()` method
falls back to field names, which Airtable accepts universally.

### Always add 422 error body logging
Without response body logging, 422 errors only show the HTTP status — not the reason.
Add 422 body extraction (same pattern as `batch_create_records()`) to any method that
writes to Airtable. The error message (e.g., `Unknown field name: "fldXXX"`) is
critical for debugging.
