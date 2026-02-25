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
