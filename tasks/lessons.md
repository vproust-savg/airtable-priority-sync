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

## General Patterns

### Always test with --dry-run first
The `--dry-run` flag previews all changes without writing. Use it before every live sync test.

### --sku flag bypasses the sync view
When using `--sku 15412`, the code fetches the record directly by SKU (filterByFormula)
rather than from the sync view. This allows re-testing a product even after it's been synced.

### python3, not python
macOS does not have `python` in PATH by default. Always use `python3`.
