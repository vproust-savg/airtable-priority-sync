# Excluded P→A Fields

Fields removed from P→A sync mappings. Circle back on these later to determine
if they should be re-added with a different Airtable target or permanently excluded.

## Removed Fields

| Date | Workflow | Priority Field | Airtable Field | Field ID | Reason |
|------|----------|---------------|----------------|----------|--------|
| 2026-03-05 | fncpart | `WEIGHT` | Filemaker Weight | `fldmWMrWRPRf80YvB` | Airtable field is computed/formula — 422 error on write |

## Notes
- A→P mapping for these fields is NOT affected (kept in A2P_FIELD_MAP)
- Only the P→A direction was removed
- Before re-adding, verify the Airtable field type is writable (singleLineText, number, singleSelect, etc.)
