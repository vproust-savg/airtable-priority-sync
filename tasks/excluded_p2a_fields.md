# Excluded P→A Fields

Fields removed from P→A sync mappings. Circle back on these later to determine
if they should be re-added with a different Airtable target or permanently excluded.

## Removed Fields

| Date | Workflow | Priority Field | Airtable Field | Field ID | Reason |
|------|----------|---------------|----------------|----------|--------|
| 2026-03-05 | fncpart | `WEIGHT` | Filemaker Weight | `fldmWMrWRPRf80YvB` | Airtable field is computed/formula — 422 error on write |
| 2026-03-06 | products | `BASEPLPRICE` | Base Price | `fldPwojUSmVpiHA2C` | Airtable field is formula/lookup — read-only |
| 2026-03-06 | products | `BASEPLCODE` | Base Price Currency | `fldpYviH2hsfvEEZQ` | Airtable field is formula/lookup — read-only |
| 2026-03-06 | products | `PRICE` | Standard Cost | `fldjS688T5OawrcjM` | Airtable field is lookup from Price Import — read-only |
| 2026-03-06 | products | `WSPLPRICE` | LVL 2 SALE PRICE (from Price Import) | `fld9Ku04QU2bJiBYX` | Airtable field is formula/lookup — read-only |
| 2026-03-06 | products (sub-form) | `PARTINCUSTPLISTS_SUBFORM` | LVL 1/2/3 SALE PRICE, Price List Codes | various | Price list sub-form — A→P only, Airtable price fields are read-only |
| 2026-03-06 | products (sub-form) | `PARTLOCATIONS_SUBFORM` | Bin Locations | various | Not needed for P→A — A→P only for now |

## Notes
- A→P mapping for these fields is NOT affected (kept in A2P_FIELD_MAP)
- Only the P→A direction was removed
- Before re-adding, verify the Airtable field type is writable (singleLineText, number, singleSelect, etc.)
