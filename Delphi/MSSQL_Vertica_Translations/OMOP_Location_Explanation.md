# OMOP Location ETL — Workflow Explanation

## Overview

This workflow populates the OMOP `LOCATION` table (`omop_location`) from two distinct address
populations that are combined with a `UNION`:

1. **Person addresses** — patient home, mailing, and billing addresses from the Cerner person
   address table.
2. **Care site addresses** — facility physical addresses, derived by joining the already-populated
   `OMOP_CARE_SITE` table back to the raw Cerner `ADDRESS` table.

> **Dependency note:** The `Caresite_ADDRESS` CTE reads from `UNMHSC_EDW_OMOP.OMOP_CARE_SITE`,
> which must be fully populated before this workflow runs.

---

## Query Structure

```
person_address CTE
  └── Three-level nesting:
        Inner: DISTINCT raw address rows (MD_D_PERSON_ADDRESS ⋈ MD_D_PERSON)
        Middle: ROW_NUMBER() to rank duplicates per LOCATION_ID
        Outer: Filter to most-recent row only (current_updt = 1)

Caresite_ADDRESS CTE
  └── OMOP_CARE_SITE ⟕ ADDRESS (ranked)
        → DISTINCT on all columns
        → GROUP BY to collapse address-level duplicates

Final SELECT
  └── UNION of person_address and caresite_address
```

---

## CTE 1 — `person_address`

**Source:** `UNMHSC_EDW_MILL_CDS.MD_D_PERSON_ADDRESS` joined to `UNMHSC_EDW_MILL_CDS.MD_D_PERSON`

### Address type and currency filter

Only addresses of the following types are included, and only when marked as current:

| Filter | Detail |
|---|---|
| `ADDRESS_TYPE_RAW_DISPLAY IN ('home', 'mailing', 'billing', 'Bill To')` | Residential and billing addresses only; clinical or facility-type addresses excluded |
| `current_ind = 1` | Only the active (non-historical) address record |

The join to `MD_D_PERSON` validates that a corresponding person record exists.

### Deduplication with ROW_NUMBER

After the innermost DISTINCT, a `ROW_NUMBER()` window function is applied:

```sql
ROW_NUMBER() OVER (PARTITION BY LOCATION_ID ORDER BY UPDT_DT_TM DESC)
```

This ranks records per `LOCATION_ID` (address_id), with the
most-recently-updated row ranked 1.  The outer filter `WHERE
current_updt = 1` retains only that row.

### OMOP Field Mapping

| OMOP Field | Source / Value |
|---|---|
| `IDENTITY_CONTEXT` | `'UNMHSC_EDW_MILL_CDS.MD_D_PERSON_ADDRESS'` (literal) |
| `LOCATION_ID` | `pa.address_id` (Cerner address record ID) |
| `ADDRESS_1` | `pa.ADDRESS_LINE_1` |
| `ADDRESS_2` | `pa.ADDRESS_LINE_2` |
| `city` | `pa.city` |
| `STATE` | `pa.STATE_RAW_DISPLAY` |
| `ZIP` | `LEFT(pa.POSTAL_CODE, 5)` — truncated to 5-digit zip |
| `COUNTY` | `NULL` (not sourced) |
| `LOCATION_SOURCE_VALUE` | `pa.address_id` (same as LOCATION_ID) |
| `COUNTRY_CONCEPT_ID` | `pa.COUNTRY_RAW_CODE` (cast to VARCHAR) |
| `COUNTRY_SOURCE_VALUE` | `pa.COUNTRY_RAW_DISPLAY` |
| `LATITUDE` | `0.0` (not geocoded) |
| `LONGITUDE` | `0.0` (not geocoded) |
| `UPDT_DT_TM` | `pa.updt_raw_dt_tm` |

---

## CTE 2 — `Caresite_ADDRESS`

**Sources:** `UNMHSC_EDW_OMOP.OMOP_CARE_SITE` joined to `UNMHSC_P126.ADDRESS`

This CTE mirrors the address-joining logic from `Omop_Care_Site.txt` —
it reads care site records from the OMOP target table, then LEFT JOINs
back to the raw Cerner ADDRESS table to retrieve full address details.

### Address ranking

Identical to the Care Site workflow:

| Filter / Rule | Detail |
|---|---|
| `PARENT_ENTITY_NAME = 'LOCATION'` | Only facility/location address rows |
| `ADDRESS_TYPE_CD = '405134056'` | Specific Cerner address-type code |
| `ROW_NUMBER() OVER (PARTITION BY PARENT_ENTITY_ID ORDER BY ADDRESS_TYPE_SEQ, updt_dt_tm DESC)` | Selects best address per facility |
| `ACTIVE_IND = 1 AND updt_ord = 1` | Active, top-ranked row only |

> **Note:** Unlike the Care Site query, the exclusion of `PARENT_ENTITY_ID NOT IN ('1133767')` is
> absent here — that filter was applied when originally loading OMOP_CARE_SITE; records that passed
> that filter are already in the source table.

### Address validity filter

The same street-address validity check as in the Care Site workflow:

```sql
WHERE STREET_ADDR LIKE 'PO%'
   OR LEFT(STREET_ADDR, 1) BETWEEN '1' AND '9'
```

### OMOP Field Mapping

| OMOP Field | Source / Value |
|---|---|
| `IDENTITY_CONTEXT` | Carried from `OMOP_CARE_SITE.identity_context` |
| `LOCATION_ID` | `HASH(PARENT_ENTITY_ID \|\| STREET_ADDR)` — **must match the value in Care Site ETL** |
| `ADDRESS_1` | `a.STREET_ADDR` |
| `ADDRESS_2` | `a.STREET_ADDR2` |
| `city` | `a.CITY` |
| `STATE` | `a.STATE` when `LENGTH(STATE) = 2`, otherwise `NULL` — filters out invalid state values |
| `ZIP` | `CAST(a.ZIPCODE AS VARCHAR)` |
| `COUNTY` | `NULL` (not sourced) |
| `LOCATION_SOURCE_VALUE` | `HASH(STREET_ADDR \|\| ZIPCODE)` — address-level hash for source tracking |
| `COUNTRY_CONCEPT_ID` | `NULL` |
| `COUNTRY_SOURCE_VALUE` | `NULL` |
| `LATITUDE` | `0.0` |
| `LONGITUDE` | `0.0` |
| `UPDT_DT_TM` | `a.updt_dt_tm` |

The `SELECT DISTINCT *` on the outer wrapper collapses any remaining duplicates after the GROUP BY.

---

## Final UNION

```sql
SELECT * FROM person_address
UNION
SELECT * FROM caresite_address
```

`UNION` (not `UNION ALL`) is used, providing a final deduplication across both populations in case
any address record appears in both.  Both CTEs project the same 14 columns in the same order.

---

## Source Tables Summary

| Table | Schema | Purpose |
|---|---|---|
| `MD_D_PERSON_ADDRESS` | `UNMHSC_EDW_MILL_CDS.dbo` | Patient address records |
| `MD_D_PERSON` | `UNMHSC_EDW_MILL_CDS.dbo` | Patient demographic records (existence check) |
| `OMOP_CARE_SITE` | `UNMHSC_EDW_OMOP.dbo` | Previously loaded OMOP care site records |
| `ADDRESS` | `UNMHSC_P126.dbo` | Raw Cerner facility/location address records |

---

## ETL Execution Order Dependencies

```
Omop_Care_Site  ──▶  Omop_Location  (Caresite_ADDRESS CTE reads OMOP_CARE_SITE)
Omop_Location   ──▶  Omop_Person    (Person Step 2 joins to ua_omop_location)
```

---

## Porting Notes (Vertica → MS SQL Server)

| Concern | Vertica | MS SQL Server |
|---|---|---|
| `HASH(a \|\| b)` | Built-in hash returning UINT64 | `CHECKSUM(expr1 + expr2)` returns INT; used consistently in both Care Site and Location ETLs |
| `HASH(STREET_ADDR \|\| ZIPCODE)` for `LOCATION_SOURCE_VALUE` | UINT64 hash | `CHECKSUM(a.STREET_ADDR + CAST(a.ZIPCODE AS VARCHAR))` |
| `pa.COUNTRY_RAW_CODE::VARCHAR` | Vertica cast syntax | `CAST(pa.COUNTRY_RAW_CODE AS VARCHAR)` |
| `CASE LENGTH(a.STATE) WHEN 2 THEN a.STATE END` | `LENGTH()` function | `CASE LEN(a.STATE) WHEN 2 THEN a.STATE END` (`LEN` in T-SQL) |
| String concatenation | `\|\|` | `+` |
| `ILIKE` | Supported natively | `LIKE` (SQL Server is case-insensitive under default collation) |
| `ORDER BY` inside subqueries | Allowed | Only valid with `TOP` or `OFFSET-FETCH`; removed in ported version |
| `ORDER BY` inside CTE body | Allowed | Not valid in T-SQL CTEs; removed |
| Schema separator | `schema.table` | `database.dbo.table` (3-part naming) |

> **LOCATION_ID consistency:** The expression `CHECKSUM(CAST(PARENT_ENTITY_ID AS VARCHAR) + STREET_ADDR)`
> must be identical in both `Omop_Care_Site_consolidated.py` and `Omop_Location_consolidated.py`.
> The Care Site ETL writes this value as `CARE_SITE.LOCATION_ID`; the Location ETL writes it as
> `LOCATION.LOCATION_ID`.  A mismatch would break the referential link between the two tables.
