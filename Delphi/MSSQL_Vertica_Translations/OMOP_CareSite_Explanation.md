# OMOP Care Site ETL — Workflow Explanation

## Overview

This workflow produces the OMOP `CARE_SITE` table (`omop_care_site`) from Cerner Millennium source
data.  Unlike the Person and Provider workflows, there is no insert-then-update pattern; the entire
result is produced by a single SELECT with one level of subquery nesting.  The query identifies
distinct facility/location records that are referenced in encounter data and have a valid physical
address on file.

> **Dependency note:** The `LOCATION_ID` derived here (a hash of `PARENT_ENTITY_ID` and
> `STREET_ADDR`) is the same value used by the Location ETL's `Caresite_ADDRESS` CTE.  The two
> workflows must use an identical hashing expression so that `CARE_SITE.LOCATION_ID` resolves
> correctly against the `LOCATION` table.

---

## Query Structure

The query has two layers:

```
Outer SELECT  (transforms + deduplicates)
  └── csite subquery  (joins Encounter, Address, Code Value)
        ├── ENCOUNTER            — source of all facility codes used in encounters
        ├── ADDRESS (ranked)     — physical address for each facility
        └── CODE_VALUE           — human-readable facility name from Cerner code table
```

---

## Inner Subquery — `csite`

**Sources:** `UNMHSC_P126.ENCOUNTER`, `UNMHSC_P126.ADDRESS`, `UNMHSC_P126.CODE_VALUE`

### Address ranking

The ADDRESS table is pre-filtered and ranked inside its own subquery before being joined:

| Filter / Rule | Detail |
|---|---|
| `PARENT_ENTITY_NAME = 'LOCATION'` | Only facility/location address rows |
| `ADDRESS_TYPE_CD = '405134056'` | Specific Cerner address-type code (facility)|
| `PARENT_ENTITY_ID NOT IN ('1133767')` | Excludes a known bad/test location record |
| `ROW_NUMBER() OVER (PARTITION BY PARENT_ENTITY_ID ORDER BY ADDRESS_TYPE_SEQ, updt_dt_tm DESC)` | Ranks addresses per facility; lower `ADDRESS_TYPE_SEQ` wins, ties broken by most-recently-updated |
| `ACTIVE_IND = 1 AND updt_ord = 1` | Join condition selects only the active, top-ranked row |

### Address validity filter

After joining, only rows with a plausible physical address are kept:

```
WHERE STREET_ADDR ILIKE 'PO%'           -- PO Box addresses
   OR LEFT(STREET_ADDR, 1) BETWEEN '1' AND '9'   -- Street addresses (leading digit)
```

This rejects NULL addresses, internal codes, and non-addressable placeholders.

### Deduplication

A `GROUP BY` on all nine projected columns collapses any remaining duplicates that arise when
multiple encounter rows point to the same facility/address combination.

---

## Outer SELECT — OMOP Field Mapping

| OMOP Field | Source / Expression |
|---|---|
| `IDENTITY_CONTEXT` | `'UNMHSC_P126.ADDRESS'` (literal) |
| `CARE_SITE_ID` | `PARENT_ENTITY_ID` (Cerner location code) |
| `care_site_name` | `CODE_VALUE.DESCRIPTION` |
| `place_of_service_concept_id` | `0` (unmapped — no standard concept assigned) |
| `LOCATION_ID` | `CAST(HASH(PARENT_ENTITY_ID \|\| STREET_ADDR) AS INT)` — composite hash linking to LOCATION table |
| `care_site_source_value` | `CODE_VALUE.DESCRIPTION` (same as name) |
| `place_of_service_source_value` | `'Outpatient'` for two named facilities; `NULL` otherwise (see below) |
| `UPDT_DT_TM` | `updt_dt_tm::TIMESTAMP` |

### `place_of_service_source_value` logic

Only two specific facility descriptions receive a value; all others are left NULL:

| DESCRIPTION | Mapped Value |
|---|---|
| `'UNMHSC/Cancer Center'` | `'Outpatient'` |
| `'UNMHSC/CASAA + Milagro Programs'` | `'Outpatient'` |
| *(all others)* | `NULL` |

### Final deduplication

The outer `GROUP BY 1, 2, 3, 4, 5, 6, 7, 8` (all eight output columns) provides a final
deduplication pass over the results of the inner subquery.  In the ported SQL Server version this
is replaced with `SELECT DISTINCT` for clarity.

---

## Source Tables Summary

| Table | Schema | Purpose |
|---|---|---|
| `ENCOUNTER` | `UNMHSC_P126.dbo` | Source of all facility codes referenced in encounters |
| `ADDRESS` | `UNMHSC_P126.dbo` | Physical address records for facilities/locations |
| `CODE_VALUE` | `UNMHSC_P126.dbo` | Cerner code-value lookup for facility descriptions |

---

## ETL Execution Order Dependencies

```
Omop_Care_Site  ──▶  Omop_Location  (Location's Caresite_ADDRESS CTE reads OMOP_CARE_SITE)
Omop_Care_Site  ──▶  Omop_Person    (Person Step 3 reads CARE_SITE_ID from UA_OMOP_PROVIDER
                                      which in turn links back to care sites)
```

---

## Porting Notes (Vertica → MS SQL Server)

| Concern | Vertica | MS SQL Server |
|---|---|---|
| `HASH(a \|\| b)` | Built-in hash returning UINT64 | No direct equivalent; `CHECKSUM(expr1 + expr2)` returns INT and is used as a consistent substitute. **Both Care Site and Location ETLs must use the same expression.** |
| String concatenation | `\|\|` | `+` |
| `::TIMESTAMP` cast | Vertica cast syntax | `CAST(... AS DATETIME2)` |
| `ILIKE` (case-insensitive LIKE) | Supported natively | Use `LIKE`; SQL Server string comparisons are case-insensitive under the default collation |
| `GROUP BY 1,2,...` ordinal references | Supported | Not valid in T-SQL; replace with `SELECT DISTINCT` or explicit column list |
| `ORDER BY` inside derived table | Allowed | Only valid with `TOP` or `OFFSET-FETCH`; remove when no TOP is present |
| Schema separator | `schema.table` | `database.dbo.table` (3-part naming) |

> **CHECKSUM consistency:** `CHECKSUM(CAST(PARENT_ENTITY_ID AS VARCHAR) + STREET_ADDR)` must be
> used identically in both `Omop_Care_Site_consolidated.py` and `Omop_Location_consolidated.py`
> so that `CARE_SITE.LOCATION_ID` joins correctly to `LOCATION.LOCATION_ID`.
