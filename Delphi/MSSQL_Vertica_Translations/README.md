# OMOP Incremental Workflows — MS SQL Server

This directory contains MS SQL Server translations of the Vertica-dialect OMOP incremental workflow queries found in the parent directory. Each `.sql` file corresponds to a source `.txt` workflow file.

## Workflow Overview

**Trigger:** Daily at 10:00 PM (America/Denver)
**Owner:** 
**Source Schema:** 

The workflow loads incrementally updated OMOP CDM records from upstream clinical source tables. The lookback window (number of days to re-process) is controlled by a single config table:

```sql
SELECT CONFIG_VALUE FROM ??.OMOP_INCR_CONFIG
```

Default value: **14 days** (set by `Omop_Incremental_Config.sql`).

---

## Execution Order

Files should be run in dependency order:

| Order | File | Target Table | Operation |
|-------|------|--------------|-----------|
| 1 | `Omop_Incremental_Config.sql` | `OMOP_INCR_CONFIG` | Config seed |
| 2 | `Omop_Care_Site.sql` | `OMOP_CARE_SITE` | INSERT |
| 3 | `Omop_Location.sql` | `OMOP_LOCATION` | INSERT |
| 4 | `Omop_Provider.sql` | `OMOP_PROVIDER` | INSERT + 3 UPDATEs |
| 5 | `Omop_Person.sql` | `OMOP_PERSON` | INSERT + 2 UPDATEs |
| 6 | `Omop_Incremental_Visit_Ocurrence.sql` | `OMOP_INCR_VISIT_OCCERRENCE` | INSERT + UPDATE |
| 7 | `Omop_Incremental_Visit_Detail.sql` | `OMOP_INCR_VISIT_DETAIL` | INSERT + UPDATE |
| 8 | `Omop_Incremental_Condition_Ocurrence.sql` | `OMOP_INCR_CONDITION_OCCURRENCE` | INSERT |
| 9 | `Omop_Incremental_Procedure_Ocurrence.sql` | `OMOP_INCR_PROCEDURE_OCCURRENCE` | INSERT |
| 10 | `Omop_Incremental_Drug_Exposure.sql` | `OMOP_INCR_DRUG_EXPOSURE` | INSERT |
| 11 | `OMOP_Incremental_Observation.sql` | `OMOP_INCR_OBSERVATION` | INSERT |
| 12 | `OMOP_Incremental_Observation_Final.sql` | `OMOP_INCR_OBSERVATION` | INSERT |
| 13 | `Omop_Incremental_Specimen.sql` | `OMOP_INCR_SPECIMEN` | INSERT |
| 14 | `Omop_Incremental_Measurement.sql` | `OMOP_INCR_MEASUREMENT` | INSERT + UPDATE |
| — | `Omop_Code_Value.sql` | `OMOP_CODE_VALUE` | Reference inserts (7 domains) |

> **Note:** Visit Occurrence must complete before Visit Detail, as Visit Detail joins back to `OMOP_INCR_VISIT_OCCERRENCE`. Visit Detail must complete before Condition, Procedure, Drug, Observation, and Measurement loads that join to `OMOP_VISIT_DETAIL`.

---

## File Descriptions

### `Omop_Incremental_Config.sql`
Seeds the `OMOP_INCR_CONFIG` table with the lookback window value (14 days). All incremental queries reference this value in their `WHERE` clauses via `DATEDIFF`.

### `Omop_Care_Site.sql`
Loads care site records from `UNMHSC_P126.ENCOUNTER` and `UNMHSC_P126.ADDRESS`. Generates a `LOCATION_ID` using `CHECKSUM()` on the facility ID and street address.

### `Omop_Location.sql`
Loads location records from two sources unioned together:
- **Person addresses** from `MD_D_PERSON_ADDRESS` (home, mailing, billing)
- **Care site addresses** from `UNMHSC_P126.ADDRESS` joined to `OMOP_CARE_SITE`

### `Omop_Provider.sql`
Four-step load:
1. Inserts provider base records from `MD_D_PERSONNEL`
2. Updates DEA numbers from `MD_D_PERSONNEL_ALIAS`
3. Updates specialty fields from `REFERENCE.NPPES_TAXONOMY`
4. Updates care site from the provider's most frequent encounter facility

### `Omop_Person.sql`
Three-step load:
1. Inserts person demographics from `MD_D_PERSON`
2. Updates `LOCATION_ID` from current person address records
3. Updates `PROVIDER_ID` and `CARE_SITE_ID` from the person–provider relationship table

### `Omop_Incremental_Visit_Ocurrence.sql`
Two-step load:
1. Inserts encounter records from `MD_F_ENCOUNTER`, joining to `MD_F_ENCOUNTER_PERSONNEL_RELTN` to resolve the attending provider
2. Updates `PRECEDING_VISIT_OCCURRENCE_ID` using `LAG()` partitioned by `PERSON_ID`

### `Omop_Incremental_Visit_Detail.sql`
Two-step load:
1. Inserts one row per provider–encounter combination from `MD_F_ENCOUNTER_PERSONNEL_RELTN`, filtered to personnel whose effective dates fall within the encounter window
2. Updates `PRECEDING_VISIT_DETAIL_ID` using `LAG()` partitioned by `VISIT_OCCURRENCE_ID`

### `Omop_Incremental_Condition_Ocurrence.sql`
Loads condition records from `MD_F_CONDITION`. Deduplicates by `CONDITION_ID`/`PERSON_ID` using `ROW_NUMBER()`, then aggregates start/end dates via `MIN`/`MAX` coalescing across the condition, encounter, and problem tables.

### `Omop_Incremental_Procedure_Ocurrence.sql`
Loads procedure records from `MD_F_PROCEDURE`, filtered to `PROCEDURE_SOURCE_NAME = 'UNIV_NM_HIM'`. Joins to `PH_F_PROCEDURE_MODIFIER_CODE` for modifier codes, deduplicating modifiers per procedure using `ROW_NUMBER()`.

### `Omop_Incremental_Drug_Exposure.sql`
Loads drug exposure records from two sources unioned together:
- **Pharmacy** — `MD_F_MEDICATION`
- **Immunizations** — `MD_F_IMMUNIZATION`

Both sources join to `OMOP_VISIT_DETAIL` for visit linkage. Final result is filtered to rows where `drug_concept_id IS NOT NULL`.

### `OMOP_Incremental_Observation.sql`
Loads observation records sourced from DCP form definitions. Uses two CTEs:
- **`form`** — resolves active form/section/input/task-assay chains
- **`death_description`** — links death-related clinical events (age, cause, status)

Records are sampled using `ABS(CHECKSUM(CLINICAL_EVENT_ID) % 10) = 0` (10% sample).

### `OMOP_Incremental_Observation_Final.sql`
Deduplicates `OMOP_INCREMENTAL_OBSERVATION` by keeping one record per `(PERSON_ID, observation date, description)` — the most recent by date — using `ROW_NUMBER()`.

### `Omop_Incremental_Specimen.sql`
Loads specimen records from `UNMHSC_P126.ORDERS` joined to `ORDER_DETAIL` where `OE_FIELD_MEANING = 'SPECIMEN TYPE'`. Links primary and secondary diagnosis codes from `MD_F_CONDITION`. Deduplicates to one specimen per encounter/specimen-type combination.

### `Omop_Incremental_Measurement.sql`
Two-step load:
1. Inserts measurement records from `PH_F_RESULT` (vital signs and laboratory category), joining to `PH_D_PERSON_ALIAS` for person resolution and a pre-built provider lookup table
2. Updates `VISIT_OCCURRENCE_ID` and `VISIT_DETAIL_ID` from `OMOP_INCREMENTAL_VISIT_DETAIL`

### `Omop_Code_Value.sql`
Loads the `OMOP_CODE_VALUE` reference table across seven clinical domains: Person (gender/race/ethnicity), Provider (NPI taxonomy), Visit (type/admission/discharge), Condition (type/status), Drug (medication/immunization routes/units), Procedure (codes/modifiers), and Measurement (result codes/units).

---

## Vertica → MS SQL Server Translation Reference

| Vertica | MS SQL Server |
|---------|---------------|
| `datediff('day', dt1, dt2)` | `DATEDIFF(DAY, dt1, dt2)` |
| `getdate()` | `GETDATE()` |
| `ILIKE 'pattern'` | `LIKE 'pattern'` *(requires case-insensitive collation; use `LOWER(x) LIKE LOWER(pattern)` for case-sensitive DBs)* |
| `x \|\| y` | `x + y` or `CONCAT(x, y)` |
| `x::INT` | `TRY_CAST(x AS INT)` |
| `x::VARCHAR` | `TRY_CAST(x AS VARCHAR(MAX))` |
| `x::! INT` *(soft cast — NULL on failure)* | `TRY_CAST(x AS INT)` |
| `x::TIMESTAMP` | `TRY_CAST(x AS DATETIME2)` |
| `TIMESTAMPTZ` column type | `DATETIMEOFFSET` |
| `HASH(x)` | `CHECKSUM(x)` *(32-bit; use `HASHBYTES` for collision resistance)* |
| `CHR(n)` | `CHAR(n)` |
| `DATE(x)` | `CAST(x AS DATE)` |
| `ifnull(x, y)` | `ISNULL(x, y)` |
| `LENGTH(x)` | `LEN(x)` |
| `ascii(x)` | `ASCII(x)` |
| `abs(mod(hash(x), 10))` | `ABS(CHECKSUM(x) % 10)` |
| `split_part(str, ':', 3)` | `SUBSTRING` + `CHARINDEX` expression *(see measurement file)* |
| `GROUP BY 1, 2, 3` | Expanded to explicit column expressions |
| `ORDER BY` inside CTE or subquery | Removed — invalid in SQL Server without `TOP` |
| `CASE x WHEN NULL` | `CASE WHEN x IS NULL` |
