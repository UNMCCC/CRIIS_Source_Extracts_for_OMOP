# OMOP Person ETL — Workflow Explanation

## Overview

This workflow populates the OMOP `PERSON` table (`ua_omop_person`) from Cerner Millennium source data.
It follows the same **insert-then-update** pattern as the Provider ETL: a base record is inserted for
every patient, then enrichment data from other tables is applied in two subsequent update passes.

> **Dependency note:** Steps 2 and 3 read from other OMOP output tables (`ua_omop_location` and
> `UA_OMOP_PROVIDER`) and therefore require those tables to be populated before this workflow runs.

---

## Step 1 — Initial Load

**Source:** `UNMHSC_EDW_MILL_CDS.MD_D_PERSON`

Selects all distinct patient records and inserts them into `ua_omop_person`. Fields that require
data from other sources (location, provider, care site) are populated with placeholder values (`0`)
at this stage.

| Target Field               | Source / Value                          |
|----------------------------|-----------------------------------------|
| IDENTITY_CONTEXT           | `'UNMHSC_EDW_MILL_CDS.MD_D_PERSON'`    |
| PERSON_ID                  | `PERSON_ID`                             |
| MRN                        | `MRN` *(site extension, not standard OMOP)* |
| GENDER_CONCEPT_ID          | `GENDER_CODE`                           |
| YEAR_OF_BIRTH              | `YEAR(BIRTH_DATE)`                      |
| MONTH_OF_BIRTH             | `MONTH(BIRTH_DATE)`                     |
| DAY_OF_BIRTH               | `DAY(BIRTH_DATE)`                       |
| BIRTH_DATETIME             | `BIRTH_DT_TM`                           |
| DECEASED_DATE              | `DECEASED_DATE`                         |
| RACE_CONCEPT_ID            | `RACE_RAW_CODE`                         |
| ETHNICITY_CONCEPT_ID       | `ETHNICITY_RAW_CODE`                    |
| LOCATION_ID                | `0` (enriched in Step 2)                |
| PROVIDER_ID                | `0` (enriched in Step 3)                |
| CARE_SITE_ID               | `0` (enriched in Step 3)                |
| PERSON_SOURCE_VALUE        | `PERSON_ID`                             |
| GENDER_SOURCE_VALUE        | `GENDER_CODE`                           |
| GENDER_SOURCE_CONCEPT_ID   | `GENDER_CODE`                           |
| RACE_SOURCE_VALUE          | `RACE_RAW_CODE`                         |
| RACE_SOURCE_CONCEPT_ID     | `RACE_RAW_CODE`                         |
| ETHNICITY_SOURCE_VALUE     | `ETHNICITY_RAW_CODE`                    |
| ETHNICITY_SOURCE_CONCEPT_ID| `ETHNICITY_RAW_CODE`                    |
| UPDT_DT_TM                 | `MILL_UPDATE_DT_TM`                     |

> **Note on MRN:** `MRN` is not part of the standard OMOP CDM `PERSON` table. Its presence here
> indicates a site-level extension column added to `ua_omop_person` for local use.

---

## Step 2 — Update Location ID

**Source:** `UNMHSC_EDW_MILL_CDS.MD_D_PERSON_ADDRESS` joined to `UNMHSC_EDW_OMOP.ua_omop_location`

Finds each patient's current address and validates it against the already-populated OMOP location
table. Two filters narrow the address records:

- `ADDRESS_TYPE_RAW_DISPLAY IN ('home', 'mailing', 'billing', 'Bill To')` — only residential/billing
  addresses are considered; clinical or facility addresses are excluded.
- `CURRENT_IND = 1` — only the active address record is used.

The inner join to `ua_omop_location` acts as a validity check: only addresses that have already
been loaded into the OMOP location table are used. The matched `ADDRESS_ID` becomes the `LOCATION_ID`.

---

## Step 3 — Update Provider ID and Care Site ID

**Source:** `UNMHSC_EDW_MILL_CDS.MD_D_PERSON_PRSNL_RELTN` joined to `UNMHSC_EDW_OMOP.UA_OMOP_PROVIDER`

Links each patient to their primary provider using the Cerner person-to-personnel relationship table.
Three filters are applied:

- `ACTIVE_IND = 1` — only active relationships.
- `PRSNL_PERSON_ID != 0` — excludes unassigned/placeholder personnel records.
- `PERSON_PRSNL_R_RAW_CODE = 881` — restricts to a specific Cerner relationship type (typically
  the attending or primary care provider relationship).

The left join to `UA_OMOP_PROVIDER` retrieves the `CARE_SITE_ID` that was assigned to that provider
during the Provider ETL, propagating the facility association to the patient record.

| Target Field | Source |
|---|---|
| PROVIDER_ID  | `MD_D_PERSON_PRSNL_RELTN.PRSNL_PERSON_ID` |
| CARE_SITE_ID | `UA_OMOP_PROVIDER.CARE_SITE_ID` (via provider join) |

---

## Source Tables Summary

| Table | Schema | Purpose |
|-------|--------|---------|
| `MD_D_PERSON` | `UNMHSC_EDW_MILL_CDS.dbo` | Base patient demographics |
| `MD_D_PERSON_ADDRESS` | `UNMHSC_EDW_MILL_CDS.dbo` | Patient address records |
| `MD_D_PERSON_PRSNL_RELTN` | `UNMHSC_EDW_MILL_CDS.dbo` | Patient-to-provider relationships |
| `ua_omop_location` | `UNMHSC_EDW_OMOP.dbo` | Previously loaded OMOP location records |
| `UA_OMOP_PROVIDER` | `UNMHSC_EDW_OMOP.dbo` | Previously loaded OMOP provider records |

---

## ETL Execution Order Dependencies

```
Omop_Location  ──┐
                 ├──▶  Omop_Person (Step 2 needs ua_omop_location)
Omop_Provider  ──┘     (Step 3 needs UA_OMOP_PROVIDER)
```

Both the Location and Provider ETLs must complete successfully before this workflow is run.

---

## Porting Notes (Vertica → MS SQL Server)

| Concern | Vertica | MS SQL Server |
|---------|---------|---------------|
| `YEAR()` / `MONTH()` / `DAY()` | Supported | Supported identically |
| Schema separator | `schema.table` | `database.schema.table` (3-part naming) |
| `UPDATE...FROM` | Supported | Supported natively in T-SQL |
| `ORDER BY` in subquery | Allowed | Requires `TOP` or must be removed — the `ORDER BY 1, 2` in the original location subquery is not valid in SQL Server without `TOP` |
| Cross-database joins | Via schema references | Requires linked servers or 3-part naming |

> **Important:** The `ORDER BY 1, 2` inside the location subquery in the original source is a
> Vertica-ism. In SQL Server, `ORDER BY` inside a subquery is only valid when paired with `TOP`.
> The consolidated Python script resolves this with `ROW_NUMBER()` to deterministically pick one
> address and one provider per patient.
