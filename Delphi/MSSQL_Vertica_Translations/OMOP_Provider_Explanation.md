# OMOP Provider ETL — Workflow Explanation

## Overview

This workflow populates the OMOP `PROVIDER` table (`ua_omop_provider`) from Cerner Millennium source data.
It follows a **insert-then-update** pattern: a base record is inserted for every provider, then enrichment
data from other tables is applied in subsequent update passes.

---

## Step 1 — Initial Load

**Source:** `UNMHSC_EDW_MILL_CDS.md_d_Personnel`

Selects all distinct personnel records and inserts them into `ua_omop_provider`. Fields that require
data from other sources (DEA, specialty, care site, gender, year of birth) are populated with placeholder
values (`0` or `NULL`) at this stage.

| Target Field             | Source / Value               |
|--------------------------|------------------------------|
| PROVIDER_ID              | `PERSON_ID`                  |
| PROVIDER_NAME            | `NAME_FULL_FORMATTED`        |
| NPI                      | `NPI`                        |
| DEA                      | `NULL` (enriched in Step 2)  |
| SPECIALTY_CONCEPT_ID     | `0` (enriched in Step 3)     |
| CARE_SITE_ID             | `0` (enriched in Step 4)     |
| YEAR_OF_BIRTH            | `0` (not enriched here)      |
| GENDER_CONCEPT_ID        | `0` (not enriched here)      |
| PROVIDER_SOURCE_VALUE    | `PERSON_ID`                  |
| SPECIALTY_SOURCE_VALUE   | `NULL` (enriched in Step 3)  |
| SPECIALTY_SOURCE_CONCEPT_ID | `0` (enriched in Step 3)  |
| GENDER_SOURCE_VALUE      | `NULL` (not enriched here)   |
| GENDER_SOURCE_CONCEPT_ID | `0` (not enriched here)      |
| UPDT_DT_TM               | `UPDATE_DT_TM`               |

---

## Step 2 — Update DEA

**Source:** `UNMHSC_EDW_MILL_CDS.MD_D_PERSONNEL_ALIAS`

Finds each provider's DEA number by filtering alias records to type `'DOCDEA'` with `ACTIVE_IND = 1`.
Matches to `ua_omop_provider` on `PROVIDER_ID = PERSON_ID` and sets the `DEA` field.

---

## Step 3 — Update Specialty

**Source:** `REFERENCE.NPPES_TAXONOMY`

Joins on `NPI` where `PRIMARY_TAXONOMY_SWITCH = 'Y'` to retrieve the provider's primary taxonomy.
Updates three fields:

- **SPECIALTY_SOURCE_VALUE** — prefers `TAXONOMY_SPECIALIZATION` when non-empty, falls back to
  `TAXONOMY_CLASSIFICATION` via `COALESCE(NULLIF(..., ''), ...)`.
- **SPECIALTY_CONCEPT_ID** — set to `TAXONOMY_CODE`.
- **SPECIALTY_SOURCE_CONCEPT_ID** — set to `TAXONOMY_CODE`.

---

## Step 4 — Update Care Site

**Source:** `UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER_PERSONNEL_RELTN` joined to `MD_F_ENCOUNTER`

Determines each provider's **most common facility** by counting distinct encounters per
`(PRSNL_ID, FACILITY_SOURCE_ID)` combination and ranking with `ROW_NUMBER() OVER (PARTITION BY prsnl_id
ORDER BY tot_enc DESC)`. The top-ranked facility (`rank_fac = 1`) is used to set `CARE_SITE_ID`.

---

## Source Tables Summary

| Table | Schema | Purpose |
|-------|--------|---------|
| `md_d_Personnel` | `UNMHSC_EDW_MILL_CDS` | Base provider demographics |
| `MD_D_PERSONNEL_ALIAS` | `UNMHSC_EDW_MILL_CDS` | DEA and other provider identifiers |
| `NPPES_TAXONOMY` | `REFERENCE` | NPI taxonomy / specialty lookup |
| `MD_F_ENCOUNTER_PERSONNEL_RELTN` | `UNMHSC_EDW_MILL_CDS` | Provider-to-encounter relationships |
| `MD_F_ENCOUNTER` | `UNMHSC_EDW_MILL_CDS` | Encounter facility information |

---

## Porting Notes (Vertica → MS SQL Server)

| Concern | Vertica | MS SQL Server |
|---------|---------|---------------|
| Schema separator | `schema.table` | `database.schema.table` (3-part naming) |
| `UPDATE...FROM` | Supported | Supported natively in T-SQL |
| `ROW_NUMBER()` in `WHERE` | Via subquery | Must wrap in a subquery — same approach |
| `COALESCE` / `NULLIF` | Supported | Supported identically |
| Cross-database joins | Via schema references | Requires linked servers or 3-part naming |

> **Important:** If `REFERENCE.NPPES_TAXONOMY` lives in a separate database, a linked server or
> a local staging copy of that table will be required.
