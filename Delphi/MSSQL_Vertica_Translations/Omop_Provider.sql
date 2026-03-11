-- ============================================================
-- OMOP Provider (MS SQL Server)
-- Translated from Vertica dialect
--
-- Contains three operations:
--   1. INSERT  - initial provider load
--   2. UPDATE  - set DEA from personnel alias
--   3. UPDATE  - set specialty from NPPES taxonomy
--   4. UPDATE  - set care site from encounter data
-- ============================================================

-- ============================================================
-- 1. INSERT: Load provider records
-- ============================================================
-- TRUNCATE TABLE UNMHSC_EDW_OMOP.UA_OMOP_PROVIDER;  -- uncomment if needed

INSERT INTO UNMHSC_EDW_OMOP.UA_OMOP_PROVIDER (
    identity_context,
    provider_id,
    provider_name,
    npi,
    dea,
    specialty_concept_id,
    care_site_id,
    year_of_birth,
    gender_concept_id,
    provider_source_value,
    specialty_source_value,
    specialty_source_concept_id,
    gender_source_value,
    gender_source_concept_id,
    updt_dt_tm
)
SELECT DISTINCT
    'UNMHSC_EDW_MILL_CDS.md_d_Personnel'   AS Identity_Context,
    p.PERSON_ID                             AS PROVIDER_ID,
    p.NAME_FULL_FORMATTED                   AS PROVIDER_NAME,
    p.NPI,
    NULL                                    AS DEA,
    0                                       AS SPECIALTY_CONCEPT_ID,
    0                                       AS CARE_SITE_ID,
    0                                       AS YEAR_OF_BIRTH,
    0                                       AS GENDER_CONCEPT_ID,
    p.PERSON_ID                             AS PROVIDER_SOURCE_VALUE,
    NULL                                    AS SPECIALTY_SOURCE_VALUE,
    0                                       AS specialty_source_concept_id,
    NULL                                    AS GENDER_SOURCE_VALUE,
    0                                       AS GENDER_SOURCE_CONCEPT_ID,
    p.update_dt_tm                          AS updt_dt_tm
FROM UNMHSC_EDW_MILL_CDS.md_d_Personnel p;


-- ============================================================
-- 2. UPDATE: Set DEA from personnel alias
-- ============================================================
UPDATE prov
SET    prov.dea = dea.alias
FROM   UNMHSC_EDW_OMOP.UA_OMOP_PROVIDER prov
JOIN (
    SELECT DISTINCT
        pa.PERSON_ID,
        pa.ALIAS
    FROM UNMHSC_EDW_MILL_CDS.MD_D_PERSONNEL_ALIAS pa
    WHERE pa.PRSNL_ALIAS_TYPE_RAW_DISPLAY IN ('DOCDEA')
      AND pa.active_ind = 1
) dea ON prov.provider_id = dea.person_id;


-- ============================================================
-- 3. UPDATE: Set specialty fields from NPPES taxonomy
-- ============================================================
UPDATE prov
SET    prov.specialty_source_value      = COALESCE(NULLIF(npi.TAXONOMY_SPECIALIZATION, ''), npi.TAXONOMY_CLASSIFICATION),
       prov.specialty_concept_id        = npi.taxonomy_code,
       prov.specialty_source_concept_id = npi.taxonomy_code
FROM   UNMHSC_EDW_OMOP.UA_OMOP_PROVIDER prov
JOIN (
    SELECT
        NPI,
        TAXONOMY_CODE,
        TAXONOMY_CLASSIFICATION,
        TAXONOMY_SPECIALIZATION
    FROM REFERENCE.NPPES_TAXONOMY
    WHERE PRIMARY_TAXONOMY_SWITCH = 'Y'
) npi ON prov.npi = npi.npi;


-- ============================================================
-- 4. UPDATE: Set care site from most frequent encounter facility
-- ============================================================
UPDATE prov
SET    prov.care_site_id = care_site.facility_source_id
FROM   UNMHSC_EDW_OMOP.UA_OMOP_PROVIDER prov
JOIN (
    SELECT
        b.encounter_id,
        b.prsnl_id,
        b.facility_source_id
    FROM (
        SELECT
            a.*,
            ROW_NUMBER() OVER (
                PARTITION BY prsnl_id
                ORDER BY tot_enc DESC
            ) AS rank_fac
        FROM (
            SELECT
                epr.ENCOUNTER_ID,
                epr.PRSNL_ID,
                e.FACILITY_SOURCE_ID,
                COUNT(DISTINCT e.encounter_id) AS tot_enc
            FROM UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER_PERSONNEL_RELTN epr
            LEFT JOIN UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER e
                ON epr.ENCOUNTER_ID = e.ENCOUNTER_ID
            GROUP BY
                epr.ENCOUNTER_ID,
                epr.PRSNL_ID,
                e.FACILITY_SOURCE_ID
        ) a
    ) b
    WHERE rank_fac = 1
) care_site ON prov.provider_id = care_site.prsnl_id;
