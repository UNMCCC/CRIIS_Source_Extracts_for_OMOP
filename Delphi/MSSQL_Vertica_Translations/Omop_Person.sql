-- ============================================================
-- OMOP Person (MS SQL Server)
-- Translated from Vertica dialect
--
-- Contains three operations:
--   1. INSERT  - load person records
--   2. UPDATE  - set location_id from person address
--   3. UPDATE  - set provider_id and care_site_id from provider relation
--
-- Key translation notes:
--   YEAR()/MONTH()/DAY() -> same in SQL Server
-- ============================================================

-- ============================================================
-- 1. INSERT: Load person records
-- ============================================================
-- TRUNCATE TABLE UNMHSC_EDW_OMOP.UA_OMOP_PERSON;  -- uncomment if needed

INSERT INTO UNMHSC_EDW_OMOP.UA_OMOP_PERSON (
    identity_context,
    person_id,
    mrn,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    birth_datetime,
    deceased_date,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value,
    gender_source_value,
    gender_source_concept_id,
    race_source_value,
    race_source_concept_id,
    ethnicity_source_value,
    ethnicity_source_concept_id,
    updt_dt_tm
)
SELECT DISTINCT
    'UNMHSC_EDW_MILL_CDS.MD_D_PERSON'      AS Identity_Context,
    person_id,
    mrn,
    GENDER_CODE                             AS GENDER_CONCEPT_ID,
    YEAR(birth_date)                        AS YEAR_OF_BIRTH,
    MONTH(BIRTH_DATE)                       AS MONTH_OF_BIRTH,
    DAY(birth_date)                         AS DAY_OF_BIRTH,
    BIRTH_DT_TM                             AS BIRTH_DATETIME,
    DECEASED_DATE,
    RACE_RAW_CODE                           AS RACE_CONCEPT_ID,
    ETHNICITY_raw_CODE                      AS ETHNICITY_CONCEPT_ID,
    0                                       AS LOCATION_ID,
    0                                       AS provider_id,
    0                                       AS CARE_SITE_ID,
    PERSON_ID                               AS PERSON_SOURCE_VALUE,
    gender_code                             AS GENDER_SOURCE_VALUE,
    GENDER_CODE                             AS GENDER_SOURCE_CONCEPT_ID,
    RACE_RAW_CODE                           AS RACE_SOURCE_VALUE,
    RACE_RAW_CODE                           AS RACE_SOURCE_CONCEPT_ID,
    ETHNICITY_raw_CODE                      AS ETHNICITY_SOURCE_VALUE,
    ETHNICITY_raw_CODE                      AS ETHNICITY_SOURCE_CONCEPT_ID,
    mill_update_dt_tm                       AS UPDT_DT_TM
FROM UNMHSC_EDW_MILL_CDS.MD_D_PERSON;


-- ============================================================
-- 2. UPDATE: Set location_id from person address
-- ============================================================
UPDATE per
SET    per.location_id = updt.location_id
FROM   UNMHSC_EDW_OMOP.UA_OMOP_PERSON per
JOIN (
    SELECT DISTINCT
        a.person_id,
        a.location_id
    FROM (
        SELECT
            PERSON_ID,
            ADDRESS_ID AS location_id
        FROM UNMHSC_EDW_MILL_CDS.MD_D_PERSON_ADDRESS
        WHERE ADDRESS_TYPE_RAW_DISPLAY IN ('home', 'mailing', 'billing', 'Bill To')
          AND CURRENT_IND = 1
    ) a
    JOIN UNMHSC_EDW_OMOP.ua_omop_location l
        ON a.location_id = l.location_id
) updt ON updt.person_id = per.person_id;


-- ============================================================
-- 3. UPDATE: Set provider_id and care_site_id from provider relation
-- ============================================================
UPDATE per
SET    per.provider_id   = updt.provider_id,
       per.care_site_id  = updt.care_site_id
FROM   UNMHSC_EDW_OMOP.UA_OMOP_PERSON per
JOIN (
    SELECT DISTINCT
        ppr.person_id,
        ppr.provider_id,
        p.CARE_SITE_ID
    FROM (
        SELECT
            ppr.PERSON_ID,
            ppr.PRSNL_PERSON_ID AS provider_id
        FROM UNMHSC_EDW_MILL_CDS.MD_D_PERSON_PRSNL_RELTN ppr
        WHERE active_ind              = 1
          AND PRSNL_PERSON_ID        != 0
          AND PERSON_PRSNL_R_RAW_CODE = 881
    ) ppr
    LEFT JOIN UNMHSC_EDW_OMOP.UA_OMOP_PROVIDER p
        ON ppr.provider_ID = p.PROVIDER_ID
) updt ON updt.person_id = per.person_id;
