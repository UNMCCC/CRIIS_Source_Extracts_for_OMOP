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
    'UNMMGDSS_P126_RAW.PERSON'              AS Identity_Context,
    PERSON_ID,
    p126_reporting.get_mrn_from_person_id(PERSON_ID) AS MRN,
    CASE
        WHEN SEX_CD = 358
            THEN 'F'
        WHEN SEX_CD = 359
            THEN 'M'
        ELSE ''
    END                                     AS GENDER_CONCEPT_ID,
    YEAR(BIRTH_DT_TM)                       AS YEAR_OF_BIRTH,
    MONTH(BIRTH_DT_TM)                      AS MONTH_OF_BIRTH,
    DAY(BIRTH_DT_TM)                        AS DAY_OF_BIRTH,
    FORMAT(TRY_CAST(BIRTH_DT_TM AS DATETIME), 'yyyy-MM-dd HH:mm:ss') AS BIRTH_DATETIME,
    ISNULL(FORMAT(TRY_CAST(DECEASED_DT_TM AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') AS DECEASED_DATE,
    RACE_CD                                 AS RACE_CONCEPT_ID,
    ETHNIC_GRP_CD                           AS ETHNICITY_CONCEPT_ID,
    0                                       AS LOCATION_ID,
    0                                       AS PROVIDER_ID,
    0                                       AS CARE_SITE_ID,
    PERSON_ID                               AS PERSON_SOURCE_VALUE,
    CASE
        WHEN SEX_CD = 358
            THEN 'F'
        WHEN SEX_CD = 359
            THEN 'M'
        ELSE ''
    END                                     AS GENDER_SOURCE_VALUE,
    CASE
        WHEN SEX_CD = 358
            THEN 'F'
        WHEN SEX_CD = 359
            THEN 'M'
        ELSE ''
    END                                     AS GENDER_SOURCE_CONCEPT_ID,
    RACE_CD                                 AS RACE_SOURCE_VALUE,
    RACE_CD                                 AS RACE_SOURCE_CONCEPT_ID,
    ETHNIC_GRP_CD                           AS ETHNICITY_SOURCE_VALUE,
    ETHNIC_GRP_CD                           AS ETHNICITY_SOURCE_CONCEPT_ID,
    UPDT_DT_TM
FROM unmmgdss.p126_raw.person;

